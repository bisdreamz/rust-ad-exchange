use crate::app::lifecycle::context::StartupContext;
use crate::app::pipeline::events::billing::context::BillingEventContext;
use crate::app::pipeline::ortb::AuctionContext;
use actix_web::web;
use actix_web::{HttpRequest, HttpResponse, Responder};
use anyhow::{Error, anyhow, bail};
use async_trait::async_trait;
use log::warn;
use opentelemetry::metrics::{Counter, Histogram};
use opentelemetry::{KeyValue, global};
use pipeline::{AsyncTask, Pipeline};
use rtb::common::bidresponsestate::BidResponseState;
use rtb::server::json::{FastJson, JsonBidResponseState};
use rtb::server::{Server, ServerConfig};
use rtb::{BidRequest, sample_or_attach_root_span};
use std::sync::{Arc, LazyLock};
use tracing::{Instrument, debug, info, instrument};

static REQUESTS_TOTAL: LazyLock<Counter<u64>> = LazyLock::new(|| {
    global::meter("rex")
        .u64_counter("requests")
        .with_description("All non-broken requests received")
        .with_unit("1")
        .build()
});

static REQUEST_DURATION: LazyLock<Histogram<f64>> = LazyLock::new(|| {
    global::meter("rex")
        .f64_histogram("http.server.duration")
        .with_description("HTTP request duration")
        .with_unit("s")
        .build()
});

pub struct StartServerTask;

fn record_request_metric(
    path: String,
    source: &str,
    pubid: String,
    brs: &BidResponseState,
    pipeline_ok: bool,
    duration: std::time::Duration,
) {
    let mut attrs = vec![
        KeyValue::new("pubid", pubid),
        KeyValue::new("source", source.to_string()),
        KeyValue::new("pipeline_completed", pipeline_ok),
    ];

    let mut status_code = 204;
    match brs {
        BidResponseState::Bid(_) => {
            attrs.push(KeyValue::new("outcome", "bid"));
            status_code = 200;
        }
        BidResponseState::NoBid { desc } => {
            attrs.push(KeyValue::new("outcome", "no_bid"));
            attrs.push(KeyValue::new("no_bid_desc", desc.unwrap_or("none")));
        }
        BidResponseState::NoBidReason { nbr, desc, .. } => {
            attrs.push(KeyValue::new("outcome", "no_bid_reason"));
            attrs.push(KeyValue::new("no_bid_desc", desc.unwrap_or("none")));
            attrs.push(KeyValue::new("no_bid_code", nbr.to_string()));
        }
    }

    REQUESTS_TOTAL.add(1, &attrs);

    attrs.push(KeyValue::new("http.response.status_code", status_code));
    attrs.push(KeyValue::new("http.route", path));

    REQUEST_DURATION.record(duration.as_secs_f64(), &attrs);
}

async fn handle_bid_request_instrumented(
    pubid: String,
    source: String,
    req: BidRequest,
    pipeline: Arc<Pipeline<AuctionContext, Error>>,
) -> (BidResponseState, bool) {
    let mut ctx = AuctionContext::new(source.clone(), pubid, req);

    let pipeline_result = pipeline.run(&ctx).await;

    match &pipeline_result {
        Ok(_) => debug!("Request pipeline success"),
        Err(e) => debug!("Request pipeline aborted: {}", e),
    }

    let brs = ctx.res.take().unwrap_or_else(|| BidResponseState::NoBid {
        desc: if pipeline_result.is_err() {
            Some("Failed processing req".into())
        } else {
            Some("No Bid".into())
        },
    });

    (brs, pipeline_result.is_ok())
}

async fn handle_bid_request(
    pubid: String,
    source: String,
    req: BidRequest,
    pipeline: Arc<Pipeline<AuctionContext, Error>>,
    span_sample_rate: f32,
) -> (BidResponseState, bool) {
    let root_span = sample_or_attach_root_span!(
        span_sample_rate,
        "handle_bid_request",
        pub_id = pubid,
        source = source,
    );

    handle_bid_request_instrumented(pubid, source, req, pipeline)
        .instrument(root_span)
        .await
}

async fn json_bid_handler(
    pubid: String,
    req: FastJson<BidRequest>,
    http_req: HttpRequest,
    pipeline: Arc<Pipeline<AuctionContext, Error>>,
    span_sample_rate: f32,
) -> JsonBidResponseState {
    let source = http_req.match_pattern().unwrap_or("unknown".to_string());
    let start = std::time::Instant::now();
    let path = http_req.path().to_string();

    let (brs, pipeline_completed) = handle_bid_request(
        pubid.clone(),
        path.clone(),
        req.into_inner(),
        pipeline.clone(),
        span_sample_rate,
    )
    .await;

    let duration = start.elapsed();
    record_request_metric(path, &source, pubid, &brs, pipeline_completed, duration);

    JsonBidResponseState(brs)
}

async fn billing_event_handler(
    http_req: HttpRequest,
    event_pipeline: Arc<Pipeline<BillingEventContext, Error>>,
) -> impl Responder {
    debug!("Billing event handler called");

    let context = BillingEventContext::new(http_req.full_url().to_string());

    match event_pipeline.run(&context).await {
        Ok(_) => {
            debug!("Billing event success");
            HttpResponse::Ok().finish()
        }
        Err(e) => {
            warn!("Failed to record billing event: {}", e);
            HttpResponse::BadRequest().finish()
        }
    }
}

#[async_trait]
impl AsyncTask<StartupContext, anyhow::Error> for StartServerTask {
    #[instrument(skip_all, name = "start_server_task")]
    async fn run(&self, ctx: &StartupContext) -> Result<(), Error> {
        let config = match ctx.config.get() {
            Some(config) => config,
            None => bail!("Config missing during start server task"),
        };

        let server_cfg = ServerConfig {
            http_port: Some(80),
            ssl_port: None,
            tls: config.ssl.clone(),
            tcp_backlog: None,
            max_conns: None,
            threads: None,
            tls_rate_per_worker: None,
        };

        let rtb_pipeline = ctx
            .auction_pipeline
            .get()
            .ok_or(anyhow::anyhow!("RTB pipeline not built"))?
            .clone();

        let span_sample_rate = ctx
            .config
            .get()
            .ok_or(anyhow::anyhow!("Server pipeline context missing config!"))?
            .logging
            .span_sample_rate;

        let billing_event_path = config.notifications.billing_path.clone();
        if billing_event_path.is_empty() {
            bail!("Billing event path cannot be empty");
        }

        let billing_event_pipeline = ctx
            .event_pipeline
            .get()
            .ok_or(anyhow::anyhow!("Event pipeline not built"))?
            .clone();

        let server = Server::listen(server_cfg, move |app| {
            app.route("/hi", web::get().to(|| async { "hi!" }))
                .route(
                    billing_event_path.as_str(),
                    web::get().to({
                        let pipeline = billing_event_pipeline.clone();
                        move |http_req: HttpRequest| {
                            let p = pipeline.clone();

                            async move { billing_event_handler(http_req, p).await }
                        }
                    }),
                )
                .route(
                    "/br/json/{pubid}",
                    web::post().to({
                        let pipeline = rtb_pipeline.clone();
                        move |pubid: web::Path<String>,
                              req: FastJson<BidRequest>,
                              http_req: HttpRequest| {
                            let pubid = pubid.into_inner();
                            let p = pipeline.clone();
                            async move {
                                json_bid_handler(pubid, req, http_req, p, span_sample_rate).await
                            }
                        }
                    }),
                );
        })
        .await?;

        // TODO prebid handler should define its own pipeline to record metrics
        // and extract pubid context and adapted bidrequest, then should pass
        // through existing re-useable pipeline stuff

        ctx.server
            .set(server)
            .map_err(|_| anyhow!("Could not set server"))?;

        info!("Started http server, ready for requests");

        Ok(())
    }
}
