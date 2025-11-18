use crate::app::lifecycle::context::StartupContext;
use crate::app::pipeline::events::billing::context::BillingEventContext;
use crate::app::pipeline::ortb::AuctionContext;
use crate::app::pipeline::syncing::r#in::context::SyncInContext;
use crate::app::pipeline::syncing::out::context::{SyncOutContext, SyncResponse};
use crate::core::usersync;
use actix_web::cookie::Cookie;
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
use std::collections::HashMap;
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
    cookies: Option<HashMap<String, String>>,
    pipeline: Arc<Pipeline<AuctionContext, Error>>,
) -> (BidResponseState, bool) {
    let mut ctx = AuctionContext::new(source.clone(), pubid, req, cookies);

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
    cookies: Option<HashMap<String, String>>,
    pipeline: Arc<Pipeline<AuctionContext, Error>>,
    span_sample_rate: f32,
) -> (BidResponseState, bool) {
    let root_span = sample_or_attach_root_span!(
        span_sample_rate,
        "handle_bid_request",
        pub_id = pubid,
        source = source,
    );

    handle_bid_request_instrumented(pubid, source, req, cookies, pipeline)
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
    let cookies = extract_cookies(&http_req);

    let (brs, pipeline_completed) = handle_bid_request(
        pubid.clone(),
        path.clone(),
        req.into_inner(),
        Some(cookies),
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

fn extract_cookies(http_req: &HttpRequest) -> HashMap<String, String> {
    http_req.cookies().map_or_else(
        |_| HashMap::new(),
        |jar| {
            jar.iter()
                .map(|c| (c.name().to_string(), c.value().to_string()))
                .collect()
        },
    )
}

async fn sync_out_handler(
    pubid: String,
    http_req: HttpRequest,
    pipeline: Arc<Pipeline<SyncOutContext, Error>>,
) -> impl Responder {
    let cookies = extract_cookies(&http_req);
    let context = SyncOutContext::new(pubid, cookies);
    let cookie_param_key = usersync::constants::CONST_REX_COOKIE_ID_PARAM;

    if let Err(e) = pipeline.run(&context).await {
        debug!("Sync-out pipeline aborted: {}", e);
        let mut response = HttpResponse::BadRequest();
        if let Some(uid) = context.local_uid.get() {
            let cookie = Cookie::build(cookie_param_key, uid.clone())
                .path("/")
                .finish();
            response.cookie(cookie);
        }
        return response.finish();
    }

    if context.response.get().is_none() {
        warn!("Cookie init sync returned Ok but no response attached!");
        let mut response = HttpResponse::InternalServerError();
        if let Some(uid) = context.local_uid.get() {
            let cookie = Cookie::build(cookie_param_key, uid.clone())
                .path("/")
                .finish();
            response.cookie(cookie);
        }
        return response.finish();
    }

    let local_uid = context.local_uid.get();

    match context.response.get().unwrap() {
        SyncResponse::Content(html) => {
            debug!("Sync-out response content: {}", html);
            let mut response = HttpResponse::Ok();
            response.content_type("text/html");
            if let Some(uid) = local_uid {
                let cookie = Cookie::build(cookie_param_key, uid.clone())
                    .path("/")
                    .finish();
                response.cookie(cookie);
            }
            response.body(html.clone())
        }
        SyncResponse::Error(err) => {
            warn!(
                "Error response encountered during sync out pipeline: {}",
                err
            );
            let mut response = HttpResponse::InternalServerError();
            if let Some(uid) = local_uid {
                let cookie = Cookie::build(cookie_param_key, uid.clone())
                    .path("/")
                    .finish();
                response.cookie(cookie);
            }
            response.finish()
        }
        SyncResponse::NoContent => {
            debug!("No content response during sync. Perhaps no bidders?");
            let mut response = HttpResponse::NoContent();
            if let Some(uid) = local_uid {
                let cookie = Cookie::build(cookie_param_key, uid.clone())
                    .path("/")
                    .finish();
                response.cookie(cookie);
            }
            response.finish()
        }
    }
}

/// Partners syncing in to us their buyeruid value
async fn sync_in_handler(
    http_req: HttpRequest,
    pipeline: Arc<Pipeline<SyncInContext, Error>>,
) -> impl Responder {
    let cookies = extract_cookies(&http_req);
    let context = SyncInContext::new(http_req.full_url().to_string(), cookies);

    match pipeline.run(&context).await {
        Ok(_) => HttpResponse::Ok().finish(),
        Err(e) => {
            warn!("Sync-in pipeline aborted: {}", e);
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

        let sync_out_pipeline = ctx
            .sync_out_pipeline
            .get()
            .ok_or(anyhow::anyhow!("Sync-out pipeline not built"))?
            .clone();

        let sync_in_pipeline = ctx
            .sync_in_pipeline
            .get()
            .ok_or(anyhow::anyhow!("Sync-in pipeline not built"))?
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
                )
                .route(
                    "/sync/out/{pubid}",
                    web::get().to({
                        let pipeline = sync_out_pipeline.clone();

                        move |pubid: web::Path<String>, http_req: HttpRequest| {
                            let pubid = pubid.into_inner();
                            let p = pipeline.clone();
                            async move { sync_out_handler(pubid, http_req, p).await }
                        }
                    }),
                )
                .route(
                    "/sync/in",
                    web::get().to({
                        let pipeline = sync_in_pipeline.clone();

                        move |http_req: HttpRequest| {
                            let p = pipeline.clone();
                            async move { sync_in_handler(http_req, p).await }
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
