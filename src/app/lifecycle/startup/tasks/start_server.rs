use crate::app::lifecycle::context::StartupContext;
use crate::app::pipeline::ortb::AuctionContext;
use actix_web::web;
use actix_web::web::Json;
use anyhow::{Error, anyhow};
use async_trait::async_trait;
use opentelemetry::metrics::Counter;
use opentelemetry::{KeyValue, global};
use pipeline::{AsyncTask, Pipeline};
use rtb::common::bidresponsestate::BidResponseState;
use rtb::server::json::JsonBidResponseState;
use rtb::server::{Server, ServerConfig};
use rtb::{BidRequest, sample_or_attach_root_span};
use std::sync::{Arc, LazyLock};
use tracing::log::debug;
use tracing::{info, instrument};

static REQUESTS_TOTAL: LazyLock<Counter<u64>> = LazyLock::new(|| {
    global::meter("rex")
        .u64_counter("requests")
        .with_description("All non-broken requests received")
        .with_unit("1")
        .build()
});

pub struct StartServerTask;

fn record_request_metric(pubid: String, source: &str, brs: &BidResponseState, pipeline_ok: bool) {
    let mut attrs = vec![
        KeyValue::new("pubid", pubid),
        KeyValue::new("source", source.to_string()),
        KeyValue::new("pipeline_completed", pipeline_ok),
    ];

    match brs {
        BidResponseState::Bid(_) => {
            attrs.push(KeyValue::new("outcome", "bid"));
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
    ).entered();

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

async fn json_bid_handler(
    pubid: String,
    req: Json<BidRequest>,
    pipeline: Arc<Pipeline<AuctionContext, anyhow::Error>>,
    span_sample_rate: f32,
) -> JsonBidResponseState {
    let source = "rtb_json".to_string();

    let (brs, pipeline_completed) = handle_bid_request(
        pubid.clone(),
        source.clone(),
        req.into_inner(),
        pipeline.clone(),
        span_sample_rate,
    )
    .await;

    record_request_metric(pubid, &source, &brs, pipeline_completed);

    JsonBidResponseState(brs)
}

#[async_trait]
impl AsyncTask<StartupContext, anyhow::Error> for StartServerTask {
    #[instrument(skip_all, name = "start_server_task")]
    async fn run(&self, ctx: &StartupContext) -> Result<(), Error> {
        let cfg = ServerConfig {
            http_port: Some(80),
            ssl_port: None,
            tls: None,
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

        let server = Server::listen(cfg, move |app| {
            app.route("/hi", web::get().to(|| async { "hi!" })).route(
                "/br/json/{pubid}",
                web::post().to({
                    let pipeline = rtb_pipeline.clone();
                    move |pubid: web::Path<String>, req: Json<BidRequest>| {
                        let pubid = pubid.into_inner();
                        let p = pipeline.clone();
                        async move { json_bid_handler(pubid, req, p, span_sample_rate).await }
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
