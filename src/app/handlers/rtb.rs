use crate::app::http::extract_http_context;
use crate::app::pipeline::ortb::{AuctionContext, HttpRequestContext};
use crate::core::managers::PublisherManager;
use crate::core::models::publisher::Publisher;
use crate::core::spec::nobidreasons;
use actix_web::HttpRequest;
use anyhow::Error;
use opentelemetry::metrics::{Counter, Histogram};
use opentelemetry::{KeyValue, global};
use pipeline::Pipeline;
use rtb::common::bidresponsestate::BidResponseState;
use rtb::server::json::{FastJson, JsonBidResponseState};
use rtb::{BidRequest, sample_or_attach_root_span};
use std::sync::{Arc, LazyLock};
use tracing::{Instrument, debug};

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
    auction_id: String,
    source: String,
    publisher: Arc<Publisher>,
    req: BidRequest,
    http: HttpRequestContext,
    pipeline: Arc<Pipeline<AuctionContext, Error>>,
) -> (BidResponseState, bool) {
    let mut ctx = AuctionContext::new(auction_id, source, publisher, None, req, http);

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
    auction_id: String,
    pubid: String,
    source: String,
    publisher: Arc<Publisher>,
    req: BidRequest,
    http: HttpRequestContext,
    pipeline: Arc<Pipeline<AuctionContext, Error>>,
    span_sample_rate: f32,
) -> (BidResponseState, bool) {
    let root_span = sample_or_attach_root_span!(
        span_sample_rate,
        "handle_bid_request",
        pub_id = pubid,
        source = source,
    );

    handle_bid_request_instrumented(auction_id, source, publisher, req, http, pipeline)
        .instrument(root_span)
        .await
}

pub async fn json_bid_handler(
    auction_id: String,
    pubid: String,
    req: FastJson<BidRequest>,
    http_req: HttpRequest,
    pipeline: Arc<Pipeline<AuctionContext, Error>>,
    pub_manager: Arc<PublisherManager>,
    span_sample_rate: f32,
) -> JsonBidResponseState {
    let source = http_req.match_pattern().unwrap_or("unknown".to_string());
    let start = std::time::Instant::now();
    let path = http_req.path().to_string();
    let http = extract_http_context(&http_req);

    let publisher = match pub_manager.get(&pubid) {
        Some(p) => p,
        None => {
            let brs = BidResponseState::NoBidReason {
                reqid: auction_id,
                nbr: nobidreasons::UNKNOWN_SELLER,
                desc: Some("Unknown publisher"),
            };
            let duration = start.elapsed();
            record_request_metric(path, &source, pubid, &brs, false, duration);
            return JsonBidResponseState(brs);
        }
    };

    let (brs, pipeline_completed) = handle_bid_request(
        auction_id,
        pubid.clone(),
        path.clone(),
        publisher,
        req.into_inner(),
        http,
        pipeline.clone(),
        span_sample_rate,
    )
    .await;

    let duration = start.elapsed();
    record_request_metric(path, &source, pubid, &brs, pipeline_completed, duration);

    JsonBidResponseState(brs)
}
