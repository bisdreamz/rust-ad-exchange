use crate::app::http::extract_http_context;
use crate::app::pipeline::adtag::context::AdtagContext;
use crate::app::pipeline::adtag::request::AdTagRequest;
use crate::app::pipeline::ortb::PublisherBlockReason;
use actix_web::http::StatusCode;
use actix_web::web::Json;
use actix_web::{HttpRequest, HttpResponse};
use anyhow::Error;
use opentelemetry::metrics::Counter;
use opentelemetry::{KeyValue, global};
use pipeline::Pipeline;
use rtb::common::bidresponsestate::BidResponseState;
use rtb::{child_span_info, sample_or_attach_root_span};
use serde_json;
use std::sync::{Arc, LazyLock};
use tracing::{Instrument, Level, Span, debug, trace};

static ADTAG_REQUESTS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    global::meter("rex:adtag")
        .u64_counter("adtag.requests")
        .with_description("Total adtag requests received")
        .with_unit("1")
        .build()
});

/// Applies CORS headers required for browser-initiated cross-origin ad requests.
/// The adtag endpoint is called from arbitrary publisher origins, so we allow any.
fn apply_cors(res: &mut actix_web::HttpResponseBuilder) {
    res.insert_header(("Access-Control-Allow-Origin", "*"));
}

pub async fn adtag_handler(
    req: Json<AdTagRequest>,
    http_req: HttpRequest,
    pipeline: Arc<Pipeline<AdtagContext, Error>>,
    span_sample_rate: f32,
) -> HttpResponse {
    let request = req.into_inner();
    let http = extract_http_context(&http_req);

    let root_span = sample_or_attach_root_span!(
        span_sample_rate,
        "handle_adtag_request",
        adtag_response = tracing::field::Empty,
    );

    async move {
        {
            let _request_span = child_span_info!(
                "raw_adtag_request",
                raw_request = tracing::field::debug(&request)
            )
            .entered();
        }

        handle_adtag(request, http, pipeline).await
    }
    .instrument(root_span)
    .await
}

/// Handles the OPTIONS preflight for /publisher/adtag.
pub async fn adtag_preflight(_http_req: HttpRequest) -> HttpResponse {
    let mut res = HttpResponse::Ok();
    apply_cors(&mut res);
    res.insert_header(("Access-Control-Allow-Methods", "POST, OPTIONS"));
    res.insert_header(("Access-Control-Allow-Headers", "Content-Type"));
    res.insert_header(("Access-Control-Max-Age", "86400"));
    res.finish()
}

async fn handle_adtag(
    request: AdTagRequest,
    http: crate::app::pipeline::ortb::HttpRequestContext,
    pipeline: Arc<Pipeline<AdtagContext, Error>>,
) -> HttpResponse {
    let placement_id = request.placement_id.clone();
    let ctx = AdtagContext::new(request, http);

    let pipeline_result = pipeline.run(&ctx).await;

    match &pipeline_result {
        Ok(_) => debug!("Adtag pipeline completed"),
        Err(e) => debug!("Adtag pipeline aborted: {}", e),
    }

    match ctx.response.get() {
        Some(response) => {
            attach_adtag_response_to_parent_span(response);
            let outcome = if response.bid.is_some() {
                "bid"
            } else if response.sync_frame_url.is_some() {
                "sync_only"
            } else {
                "no_fill"
            };
            record_request(&placement_id, outcome);

            let status = if response.bid.is_some() || response.sync_frame_url.is_some() {
                StatusCode::OK
            } else {
                StatusCode::NO_CONTENT
            };
            let mut res = HttpResponse::build(status);
            apply_cors(&mut res);
            res.reason(adtag_reason_phrase(&ctx, Some(response)));

            if status == StatusCode::NO_CONTENT {
                res.finish()
            } else {
                res.json(response)
            }
        }
        None => {
            if has_clean_no_fill(&ctx) {
                record_request(&placement_id, "no_fill");
                let mut res = HttpResponse::NoContent();
                apply_cors(&mut res);
                res.reason(adtag_reason_phrase(&ctx, None));
                return res.finish();
            }

            record_request(&placement_id, "error");
            let mut res = HttpResponse::InternalServerError();
            apply_cors(&mut res);
            res.reason("Adtag Response Missing");
            res.finish()
        }
    }
}

fn record_request(placement_id: &str, outcome: &str) {
    ADTAG_REQUESTS.add(
        1,
        &[
            KeyValue::new("placement_id", placement_id.to_string()),
            KeyValue::new("outcome", outcome.to_string()),
        ],
    );
}

fn attach_adtag_response_to_parent_span(
    response: &crate::app::pipeline::adtag::response::AdTagResponse,
) {
    let span = Span::current();
    if span.is_disabled() {
        return;
    }

    span.record("adtag_response", tracing::field::debug(response));

    if tracing::enabled!(Level::TRACE) {
        match serde_json::to_string_pretty(response) {
            Ok(pretty_json) => {
                trace!(adtag_response_pretty = %pretty_json, "Final adtag response");
            }
            Err(err) => {
                debug!(error = %err, "Failed to serialize adtag response as pretty JSON");
            }
        }
    }
}

fn has_clean_no_fill(ctx: &AdtagContext) -> bool {
    matches!(
        ctx.auction_ctx
            .get()
            .and_then(|auction_ctx| auction_ctx.res.get()),
        Some(BidResponseState::NoBid { .. } | BidResponseState::NoBidReason { .. })
    ) || ctx.block_reason.get().is_some()
}

fn adtag_reason_phrase(
    ctx: &AdtagContext,
    response: Option<&crate::app::pipeline::adtag::response::AdTagResponse>,
) -> &'static str {
    if response.and_then(|r| r.bid.as_ref()).is_some() {
        return "Had Bid";
    }

    if let Some(BidResponseState::NoBidReason {
        desc: Some(desc), ..
    }) = ctx
        .auction_ctx
        .get()
        .and_then(|auction_ctx| auction_ctx.res.get())
    {
        return desc;
    }

    if let Some(BidResponseState::NoBid { desc: Some(desc) }) = ctx
        .auction_ctx
        .get()
        .and_then(|auction_ctx| auction_ctx.res.get())
    {
        return desc;
    }

    if response.and_then(|r| r.sync_frame_url.as_ref()).is_some() {
        return "Had Sync";
    }

    ctx.block_reason
        .get()
        .map(block_reason_phrase)
        .unwrap_or("No Fill")
}

fn block_reason_phrase(reason: &PublisherBlockReason) -> &'static str {
    match reason {
        PublisherBlockReason::UnknownSeller => "Unknown Seller",
        PublisherBlockReason::DisabledSeller => "Disabled Seller",
        PublisherBlockReason::UnknownPlacement => "Unknown Placement",
        PublisherBlockReason::UnknownProperty => "Unknown Property",
        PublisherBlockReason::IpInvalid => "Invalid IP",
        PublisherBlockReason::IpDatacenter => "Datacenter IP Blocked",
        PublisherBlockReason::DeviceUnknown => "Unknown Device",
        PublisherBlockReason::DeviceBot => "Bot Device",
        PublisherBlockReason::TooManyHops => "Too Many Supply Hops",
        PublisherBlockReason::BidsProcessingError => "Bid Processing Error",
        PublisherBlockReason::MissingAuctionId => "Missing Auction Id",
        PublisherBlockReason::MissingDevice => "Missing Device",
        PublisherBlockReason::MissingDeviceDetails => "Missing Device Details",
        PublisherBlockReason::MissingAppSite => "Missing App Or Site",
        PublisherBlockReason::MissingAppSiteDomain => "Missing Domain Or Bundle",
        PublisherBlockReason::TmaxTooLow => "Auction Tmax Too Low",
    }
}
