use crate::app::pipeline::events::billing::context::BillingEventContext;
use actix_web::{HttpRequest, HttpResponse, Responder};
use anyhow::Error;
use pipeline::Pipeline;
use rtb::child_span_info;
use std::sync::Arc;
use tracing::{Instrument, debug, warn};

pub async fn billing_event_handler(
    http_req: HttpRequest,
    event_pipeline: Arc<Pipeline<BillingEventContext, Error>>,
) -> impl Responder {
    let url = http_req.full_url().to_string();

    let span = child_span_info!(
        "billing_event_handler",
        raw_url = tracing::field::Empty,
        result = tracing::field::Empty,
        error = tracing::field::Empty,
    );
    span.record("raw_url", url.as_str());

    async move {
        let context = BillingEventContext::new(url);

        match event_pipeline.run(&context).await {
            Ok(_) => {
                tracing::Span::current().record("result", "ok");
                debug!("Billing event success");
                HttpResponse::Ok().finish()
            }
            Err(e) => {
                let err_str = e.to_string();
                tracing::Span::current().record("result", "error");
                tracing::Span::current().record("error", err_str.as_str());
                warn!("Failed to record billing event: {}", e);
                HttpResponse::BadRequest().finish()
            }
        }
    }
    .instrument(span)
    .await
}
