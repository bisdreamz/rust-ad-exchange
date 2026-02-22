use crate::app::pipeline::events::billing::context::BillingEventContext;
use actix_web::{HttpRequest, HttpResponse, Responder};
use anyhow::Error;
use pipeline::Pipeline;
use std::sync::Arc;
use tracing::{debug, warn};

pub async fn billing_event_handler(
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
