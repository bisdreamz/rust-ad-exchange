use crate::app::pipeline::creatives::raw::RawCreativeContext;
use actix_web::{HttpResponse, Responder};
use anyhow::Error;
use pipeline::Pipeline;
use rtb::sample_or_attach_root_span;
use std::sync::Arc;
use tracing::{Instrument, error, warn};

pub async fn raw_creative_handler(
    creative_id: String,
    pipeline: Option<Arc<Pipeline<RawCreativeContext, Error>>>,
    span_sample_rate: f32,
) -> impl Responder {
    let root_span = sample_or_attach_root_span!(
        span_sample_rate,
        "raw_creative_handler",
        creative_id = creative_id.as_str(),
        result = tracing::field::Empty,
    );

    async move {
        let Some(pipeline) = pipeline else {
            tracing::Span::current().record("result", "unconfigured");
            return HttpResponse::ServiceUnavailable().finish();
        };

        let context = RawCreativeContext::new(creative_id);

        if let Err(e) = pipeline.run(&context).await {
            tracing::Span::current().record("result", "error");
            warn!("Raw creative pipeline error: {}", e);
            return HttpResponse::InternalServerError().finish();
        }

        match context.resolved_content.get() {
            Some(content) => {
                let content_type = match context.content_type() {
                    Some(ct) => ct,
                    None => {
                        tracing::Span::current().record("result", "error");
                        error!(
                            creative_id = context.creative_id.as_str(),
                            "Creative resolved but content_type is None"
                        );
                        return HttpResponse::InternalServerError().finish();
                    }
                };
                tracing::Span::current().record("result", "ok");
                HttpResponse::Ok()
                    .content_type(content_type)
                    .insert_header(("Access-Control-Allow-Origin", "*"))
                    .body(content.clone())
            }
            None => {
                tracing::Span::current().record("result", "not_found");
                HttpResponse::NoContent().finish()
            }
        }
    }
    .instrument(root_span)
    .await
}
