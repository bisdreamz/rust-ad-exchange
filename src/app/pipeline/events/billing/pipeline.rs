use crate::app::context::StartupContext;
use crate::app::pipeline::events::billing::context::BillingEventContext;
use crate::app::pipeline::events::billing::tasks::{BailIfExpiredTask, CacheNoticeUrlsValidationTask, ExtractBillingEventTask, FireDemandBurlTask, ParseDataUrlTask, RecordBillingMetricsTask, RecordShapingEventsTask};
use anyhow::{Error, bail};
use pipeline::{Pipeline, PipelineBuilder};

pub fn build_event_pipeline(
    context: &StartupContext,
) -> Result<Pipeline<BillingEventContext, Error>, Error> {
    let shaping_manager = match context.shaping_manager.get() {
        Some(shaping_manager) => shaping_manager,
        None => bail!("No shaping manager?! Cant build event pipeline"),
    };

    let demand_url_cache = match context.demand_url_cache.get() {
        Some(demand_url_cache) => demand_url_cache.clone(),
        None => bail!("No demand url cache set! Cant build event pipeline"),
    };

    let event_pipeline = PipelineBuilder::new()
        // Parses raw URL into a rich ['DataUr'] object
        .with_blocking(Box::new(ParseDataUrlTask))
        // Extracts common structured data from the prior data url
        .with_blocking(Box::new(ExtractBillingEventTask))
        // Task validates the impression and looks up partner burls, but doesnt yet bail pipeline
        .with_blocking(Box::new(CacheNoticeUrlsValidationTask::new(demand_url_cache)))
        // Records raw billing metrics regardless of whether imp expired or not
        .with_blocking(Box::new(RecordBillingMetricsTask))
        // Now that we have insights into all event calls (expired or not), bail if invalid event
        .with_blocking(Box::new(BailIfExpiredTask))
        // From here now we know we have a valid, de-duplicated event - record shaping outcome
        .with_blocking(Box::new(RecordShapingEventsTask::new(
            shaping_manager.clone(),
        )))
        // Fire partner BURLs if any
        .with_async(Box::new(FireDemandBurlTask))
        // TODO billing storage into another db would go here
        .build();

    match event_pipeline {
        Some(pipeline) => Ok(pipeline),
        None => bail!("Failed to build event pipeline"),
    }
}
