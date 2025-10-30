use crate::app::context::StartupContext;
use crate::app::pipeline::events::billing::context::BillingEventContext;
use crate::app::pipeline::events::billing::tasks::{
    ExtractBillingEventTask, ParseDataUrlTask, RecordBillingEventTask,
};
use anyhow::{Error, bail};
use pipeline::{Pipeline, PipelineBuilder};

pub fn build_event_pipeline(
    _context: &StartupContext,
) -> Result<Pipeline<BillingEventContext, Error>, Error> {
    let event_pipeline = PipelineBuilder::new()
        .with_blocking(Box::new(ParseDataUrlTask))
        .with_blocking(Box::new(ExtractBillingEventTask))
        .with_blocking(Box::new(RecordBillingEventTask))
        .build();

    match event_pipeline {
        Some(pipeline) => Ok(pipeline),
        None => bail!("Failed to build event pipeline"),
    }
}
