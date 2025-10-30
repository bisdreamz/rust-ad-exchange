use crate::app::context::StartupContext;
use crate::app::pipeline::events::billing::pipeline::build_event_pipeline;
use crate::app::span::WrappedPipelineTask;
use anyhow::Error;
use pipeline::{BlockingTask, PipelineBuilder};
use rtb::child_span_info;
use std::sync::Arc;
use tracing::instrument;

pub struct BuildEventPipelineTask;

impl BlockingTask<StartupContext, Error> for BuildEventPipelineTask {
    #[instrument(skip_all, name = "build_event_pipeline_task")]
    fn run(&self, context: &StartupContext) -> Result<(), Error> {
        let billing_pipeline = build_event_pipeline(context)?;

        let observed_pipeline_task = WrappedPipelineTask::new(billing_pipeline, move || {
            child_span_info!("billing_event_pipeline")
        });

        let observed_pipeline = PipelineBuilder::new()
            .with_async(Box::new(observed_pipeline_task))
            .build()
            .expect("Failed to build observed billing pipeline");

        context
            .event_pipeline
            .set(Arc::new(observed_pipeline))
            .map_err(|_| anyhow::anyhow!("billing_event_pipeline already assigned!"))
    }
}
