use crate::app::context::StartupContext;
use crate::app::pipeline::adtag::build_adtag_pipeline;
use crate::app::span::WrappedPipelineTask;
use anyhow::Error;
use pipeline::{BlockingTask, PipelineBuilder};
use rtb::child_span_info;
use std::sync::Arc;
use tracing::instrument;

pub struct BuildAdtagPipelineTask;

impl BlockingTask<StartupContext, Error> for BuildAdtagPipelineTask {
    #[instrument(skip_all, name = "build_adtag_pipeline_task")]
    fn run(&self, context: &StartupContext) -> Result<(), Error> {
        let adtag_pipeline = build_adtag_pipeline(context)?;

        let observed_pipeline_task =
            WrappedPipelineTask::new(adtag_pipeline, move || child_span_info!("adtag_pipeline"));

        let observed_pipeline = PipelineBuilder::new()
            .with_async(Box::new(observed_pipeline_task))
            .build()
            .expect("Failed to build observed adtag pipeline");

        context
            .adtag_pipeline
            .set(Arc::new(observed_pipeline))
            .map_err(|_| anyhow::anyhow!("adtag_pipeline already assigned!"))
    }
}
