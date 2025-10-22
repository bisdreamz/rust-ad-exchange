use crate::app::context::StartupContext;
use crate::app::pipeline::ortb::build_auction_pipeline;
use crate::app::span::WrappedPipelineTask;
use anyhow::Error;
use pipeline::{BlockingTask, PipelineBuilder};
use rtb::child_span_info;
use std::sync::Arc;
use tracing::instrument;

pub struct BuildRtbPipelineTask;

impl BlockingTask<StartupContext, anyhow::Error> for BuildRtbPipelineTask {
    #[instrument(skip_all, name = "build_rtb_pipeline_task")]
    fn run(&self, context: &StartupContext) -> Result<(), Error> {
        let auction_pipeline = build_auction_pipeline(context)?;

        let observed_pipeline_task =
            WrappedPipelineTask::new(auction_pipeline, move || child_span_info!("rtb_pipeline"));

        let observed_pipeline = PipelineBuilder::new()
            .with_async(Box::new(observed_pipeline_task))
            .build()
            .expect("Failed to build observed rtb pipeline");

        context
            .auction_pipeline
            .set(Arc::new(observed_pipeline))
            .map_err(|_| anyhow::anyhow!("rtb_pipeline already assigned!"))
    }
}
