use crate::app::context::StartupContext;
use crate::app::pipeline::ortb::build_rtb_pipeline;
use crate::app::span::WrappedPipelineTask;
use crate::sample_or_attach_root_span;
use anyhow::{anyhow, Error};
use pipeline::{BlockingTask, PipelineBuilder};
use std::sync::Arc;
use tracing::{info, instrument};

pub struct BuildRtbPipelineTask;

impl BlockingTask<StartupContext, anyhow::Error> for BuildRtbPipelineTask {
    #[instrument(skip_all, name = "build_rtb_pipeline_task")]
    fn run(&self, context: &StartupContext) -> Result<(), Error> {
        let sample_percent = context.config.get()
            .ok_or(anyhow!("Aaa"))?.logging.span_sample_rate;

        let rtb_pipeline = build_rtb_pipeline(context)?;

        info!("Span sample rate: {}", sample_percent);

        let observed_pipeline_task = WrappedPipelineTask::new(rtb_pipeline,
                                                              move || sample_or_attach_root_span!(sample_percent, "rtb_pipeline"));

        let observed_pipeline = PipelineBuilder::new()
            .with_async(Box::new(observed_pipeline_task))
            .build()
            .expect("Failed to build observed rtb pipeline");

        context
            .rtb_pipeline
            .set(Arc::new(observed_pipeline))
            .map_err(|_| anyhow::anyhow!("rtb_pipeline already assigned!"))
    }
}
