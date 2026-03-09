use crate::app::lifecycle::context::StartupContext;
use crate::app::pipeline::creatives::raw::pipeline::build_raw_creative_pipeline;
use crate::app::span::WrappedPipelineTask;
use anyhow::Error;
use pipeline::{BlockingTask, PipelineBuilder};
use rtb::child_span_info;
use std::sync::Arc;
use tracing::{info, instrument};

pub struct BuildCreativePipelineTask;

impl BlockingTask<StartupContext, Error> for BuildCreativePipelineTask {
    #[instrument(skip_all, name = "build_creative_pipeline_task")]
    fn run(&self, context: &StartupContext) -> Result<(), Error> {
        let Some(cdn_base) = context.cdn_base.get().cloned() else {
            info!("cdn_base not configured — skipping creative pipeline");
            return Ok(());
        };

        let raw_pipeline = build_raw_creative_pipeline(context, cdn_base)?;

        let observed_pipeline_task = WrappedPipelineTask::new(raw_pipeline, move || {
            child_span_info!("raw_creative_pipeline")
        });

        let observed_pipeline = PipelineBuilder::new()
            .with_async(Box::new(observed_pipeline_task))
            .build()
            .expect("Failed to build observed raw creative pipeline");

        context
            .raw_creative_pipeline
            .set(Arc::new(observed_pipeline))
            .map_err(|_| anyhow::anyhow!("raw_creative_pipeline already assigned!"))
    }
}
