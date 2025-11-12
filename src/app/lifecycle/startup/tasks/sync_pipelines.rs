use std::sync::Arc;
use anyhow::{anyhow, Error};
use pipeline::BlockingTask;
use rtb::child_span_info;
use crate::app::context::StartupContext;
use crate::app::pipeline::syncing::out::pipeline::build_user_sync_out_pipeline;

pub struct BuildSyncPipelinesTask;

impl BlockingTask<StartupContext, Error> for BuildSyncPipelinesTask {
    fn run(&self, context: &StartupContext) -> Result<(), Error> {
        let _span = child_span_info!("build_sync_pipelines_task").entered();

        let sync_out_pipeline = build_user_sync_out_pipeline(context)?;

        context.sync_out_pipeline.set(Arc::new(sync_out_pipeline))
            .map_err(|_err| anyhow!("Failed to attach sync out pipeline to start context!"))?;

        Ok(())
    }
}