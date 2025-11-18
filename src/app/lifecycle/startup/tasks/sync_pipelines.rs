use crate::app::context::StartupContext;
use crate::app::pipeline::syncing::r#in::pipeline::build_user_sync_in_pipeline;
use crate::app::pipeline::syncing::out::pipeline::build_user_sync_out_pipeline;
use anyhow::{Error, anyhow};
use pipeline::BlockingTask;
use rtb::child_span_info;
use std::sync::Arc;

pub struct BuildSyncPipelinesTask;

impl BlockingTask<StartupContext, Error> for BuildSyncPipelinesTask {
    fn run(&self, context: &StartupContext) -> Result<(), Error> {
        let _span = child_span_info!("build_sync_pipelines_task").entered();

        let sync_out_pipeline = build_user_sync_out_pipeline(context)?;

        context
            .sync_out_pipeline
            .set(Arc::new(sync_out_pipeline))
            .map_err(|_err| anyhow!("Failed to attach sync out pipeline to start context!"))?;

        let sync_in_pipeline = build_user_sync_in_pipeline(context)?;

        context
            .sync_in_pipeline
            .set(Arc::new(sync_in_pipeline))
            .map_err(|_err| anyhow!("Failed to attach sync in pipeline to start context!"))?;

        Ok(())
    }
}
