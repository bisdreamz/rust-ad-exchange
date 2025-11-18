use anyhow::{Error, anyhow, bail};
use pipeline::{Pipeline, PipelineBuilder};

use crate::app::context::StartupContext;
use crate::app::pipeline::syncing::r#in::context::SyncInContext;
use crate::app::pipeline::syncing::r#in::tasks;

pub fn build_user_sync_in_pipeline(
    context: &StartupContext,
) -> Result<Pipeline<SyncInContext, Error>, Error> {
    let bidder_manager = match context.bidder_manager.get() {
        Some(bidder_manager) => bidder_manager,
        None => bail!("No Bidder Manager?! Cant build rtb pipeline"),
    };

    let sync_store = context
        .sync_store
        .get()
        .ok_or_else(|| anyhow!("No sync store created on context!"))?;

    let pipeline = PipelineBuilder::new()
        .with_blocking(Box::new(tasks::ParseUrlTask))
        .with_blocking(Box::new(tasks::ExtractEventTask))
        .with_blocking(Box::new(tasks::ExtractDemandTask::new(
            bidder_manager.clone(),
        )))
        .with_async(Box::new(tasks::StoreBuyeruidTask::new(sync_store.clone())))
        .build()
        .expect("Sync in pipeline should have tasks");

    Ok(pipeline)
}
