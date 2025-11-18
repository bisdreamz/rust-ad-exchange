use crate::app::context::StartupContext;
use crate::app::pipeline::syncing::out::context::SyncOutContext;
use crate::app::pipeline::syncing::out::tasks;
use anyhow::{Error, bail};
use pipeline::{Pipeline, PipelineBuilder};

/// Builds the pipeline responsible for handling our user sync pixel calls,
/// such as a supplier calling our sync url (pixel or iframe) or us dropping
/// our sync pixel in ad markup. Initiates the process which append demand
/// pixels and starts the outbound partner syncing, which requests their
/// user IDs that should be sent back to us in a redirect so we can send
/// those IDs to them in the buyeruid field (if a demand partner)
pub fn build_user_sync_out_pipeline(
    context: &StartupContext,
) -> Result<Pipeline<SyncOutContext, Error>, Error> {
    let pub_manager = match context.pub_manager.get() {
        Some(pub_manager) => pub_manager,
        None => bail!("No publisher manager?! Cant build rtb pipeline"),
    };

    let bidder_manager = match context.bidder_manager.get() {
        Some(bidder_manager) => bidder_manager,
        None => bail!("No Bidder Manager?! Cant build rtb pipeline"),
    };

    let pipeline = PipelineBuilder::new()
        .with_blocking(Box::new(tasks::ExtractLocalUidTask))
        .with_blocking(Box::new(tasks::ExtractPublisherTask::new(
            pub_manager.clone(),
        )))
        .with_blocking(Box::new(tasks::BuildSyncOutResponseTask::new(
            bidder_manager.clone(),
        )))
        .build()
        .expect("Sync out pipeline should have tasks");

    Ok(pipeline)
}
