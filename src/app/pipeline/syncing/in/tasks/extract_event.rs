use crate::app::pipeline::syncing::r#in::context::SyncInContext;
use crate::app::pipeline::syncing::utils;
use crate::core::usersync::model::SyncInEvent;
use anyhow::{Error, anyhow};
use pipeline::BlockingTask;
use rtb::child_span_info;
use tracing::{debug, warn};

/// Task to extract a well formed ['SyncInEvent'] from
/// the data url on the context. Also assigns a new
/// localuid if there is no present uid value on
/// the context cookies
pub struct ExtractEventTask;

impl BlockingTask<SyncInContext, Error> for ExtractEventTask {
    fn run(&self, context: &SyncInContext) -> Result<(), Error> {
        let _span = child_span_info!("extract_event_task").entered();

        let data_url = context
            .data_url
            .get()
            .ok_or_else(|| anyhow!("Data url missing from context!"))?;

        let (local_uid, recognized) = utils::extract_or_assign_local_uid(&context.cookies);

        if !recognized {
            warn!("Inbound buyeruid sync but we didnt recognize the user. Unexpected");
        }

        let sync_in_event = SyncInEvent::from(data_url, local_uid)
            .map_err(|_| anyhow!("Failed to parse data url into sync in event"))?;

        debug!("Attaching sync in event to context: {:?}", &sync_in_event);

        context
            .event
            .set(sync_in_event)
            .map_err(|_| anyhow!("Could not set sync in event, already set?!"))?;

        Ok(())
    }
}
