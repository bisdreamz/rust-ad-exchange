use crate::app::pipeline::syncing::out::context::SyncOutContext;
use crate::app::pipeline::syncing::utils;
use anyhow::{Error, anyhow};
use pipeline::BlockingTask;
use rtb::child_span_info;
use tracing::debug;

pub struct ExtractLocalUidTask;

impl BlockingTask<SyncOutContext, Error> for ExtractLocalUidTask {
    fn run(&self, context: &SyncOutContext) -> Result<(), Error> {
        let _span = child_span_info!(
            "extract_local_uid_task",
            "local_uid" = tracing::field::Empty,
        );

        let (local_uid, cookie_existed) = utils::extract_or_assign_local_uid(&context.cookies);

        debug!(
            "Locald uid value={} recognized={}",
            local_uid, cookie_existed
        );

        context
            .local_uid
            .set(local_uid)
            .map_err(|_| anyhow!("Failed to update local id on context?!"))?;

        Ok(())
    }
}
