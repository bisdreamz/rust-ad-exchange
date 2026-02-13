use crate::app::pipeline::events::billing::context::BillingEventContext;
use anyhow::{Error, anyhow, bail};
use pipeline::BlockingTask;
use rtb::child_span_info;

/// Convenience task to bail if event is considered expired,
/// which should run after all "always run" tasks
/// have ran, but before deduplicated tasks should run
/// e.g. traffic shaping training
pub struct BailIfExpiredTask;

impl BlockingTask<BillingEventContext, Error> for BailIfExpiredTask {
    fn run(&self, context: &BillingEventContext) -> Result<(), Error> {
        let _span = child_span_info!("bail_if_expired_task").entered();

        if *context
            .expired
            .get()
            .ok_or_else(|| anyhow!("No expired flag on context!"))?
        {
            bail!("Duplicate or expired billing event!");
        }

        Ok(())
    }
}
