use crate::app::pipeline::events::billing::context::BillingEventContext;
use anyhow::{Error, bail};
use pipeline::BlockingTask;
use rtb::child_span_info;

/// Convenience task to check if the context.demand_urls sentinel is present
/// and bail if not. The demand_urls are used not only to track DSP burls
/// to call, but also as a means of de-duplicating incoming impressions
/// or enforcing an impression TTL via the event cache expiry
pub struct BailIfExpiredTask;

impl BlockingTask<BillingEventContext, Error> for BailIfExpiredTask {
    fn run(&self, context: &BillingEventContext) -> Result<(), Error> {
        let _span = child_span_info!("bail_if_expired_task").entered();

        if context.demand_urls.get().is_none() {
            bail!("No demand URLs sentinel found on billing event. Imp expired or duplicate!");
        }

        Ok(())
    }
}
