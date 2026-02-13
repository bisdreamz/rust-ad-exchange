use crate::app::pipeline::events::billing::context::BillingEventContext;
use anyhow::{Error, anyhow};
use pipeline::BlockingTask;
use rtb::child_span_info;

/// Single place to house logic which marks whether an event
/// is considered expired. Derived from whether the
/// context.demand_urls sentinel is present or not,
/// which indicates if the event if duplicate OR beyond ttl
pub struct MarkIfExpiredTask;

impl BlockingTask<BillingEventContext, Error> for MarkIfExpiredTask {
    fn run(&self, context: &BillingEventContext) -> Result<(), Error> {
        let _span = child_span_info!("mark_if_expired_task").entered();

        let expired = context.demand_urls.get().is_none();
        context
            .expired
            .set(expired)
            .map_err(|_| anyhow!("Failed to set expired flag on context!"))?;

        Ok(())
    }
}
