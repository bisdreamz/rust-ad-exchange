use crate::app::pipeline::events::billing::context::BillingEventContext;
use crate::core::firestore::counters::deal::{DealCounterStore, DealCounters};
use anyhow::{Error, anyhow};
use pipeline::BlockingTask;
use rtb::child_span_info;
use std::sync::Arc;

/// Writes Firestore deal counter documents for any billing event
/// associated with a deal (both direct and RTB). Deals are platform-wide
/// and not scoped to a specific demand source.
pub struct RecordDealBillingCountersTask {
    deal_store: Arc<DealCounterStore>,
}

impl RecordDealBillingCountersTask {
    pub fn new(deal_store: Arc<DealCounterStore>) -> Self {
        Self { deal_store }
    }
}

impl BlockingTask<BillingEventContext, Error> for RecordDealBillingCountersTask {
    fn run(&self, context: &BillingEventContext) -> Result<(), Error> {
        let _span = child_span_info!("record_deal_counters_task").entered();

        let notice = context
            .bid_notice
            .get()
            .ok_or_else(|| anyhow!("No bid notice on billing context!"))?;

        let deal = match &notice.deal {
            Some(deal) => deal,
            None => return Ok(()),
        };

        let details = context
            .details
            .get()
            .ok_or_else(|| anyhow!("No billing event details on context!"))?;

        let mut counters = DealCounters::default();
        counters.impression();
        counters.record_spend(details.cpm_gross, details.cpm_cost);
        self.deal_store.merge(&deal.id, &counters);

        Ok(())
    }
}
