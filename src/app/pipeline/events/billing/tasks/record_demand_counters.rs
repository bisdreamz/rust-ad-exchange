use crate::app::pipeline::events::billing::context::BillingEventContext;
use crate::core::firestore::counters::demand::{DemandCounterStore, DemandCounters};
use crate::core::managers::DemandManager;
use anyhow::{Error, anyhow};
use pipeline::BlockingTask;
use rtb::child_span_info;
use std::sync::Arc;

pub struct RecordDemandBillingCountersTask {
    demand_store: Arc<DemandCounterStore>,
    demand_manager: Arc<DemandManager>,
}

impl RecordDemandBillingCountersTask {
    pub fn new(store: Arc<DemandCounterStore>, manager: Arc<DemandManager>) -> Self {
        RecordDemandBillingCountersTask {
            demand_store: store,
            demand_manager: manager,
        }
    }
}

impl BlockingTask<BillingEventContext, Error> for RecordDemandBillingCountersTask {
    fn run(&self, context: &BillingEventContext) -> Result<(), Error> {
        let _span = child_span_info!("record_demand_counters_task").entered();

        let details = context
            .details
            .get()
            .ok_or_else(|| anyhow!("No billing event details on context!"))?;

        let bidder = self
            .demand_manager
            .bidder(&details.bidder_id)
            .ok_or_else(|| anyhow!("Bidder id unrecognized for billing event!"))?;

        let mut counters = DemandCounters::default();
        counters.impression(details.cpm_gross, details.cpm_cost);

        self.demand_store.merge_impression(
            bidder.id.as_str(),
            bidder.name.as_str(),
            &details.endpoint_id,
            details.channel,
            details.device_type,
            &counters,
        );

        Ok(())
    }
}
