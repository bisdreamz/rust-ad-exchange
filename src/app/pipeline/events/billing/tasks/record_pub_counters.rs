use crate::app::pipeline::events::billing::context::BillingEventContext;
use crate::core::firestore::counters::publisher::{PublisherCounterStore, PublisherCounters};
use crate::core::managers::PublisherManager;
use anyhow::{Error, anyhow};
use pipeline::BlockingTask;
use rtb::child_span_info;
use std::sync::Arc;

pub struct RecordPubBillingCountersTask {
    pub_store: Arc<PublisherCounterStore>,
    pub_manager: Arc<PublisherManager>,
}

impl RecordPubBillingCountersTask {
    pub fn new(store: Arc<PublisherCounterStore>, manager: Arc<PublisherManager>) -> Self {
        RecordPubBillingCountersTask {
            pub_store: store,
            pub_manager: manager,
        }
    }
}

impl BlockingTask<BillingEventContext, Error> for RecordPubBillingCountersTask {
    fn run(&self, context: &BillingEventContext) -> Result<(), Error> {
        let _span = child_span_info!("record_pub_counters_task").entered();

        let details = context
            .details
            .get()
            .ok_or_else(|| anyhow!("No billing event details on context!"))?;

        let notice = context
            .bid_notice
            .get()
            .ok_or_else(|| anyhow!("No bid notice on billing context!"))?;

        let mut counters = PublisherCounters::default();
        counters.impression(details.cpm_gross, details.cpm_cost);

        let publisher = self
            .pub_manager
            .get(&details.pub_id)
            .ok_or_else(|| anyhow!("No publisher found for billing event id!"))?;

        self.pub_store.merge_impression(
            publisher.id.as_str(),
            publisher.name.as_str(),
            notice.format.as_str(),
            details.channel,
            details.device_type,
            &counters,
        );

        Ok(())
    }
}
