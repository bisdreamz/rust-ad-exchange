use crate::app::pipeline::events::billing::context::BillingEventContext;
use crate::core::firestore::counters::publisher::{PublisherCounterStore, PublisherCounters};
use crate::core::managers::PublisherManager;
use anyhow::{Error, anyhow, bail};
use async_trait::async_trait;
use pipeline::AsyncTask;
use rtb::child_span_info;
use std::sync::Arc;
use tracing::Instrument;

pub struct RecordBillingCountersTask {
    pub_store: Arc<PublisherCounterStore>,
    pub_manager: Arc<PublisherManager>,
}

impl RecordBillingCountersTask {
    pub fn new(store: Arc<PublisherCounterStore>, manager: Arc<PublisherManager>) -> Self {
        RecordBillingCountersTask {
            pub_store: store,
            pub_manager: manager,
        }
    }

    pub async fn run0(&self, context: &BillingEventContext) -> Result<(), Error> {
        let details = context
            .details
            .get()
            .ok_or_else(|| anyhow!("No billing event details on context!"))?;

        let mut counters = PublisherCounters::default();
        counters.impression(details.cpm_gross, details.cpm_cost);

        let publisher = self
            .pub_manager
            .get(&details.pub_id)
            .ok_or_else(|| anyhow!("No publisher found for billing event id!"))?;

        self.pub_store.merge_impression(
            publisher.id.as_str(),
            publisher.name.as_str(),
            details.bid_ad_format,
            &counters,
        );

        Ok(())
    }
}

#[async_trait]
impl AsyncTask<BillingEventContext, Error> for RecordBillingCountersTask {
    async fn run(&self, context: &BillingEventContext) -> Result<(), Error> {
        let span = child_span_info!("record_pub_counters_task");

        self.run0(context).instrument(span).await
    }
}
