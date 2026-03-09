use crate::app::pipeline::events::billing::context::BillingEventContext;
use crate::core::firestore::counters::campaign::{CampaignCounterStore, CampaignCounters};
use crate::core::firestore::counters::publisher::PublisherCounterStore;
use crate::core::managers::{AdvertiserManager, PublisherManager};
use anyhow::{Error, anyhow};
use pipeline::BlockingTask;
use rtb::child_span_info;
use std::sync::Arc;

/// Writes Firestore counter documents for direct campaign billing events.
/// Pacing state (spend tracking, deal impressions) is handled separately
/// by `RecordPacingTask` which runs unconditionally before this task.
pub struct RecordCampaignBillingCountersTask {
    campaign_store: Arc<CampaignCounterStore>,
    pub_store: Arc<PublisherCounterStore>,
    advertiser_manager: Arc<AdvertiserManager>,
    publisher_manager: Arc<PublisherManager>,
}

impl RecordCampaignBillingCountersTask {
    pub fn new(
        campaign_store: Arc<CampaignCounterStore>,
        pub_store: Arc<PublisherCounterStore>,
        advertiser_manager: Arc<AdvertiserManager>,
        publisher_manager: Arc<PublisherManager>,
    ) -> Self {
        Self {
            campaign_store,
            pub_store,
            advertiser_manager,
            publisher_manager,
        }
    }
}

impl BlockingTask<BillingEventContext, Error> for RecordCampaignBillingCountersTask {
    fn run(&self, context: &BillingEventContext) -> Result<(), Error> {
        let _span = child_span_info!("record_campaign_counters_task").entered();

        // RTB bids have no campaign context — skip campaign counters
        let notice = context
            .bid_notice
            .get()
            .ok_or_else(|| anyhow!("No bid notice on billing context!"))?;

        let details = context
            .details
            .get()
            .ok_or_else(|| anyhow!("No billing event details on context!"))?;

        let mut counters = CampaignCounters::default();
        counters.impression(details.cpm_gross, details.cpm_cost);

        let dev_type = details.device_type.to_string();
        let dev_os = details.device_os.to_string();
        let country = &details.country;
        let deal_id = notice
            .deal
            .as_ref()
            .map(|d| d.id.as_str())
            .unwrap_or("None");
        let deal_name = notice
            .deal
            .as_ref()
            .map(|d| d.name.as_str())
            .unwrap_or("None");
        let pub_name = self
            .publisher_manager
            .get(&details.pub_id)
            .map(|p| p.name.clone())
            .unwrap_or_default();

        if let Some(direct) = &notice.direct {
            let campaign = &direct.campaign;
            let creative = &direct.creative;
            let buyer_name = &direct.buyer.buyer_name;

            let advertiser_name = self
                .advertiser_manager
                .get(&campaign.advertiser_id)
                .map(|a| a.brand.clone())
                .unwrap_or_default();

            self.campaign_store.merge(
                &campaign.buyer_id,
                buyer_name,
                &campaign.id,
                &campaign.name,
                &details.pub_id,
                &pub_name,
                &creative.id,
                &creative.name,
                creative.format.as_str(),
                deal_id,
                deal_name,
                &dev_type,
                &dev_os,
                country,
                &advertiser_name,
                "direct",
                &counters,
            );

            self.pub_store.merge_detail(
                &details.pub_id,
                &campaign.id,
                &campaign.buyer_id,
                buyer_name,
                deal_id,
                deal_name,
                &pub_name,
                &dev_type,
                &dev_os,
                country,
                "direct",
                &counters,
            );
        } else {
            // RTB bids — no campaign context, but still record pub detail
            // stats for per-bidder/deal/device/geo breakdown
            self.pub_store.merge_detail(
                &details.pub_id,
                "",
                &details.bidder_id,
                "",
                deal_id,
                deal_name,
                &pub_name,
                &dev_type,
                &dev_os,
                country,
                "rtb",
                &counters,
            );
        }

        Ok(())
    }
}
