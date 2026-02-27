use crate::app::pipeline::events::billing::context::BillingEventContext;
use crate::app::pipeline::ortb::direct::pacing::{DealPacer, SpendTracker};
use crate::core::firestore::counters::campaign::{CampaignCounterStore, CampaignCounters};
use crate::core::firestore::counters::deal::{DealCounterStore, DealCounters};
use crate::core::firestore::counters::publisher::PublisherCounterStore;
use crate::core::managers::{AdvertiserManager, BuyerManager};
use anyhow::{Error, anyhow};
use pipeline::BlockingTask;
use rtb::child_span_info;
use std::sync::Arc;
use tracing::warn;

pub struct RecordCampaignBillingCountersTask {
    campaign_store: Arc<CampaignCounterStore>,
    deal_store: Arc<DealCounterStore>,
    pub_store: Arc<PublisherCounterStore>,
    buyer_manager: Arc<BuyerManager>,
    advertiser_manager: Arc<AdvertiserManager>,
    spend_tracker: Arc<dyn SpendTracker>,
    deal_pacer: Arc<dyn DealPacer>,
}

impl RecordCampaignBillingCountersTask {
    pub fn new(
        campaign_store: Arc<CampaignCounterStore>,
        deal_store: Arc<DealCounterStore>,
        pub_store: Arc<PublisherCounterStore>,
        buyer_manager: Arc<BuyerManager>,
        advertiser_manager: Arc<AdvertiserManager>,
        spend_tracker: Arc<dyn SpendTracker>,
        deal_pacer: Arc<dyn DealPacer>,
    ) -> Self {
        Self {
            campaign_store,
            deal_store,
            pub_store,
            buyer_manager,
            advertiser_manager,
            spend_tracker,
            deal_pacer,
        }
    }
}

impl BlockingTask<BillingEventContext, Error> for RecordCampaignBillingCountersTask {
    fn run(&self, context: &BillingEventContext) -> Result<(), Error> {
        let _span = child_span_info!("record_campaign_counters_task").entered();

        let details = context
            .details
            .get()
            .ok_or_else(|| anyhow!("No billing event details on context!"))?;

        let mut counters = CampaignCounters::default();
        counters.impression(details.cpm_gross, details.cpm_cost);

        let dev_type = details.device_type.to_string();
        let dev_os = details.device_os.to_string();
        let country = &details.country;

        let notice = context
            .bid_notice
            .get()
            .ok_or_else(|| anyhow!("No bid notice on billing context!"))?;

        // Deal impression tracking — applies to both direct and RTB bids
        if let Some(deal) = &notice.deal {
            self.deal_pacer.record_impression(&deal.id);

            let mut deal_counters = DealCounters::default();
            deal_counters.impression();
            self.deal_store.merge(&deal.id, &deal_counters);
        }

        let deal_id = notice.deal.as_ref().map(|d| d.id.as_str()).unwrap_or("");

        if let Some(direct) = &notice.direct {
            let campaign = &direct.campaign;
            let creative = &direct.creative;

            let buyer_name = self
                .buyer_manager
                .get(&campaign.company_id)
                .map(|b| b.buyer_name.clone())
                .unwrap_or_else(|| {
                    warn!(buyer_id = %campaign.company_id, "Buyer not found for campaign billing");
                    campaign.company_id.clone()
                });

            let advertiser_name = self
                .advertiser_manager
                .get(&campaign.advertiser_id)
                .map(|a| a.brand.clone())
                .unwrap_or_default();

            self.campaign_store.merge(
                &campaign.company_id,
                &buyer_name,
                &campaign.id,
                &campaign.name,
                &details.pub_id,
                &creative.id,
                creative.format.as_str(),
                deal_id,
                &dev_type,
                &dev_os,
                country,
                &advertiser_name,
                "direct",
                &counters,
            );

            self.spend_tracker
                .record_spend(&campaign.id, details.cpm_gross);

            self.pub_store.merge_detail(
                &details.pub_id,
                &campaign.id,
                &campaign.company_id,
                &buyer_name,
                deal_id,
                &dev_type,
                &dev_os,
                country,
                "direct",
                &counters,
            );
        } else {
            // RTB billing — pub detail only, campaign fields empty
            self.pub_store.merge_detail(
                &details.pub_id,
                "",
                "",
                "",
                deal_id,
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
