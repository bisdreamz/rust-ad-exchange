use crate::app::pipeline::ortb::AuctionContext;
use crate::app::pipeline::ortb::context::BidderResponseState;
use crate::core::firestore::counters::campaign::{CampaignCounterStore, CampaignCounters};
use crate::core::managers::{AdvertiserManager, BuyerManager};
use crate::core::spec::StatsDeviceType;
use anyhow::Error;
use async_trait::async_trait;
use pipeline::AsyncTask;
use rtb::child_span_info;
use std::sync::Arc;
use tracing::{Instrument, warn};

/// Records auction-phase bid counts for direct campaigns.
///
/// Iterates all bidder contexts, finds direct bids via
/// `bid_ctx.direct`, and merges auction/bid stats to the
/// campaign counter store. Impressions are recorded separately
/// in the billing pipeline when the billing event fires.
pub struct CampaignCountersTask {
    store: Arc<CampaignCounterStore>,
    buyer_manager: Arc<BuyerManager>,
    advertiser_manager: Arc<AdvertiserManager>,
}

impl CampaignCountersTask {
    pub fn new(
        store: Arc<CampaignCounterStore>,
        buyer_manager: Arc<BuyerManager>,
        advertiser_manager: Arc<AdvertiserManager>,
    ) -> Self {
        Self {
            store,
            buyer_manager,
            advertiser_manager,
        }
    }

    async fn run0(&self, ctx: &AuctionContext) -> Result<(), Error> {
        let publisher = match ctx.publisher.get() {
            Some(p) => p,
            None => return Ok(()), // unrecognized seller — nothing to record
        };

        let device = ctx.device.get();
        let dev_os = device.map(|d| d.os.to_string()).unwrap_or_default();

        let (dev_type, country) = {
            let req = ctx.req.read();
            let dt = StatsDeviceType::from_openrtb(req.device.as_ref().map_or(0, |d| d.devicetype));
            let co = req
                .device
                .as_ref()
                .and_then(|d| d.geo.as_ref())
                .map(|g| g.country.clone())
                .unwrap_or_default();
            (dt.to_string(), co)
        };

        let bidders = ctx.bidders.lock().await;

        for bidder in bidders.iter() {
            for callout in &bidder.callouts {
                let Some(response) = callout.response.get() else {
                    continue;
                };

                let BidderResponseState::Bid(resp_ctx) = &response.state else {
                    continue;
                };

                for seat in &resp_ctx.seatbids {
                    for bid_ctx in &seat.bids {
                        let Some(direct) = bid_ctx.direct.get() else {
                            continue; // RTB bid — skip
                        };

                        let campaign = &direct.campaign;
                        let creative = &direct.creative;
                        let deal_id = bid_ctx.deal.get().map(|d| d.id.as_str()).unwrap_or("");

                        let mut counters = CampaignCounters::default();
                        counters.auction();
                        counters.bid();

                        if bid_ctx.filter_reason.is_some() {
                            counters.bids_filtered(1);
                        }

                        let buyer_name = self
                            .buyer_manager
                            .get(&campaign.company_id)
                            .map(|b| b.buyer_name.clone())
                            .unwrap_or_else(|| {
                                warn!(
                                    buyer_id = %campaign.company_id,
                                    "Buyer not found for campaign counters"
                                );
                                campaign.company_id.clone()
                            });

                        let advertiser_name = self
                            .advertiser_manager
                            .get(&campaign.advertiser_id)
                            .map(|a| a.brand.clone())
                            .unwrap_or_default();

                        self.store.merge(
                            &campaign.company_id,
                            &buyer_name,
                            &campaign.id,
                            &campaign.name,
                            &publisher.id,
                            &creative.id,
                            creative.format.as_str(),
                            deal_id,
                            &dev_type,
                            &dev_os,
                            &country,
                            &advertiser_name,
                            "direct",
                            &counters,
                        );
                    }
                }
            }
        }

        Ok(())
    }
}

#[async_trait]
impl AsyncTask<AuctionContext, Error> for CampaignCountersTask {
    async fn run(&self, ctx: &AuctionContext) -> Result<(), Error> {
        let span = child_span_info!("campaign_counters_task");
        self.run0(ctx).instrument(span).await
    }
}
