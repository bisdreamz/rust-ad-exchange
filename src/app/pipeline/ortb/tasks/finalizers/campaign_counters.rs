use crate::app::pipeline::ortb::AuctionContext;
use crate::app::pipeline::ortb::context::BidderResponseState;
use crate::core::firestore::counters::campaign::{CampaignCounterStore, CampaignCounters};
use crate::core::managers::AdvertiserManager;
use crate::core::spec::StatsDeviceType;
use anyhow::Error;
use async_trait::async_trait;
use pipeline::AsyncTask;
use rtb::child_span_info;
use std::sync::Arc;
use tracing::Instrument;

/// Records auction-phase bid counts for direct campaigns.
///
/// Iterates the unified `bidders` list and checks each bid for
/// `direct` context — this ensures we see the post-merge filter
/// state (e.g. bids filtered by shared tasks like margin or
/// notice URL injection). Impressions are recorded separately
/// in the billing pipeline when the billing event fires.
pub struct CampaignCountersTask {
    store: Arc<CampaignCounterStore>,
    advertiser_manager: Arc<AdvertiserManager>,
}

impl CampaignCountersTask {
    pub fn new(
        store: Arc<CampaignCounterStore>,
        advertiser_manager: Arc<AdvertiserManager>,
    ) -> Self {
        Self {
            store,
            advertiser_manager,
        }
    }

    async fn run0(&self, ctx: &AuctionContext) -> Result<(), Error> {
        let publisher = &ctx.publisher;

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

        for bidder_ctx in bidders.iter() {
            for callout in &bidder_ctx.callouts {
                let response = match callout.response.get() {
                    Some(r) => r,
                    None => continue,
                };
                let bid_response = match &response.state {
                    BidderResponseState::Bid(br) => br,
                    _ => continue,
                };

                for seat in &bid_response.seatbids {
                    for bid_ctx in &seat.bids {
                        let direct = match bid_ctx.direct.get() {
                            Some(d) => d,
                            None => continue,
                        };

                        let campaign = &direct.campaign;
                        let creative = &direct.creative;
                        let buyer_name = &direct.buyer.buyer_name;
                        let deal_id = bid_ctx.deal.get().map(|d| d.id.as_str()).unwrap_or("");

                        let mut counters = CampaignCounters::default();
                        counters.auction();
                        counters.bid();

                        if bid_ctx.filter_reason.is_some() {
                            counters.bids_filtered(1);
                        }

                        let advertiser_name = self
                            .advertiser_manager
                            .get(&campaign.advertiser_id)
                            .map(|a| a.brand.clone())
                            .unwrap_or_default();

                        self.store.merge(
                            &campaign.buyer_id,
                            buyer_name,
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
