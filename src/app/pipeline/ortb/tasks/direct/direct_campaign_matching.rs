use crate::app::pipeline::ortb::AuctionContext;
use crate::app::pipeline::ortb::direct::pacing::{DealPacer, SpendPacer};
use crate::app::pipeline::ortb::direct::{bid, creative, matching};
use crate::core::managers::{CampaignManager, CreativeManager, DealManager};
use crate::core::models::placement::FillPolicy;
use ahash::AHashSet;
use anyhow::Error;
use async_trait::async_trait;
use pipeline::AsyncTask;
use std::sync::Arc;

/// Matches direct campaigns + deals against each imp in the request.
///
/// Runs after enrichment (pub, device, identity resolved).
/// No-ops if no campaigns are loaded (e.g. Firestore not configured).
/// Never errors — direct matching failures are silent misses.
///
/// Per-imp flow:
/// 1. `match_imp` gathers unpaced candidates shuffled then stable-sorted
///    by price descending (equal-price campaigns rotate across requests)
/// 2. Walk candidates best-first: skip used advertisers → creative match
///    → spend pacer → first to pass all = winner
/// 3. Winner's advertiser_id added to used set so subsequent imps
///    won't show the same advertiser again
pub struct DirectCampaignMatchingTask {
    campaign_manager: Arc<CampaignManager>,
    creative_manager: Arc<CreativeManager>,
    deal_manager: Arc<DealManager>,
    spend_pacer: Arc<dyn SpendPacer>,
    deal_pacer: Arc<dyn DealPacer>,
}

impl DirectCampaignMatchingTask {
    pub fn new(
        campaign_manager: Arc<CampaignManager>,
        creative_manager: Arc<CreativeManager>,
        deal_manager: Arc<DealManager>,
        spend_pacer: Arc<dyn SpendPacer>,
        deal_pacer: Arc<dyn DealPacer>,
    ) -> Self {
        Self {
            campaign_manager,
            creative_manager,
            deal_manager,
            spend_pacer,
            deal_pacer,
        }
    }
}

#[async_trait]
impl AsyncTask<AuctionContext, Error> for DirectCampaignMatchingTask {
    async fn run(&self, ctx: &AuctionContext) -> Result<(), Error> {
        let all_campaigns = self.campaign_manager.all();
        let direct_deals = self.deal_manager.direct_deals();

        // No campaigns loaded — nothing to match
        if all_campaigns.is_empty() && direct_deals.is_empty() {
            return Ok(());
        }

        let fill_policy = ctx
            .placement
            .get()
            .map(|p| p.fill_policy.clone())
            .unwrap_or(FillPolicy::HighestPrice);

        let campaign_mgr = &self.campaign_manager;
        let campaigns_by_company = |company_id: &str| campaign_mgr.by_company(company_id);

        // Scope the RwLockReadGuard so it drops before .await below
        let direct_bidder_contexts = {
            let req = ctx.req.read();

            let mut results = Vec::new();
            // Track used advertisers across imps — same advertiser
            // can't win multiple slots in the same request
            let mut used_advertisers = AHashSet::new();

            for imp in &req.imp {
                // Step 1: gather unpaced candidates (shuffled + sorted by price desc)
                let result = matching::match_imp(
                    &direct_deals,
                    &all_campaigns,
                    &campaigns_by_company,
                    &fill_policy,
                    self.deal_pacer.as_ref(),
                    ctx,
                    imp,
                );

                // Step 2: walk best-first — dedup → creative → pacing → winner
                let winner = result.candidates.into_iter().find_map(|candidate| {
                    // Skip if this advertiser already won another imp
                    if used_advertisers.contains(&candidate.campaign.advertiser_id) {
                        return None;
                    }

                    let creatives = self.creative_manager.by_campaign(&candidate.campaign.id);
                    let creative = creative::select_creative(&creatives, imp)?;

                    if !self
                        .spend_pacer
                        .passes(&candidate.campaign, candidate.price)
                    {
                        return None;
                    }

                    let advertiser_id = candidate.campaign.advertiser_id.clone();

                    let bid_ctx = bid::synthesize_bid(
                        &candidate.campaign,
                        &creative,
                        candidate.deal,
                        candidate.price,
                        &imp.id,
                    );

                    // Mark advertiser as used before returning
                    used_advertisers.insert(advertiser_id);

                    Some(bid::wrap_in_bidder_context(bid_ctx))
                });

                if let Some(bidder_ctx) = winner {
                    results.push(bidder_ctx);
                }
            }

            results
        };

        if !direct_bidder_contexts.is_empty() {
            let mut bidders = ctx.bidders.lock().await;
            bidders.extend(direct_bidder_contexts);
        }

        Ok(())
    }
}
