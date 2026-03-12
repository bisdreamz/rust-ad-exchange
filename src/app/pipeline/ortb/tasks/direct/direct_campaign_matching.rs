use crate::app::pipeline::ortb::AuctionContext;
use crate::app::pipeline::ortb::context::BidContext;
use crate::app::pipeline::ortb::direct::pacing::{DealPacer, SpendPacer};
use crate::app::pipeline::ortb::direct::{bid, creative, matching};
use crate::core::managers::{BuyerManager, CampaignManager, CreativeManager, DealManager};
use crate::core::models::placement::FillPolicy;
use ahash::AHashSet;
use anyhow::Error;
use async_trait::async_trait;
use pipeline::AsyncTask;
use rtb::child_span_info;
use std::sync::Arc;
use tracing::{Instrument, Span, debug, trace, warn};

/// Matches direct campaigns + deals against each imp in the request.
///
/// Runs after enrichment (pub, device, identity resolved).
/// No-ops if no campaigns are loaded (e.g. Firestore not configured).
/// Never errors — direct matching failures are silent misses.
///
/// Per-imp flow:
/// 1. `match_imp` gathers unpaced candidates (shuffled for tie-breaking)
/// 2. Draw candidates via price-weighted random selection (`price^k`):
///    higher prices are strongly favored but lower prices still get
///    exposure. Check dedup → creative → spend pacer → buyer. If a
///    candidate fails any check, remove it and redraw from the
///    remaining pool.
/// 3. Matched advertiser_id added to used set so subsequent imps
///    won't show the same advertiser again
pub struct DirectCampaignMatchingTask {
    campaign_manager: Arc<CampaignManager>,
    creative_manager: Arc<CreativeManager>,
    deal_manager: Arc<DealManager>,
    spend_pacer: Arc<dyn SpendPacer>,
    deal_pacer: Arc<dyn DealPacer>,
    buyer_manager: Arc<BuyerManager>,
}

impl DirectCampaignMatchingTask {
    pub fn new(
        campaign_manager: Arc<CampaignManager>,
        creative_manager: Arc<CreativeManager>,
        deal_manager: Arc<DealManager>,
        spend_pacer: Arc<dyn SpendPacer>,
        deal_pacer: Arc<dyn DealPacer>,
        buyer_manager: Arc<BuyerManager>,
    ) -> Self {
        Self {
            campaign_manager,
            creative_manager,
            deal_manager,
            spend_pacer,
            deal_pacer,
            buyer_manager,
        }
    }
}

#[async_trait]
impl AsyncTask<AuctionContext, Error> for DirectCampaignMatchingTask {
    async fn run(&self, ctx: &AuctionContext) -> Result<(), Error> {
        let span = child_span_info!(
            "direct_campaign_matching",
            campaigns_available = tracing::field::Empty,
            deals_available = tracing::field::Empty,
            direct_bids_staged = tracing::field::Empty,
        );

        self.run0(ctx).instrument(span).await
    }
}

impl DirectCampaignMatchingTask {
    async fn run0(&self, ctx: &AuctionContext) -> Result<(), Error> {
        let all_campaigns = self.campaign_manager.all();
        let direct_deals = self.deal_manager.direct_deals();

        let span = Span::current();
        span.record("campaigns_available", all_campaigns.len());
        span.record("deals_available", direct_deals.len());

        // No campaigns loaded — nothing to match
        if all_campaigns.is_empty() && direct_deals.is_empty() {
            span.record("direct_bids_staged", 0u64);
            debug!("No campaigns or deals loaded, skipping direct matching");
            return Ok(());
        }

        let fill_policy = ctx
            .placement
            .as_ref()
            .map(|p| p.fill_policy.clone())
            .unwrap_or(FillPolicy::HighestPrice);

        let campaign_mgr = &self.campaign_manager;
        let campaigns_by_buyer = |buyer_id: &str| campaign_mgr.by_buyer(buyer_id);

        // Scope the RwLockReadGuard so it drops before .await below
        let staged_bids: Vec<BidContext> = {
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
                    &campaigns_by_buyer,
                    &fill_policy,
                    self.deal_pacer.as_ref(),
                    ctx,
                    imp,
                );

                let num_candidates = result.candidates.len();
                trace!(
                    imp_id = %imp.id,
                    candidates = num_candidates,
                    "Imp candidate matching complete"
                );

                // Step 2: weighted random selection — draw candidates by
                // price^k, check dedup → creative → pacer → buyer.
                // Remove failed candidates and redraw until one passes
                // or pool is exhausted.
                let mut pool = result.candidates;
                let bid = loop {
                    let idx = match matching::draw_weighted(&pool) {
                        Some(i) => i,
                        None => break None,
                    };

                    let candidate = &pool[idx];

                    // Skip if this advertiser already won another imp
                    if used_advertisers.contains(&candidate.campaign.advertiser_id) {
                        trace!(
                            campaign = %candidate.campaign.id,
                            advertiser = %candidate.campaign.advertiser_id,
                            "Skipping duplicate advertiser"
                        );
                        pool.swap_remove(idx);
                        continue;
                    }

                    let creative = match creative::select_creative(
                        &candidate.campaign.creatives,
                        &self.creative_manager,
                        imp,
                    ) {
                        Some(c) => c,
                        None => {
                            trace!(
                                campaign = %candidate.campaign.id,
                                attached = candidate.campaign.creatives.len(),
                                "No matching creative for imp"
                            );
                            pool.swap_remove(idx);
                            continue;
                        }
                    };

                    if !self
                        .spend_pacer
                        .passes(&candidate.campaign, candidate.price)
                    {
                        trace!(
                            campaign = %candidate.campaign.id,
                            price = candidate.price,
                            "Spend pacer rejected"
                        );
                        pool.swap_remove(idx);
                        continue;
                    }

                    let buyer = match self.buyer_manager.get(&candidate.campaign.buyer_id) {
                        Some(b) => b,
                        None => {
                            warn!(
                                "Buyer {} not found, skipping campaign {}",
                                candidate.campaign.buyer_id, candidate.campaign.id
                            );
                            pool.swap_remove(idx);
                            continue;
                        }
                    };

                    if !buyer.enabled {
                        trace!(
                            buyer = %candidate.campaign.buyer_id,
                            campaign = %candidate.campaign.id,
                            "Buyer disabled, skipping campaign"
                        );
                        pool.swap_remove(idx);
                        continue;
                    }

                    let candidate = pool.swap_remove(idx);
                    let advertiser_id = candidate.campaign.advertiser_id.clone();
                    let deal_id = candidate.deal.as_ref().map(|d| d.id.as_str());

                    debug!(
                        campaign = %candidate.campaign.id,
                        campaign_name = %candidate.campaign.name,
                        creative = %creative.id,
                        price = candidate.price,
                        deal = deal_id.unwrap_or("none"),
                        imp_id = %imp.id,
                        "Direct campaign bid staged"
                    );

                    let bid_ctx = bid::synthesize_bid(
                        &buyer,
                        &candidate.campaign,
                        &creative,
                        candidate.deal,
                        candidate.price,
                        &imp.id,
                    );

                    used_advertisers.insert(advertiser_id);
                    break Some(bid_ctx);
                };

                if let Some(bid) = bid {
                    results.push(bid);
                } else {
                    trace!(imp_id = %imp.id, "No direct bid for imp");
                }
            }

            results
        };

        span.record("direct_bids_staged", staged_bids.len());

        if !staged_bids.is_empty() {
            debug!(count = staged_bids.len(), "Direct bids staged for merge");
            let mut staging = ctx.direct_bid_staging.lock().await;
            staging.extend(staged_bids);
        }

        Ok(())
    }
}
