use crate::app::pipeline::ortb::AuctionContext;
use crate::app::pipeline::ortb::targeting::matches_targeting;
use crate::core::models::campaign::Campaign;
use crate::core::models::deal::{Deal, DemandPolicy};
use crate::core::models::placement::FillPolicy;
use ahash::AHashMap;
use chrono::Utc;
use rtb::bid_request::Imp;
use std::sync::Arc;
use tracing::warn;

use super::deals;
use super::pacing::{DealPacer, SpendPacer};
use super::settlement;

/// Safety cap on direct bid price. Any bid above this is
/// skipped and logged — protects against misconfigured
/// campaigns or deal pricing
const MAX_BID_PRICE: f64 = 100.0;

/// A campaign that passed targeting, deal resolution, pricing,
/// and pacing — ready for winner selection
pub struct CampaignCandidate {
    pub campaign: Arc<Campaign>,
    pub deal: Option<Arc<Deal>>,
    pub price: f64,
}

pub struct MatchResult {
    pub candidates: Vec<CampaignCandidate>,
}

impl MatchResult {
    pub fn highest_price(self) -> Option<CampaignCandidate> {
        self.candidates.into_iter().max_by(|a, b| {
            a.price
                .partial_cmp(&b.price)
                .unwrap_or(std::cmp::Ordering::Equal)
        })
    }
}

/// Matches direct deals and campaigns against a single imp.
///
/// Order of operations:
/// 1. Evaluate direct deals (small set) against imp targeting
/// 2. Build company_id → matched deals index
/// 3. Single pass over campaigns — targeting, deal resolution,
///    price computation, and pacing checked per candidate.
///    DealsOnly skips the full scan and only checks deal-company campaigns.
pub fn match_imp(
    direct_deals: &[Arc<Deal>],
    all_campaigns: &[Arc<Campaign>],
    campaigns_by_company: &dyn Fn(&str) -> Vec<Arc<Campaign>>,
    fill_policy: &FillPolicy,
    pacer: &dyn SpendPacer,
    deal_pacer: &dyn DealPacer,
    ctx: &AuctionContext,
    imp: &Imp,
) -> MatchResult {
    let now = Utc::now();
    let deal_map = build_deal_map(direct_deals, deal_pacer, ctx, imp);

    let candidates = match fill_policy {
        FillPolicy::DealsOnly => {
            match_deal_only(&deal_map, campaigns_by_company, pacer, &now, ctx, imp)
        }
        _ => match_all(all_campaigns, &deal_map, pacer, &now, ctx, imp),
    };

    MatchResult { candidates }
}

/// Evaluates direct deals against imp targeting, returns a map of
/// company_id → matched deals for downstream resolution
fn build_deal_map(
    deals: &[Arc<Deal>],
    deal_pacer: &dyn DealPacer,
    ctx: &AuctionContext,
    imp: &Imp,
) -> AHashMap<String, Vec<Arc<Deal>>> {
    let mut deal_map: AHashMap<String, Vec<Arc<Deal>>> = AHashMap::new();

    for deal in deals {
        if !matches_targeting(&deal.targeting.common, ctx, imp) {
            continue;
        }

        if !deal_pacer.passes(deal) {
            continue; // deal exhausted or rate-limited
        }

        let company_ids = match &deal.policy {
            DemandPolicy::Direct { company_ids } => company_ids,
            _ => continue,
        };

        for company_id in company_ids {
            deal_map
                .entry(company_id.clone())
                .or_default()
                .push(Arc::clone(deal));
        }
    }

    deal_map
}

/// DealsOnly: only evaluate campaigns belonging to companies
/// with matched deals whose deal_ids reference a matched deal.
fn match_deal_only(
    deal_map: &AHashMap<String, Vec<Arc<Deal>>>,
    campaigns_by_company: &dyn Fn(&str) -> Vec<Arc<Campaign>>,
    pacer: &dyn SpendPacer,
    now: &chrono::DateTime<Utc>,
    ctx: &AuctionContext,
    imp: &Imp,
) -> Vec<CampaignCandidate> {
    let mut candidates = Vec::new();

    for (company_id, matched_deals) in deal_map {
        for campaign in campaigns_by_company(company_id) {
            if !has_matching_deal_id(&campaign, matched_deals) {
                continue;
            }

            if let Some(candidate) = evaluate_campaign(campaign, deal_map, pacer, now, ctx, imp) {
                candidates.push(candidate);
            }
        }
    }

    candidates
}

/// Single pass over all campaigns. Each campaign's targeting
/// is evaluated exactly once.
/// - Campaigns with deal_ids: only included if matching deals exist
/// - Campaigns with empty deal_ids: open matching, always considered
fn match_all(
    all_campaigns: &[Arc<Campaign>],
    deal_map: &AHashMap<String, Vec<Arc<Deal>>>,
    pacer: &dyn SpendPacer,
    now: &chrono::DateTime<Utc>,
    ctx: &AuctionContext,
    imp: &Imp,
) -> Vec<CampaignCandidate> {
    let mut candidates = Vec::new();

    for campaign in all_campaigns {
        if !campaign.targeting.deal_ids.is_empty() {
            let Some(deals) = deal_map.get(&campaign.company_id) else {
                continue;
            };

            if !has_matching_deal_id(campaign, deals) {
                continue;
            }
        }

        if let Some(candidate) =
            evaluate_campaign(Arc::clone(campaign), deal_map, pacer, now, ctx, imp)
        {
            candidates.push(candidate);
        }
    }

    candidates
}

/// Full evaluation of a single campaign: targeting → deal resolution
/// → price → pacing. Returns None if any check fails.
fn evaluate_campaign(
    campaign: Arc<Campaign>,
    deal_map: &AHashMap<String, Vec<Arc<Deal>>>,
    pacer: &dyn SpendPacer,
    now: &chrono::DateTime<Utc>,
    ctx: &AuctionContext,
    imp: &Imp,
) -> Option<CampaignCandidate> {
    if !is_campaign_eligible(&campaign, now, ctx, imp) {
        return None;
    }

    let deal = deals::resolve(&campaign, deal_map);
    let price = settlement::effective_price(&campaign, deal.as_deref());

    if price > MAX_BID_PRICE {
        warn!(
            "direct campaign {} bid {:.2} exceeds safety cap {}",
            campaign.id, price, MAX_BID_PRICE
        );
        return None;
    }

    if !pacer.passes(&campaign, price) {
        return None;
    }

    Some(CampaignCandidate {
        campaign,
        deal,
        price,
    })
}

/// Checks that at least one of the campaign's targeted deal_ids
/// appears in the matched deals list
fn has_matching_deal_id(campaign: &Campaign, matched_deals: &[Arc<Deal>]) -> bool {
    campaign
        .targeting
        .deal_ids
        .iter()
        .any(|did| matched_deals.iter().any(|d| d.id == *did))
}

fn is_campaign_eligible(
    campaign: &Campaign,
    now: &chrono::DateTime<Utc>,
    ctx: &AuctionContext,
    imp: &Imp,
) -> bool {
    *now >= campaign.start_date
        && *now <= campaign.end_date
        && matches_targeting(&campaign.targeting.common, ctx, imp)
}
