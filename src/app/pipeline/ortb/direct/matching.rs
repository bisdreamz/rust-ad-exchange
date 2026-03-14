use crate::app::pipeline::ortb::AuctionContext;
use crate::app::pipeline::ortb::targeting::matches_targeting;
use crate::core::models::campaign::Campaign;
use crate::core::models::deal::{Deal, DemandPolicy};
use crate::core::models::placement::FillPolicy;
use ahash::AHashMap;
use chrono::Utc;
use rtb::bid_request::Imp;
use std::sync::Arc;
use tracing::{Level, debug, enabled, trace, warn};

use super::deals;
use super::pacing::DealPacer;
use super::settlement;

/// Safety cap on direct bid price. Any bid above this is
/// skipped and logged — protects against misconfigured
/// campaigns or deal pricing
const MAX_BID_PRICE: f64 = 100.0;

/// Exponent for price-weighted random selection among candidates.
///
/// Controls how aggressively higher prices are favored:
/// - k=1 (linear): $5 vs $10 → 33%/67%. Too flat.
/// - k=2 (squared): $5 vs $10 → 20%/80%. Good balance.
/// - k=3 (cubed): $5 vs $10 → 11%/89%. Very aggressive.
///
/// At k=2, nearly equal prices get nearly equal exposure ($9 vs $10 →
/// 45%/55%), while large gaps strongly favor the higher price ($1 vs $10
/// → 1%/99%). Revenue impact is modest since selection is proportional
/// to price². A lone candidate (e.g. PG deal) always wins regardless.
const PRICE_WEIGHT_EXPONENT: f64 = 2.0;

/// A campaign that passed targeting, deal resolution, and pricing.
/// Not yet spend-paced or creative-matched — the caller selects
/// via price-weighted random, then checks pacer and creative.
pub struct CampaignCandidate {
    pub campaign: Arc<Campaign>,
    pub deal: Option<Arc<Deal>>,
    pub price: f64,
}

/// Unpaced candidates with price-weighted random ordering.
/// Caller draws from them: each draw picks a candidate weighted by
/// `price^PRICE_WEIGHT_EXPONENT`, then checks creative + pacer.
/// If a candidate fails, it's removed and the next draw excludes it.
pub struct MatchResult {
    pub candidates: Vec<CampaignCandidate>,
}

/// Matches direct deals and campaigns against a single imp.
///
/// Returns unpaced candidates sorted by price descending.
/// The caller is responsible for walking them in order and
/// applying creative selection + spend pacing (side effects).
///
/// Order of operations:
/// 1. Evaluate direct deals (small set) against imp targeting + deal pacing
/// 2. Build buyer_id → matched deals index
/// 3. Single pass over campaigns — targeting, deal resolution,
///    price computation checked per candidate. No side effects.
///    DealsOnly skips the full scan and only checks deal-buyer campaigns.
/// 4. Sort candidates by price descending
pub fn match_imp(
    direct_deals: &[Arc<Deal>],
    all_campaigns: &[Arc<Campaign>],
    campaigns_by_buyer: &dyn Fn(&str) -> Vec<Arc<Campaign>>,
    fill_policy: &FillPolicy,
    deal_pacer: &dyn DealPacer,
    ctx: &AuctionContext,
    imp: &Imp,
) -> MatchResult {
    let now = Utc::now();
    let deal_map = build_deal_map(direct_deals, deal_pacer, ctx, imp);

    let mut candidates = match fill_policy {
        FillPolicy::DealsOnly => match_deal_only(&deal_map, campaigns_by_buyer, &now, ctx, imp),
        _ => match_all(all_campaigns, &deal_map, &now, ctx, imp),
    };

    // Shuffle for tie-breaking among equal-weight candidates.
    // Weighted selection happens at draw time via `draw_weighted`.
    fastrand::shuffle(&mut candidates);

    if enabled!(Level::DEBUG) {
        let deal_backed = candidates.iter().filter(|c| c.deal.is_some()).count();
        debug!(
            candidates = candidates.len(),
            deal_backed = deal_backed,
            open = candidates.len() - deal_backed,
            deals_matched = deal_map.len(),
            fill_policy = ?fill_policy,
            "Imp matching summary"
        );
    }

    MatchResult { candidates }
}

/// Pick a candidate from the pool weighted by `price^PRICE_WEIGHT_EXPONENT`.
/// Returns the index of the selected candidate, or `None` if the pool is empty.
///
/// Uses a single random draw across cumulative weights. O(n) per draw,
/// which is fine — candidate pools are small (typically <20).
pub fn draw_weighted(candidates: &[CampaignCandidate]) -> Option<usize> {
    if candidates.is_empty() {
        return None;
    }
    if candidates.len() == 1 {
        return Some(0);
    }

    let total: f64 = candidates
        .iter()
        .map(|c| c.price.powf(PRICE_WEIGHT_EXPONENT))
        .sum();

    if total <= 0.0 {
        return Some(fastrand::usize(..candidates.len()));
    }

    let roll = fastrand::f64() * total;
    let mut cumulative = 0.0;
    for (i, c) in candidates.iter().enumerate() {
        cumulative += c.price.powf(PRICE_WEIGHT_EXPONENT);
        if roll < cumulative {
            return Some(i);
        }
    }

    // Floating-point edge case — return last
    Some(candidates.len() - 1)
}

/// Evaluates direct deals against imp targeting, returns a map of
/// buyer_id → matched deals for downstream resolution
fn build_deal_map(
    deals: &[Arc<Deal>],
    deal_pacer: &dyn DealPacer,
    ctx: &AuctionContext,
    imp: &Imp,
) -> AHashMap<String, Vec<Arc<Deal>>> {
    let mut deal_map: AHashMap<String, Vec<Arc<Deal>>> = AHashMap::new();

    for deal in deals {
        if !matches_targeting(&deal.targeting.common, ctx, imp) {
            trace!(deal = %deal.id, "Deal didn't pass targeting");
            continue;
        }

        if !deal_pacer.passes(deal) {
            trace!(deal = %deal.id, "Deal didn't pass impression pacing");
            continue;
        }

        let buyer_ids = match &deal.policy {
            DemandPolicy::Direct { buyer_ids } => buyer_ids,
            _ => continue,
        };

        trace!(deal = %deal.id, buyers = buyer_ids.len(), "Deal matched");

        for buyer_id in buyer_ids {
            deal_map
                .entry(buyer_id.clone())
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
    campaigns_by_buyer: &dyn Fn(&str) -> Vec<Arc<Campaign>>,
    now: &chrono::DateTime<Utc>,
    ctx: &AuctionContext,
    imp: &Imp,
) -> Vec<CampaignCandidate> {
    let mut candidates = Vec::new();

    for (buyer_id, matched_deals) in deal_map {
        for campaign in campaigns_by_buyer(buyer_id) {
            if !has_matching_deal_id(&campaign, matched_deals) {
                continue;
            }

            if let Some(candidate) = evaluate_campaign(campaign, deal_map, now, ctx, imp) {
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
    now: &chrono::DateTime<Utc>,
    ctx: &AuctionContext,
    imp: &Imp,
) -> Vec<CampaignCandidate> {
    let mut candidates = Vec::new();

    for campaign in all_campaigns {
        if !campaign.targeting.deal_ids.is_empty() {
            let Some(deals) = deal_map.get(&campaign.buyer_id) else {
                continue;
            };

            if !has_matching_deal_id(campaign, deals) {
                continue;
            }
        }

        if let Some(candidate) = evaluate_campaign(Arc::clone(campaign), deal_map, now, ctx, imp) {
            candidates.push(candidate);
        }
    }

    candidates
}

/// Campaign evaluation: targeting → deal resolution → price → safety cap.
/// No side effects — does NOT call spend pacer.
fn evaluate_campaign(
    campaign: Arc<Campaign>,
    deal_map: &AHashMap<String, Vec<Arc<Deal>>>,
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

    trace!(
        campaign = %campaign.id,
        price = price,
        deal = deal.as_ref().map(|d| d.id.as_str()).unwrap_or("none"),
        "Campaign eligible"
    );

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
    if *now < campaign.start_date || *now > campaign.end_date {
        trace!(
            campaign = %campaign.id,
            start = %campaign.start_date,
            end = %campaign.end_date,
            "Campaign outside flight dates"
        );
        return false;
    }

    if !matches_targeting(&campaign.targeting.common, ctx, imp) {
        trace!(campaign = %campaign.id, "Campaign didn't pass targeting");
        return false;
    }

    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::models::campaign::{
        BudgetType, Campaign, CampaignPacing, CampaignTargeting, PricingStrategy,
    };
    use crate::core::models::common::Status;
    use crate::core::models::deal::{Deal, DealOwner, DealPricing, DealTargeting, DemandPolicy};
    use crate::core::models::targeting::CommonTargeting;
    use rtb::BidRequestBuilder;
    use rtb::bid_request::ImpBuilder;

    /// Stub DealPacer: always passes
    struct AllPassDealPacer;
    impl DealPacer for AllPassDealPacer {
        fn passes(&self, _deal: &Deal) -> bool {
            true
        }
        fn record_impression(&self, _deal_id: &str) {}
    }

    /// Stub DealPacer: always rejects
    struct RejectDealPacer;
    impl DealPacer for RejectDealPacer {
        fn passes(&self, _deal: &Deal) -> bool {
            false
        }
        fn record_impression(&self, _deal_id: &str) {}
    }

    fn stub_campaign_open(id: &str, price: f64) -> Campaign {
        Campaign {
            status: Status::Active,
            buyer_id: "co1".into(),
            id: id.into(),
            start_date: Utc::now() - chrono::Duration::hours(1),
            end_date: Utc::now() + chrono::Duration::hours(1),
            name: format!("campaign_{id}"),
            pacing: CampaignPacing::Fast,
            budget: 1000.0,
            budget_type: BudgetType::Total,
            strategy: PricingStrategy::FixedPrice(price),
            advertiser_id: format!("adv_{id}"),
            targeting: CampaignTargeting::default(),
            click_url: None,
            creatives: vec![],
            delivery_state: Default::default(),
        }
    }

    fn stub_campaign_deal(id: &str, price: f64, deal_ids: Vec<String>) -> Campaign {
        Campaign {
            targeting: CampaignTargeting {
                common: CommonTargeting::default(),
                deal_ids,
            },
            ..stub_campaign_open(id, price)
        }
    }

    fn stub_deal(id: &str, pricing: DealPricing) -> Deal {
        Deal {
            status: Status::Active,
            id: id.into(),
            name: format!("deal_{id}"),
            policy: DemandPolicy::Direct {
                buyer_ids: vec!["co1".into()],
            },
            owner: DealOwner::Platform,
            pricing,
            targeting: DealTargeting {
                common: CommonTargeting::default(),
            },
            start_date: None,
            end_date: None,
            delivery_goal: None,
            pacing: None,
            takes_priority: false,
            delivery_state: Default::default(),
        }
    }

    fn default_ctx() -> AuctionContext {
        let req = BidRequestBuilder::default()
            .id("test".to_string())
            .imp(vec![
                ImpBuilder::default()
                    .id("imp1".to_string())
                    .build()
                    .unwrap(),
            ])
            .build()
            .unwrap();
        let mut ctx = AuctionContext::test_default("pub1");
        *ctx.req.get_mut() = req;
        ctx
    }

    fn default_imp() -> Imp {
        ImpBuilder::default()
            .id("imp1".to_string())
            .build()
            .unwrap()
    }

    fn noop_by_buyer(_buyer_id: &str) -> Vec<Arc<Campaign>> {
        vec![]
    }

    #[test]
    fn no_campaigns_returns_empty() {
        let ctx = default_ctx();
        let imp = default_imp();
        let result = match_imp(
            &[],
            &[],
            &noop_by_buyer,
            &FillPolicy::HighestPrice,
            &AllPassDealPacer,
            &ctx,
            &imp,
        );
        assert!(result.candidates.is_empty());
    }

    #[test]
    fn open_campaign_becomes_candidate_with_campaign_price() {
        let c = Arc::new(stub_campaign_open("c1", 5.0));
        let ctx = default_ctx();
        let imp = default_imp();
        let result = match_imp(
            &[],
            &[c],
            &noop_by_buyer,
            &FillPolicy::HighestPrice,
            &AllPassDealPacer,
            &ctx,
            &imp,
        );
        assert_eq!(result.candidates.len(), 1);
        assert!((result.candidates[0].price - 5.0).abs() < 0.01);
        assert!(result.candidates[0].deal.is_none());
    }

    #[test]
    fn deal_backed_campaign_with_matching_deal() {
        let deal = Arc::new(stub_deal("d1", DealPricing::Fixed(8.0)));
        let c = Arc::new(stub_campaign_deal("c1", 5.0, vec!["d1".into()]));
        let ctx = default_ctx();
        let imp = default_imp();
        let result = match_imp(
            &[deal],
            &[c],
            &noop_by_buyer,
            &FillPolicy::HighestPrice,
            &AllPassDealPacer,
            &ctx,
            &imp,
        );
        assert_eq!(result.candidates.len(), 1);
        assert!((result.candidates[0].price - 8.0).abs() < 0.01);
        assert!(result.candidates[0].deal.is_some());
    }

    #[test]
    fn deal_backed_campaign_no_matching_deal_excluded() {
        // Campaign targets deal "d_other" but only "d1" exists
        let deal = Arc::new(stub_deal("d1", DealPricing::Fixed(8.0)));
        let c = Arc::new(stub_campaign_deal("c1", 5.0, vec!["d_other".into()]));
        let ctx = default_ctx();
        let imp = default_imp();
        let result = match_imp(
            &[deal],
            &[c],
            &noop_by_buyer,
            &FillPolicy::HighestPrice,
            &AllPassDealPacer,
            &ctx,
            &imp,
        );
        assert!(result.candidates.is_empty());
    }

    #[test]
    fn deals_only_skips_open_campaigns() {
        let deal = Arc::new(stub_deal("d1", DealPricing::Inherit));
        let open = Arc::new(stub_campaign_open("c_open", 10.0));
        let deal_backed = Arc::new(stub_campaign_deal("c_deal", 5.0, vec!["d1".into()]));

        let all_campaigns = vec![open, deal_backed.clone()];
        let by_buyer = |_buyer_id: &str| -> Vec<Arc<Campaign>> { vec![deal_backed.clone()] };
        let ctx = default_ctx();
        let imp = default_imp();

        let result = match_imp(
            &[deal],
            &all_campaigns,
            &by_buyer,
            &FillPolicy::DealsOnly,
            &AllPassDealPacer,
            &ctx,
            &imp,
        );
        // Only the deal-backed campaign should appear
        assert_eq!(result.candidates.len(), 1);
        assert_eq!(result.candidates[0].campaign.id, "c_deal");
    }

    #[test]
    fn direct_only_includes_open_and_deal_backed() {
        let deal = Arc::new(stub_deal("d1", DealPricing::Inherit));
        let open = Arc::new(stub_campaign_open("c_open", 5.0));
        let deal_backed = Arc::new(stub_campaign_deal("c_deal", 5.0, vec!["d1".into()]));

        let ctx = default_ctx();
        let imp = default_imp();
        let result = match_imp(
            &[deal],
            &[open, deal_backed],
            &noop_by_buyer,
            &FillPolicy::DirectOnly,
            &AllPassDealPacer,
            &ctx,
            &imp,
        );
        assert_eq!(result.candidates.len(), 2);
    }

    #[test]
    fn direct_and_rtb_deals_includes_open_and_deal_backed() {
        let deal = Arc::new(stub_deal("d1", DealPricing::Inherit));
        let open = Arc::new(stub_campaign_open("c_open", 5.0));
        let deal_backed = Arc::new(stub_campaign_deal("c_deal", 5.0, vec!["d1".into()]));

        let ctx = default_ctx();
        let imp = default_imp();
        let result = match_imp(
            &[deal],
            &[open, deal_backed],
            &noop_by_buyer,
            &FillPolicy::DirectAndRtbDeals,
            &AllPassDealPacer,
            &ctx,
            &imp,
        );
        assert_eq!(result.candidates.len(), 2);
    }

    #[test]
    fn deal_pacer_rejects_excludes_deal() {
        let deal = Arc::new(stub_deal("d1", DealPricing::Fixed(8.0)));
        let c = Arc::new(stub_campaign_deal("c1", 5.0, vec!["d1".into()]));
        let ctx = default_ctx();
        let imp = default_imp();

        let result = match_imp(
            &[deal],
            &[c],
            &noop_by_buyer,
            &FillPolicy::HighestPrice,
            &RejectDealPacer,
            &ctx,
            &imp,
        );
        // Deal rejected by pacer, campaign has deal_ids so excluded
        assert!(result.candidates.is_empty());
    }

    #[test]
    fn outside_flight_excluded() {
        let mut c = stub_campaign_open("c1", 5.0);
        // Set flight to the past
        c.start_date = Utc::now() - chrono::Duration::hours(10);
        c.end_date = Utc::now() - chrono::Duration::hours(5);
        let c = Arc::new(c);

        let ctx = default_ctx();
        let imp = default_imp();
        let result = match_imp(
            &[],
            &[c],
            &noop_by_buyer,
            &FillPolicy::HighestPrice,
            &AllPassDealPacer,
            &ctx,
            &imp,
        );
        assert!(result.candidates.is_empty());
    }

    #[test]
    fn all_eligible_become_candidates() {
        let c_low = Arc::new(stub_campaign_open("c_low", 2.0));
        let c_high = Arc::new(stub_campaign_open("c_high", 9.0));
        let c_mid = Arc::new(stub_campaign_open("c_mid", 5.0));

        let ctx = default_ctx();
        let imp = default_imp();
        let result = match_imp(
            &[],
            &[c_low, c_high, c_mid],
            &noop_by_buyer,
            &FillPolicy::HighestPrice,
            &AllPassDealPacer,
            &ctx,
            &imp,
        );
        assert_eq!(result.candidates.len(), 3);
    }

    #[test]
    fn safety_cap_excludes_high_price() {
        // MAX_BID_PRICE is 100.0
        let c = Arc::new(stub_campaign_open("c1", 150.0));
        let ctx = default_ctx();
        let imp = default_imp();
        let result = match_imp(
            &[],
            &[c],
            &noop_by_buyer,
            &FillPolicy::HighestPrice,
            &AllPassDealPacer,
            &ctx,
            &imp,
        );
        assert!(result.candidates.is_empty());
    }

    // ---- draw_weighted tests ----

    #[test]
    fn draw_weighted_empty_returns_none() {
        assert!(draw_weighted(&[]).is_none());
    }

    #[test]
    fn draw_weighted_single_always_returns_zero() {
        let candidates = vec![CampaignCandidate {
            campaign: Arc::new(stub_campaign_open("c1", 5.0)),
            deal: None,
            price: 5.0,
        }];
        for _ in 0..100 {
            assert_eq!(draw_weighted(&candidates), Some(0));
        }
    }

    /// Over many draws, the $10 candidate should win ~80% (weight 100)
    /// vs the $5 candidate at ~20% (weight 25). With k=2: 100/(100+25) = 80%.
    #[test]
    fn draw_weighted_favors_higher_price() {
        let candidates = vec![
            CampaignCandidate {
                campaign: Arc::new(stub_campaign_open("c_high", 10.0)),
                deal: None,
                price: 10.0,
            },
            CampaignCandidate {
                campaign: Arc::new(stub_campaign_open("c_low", 5.0)),
                deal: None,
                price: 5.0,
            },
        ];

        let mut high_wins = 0u32;
        let rounds = 10_000;
        for _ in 0..rounds {
            if draw_weighted(&candidates) == Some(0) {
                high_wins += 1;
            }
        }

        let high_pct = high_wins as f64 / rounds as f64;
        // Expected ~80% (100/125). Allow ±5% for randomness.
        assert!(
            high_pct > 0.74 && high_pct < 0.86,
            "$10 should win ~80% of draws, got {:.1}%",
            high_pct * 100.0,
        );
    }

    /// Near-equal prices should produce near-equal selection rates.
    /// $9 vs $10: weights 81 vs 100 → 55%/45%.
    #[test]
    fn draw_weighted_near_equal_prices() {
        let candidates = vec![
            CampaignCandidate {
                campaign: Arc::new(stub_campaign_open("c10", 10.0)),
                deal: None,
                price: 10.0,
            },
            CampaignCandidate {
                campaign: Arc::new(stub_campaign_open("c9", 9.0)),
                deal: None,
                price: 9.0,
            },
        ];

        let mut c10_wins = 0u32;
        let rounds = 10_000;
        for _ in 0..rounds {
            if draw_weighted(&candidates) == Some(0) {
                c10_wins += 1;
            }
        }

        let pct = c10_wins as f64 / rounds as f64;
        // Expected ~55% (100/181). Allow ±5%.
        assert!(
            pct > 0.49 && pct < 0.61,
            "$10 vs $9 should be ~55%/45%, got {:.1}%",
            pct * 100.0,
        );
    }
}
