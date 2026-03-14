use crate::core::models::campaign::{Campaign, PricingStrategy};
use crate::core::models::deal::{Deal, DealPricing};
use tracing::trace;

/// Determines the effective bid price from the campaign strategy
/// and optional deal pricing.
///
/// Deal pricing takes precedence:
/// - Fixed: bid price is the deal's fixed price
/// - Floor: campaign strategy price, but no lower than deal floor
/// - Inherit: campaign strategy price alone
pub fn effective_price(campaign: &Campaign, deal: Option<&Deal>) -> f64 {
    let campaign_price = match &campaign.strategy {
        PricingStrategy::FixedPrice(p) => *p,
    };

    let Some(deal) = deal else {
        trace!(campaign = %campaign.id, price = campaign_price, "Price from campaign strategy");
        return campaign_price;
    };

    let price = match &deal.pricing {
        DealPricing::Fixed(p) => *p,
        DealPricing::Floor(floor) => campaign_price.max(*floor),
        DealPricing::Inherit => campaign_price,
    };

    trace!(
        campaign = %campaign.id,
        deal = %deal.id,
        campaign_price = campaign_price,
        deal_pricing = ?deal.pricing,
        effective = price,
        "Price from deal settlement"
    );

    price
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
    use chrono::Utc;

    fn stub_campaign(price: f64) -> Campaign {
        Campaign {
            status: Status::Active,
            buyer_id: "co1".into(),
            id: "c1".into(),
            start_date: Utc::now() - chrono::Duration::hours(1),
            end_date: Utc::now() + chrono::Duration::hours(1),
            name: "test".into(),
            pacing: CampaignPacing::Fast,
            budget: 1000.0,
            budget_type: BudgetType::Total,
            strategy: PricingStrategy::FixedPrice(price),
            advertiser_id: "adv1".into(),
            targeting: CampaignTargeting::default(),
            click_url: None,
            creatives: vec![],
            delivery_state: Default::default(),
        }
    }

    fn stub_deal(pricing: DealPricing) -> Deal {
        Deal {
            status: Status::Active,
            id: "d1".into(),
            name: "test deal".into(),
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

    #[test]
    fn no_deal_returns_campaign_price() {
        let c = stub_campaign(5.0);
        assert_eq!(effective_price(&c, None), 5.0);
    }

    #[test]
    fn fixed_deal_overrides_campaign_price() {
        let c = stub_campaign(5.0);
        let d = stub_deal(DealPricing::Fixed(8.0));
        assert_eq!(effective_price(&c, Some(&d)), 8.0);
    }

    #[test]
    fn floor_above_campaign_uses_floor() {
        let c = stub_campaign(3.0);
        let d = stub_deal(DealPricing::Floor(6.0));
        assert_eq!(effective_price(&c, Some(&d)), 6.0);
    }

    #[test]
    fn floor_below_campaign_uses_campaign() {
        let c = stub_campaign(7.0);
        let d = stub_deal(DealPricing::Floor(4.0));
        assert_eq!(effective_price(&c, Some(&d)), 7.0);
    }

    #[test]
    fn inherit_returns_campaign_price() {
        let c = stub_campaign(5.5);
        let d = stub_deal(DealPricing::Inherit);
        assert_eq!(effective_price(&c, Some(&d)), 5.5);
    }
}
