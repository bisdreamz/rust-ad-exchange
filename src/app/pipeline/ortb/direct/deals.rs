use crate::core::models::campaign::Campaign;
use crate::core::models::deal::Deal;
use ahash::AHashMap;
use std::sync::Arc;
use tracing::trace;

/// Resolves which deal applies for a winning campaign.
/// If the campaign targets multiple deals and more than one matched,
/// picks the first match (order from deal_map is arbitrary).
/// Returns None if the campaign has no deal_ids (open match).
pub fn resolve(
    campaign: &Campaign,
    deal_map: &AHashMap<String, Vec<Arc<Deal>>>,
) -> Option<Arc<Deal>> {
    if campaign.targeting.deal_ids.is_empty() {
        return None;
    }

    let deals = deal_map.get(&campaign.buyer_id)?;

    let result = campaign
        .targeting
        .deal_ids
        .iter()
        .find_map(|did| deals.iter().find(|d| d.id == *did))
        .cloned();

    match &result {
        Some(deal) => trace!(campaign = %campaign.id, deal = %deal.id, "Deal resolved"),
        None => trace!(campaign = %campaign.id, "No matching deal found"),
    }

    result
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

    fn stub_campaign(deal_ids: Vec<String>) -> Campaign {
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
            strategy: PricingStrategy::FixedPrice(5.0),
            advertiser_id: "adv1".into(),
            targeting: CampaignTargeting {
                common: CommonTargeting::default(),
                deal_ids,
            },
            click_url: None,
            creatives: vec![],
            delivery_state: Default::default(),
        }
    }

    fn stub_deal(id: &str) -> Deal {
        Deal {
            status: Status::Active,
            id: id.into(),
            name: "test deal".into(),
            policy: DemandPolicy::Direct {
                buyer_ids: vec!["co1".into()],
            },
            owner: DealOwner::Platform,
            pricing: DealPricing::Inherit,
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
    fn empty_deal_ids_returns_none() {
        let c = stub_campaign(vec![]);
        let map = AHashMap::new();
        assert!(resolve(&c, &map).is_none());
    }

    #[test]
    fn matching_deal_returns_some() {
        let c = stub_campaign(vec!["d1".into()]);
        let d = Arc::new(stub_deal("d1"));
        let mut map = AHashMap::new();
        map.insert("co1".to_owned(), vec![d.clone()]);
        let result = resolve(&c, &map);
        assert!(result.is_some());
        assert_eq!(result.unwrap().id, "d1");
    }

    #[test]
    fn no_matching_deal_returns_none() {
        let c = stub_campaign(vec!["d_other".into()]);
        let d = Arc::new(stub_deal("d1"));
        let mut map = AHashMap::new();
        map.insert("co1".to_owned(), vec![d]);
        assert!(resolve(&c, &map).is_none());
    }

    #[test]
    fn company_not_in_map_returns_none() {
        let c = stub_campaign(vec!["d1".into()]);
        let map = AHashMap::new();
        assert!(resolve(&c, &map).is_none());
    }
}
