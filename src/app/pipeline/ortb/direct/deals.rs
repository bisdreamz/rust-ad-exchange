use crate::core::models::campaign::Campaign;
use crate::core::models::deal::Deal;
use ahash::AHashMap;
use std::sync::Arc;

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

    let deals = deal_map.get(&campaign.company_id)?;

    campaign
        .targeting
        .deal_ids
        .iter()
        .find_map(|did| deals.iter().find(|d| d.id == *did))
        .cloned()
}
