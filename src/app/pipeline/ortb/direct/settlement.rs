use crate::core::models::campaign::{Campaign, PricingStrategy};
use crate::core::models::deal::{Deal, DealPricing};

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
        return campaign_price;
    };

    match &deal.pricing {
        DealPricing::Fixed(p) => *p,
        DealPricing::Floor(floor) => campaign_price.max(*floor),
        DealPricing::Inherit => campaign_price,
    }
}
