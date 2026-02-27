use crate::core::models::common::Status;
use crate::core::models::creative::CreativeFormat;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum FillPolicy {
    /// Only deal-backed direct campaigns. No open campaigns, no RTB.
    DealsOnly,
    /// All direct campaigns (open + deal-backed). No RTB.
    DirectOnly,
    /// All direct campaigns + RTB deals. No open RTB.
    DirectAndRtbDeals,
    /// Direct fills first, full RTB backfills remaining imps.
    RtbBackfill,
    /// All sources compete on price.
    HighestPrice,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Placement {
    pub id: String,
    pub pub_id: String,
    pub property_id: String,
    pub status: Status,
    pub name: String,
    pub fill_policy: FillPolicy,
    pub ad_units: Vec<CreativeFormat>,
}
