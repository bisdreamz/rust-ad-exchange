use crate::core::models::common::Status;
use crate::core::models::creative::CreativeFormat;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum FillPolicy {
    DealsOnly,
    DirectAndDeals,
    RtbBackfill,
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
