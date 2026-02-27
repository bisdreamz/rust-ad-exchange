use crate::core::models::common::Status;
use crate::core::models::targeting::CommonTargeting;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum CampaignPacing {
    /// Flat even pacing — equal spend rate every hour
    Even,
    /// Even pacing with smooth daypart curve peaking ~6pm local.
    /// `tz_offset` is hours from UTC (e.g. -5 for EST, +1 for CET).
    WeightedEven { tz_offset: i8 },
    /// Spend as fast as reasonably possible
    Fast,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PricingStrategy {
    FixedPrice(f64),
    // Dynamic (max_price: f64) etc
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CampaignTargeting {
    pub common: CommonTargeting,
    /// Explicit deal IDs this campaign bids through.
    /// Non-empty: only matches via these deals, never open.
    /// Empty: open matching only, no deal association.
    #[serde(default)]
    pub deal_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Campaign {
    pub status: Status,
    /// The owning company ID of this
    /// advertiser within our direct campaign system
    pub company_id: String,
    pub id: String,
    pub start_date: DateTime<Utc>,
    pub end_date: DateTime<Utc>,
    pub name: String,
    pub pacing: CampaignPacing,
    pub budget: f64,
    pub strategy: PricingStrategy,
    pub advertiser_id: String,
    pub targeting: CampaignTargeting,
}
