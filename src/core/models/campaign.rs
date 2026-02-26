use crate::core::models::common::Status;
use crate::core::models::targeting::CommonTargeting;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Pacing {
    /// Attempt to pace budget evenly
    /// across start and end dates
    Even,
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
    // future campaign specific targeting goes here
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Campaign {
    pub status: Status,
    pub id: String,
    pub start_date: DateTime<Utc>,
    pub end_date: DateTime<Utc>,
    pub name: String,
    pub pacing: Pacing,
    pub budget: f64,
    pub strategy: PricingStrategy,
    pub advertiser_id: String,
    pub targeting: CampaignTargeting,
}
