use crate::core::models::common::Status;
use crate::core::models::targeting::CommonTargeting;
use serde::{Deserialize, Serialize};

/// Policy for who the deal is sent to
/// or allowed to bid. Primarily split between
/// targeting specific buyer(s) direct on platform
/// OR specific bidder(s) and optional wseats
/// on remote RTB partners
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DemandPolicy {
    /// Direct advertisers by seat id (internal company id)
    /// Only allows bidding by the selected on-platform
    /// direct advertisers, no RTB exposure. Used if
    /// a publishers tag is deal-only for sales exposure.
    Direct { seat_ids: Vec<String> },
    /// Deal sent to RTB specific buyer(s) of
    /// dsp id (company id)-> weat(s) (the wseat
    /// value from the dsp). wseats is optional
    /// if private=false, but required if private=true
    Rtb {
        wdsps: Vec<(String, Vec<String>)>,
        private: bool,
    },
}

/// Who created/owns/manages a deal
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum DealOwner {
    /// Publisher created and specfic deal.
    /// Inventory included should only belong to
    /// associated pub id
    Publisher { id: String },
    // Buyer(id String) if we want buyer self-curated deals?
    /// A platform wide (admin) generated deal
    /// (no attached id)
    Platform,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DealPricing {
    Inherit,
    Floor(f64),
    Fixed(f64),
}

/// Deal targeting object which houses
/// the ['CommonTargeting'] as well as
/// deal specific targeting fields
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DealTargeting {
    pub common: CommonTargeting,
    // future deal specific targeting goes here
}

/// Deal - a collection of inventory
/// matching specific criteria for pre selected
/// targeting. Can be targeted by campaigns
/// directly on platform, or communicated
/// over OpenRTB for remote bidding
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Deal {
    /// Deal active status
    pub status: Status,
    /// Deal id as targeted in the platform
    /// or as sent in the deal.id value over OpenRTB.
    /// E.g. domain_car_shoppers
    pub id: String,
    /// A human reference deal name, which
    /// advertisers may be able to see
    /// in the platform. e.g. "Summer car shoppers"
    pub name: String,
    /// Policy on what advertising source this deal
    /// is sent to and is allowed to spend on.
    /// See ['DemandPolicy'] which splits primarily
    /// between direct on-platform campaigns,
    /// and remote RTB buyers
    pub policy: DemandPolicy,
    /// The owner of this deal, since they may be
    /// created by admins (such as SSP wide auction packages),
    /// by publishers (for their own inventory to buyers), etc
    pub owner: DealOwner,
    /// The pricing strategy for the deal such as floor behavior
    /// or fixed price
    pub pricing: DealPricing,
    /// The targeting criteria for deal matching
    pub targeting: DealTargeting,
}
