use serde::{Deserialize, Serialize};

/// An advertiser / brand definition owned by a buyer.
/// e.g. Samsung / samsung.com, Nike / nike.com
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Advertiser {
    pub id: String,
    /// The buyer (company) that owns this advertiser definition
    pub buyer_id: String,
    /// Public brand name e.g. "Samsung"
    pub brand: String,
    /// Advertiser domain, the adomain in rtb, e.g. "samsung.com"
    pub domain: String,
}
