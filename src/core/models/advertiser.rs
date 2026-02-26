use serde::{Deserialize, Serialize};

/// Advertiser attached to a campaign or creative
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Advertiser {
    pub id: String,
    /// Public brand name e.g. "Samsung"
    pub brand: String,
    /// Advertiser domain, the adomain in rtb, e.g. "samsung.com"
    pub domain: String,
}
