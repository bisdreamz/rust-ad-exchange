use crate::core::models::campaign::Campaign;
use crate::core::models::creative::Creative;
use crate::core::models::deal::Deal;
use moka::sync::Cache;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;

/// Represents a set of notification urls we may need to
/// invoke for a demand partner
#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct NoticeUrls {
    /// The bid.burl billing notice url if provided
    pub burl: Option<String>,
    /// The bid.lurl loss notice url if provided
    pub lurl: Option<String>,
}

/// Campaign, creative, and deal context attached to a direct bid.
/// Stored on the cache entry so billing events can record
/// campaign counters without encoding IDs in the URL.
#[derive(Clone, Debug)]
pub struct DirectCampaignDetails {
    pub campaign: Arc<Campaign>,
    pub creative: Arc<Creative>,
}

/// Per-bid cache entry combining demand notice URLs with
/// optional direct campaign details and deal context
#[derive(Clone, Debug, Default)]
pub struct CachedBidNotice {
    pub urls: NoticeUrls,
    pub direct: Option<DirectCampaignDetails>,
    /// Deal this bid was matched through, if any.
    /// Present for both direct and RTB bids.
    pub deal: Option<Arc<Deal>>,
}

/// Responsible for caching demand notice URLs
/// such as BURLs
pub struct DemandNotificationsCache {
    cache: Cache<String, CachedBidNotice>,
}

impl DemandNotificationsCache {
    pub fn new(event_ttl: Duration) -> Self {
        DemandNotificationsCache {
            cache: Cache::builder().time_to_live(event_ttl).build(),
        }
    }

    /// Inserts a bid notice entry into the cache under the
    /// provided unique bid event id
    pub fn cache(&self, bid_event_id: &str, notice: CachedBidNotice) {
        self.cache.insert(bid_event_id.to_string(), notice);
    }

    /// Get and remove a bid notice entry from the cache under the
    /// provided bid event id
    pub fn get(&self, bid_event_id: &str) -> Option<CachedBidNotice> {
        self.cache.remove(bid_event_id)
    }
}
