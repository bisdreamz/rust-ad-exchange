use moka::sync::Cache;
use serde::{Deserialize, Serialize};
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

/// Responsible for caching demand notice URLs
/// such as BURLs
pub struct DemandNotificationsCache {
    cache: Cache<String, NoticeUrls>,
}

impl DemandNotificationsCache {
    pub fn new(event_ttl: Duration) -> Self {
        DemandNotificationsCache {
            cache: Cache::builder().time_to_live(event_ttl).build(),
        }
    }

    /// Inserts the ['NoticeUrls'] into the cache under the
    /// provided unique bid event id
    pub fn cache(&self, bid_event_id: &String, urls: NoticeUrls) {
        self.cache.insert(bid_event_id.to_string(), urls);
    }

    /// Get and remove a ['NoticeUrls'] entry from the cache under the
    /// provided bid event id
    pub fn get(&self, bid_event_id: &str) -> Option<NoticeUrls> {
        self.cache.remove(bid_event_id)
    }
}
