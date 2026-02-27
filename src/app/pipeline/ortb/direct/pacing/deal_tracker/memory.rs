use crate::app::pipeline::ortb::direct::pacing::DealImpressionTracker;
use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

/// In-memory deal impression tracker using atomic counters.
/// Deals must be registered via `register()` before tracking.
/// Unregistered deals return None (fail closed).
/// State is lost on restart.
pub struct InMemoryDealTracker {
    impressions: DashMap<String, AtomicU64>,
}

impl InMemoryDealTracker {
    pub fn new() -> Self {
        Self {
            impressions: DashMap::new(),
        }
    }
}

impl DealImpressionTracker for InMemoryDealTracker {
    fn total_impressions(&self, deal_id: &str) -> Option<u64> {
        self.impressions
            .get(deal_id)
            .map(|v| v.load(Relaxed))
    }

    fn record_impression(&self, deal_id: &str) {
        if let Some(v) = self.impressions.get(deal_id) {
            v.fetch_add(1, Relaxed);
        }
    }

    fn register(&self, deal_id: &str, initial_impressions: u64) {
        self.impressions
            .entry(deal_id.to_owned())
            .or_insert_with(|| AtomicU64::new(initial_impressions));
    }
}
