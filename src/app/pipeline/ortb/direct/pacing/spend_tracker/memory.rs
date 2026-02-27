use crate::app::pipeline::ortb::direct::pacing::SpendTracker;
use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

const MICROS_PER_DOLLAR: f64 = 1_000_000.0;

/// In-memory spend tracker using atomic counters.
/// Campaigns must be registered via `register()` before tracking.
/// Unregistered campaigns return None (fail closed).
/// State is lost on restart.
pub struct InMemorySpendTracker {
    spend: DashMap<String, AtomicU64>,
}

impl InMemorySpendTracker {
    pub fn new() -> Self {
        Self {
            spend: DashMap::new(),
        }
    }
}

impl SpendTracker for InMemorySpendTracker {
    fn total_spend(&self, campaign_id: &str) -> Option<f64> {
        self.spend
            .get(campaign_id)
            .map(|v| v.load(Relaxed) as f64 / MICROS_PER_DOLLAR)
    }

    fn record_spend(&self, campaign_id: &str, amount: f64) {
        if let Some(v) = self.spend.get(campaign_id) {
            v.fetch_add((amount * MICROS_PER_DOLLAR) as u64, Relaxed);
        }
    }

    fn register(&self, campaign_id: &str, initial_spend: f64) {
        let micros = (initial_spend * MICROS_PER_DOLLAR) as u64;
        self.spend
            .entry(campaign_id.to_owned())
            .or_insert_with(|| AtomicU64::new(micros));
    }
}
