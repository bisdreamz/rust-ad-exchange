use crate::app::pipeline::ortb::direct::pacing::SpendTracker;
use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

const MICROS_PER_DOLLAR: f64 = 1_000_000.0;

/// In-memory spend tracker using atomic counters.
/// Campaigns are auto-inserted on first `record_spend`.
/// Unknown campaigns return 0.0 from `total_spend`.
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
    fn total_spend(&self, campaign_id: &str) -> f64 {
        self.spend
            .get(campaign_id)
            .map(|v| v.load(Relaxed) as f64 / MICROS_PER_DOLLAR)
            .unwrap_or(0.0)
    }

    fn record_spend(&self, campaign_id: &str, amount: f64) {
        let micros = (amount * MICROS_PER_DOLLAR) as u64;
        self.spend
            .entry(campaign_id.to_owned())
            .or_insert_with(|| AtomicU64::new(0))
            .fetch_add(micros, Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_campaign_returns_zero() {
        let tracker = InMemorySpendTracker::new();
        assert!((tracker.total_spend("unknown") - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn auto_insert_on_record() {
        let tracker = InMemorySpendTracker::new();
        tracker.record_spend("c1", 1.50);
        let spend = tracker.total_spend("c1");
        assert!((spend - 1.50).abs() < 0.01, "expected ~1.50, got {spend}");
    }

    #[test]
    fn multiple_records_accumulate() {
        let tracker = InMemorySpendTracker::new();
        tracker.record_spend("c1", 2.0);
        tracker.record_spend("c1", 3.0);
        let spend = tracker.total_spend("c1");
        assert!((spend - 5.0).abs() < 0.01, "expected ~5.0, got {spend}");
    }

    #[test]
    fn independent_campaigns() {
        let tracker = InMemorySpendTracker::new();
        tracker.record_spend("c1", 1.0);
        tracker.record_spend("c2", 2.0);
        assert!((tracker.total_spend("c1") - 1.0).abs() < 0.01);
        assert!((tracker.total_spend("c2") - 2.0).abs() < 0.01);
    }
}
