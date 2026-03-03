use crate::app::pipeline::ortb::direct::pacing::SpendTracker;
use dashmap::DashMap;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering::Relaxed};

/// Precision multiplier for atomic integer storage.
/// CPM values are stored as `cpm * MICROS` for u64 atomics.
const MICROS: f64 = 1_000_000.0;
const SECS_PER_DAY: u64 = 86400;

fn current_day_number() -> u32 {
    (std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        / SECS_PER_DAY) as u32
}

/// In-memory spend tracker using atomic counters.
///
/// All values are **CPM sums** — the sum of per-impression CPM rates.
/// Actual dollars = cpm_sum / 1000. Internally stored as CPM-micros
/// (`cpm * 1_000_000`) in u64 atomics for precision.
///
/// Campaigns are auto-inserted on first `record_spend`.
/// Unknown campaigns return 0.0 from `total_spend`.
/// Daily spend resets automatically at midnight UTC.
/// State is lost on restart.
pub struct InMemorySpendTracker {
    spend: DashMap<String, AtomicU64>,
    spend_daily: DashMap<String, AtomicU64>,
    /// UTC day number when daily counters were last written.
    /// When the current day differs, daily counters are stale and read as 0.
    daily_day: AtomicU32,
}

impl InMemorySpendTracker {
    pub fn new() -> Self {
        Self {
            spend: DashMap::new(),
            spend_daily: DashMap::new(),
            daily_day: AtomicU32::new(current_day_number()),
        }
    }

    /// If we've crossed midnight, clear all daily counters.
    fn maybe_reset_daily(&self) {
        let today = current_day_number();
        let stored = self.daily_day.load(Relaxed);
        if today != stored {
            if self
                .daily_day
                .compare_exchange(stored, today, Relaxed, Relaxed)
                .is_ok()
            {
                self.spend_daily.clear();
            }
        }
    }
}

impl SpendTracker for InMemorySpendTracker {
    fn total_spend(&self, campaign_id: &str) -> f64 {
        self.spend
            .get(campaign_id)
            .map(|v| v.load(Relaxed) as f64 / MICROS)
            .unwrap_or(0.0)
    }

    fn daily_spend(&self, campaign_id: &str) -> f64 {
        self.maybe_reset_daily();
        self.spend_daily
            .get(campaign_id)
            .map(|v| v.load(Relaxed) as f64 / MICROS)
            .unwrap_or(0.0)
    }

    fn record_spend(&self, campaign_id: &str, amount: f64) {
        let micros = (amount * MICROS) as u64;
        self.spend
            .entry(campaign_id.to_owned())
            .or_insert_with(|| AtomicU64::new(0))
            .fetch_add(micros, Relaxed);
        self.maybe_reset_daily();
        self.spend_daily
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

    /// record_spend receives CPM values; total_spend returns CPM sum.
    #[test]
    fn auto_insert_on_record() {
        let tracker = InMemorySpendTracker::new();
        // Two impressions at $5 CPM → CPM sum = 10.0
        tracker.record_spend("c1", 5.0);
        tracker.record_spend("c1", 5.0);
        let cpm = tracker.total_spend("c1");
        assert!(
            (cpm - 10.0).abs() < 0.01,
            "expected CPM sum ~10.0, got {cpm}"
        );
        // Real spend = 10.0 / 1000 = $0.01
    }

    #[test]
    fn multiple_records_accumulate() {
        let tracker = InMemorySpendTracker::new();
        tracker.record_spend("c1", 2.0);
        tracker.record_spend("c1", 3.0);
        let cpm = tracker.total_spend("c1");
        assert!((cpm - 5.0).abs() < 0.01, "expected CPM sum ~5.0, got {cpm}");
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
