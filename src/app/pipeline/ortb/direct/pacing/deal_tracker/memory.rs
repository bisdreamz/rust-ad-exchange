use crate::app::pipeline::ortb::direct::pacing::DealImpressionTracker;
use dashmap::DashMap;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering::Relaxed};

const SECS_PER_DAY: u64 = 86400;

fn current_day_number() -> u32 {
    (std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        / SECS_PER_DAY) as u32
}

/// In-memory deal impression tracker using atomic counters.
/// Deals are auto-inserted on first `record_impression`.
/// Unknown deals return 0 from `total_impressions`.
/// Daily impressions reset automatically at midnight UTC.
/// State is lost on restart.
pub struct InMemoryDealTracker {
    impressions: DashMap<String, AtomicU64>,
    impressions_daily: DashMap<String, AtomicU64>,
    /// UTC day number when daily counters were last written.
    daily_day: AtomicU32,
}

impl InMemoryDealTracker {
    pub fn new() -> Self {
        Self {
            impressions: DashMap::new(),
            impressions_daily: DashMap::new(),
            daily_day: AtomicU32::new(current_day_number()),
        }
    }

    fn maybe_reset_daily(&self) {
        let today = current_day_number();
        let stored = self.daily_day.load(Relaxed);
        if today != stored {
            if self
                .daily_day
                .compare_exchange(stored, today, Relaxed, Relaxed)
                .is_ok()
            {
                self.impressions_daily.clear();
            }
        }
    }
}

impl DealImpressionTracker for InMemoryDealTracker {
    fn total_impressions(&self, deal_id: &str) -> u64 {
        self.impressions
            .get(deal_id)
            .map(|v| v.load(Relaxed))
            .unwrap_or(0)
    }

    fn daily_impressions(&self, deal_id: &str) -> u64 {
        self.maybe_reset_daily();
        self.impressions_daily
            .get(deal_id)
            .map(|v| v.load(Relaxed))
            .unwrap_or(0)
    }

    fn record_impression(&self, deal_id: &str) {
        self.impressions
            .entry(deal_id.to_owned())
            .or_insert_with(|| AtomicU64::new(0))
            .fetch_add(1, Relaxed);
        self.maybe_reset_daily();
        self.impressions_daily
            .entry(deal_id.to_owned())
            .or_insert_with(|| AtomicU64::new(0))
            .fetch_add(1, Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_deal_returns_zero() {
        let tracker = InMemoryDealTracker::new();
        assert_eq!(tracker.total_impressions("unknown"), 0);
    }

    #[test]
    fn auto_insert_on_record() {
        let tracker = InMemoryDealTracker::new();
        tracker.record_impression("d1");
        assert_eq!(tracker.total_impressions("d1"), 1);
    }

    #[test]
    fn multiple_records_accumulate() {
        let tracker = InMemoryDealTracker::new();
        tracker.record_impression("d1");
        tracker.record_impression("d1");
        tracker.record_impression("d1");
        assert_eq!(tracker.total_impressions("d1"), 3);
    }

    #[test]
    fn independent_deals() {
        let tracker = InMemoryDealTracker::new();
        tracker.record_impression("d1");
        tracker.record_impression("d2");
        tracker.record_impression("d2");
        assert_eq!(tracker.total_impressions("d1"), 1);
        assert_eq!(tracker.total_impressions("d2"), 2);
    }
}
