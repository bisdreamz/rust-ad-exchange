use super::reservation::BidReservationRing;
use super::traits::{DealImpressionTracker, DealPacer};
use crate::core::models::deal::{Deal, DealPacing, DeliveryGoal};
use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::Arc;

/// Window target is clamped to at most this many times the flat
/// even rate, preventing burst flooding when catching up.
const DEFAULT_SAFETY_MULTIPLIER: f64 = 3.0;

const SECS_PER_DAY: f64 = 86400.0;

struct WindowState {
    imps_this_window: AtomicU64,
    window_start: AtomicU64,
}

impl WindowState {
    fn new(now_epoch: u64) -> Self {
        Self {
            imps_this_window: AtomicU64::new(0),
            window_start: AtomicU64::new(now_epoch),
        }
    }

    fn maybe_reset(&self, now_epoch: u64, window_secs: u64) {
        let start = self.window_start.load(Relaxed);
        if now_epoch - start >= window_secs {
            self.imps_this_window.store(0, Relaxed);
            self.window_start.store(now_epoch, Relaxed);
        }
    }
}

pub struct EvenDealPacer {
    tracker: Arc<dyn DealImpressionTracker>,
    /// Reuses BidReservationRing — reserve(1.0) per imp, total() = imp count
    rings: DashMap<String, BidReservationRing>,
    windows: DashMap<String, WindowState>,
    cluster_size: Box<dyn Fn() -> usize + Send + Sync>,
    window_secs: u64,
    safety_multiplier: f64,
}

impl EvenDealPacer {
    pub fn new(
        tracker: Arc<dyn DealImpressionTracker>,
        cluster_size: Box<dyn Fn() -> usize + Send + Sync>,
        window_secs: u64,
    ) -> Self {
        Self {
            tracker,
            rings: DashMap::new(),
            windows: DashMap::new(),
            cluster_size,
            window_secs,
            safety_multiplier: DEFAULT_SAFETY_MULTIPLIER,
        }
    }

    fn effective_imps(&self, deal_id: &str) -> Option<u64> {
        let persisted = self.tracker.total_impressions(deal_id)?;
        let buffered = self
            .rings
            .get(deal_id)
            .map(|r| r.total() as u64)
            .unwrap_or(0);
        Some(persisted + buffered)
    }

    fn now_epoch() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock before epoch")
            .as_secs()
    }

    fn reserve(&self, deal_id: &str) {
        self.rings
            .entry(deal_id.to_owned())
            .or_insert_with(BidReservationRing::default)
            .reserve(1.0);
    }

    fn nodes(&self) -> f64 {
        (self.cluster_size)().max(1) as f64
    }

    fn windows_per_day(&self) -> f64 {
        (SECS_PER_DAY / self.window_secs as f64).max(1.0)
    }

    /// Apply windowed rate limiting with the given target per window.
    fn window_check(&self, deal: &Deal, target: f64) -> bool {
        let now_epoch = Self::now_epoch();
        let ws = self
            .windows
            .entry(deal.id.clone())
            .or_insert_with(|| WindowState::new(now_epoch));
        ws.maybe_reset(now_epoch, self.window_secs);

        let window_delivered = ws.imps_this_window.load(Relaxed);
        if window_delivered as f64 >= target {
            return false;
        }

        ws.imps_this_window.fetch_add(1, Relaxed);
        self.reserve(&deal.id);
        true
    }

    /// Even pacing for Total goal with end_date:
    /// adaptive target from remaining imps / remaining time,
    /// capped by safety multiplier × flat rate.
    fn even_total_flight(&self, deal: &Deal, limit: u64, delivered: u64, end_epoch: u64) -> bool {
        let now_epoch = Self::now_epoch();
        let remaining_imps = limit - delivered;
        let nodes = self.nodes();

        // Remaining seconds until end_date
        let remaining_secs = end_epoch.saturating_sub(now_epoch).max(1) as f64;
        let windows_remaining = (remaining_secs / self.window_secs as f64).max(1.0);

        // Adaptive: recalculates each check so delivery catches up or slows
        let adaptive = remaining_imps as f64 / windows_remaining / nodes;

        // Flat rate: what even delivery would be across the full flight
        let flight_start = deal
            .start_date
            .map(|s| s.timestamp() as u64)
            .unwrap_or(now_epoch);
        let total_flight_secs = (end_epoch.saturating_sub(flight_start)).max(1) as f64;
        let total_windows = (total_flight_secs / self.window_secs as f64).max(1.0);
        let flat_rate = limit as f64 / total_windows / nodes;

        // Clamp: safety cap prevents flooding, floor of 1.0 prevents stalling
        let target = adaptive.min(flat_rate * self.safety_multiplier).max(1.0);

        self.window_check(deal, target)
    }

    /// Even pacing for Total goal without end_date:
    /// no time horizon to spread across, so just hard cap.
    fn even_total_no_end(&self, deal: &Deal) -> bool {
        self.reserve(&deal.id);
        true
    }

    /// Even pacing for Daily goal:
    /// steady daily_limit / windows_per_day / nodes rate.
    fn even_daily(&self, deal: &Deal, daily_limit: u64) -> bool {
        let nodes = self.nodes();
        let target = (daily_limit as f64 / self.windows_per_day() / nodes).max(1.0);
        self.window_check(deal, target)
    }
}

impl DealPacer for EvenDealPacer {
    fn passes(&self, deal: &Deal) -> bool {
        let goal = match &deal.delivery_goal {
            Some(goal) => goal,
            None => return true,
        };

        match goal {
            DeliveryGoal::Total(limit) => {
                let delivered = match self.effective_imps(&deal.id) {
                    Some(d) => d,
                    None => return false, // unregistered deal — fail closed
                };
                if delivered >= *limit {
                    return false;
                }

                match deal.pacing {
                    Some(DealPacing::Even) => match deal.end_date {
                        Some(end) => {
                            self.even_total_flight(deal, *limit, delivered, end.timestamp() as u64)
                        }
                        None => self.even_total_no_end(deal),
                    },
                    Some(DealPacing::Fast) | None => {
                        self.reserve(&deal.id);
                        true
                    }
                }
            }
            DeliveryGoal::Daily(daily_limit) => {
                match deal.pacing {
                    Some(DealPacing::Even) => self.even_daily(deal, *daily_limit),
                    Some(DealPacing::Fast) | None => {
                        // Fast daily: hard cap per day, no spread
                        // Ring naturally expires old slots, but we need
                        // a day-scoped counter for the daily cap
                        self.even_daily(deal, *daily_limit)
                    }
                }
            }
        }
    }

    fn record_impression(&self, deal_id: &str) {
        self.tracker.record_impression(deal_id);
    }
}
