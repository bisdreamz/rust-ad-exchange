use super::{BidReservationRing, SpendPacer, SpendTracker};
use crate::core::models::campaign::Campaign;
use chrono::Utc;
use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::Arc;

const MICROS_PER_DOLLAR: f64 = 1_000_000.0;

/// Per-campaign window rate-limiting state.
/// Tracks approved bid value within the current time window on this
/// node — the global budget guard uses SpendTracker + ring instead.
struct WindowState {
    /// Approved bid value in microdollars this window on this node
    spent_this_window: AtomicU64,
    /// Window start as epoch seconds
    window_start: AtomicU64,
}

impl WindowState {
    fn new(now_epoch: u64) -> Self {
        Self {
            spent_this_window: AtomicU64::new(0),
            window_start: AtomicU64::new(now_epoch),
        }
    }

    fn maybe_reset_window(&self, now_epoch: u64, window_secs: u64) {
        let start = self.window_start.load(Relaxed);

        if now_epoch - start >= window_secs {
            self.spent_this_window.store(0, Relaxed);
            self.window_start.store(now_epoch, Relaxed);
        }
    }
}

/// Smooth daypart weight using a cosine curve.
/// Peak at 18:00 local (1.3), trough at 06:00 (0.7).
/// 3am ≈ 0.79, midnight ≈ 1.0, noon ≈ 1.0.
/// Average over a full day = 1.0, so total budget is preserved.
fn daypart_weight(utc_epoch_secs: i64, tz_offset_hours: i8) -> f64 {
    let utc_hour_frac = (utc_epoch_secs % 86400) as f64 / 3600.0;
    let local_hour = (utc_hour_frac + tz_offset_hours as f64).rem_euclid(24.0);

    let radians = (local_hour - 18.0) * std::f64::consts::TAU / 24.0;
    1.0 + 0.3 * radians.cos()
}

/// Spread spend evenly across the flight.
///
/// Two-layer check:
/// 1. Budget guard — effective spend (tracker + ring) must not exceed total budget
/// 2. Window rate limit — approved bid value this window must not exceed target
///
/// The per-window target recalculates from *remaining* budget each window,
/// so the pacer self-corrects if early windows under- or over-deliver.
///
/// When `tz_offset` is set, a smooth daypart curve weights the window
/// target higher during peak hours (~6pm local) and lower overnight.
pub struct EvenPacer {
    tracker: Arc<dyn SpendTracker>,
    rings: DashMap<String, BidReservationRing>,
    /// Returns current cluster node count for rate splitting
    cluster_size: Box<dyn Fn() -> usize + Send + Sync>,
    /// Per-campaign window spend state
    windows: DashMap<String, WindowState>,
    /// Window duration in seconds (default 3600 = 1 hour)
    window_secs: u64,
    /// UTC offset for daypart weighting. None = flat pacing.
    tz_offset: Option<i8>,
}

impl EvenPacer {
    pub fn new(
        tracker: Arc<dyn SpendTracker>,
        cluster_size: Box<dyn Fn() -> usize + Send + Sync>,
        window_secs: u64,
        tz_offset: Option<i8>,
    ) -> Self {
        Self {
            tracker,
            rings: DashMap::new(),
            cluster_size,
            windows: DashMap::new(),
            window_secs,
            tz_offset,
        }
    }

    fn effective_spend(&self, campaign_id: &str) -> Option<f64> {
        let persisted = self.tracker.total_spend(campaign_id)?;
        let buffered = self
            .rings
            .get(campaign_id)
            .map(|r| r.total())
            .unwrap_or(0.0);

        Some(persisted + buffered)
    }

    /// Target spend per window per node, based on remaining budget.
    /// When daypart weighting is active, scales by the current weight.
    fn target_per_node_per_window(&self, campaign: &Campaign, effective_spend: f64) -> f64 {
        let remaining = (campaign.budget - effective_spend).max(0.0);
        let now = Utc::now();

        let remaining_secs = (campaign.end_date - now).num_seconds().max(1) as f64;
        let windows_remaining = (remaining_secs / self.window_secs as f64).max(1.0);
        let nodes = (self.cluster_size)().max(1) as f64;

        let flat_target = remaining / windows_remaining / nodes;

        match self.tz_offset {
            Some(tz) => flat_target * daypart_weight(now.timestamp(), tz),
            None => flat_target,
        }
    }
}

impl SpendPacer for EvenPacer {
    fn passes(&self, campaign: &Campaign, price: f64) -> bool {
        let now_epoch = Utc::now().timestamp() as u64;
        let spent = match self.effective_spend(&campaign.id) {
            Some(s) => s,
            None => return false, // unregistered campaign — fail closed
        };

        // 1. Budget guard — hard cap
        if spent + price > campaign.budget {
            return false;
        }

        // 2. Window rate limit
        let target = self.target_per_node_per_window(campaign, spent);

        let ws = self
            .windows
            .entry(campaign.id.clone())
            .or_insert_with(|| WindowState::new(now_epoch));

        ws.maybe_reset_window(now_epoch, self.window_secs);
        let window_spent = ws.spent_this_window.load(Relaxed) as f64 / MICROS_PER_DOLLAR;

        if window_spent + price > target {
            return false;
        }

        // Approved — track in window and buffer in ring
        ws.spent_this_window
            .fetch_add((price * MICROS_PER_DOLLAR) as u64, Relaxed);

        self.rings
            .entry(campaign.id.clone())
            .or_insert_with(BidReservationRing::default)
            .reserve(price);

        true
    }
}
