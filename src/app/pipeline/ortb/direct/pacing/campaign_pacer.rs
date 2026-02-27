use super::{BidReservationRing, SpendPacer, SpendTracker};
use crate::core::cluster::ClusterDiscovery;
use crate::core::models::campaign::{Campaign, CampaignPacing};
use chrono::Utc;
use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::Arc;

const MICROS_PER_DOLLAR: f64 = 1_000_000.0;

struct WindowState {
    spent_this_window: AtomicU64,
    window_start: AtomicU64,
}

impl WindowState {
    fn new(now_epoch: u64) -> Self {
        Self {
            spent_this_window: AtomicU64::new(0),
            window_start: AtomicU64::new(now_epoch),
        }
    }

    fn maybe_reset(&self, now_epoch: u64, window_secs: u64) {
        let start = self.window_start.load(Relaxed);
        if now_epoch - start >= window_secs {
            self.spent_this_window.store(0, Relaxed);
            self.window_start.store(now_epoch, Relaxed);
        }
    }
}

/// Smooth daypart weight using a cosine curve.
/// Peak at 18:00 local (1.3), trough at 06:00 (0.7).
/// Average over a full day = 1.0, so total budget is preserved.
fn daypart_weight(utc_epoch_secs: i64, tz_offset_hours: i8) -> f64 {
    let utc_hour_frac = (utc_epoch_secs % 86400) as f64 / 3600.0;
    let local_hour = (utc_hour_frac + tz_offset_hours as f64).rem_euclid(24.0);

    let radians = (local_hour - 18.0) * std::f64::consts::TAU / 24.0;
    1.0 + 0.3 * radians.cos()
}

/// Unified campaign spend pacer that reads `campaign.pacing` per call.
///
/// Merges Even, WeightedEven, and Fast pacing into a single struct.
/// When campaign settings change via Firestore listener → CampaignManager,
/// the next `passes()` call automatically uses the new strategy.
pub struct CampaignSpendPacer {
    tracker: Arc<dyn SpendTracker>,
    rings: DashMap<String, BidReservationRing>,
    windows: DashMap<String, WindowState>,
    cluster: Arc<dyn ClusterDiscovery>,
    window_secs: u64,
}

impl CampaignSpendPacer {
    pub fn new(
        tracker: Arc<dyn SpendTracker>,
        cluster: Arc<dyn ClusterDiscovery>,
        window_secs: u64,
    ) -> Self {
        Self {
            tracker,
            rings: DashMap::new(),
            windows: DashMap::new(),
            cluster,
            window_secs,
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

    fn reserve(&self, campaign_id: &str, price: f64) {
        self.rings
            .entry(campaign_id.to_owned())
            .or_insert_with(BidReservationRing::default)
            .reserve(price);
    }

    fn target_per_node_per_window(
        &self,
        campaign: &Campaign,
        effective_spend: f64,
        tz_offset: Option<i8>,
    ) -> f64 {
        let remaining = (campaign.budget - effective_spend).max(0.0);
        let now = Utc::now();

        let remaining_secs = (campaign.end_date - now).num_seconds().max(1) as f64;
        let windows_remaining = (remaining_secs / self.window_secs as f64).max(1.0);
        let nodes = self.cluster.cluster_size().max(1) as f64;

        let flat_target = remaining / windows_remaining / nodes;

        match tz_offset {
            Some(tz) => flat_target * daypart_weight(now.timestamp(), tz),
            None => flat_target,
        }
    }

    fn even_check(
        &self,
        campaign: &Campaign,
        spent: f64,
        price: f64,
        tz_offset: Option<i8>,
    ) -> bool {
        let now_epoch = Utc::now().timestamp() as u64;
        let target = self.target_per_node_per_window(campaign, spent, tz_offset);

        let ws = self
            .windows
            .entry(campaign.id.clone())
            .or_insert_with(|| WindowState::new(now_epoch));

        ws.maybe_reset(now_epoch, self.window_secs);
        let window_spent = ws.spent_this_window.load(Relaxed) as f64 / MICROS_PER_DOLLAR;

        if window_spent + price > target {
            return false;
        }

        ws.spent_this_window
            .fetch_add((price * MICROS_PER_DOLLAR) as u64, Relaxed);

        self.reserve(&campaign.id, price);
        true
    }
}

impl SpendPacer for CampaignSpendPacer {
    fn passes(&self, campaign: &Campaign, price: f64) -> bool {
        let spent = match self.effective_spend(&campaign.id) {
            Some(s) => s,
            None => return false, // unregistered campaign — fail closed
        };

        // Hard budget cap — all strategies
        if spent + price > campaign.budget {
            return false;
        }

        match &campaign.pacing {
            CampaignPacing::Fast => {
                self.reserve(&campaign.id, price);
                true
            }
            CampaignPacing::Even => self.even_check(campaign, spent, price, None),
            CampaignPacing::WeightedEven { tz_offset } => {
                self.even_check(campaign, spent, price, Some(*tz_offset))
            }
        }
    }
}
