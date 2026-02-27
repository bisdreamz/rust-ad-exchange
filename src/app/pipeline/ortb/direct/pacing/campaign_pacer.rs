use super::reservation::{DEFAULT_BUCKET_SECS, EpochClock, ReservationRing};
use super::{SpendPacer, SpendTracker};
use crate::core::cluster::ClusterDiscovery;
use crate::core::models::campaign::{Campaign, CampaignPacing};
use dashmap::DashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

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
    rings: DashMap<String, ReservationRing>,
    windows: DashMap<String, WindowState>,
    cluster: Arc<dyn ClusterDiscovery>,
    window_secs: u64,
    clock: EpochClock,
}

impl CampaignSpendPacer {
    pub fn new(
        tracker: Arc<dyn SpendTracker>,
        cluster: Arc<dyn ClusterDiscovery>,
        window_secs: u64,
        clock: EpochClock,
    ) -> Self {
        Self {
            tracker,
            rings: DashMap::new(),
            windows: DashMap::new(),
            cluster,
            window_secs,
            clock,
        }
    }

    fn effective_spend(&self, campaign_id: &str) -> f64 {
        let persisted = self.tracker.total_spend(campaign_id);
        let buffered = self
            .rings
            .get(campaign_id)
            .map(|r| r.total())
            .unwrap_or(0.0);
        persisted + buffered
    }

    fn reserve(&self, campaign_id: &str, price: f64) {
        self.rings
            .entry(campaign_id.to_owned())
            .or_insert_with(|| ReservationRing::new(DEFAULT_BUCKET_SECS, self.clock.clone()))
            .reserve(price);
    }

    fn target_per_node_per_window(
        &self,
        campaign: &Campaign,
        effective_spend: f64,
        tz_offset: Option<i8>,
    ) -> f64 {
        let remaining = (campaign.budget - effective_spend).max(0.0);
        let now_epoch = (self.clock)() as i64;

        let remaining_secs = (campaign.end_date.timestamp() - now_epoch).max(1) as f64;
        let windows_remaining = (remaining_secs / self.window_secs as f64).max(1.0);
        let nodes = self.cluster.cluster_size().max(1) as f64;

        let flat_target = remaining / windows_remaining / nodes;

        match tz_offset {
            Some(tz) => flat_target * daypart_weight(now_epoch, tz),
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
        let now_epoch = (self.clock)();
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
        let spent = self.effective_spend(&campaign.id);

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::pipeline::ortb::direct::pacing::reservation::system_epoch_clock;
    use crate::core::models::campaign::{CampaignTargeting, PricingStrategy};
    use crate::core::models::common::Status;
    use async_trait::async_trait;
    use chrono::{DateTime, TimeZone, Utc};

    struct StubTracker {
        initial_spend: f64,
    }

    impl SpendTracker for StubTracker {
        fn total_spend(&self, _campaign_id: &str) -> f64 {
            self.initial_spend
        }
        fn record_spend(&self, _campaign_id: &str, _amount: f64) {}
    }

    /// Tracker that reflects the true cumulative spend set by the test.
    /// Simulates what the billing pipeline does: periodically flush
    /// spend to the persistent store so the pacer sees accurate totals.
    struct SyncTracker {
        total_micros: AtomicU64,
    }

    impl SyncTracker {
        fn new() -> Self {
            Self {
                total_micros: AtomicU64::new(0),
            }
        }
        fn set_total(&self, dollars: f64) {
            self.total_micros
                .store((dollars * 1_000_000.0) as u64, Relaxed);
        }
    }

    impl SpendTracker for SyncTracker {
        fn total_spend(&self, _campaign_id: &str) -> f64 {
            self.total_micros.load(Relaxed) as f64 / 1_000_000.0
        }
        fn record_spend(&self, _campaign_id: &str, _amount: f64) {}
    }

    struct StubCluster(usize);
    #[async_trait]
    impl ClusterDiscovery for StubCluster {
        fn cluster_size(&self) -> usize {
            self.0
        }
        fn on_change(&self, _cb: Box<dyn Fn(usize) + Send + Sync>) {}
    }

    fn fake_clock(start_epoch: u64) -> (EpochClock, Arc<AtomicU64>) {
        let epoch = Arc::new(AtomicU64::new(start_epoch));
        let e = epoch.clone();
        let clock: EpochClock = Arc::new(move || e.load(Relaxed));
        (clock, epoch)
    }

    fn epoch_to_dt(epoch: u64) -> DateTime<Utc> {
        Utc.timestamp_opt(epoch as i64, 0).unwrap()
    }

    fn campaign_with_dates(
        pacing: CampaignPacing,
        budget: f64,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Campaign {
        Campaign {
            status: Status::Active,
            company_id: "co1".into(),
            id: "c1".into(),
            start_date: start,
            end_date: end,
            name: "test".into(),
            pacing,
            budget,
            strategy: PricingStrategy::FixedPrice(5.0),
            advertiser_id: "adv1".into(),
            targeting: CampaignTargeting::default(),
        }
    }

    fn campaign(pacing: CampaignPacing, budget: f64) -> Campaign {
        Campaign {
            status: Status::Active,
            company_id: "co1".into(),
            id: "c1".into(),
            start_date: Utc::now() - chrono::Duration::hours(1),
            end_date: Utc::now() + chrono::Duration::hours(1),
            name: "test".into(),
            pacing,
            budget,
            strategy: PricingStrategy::FixedPrice(5.0),
            advertiser_id: "adv1".into(),
            targeting: CampaignTargeting::default(),
        }
    }

    /// Fast pacing passes bids until the ring + tracker reaches the budget cap.
    #[test]
    fn fast_passes_until_budget_gone() {
        let tracker = Arc::new(StubTracker { initial_spend: 0.0 });
        let cluster = Arc::new(StubCluster(1));
        let pacer = CampaignSpendPacer::new(tracker, cluster, 60, system_epoch_clock());

        let c = campaign(CampaignPacing::Fast, 10.0);
        assert!(pacer.passes(&c, 5.0));
        assert!(pacer.passes(&c, 5.0));
        // Ring now holds 10.0, next should fail (10 + 5 > 10)
        assert!(!pacer.passes(&c, 5.0));
    }

    /// Even pacing allows the first bid when the window target is nonzero.
    #[test]
    fn even_within_window_passes() {
        let tracker = Arc::new(StubTracker { initial_spend: 0.0 });
        let cluster = Arc::new(StubCluster(1));
        let pacer = CampaignSpendPacer::new(tracker, cluster, 60, system_epoch_clock());

        let c = campaign(CampaignPacing::Even, 1000.0);
        // First bid should always pass — window target > 0
        assert!(pacer.passes(&c, 0.01));
    }

    /// Even pacing rejects once window spend exceeds the per-window target.
    #[test]
    fn even_window_spent_rejects() {
        let tracker = Arc::new(StubTracker { initial_spend: 0.0 });
        let cluster = Arc::new(StubCluster(1));
        let pacer = CampaignSpendPacer::new(tracker, cluster, 60, system_epoch_clock());

        // Campaign with 2h flight, 1000 budget, 1 node, 60s windows
        // = 120 windows, ~8.33/window target
        let c = campaign(CampaignPacing::Even, 1000.0);

        // Exhaust window budget by passing many small bids
        let mut passes = 0;
        for _ in 0..200 {
            if pacer.passes(&c, 5.0) {
                passes += 1;
            } else {
                break;
            }
        }
        // Should have passed at least once but not 200 times
        assert!(passes >= 1, "should pass at least once");
        assert!(passes < 200, "should eventually reject");
    }

    /// Hard budget cap rejects any bid that would exceed lifetime budget.
    #[test]
    fn budget_exceeded_rejected() {
        let tracker = Arc::new(StubTracker {
            initial_spend: 999.0,
        });
        let cluster = Arc::new(StubCluster(1));
        let pacer = CampaignSpendPacer::new(tracker, cluster, 60, system_epoch_clock());

        let c = campaign(CampaignPacing::Fast, 1000.0);
        // 999 + 5 > 1000
        assert!(!pacer.passes(&c, 5.0));
    }

    /// After exhausting a window, advancing the fake clock past window_secs
    /// resets the window counter and allows bids again.
    #[test]
    fn even_window_resets_after_advance() {
        // Start epoch: campaign runs from t-3600 to t+3600 (2h flight)
        let start_epoch: u64 = 1_700_000_000;
        let (clock, epoch) = fake_clock(start_epoch);

        let tracker = Arc::new(StubTracker { initial_spend: 0.0 });
        let cluster = Arc::new(StubCluster(1));
        let window_secs = 60;
        let pacer = CampaignSpendPacer::new(tracker, cluster, window_secs, clock);

        let c = campaign_with_dates(
            CampaignPacing::Even,
            1000.0,
            epoch_to_dt(start_epoch - 3600),
            epoch_to_dt(start_epoch + 3600),
        );

        // Exhaust the current window
        let mut passes = 0;
        for _ in 0..200 {
            if pacer.passes(&c, 5.0) {
                passes += 1;
            } else {
                break;
            }
        }
        assert!(passes >= 1, "should pass at least once");
        assert!(passes < 200, "should eventually reject");

        // Window is exhausted — next bid fails
        assert!(!pacer.passes(&c, 5.0));

        // Advance past window_secs → new window
        epoch.store(start_epoch + window_secs as u64, Relaxed);
        assert!(pacer.passes(&c, 5.0), "should pass after window reset");
    }

    /// As remaining flight time shrinks, the per-window target increases
    /// (more budget per fewer windows), allowing more bids per window.
    #[test]
    fn budget_target_adapts_over_time() {
        let start_epoch: u64 = 1_700_000_000;
        let (clock, epoch) = fake_clock(start_epoch);

        let tracker = Arc::new(StubTracker { initial_spend: 0.0 });
        let cluster = Arc::new(StubCluster(1));
        let window_secs: u64 = 60;
        let pacer = CampaignSpendPacer::new(tracker, cluster, window_secs, clock);

        // 2h flight, 1000 budget
        let c = campaign_with_dates(
            CampaignPacing::Even,
            1000.0,
            epoch_to_dt(start_epoch - 3600),
            epoch_to_dt(start_epoch + 3600),
        );

        // At t=start_epoch: 3600s remaining = 60 windows, target ≈ 1000/60 ≈ 16.67
        let mut passes_early = 0;
        for _ in 0..200 {
            if pacer.passes(&c, 1.0) {
                passes_early += 1;
            } else {
                break;
            }
        }

        // Jump to 600s before end — remaining time shrinks, target per window rises
        // (budget still appears large since StubTracker always returns 0)
        epoch.store(start_epoch + 3600 - 600, Relaxed);
        let mut passes_late = 0;
        for _ in 0..200 {
            if pacer.passes(&c, 1.0) {
                passes_late += 1;
            } else {
                break;
            }
        }

        // With less time remaining, the target per window should be larger
        assert!(
            passes_late > passes_early,
            "target should increase as remaining time shrinks: early={passes_early}, late={passes_late}"
        );
    }

    // ---- Delivery accuracy & overshoot tests ----

    /// Simulates an entire 2-hour campaign flight with even pacing.
    /// Steps through every window, bids until the pacer throttles,
    /// then flushes cumulative spend to the tracker (simulating billing).
    ///
    /// Verifies total spend lands within tolerance of the budget:
    /// the adaptive algorithm should deliver most of the budget
    /// without overshooting.
    #[test]
    fn even_delivers_full_budget_over_flight() {
        let start_epoch: u64 = 1_700_000_000;
        let flight_secs: u64 = 7200; // 2 hours
        let window_secs: u64 = 60;
        let budget = 1000.0;
        let bid_price = 1.0;
        let num_windows = flight_secs / window_secs; // 120

        let (clock, epoch) = fake_clock(start_epoch);
        let tracker = Arc::new(SyncTracker::new());
        let cluster = Arc::new(StubCluster(1));
        let pacer = CampaignSpendPacer::new(tracker.clone(), cluster, window_secs, clock);

        let c = campaign_with_dates(
            CampaignPacing::Even,
            budget,
            epoch_to_dt(start_epoch),
            epoch_to_dt(start_epoch + flight_secs),
        );

        let mut total_spent = 0.0;

        for w in 0..num_windows {
            epoch.store(start_epoch + w * window_secs, Relaxed);

            // Try many bids — pacer limits per window internally
            for _ in 0..1000 {
                if pacer.passes(&c, bid_price) {
                    total_spent += bid_price;
                }
            }

            // Simulate billing flush: tell tracker the true cumulative spend.
            // The ring also holds recent spend, so effective_spend slightly
            // double-counts — this makes the pacer conservative (good).
            tracker.set_total(total_spent);
        }

        assert!(
            total_spent >= budget * 0.90,
            "should deliver at least 90% of budget: spent {total_spent}, budget {budget}"
        );
        assert!(
            total_spent <= budget * 1.05,
            "should not overshoot by more than 5%: spent {total_spent}, budget {budget}"
        );
    }

    /// Simulates uneven supply: some windows have many bids (hot), others
    /// have few (cold). The adaptive algorithm should still deliver close
    /// to the full budget by spending more in windows that have supply
    /// and less in windows that don't.
    #[test]
    fn even_adapts_to_uneven_supply() {
        let start_epoch: u64 = 1_700_000_000;
        let flight_secs: u64 = 7200;
        let window_secs: u64 = 60;
        let budget = 1000.0;
        let bid_price = 1.0;
        let num_windows = flight_secs / window_secs;

        let (clock, epoch) = fake_clock(start_epoch);
        let tracker = Arc::new(SyncTracker::new());
        let cluster = Arc::new(StubCluster(1));
        let pacer = CampaignSpendPacer::new(tracker.clone(), cluster, window_secs, clock);

        let c = campaign_with_dates(
            CampaignPacing::Even,
            budget,
            epoch_to_dt(start_epoch),
            epoch_to_dt(start_epoch + flight_secs),
        );

        let mut total_spent = 0.0;

        for w in 0..num_windows {
            epoch.store(start_epoch + w * window_secs, Relaxed);

            // Alternate: even windows get plenty of supply, odd windows get none.
            // This means roughly half the windows contribute zero spend.
            let supply = if w % 2 == 0 { 500 } else { 0 };

            for _ in 0..supply {
                if pacer.passes(&c, bid_price) {
                    total_spent += bid_price;
                }
            }

            tracker.set_total(total_spent);
        }

        // With half the windows having zero supply, the adaptive rate should
        // ramp up in the windows that do have supply to compensate.
        // We expect at least 70% delivery (supply is available every other window,
        // and adaptive catch-up should make use of it).
        assert!(
            total_spent >= budget * 0.70,
            "should deliver at least 70% of budget with uneven supply: spent {total_spent}"
        );
        assert!(
            total_spent <= budget * 1.05,
            "should not overshoot: spent {total_spent}, budget {budget}"
        );
    }

    /// Verifies the hard budget cap prevents overshoot in single-threaded use.
    /// When the tracker reports spend near the cap, the pacer should allow
    /// only enough bids to reach exactly the budget, not beyond.
    #[test]
    fn hard_cap_prevents_overshoot() {
        let start_epoch: u64 = 1_700_000_000;
        let (clock, _) = fake_clock(start_epoch);

        // Tracker reports 995 of 1000 already spent
        let tracker = Arc::new(StubTracker {
            initial_spend: 995.0,
        });
        let cluster = Arc::new(StubCluster(1));
        let pacer = CampaignSpendPacer::new(tracker, cluster, 60, clock);

        let c = campaign_with_dates(
            CampaignPacing::Fast,
            1000.0,
            epoch_to_dt(start_epoch - 3600),
            epoch_to_dt(start_epoch + 3600),
        );

        // Try many bids — should only allow exactly 5 ($1 each, 995→1000)
        let mut total_allowed = 0.0;
        for _ in 0..1000 {
            if pacer.passes(&c, 1.0) {
                total_allowed += 1.0;
            }
        }

        // effective_spend = tracker(995) + ring(accumulating).
        // Once ring reaches 5.0, effective_spend = 1000, next bid rejected.
        assert!(
            (total_allowed - 5.0f64).abs() < 0.01,
            "should allow exactly 5 bids to reach budget cap, got {total_allowed}"
        );
    }
}
