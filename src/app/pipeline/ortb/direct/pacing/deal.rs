use super::reservation::{DEFAULT_BUCKET_SECS, EpochClock, ReservationRing};
use super::traits::{DealImpressionTracker, DealPacer};
use crate::core::models::deal::{Deal, DealPacing, DeliveryGoal};
use dashmap::DashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

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
    /// Reuses ReservationRing — reserve(1.0) per imp, total() = imp count
    rings: DashMap<String, ReservationRing>,
    windows: DashMap<String, WindowState>,
    cluster_size: Box<dyn Fn() -> usize + Send + Sync>,
    window_secs: u64,
    safety_multiplier: f64,
    clock: EpochClock,
}

impl EvenDealPacer {
    pub fn new(
        tracker: Arc<dyn DealImpressionTracker>,
        cluster_size: Box<dyn Fn() -> usize + Send + Sync>,
        window_secs: u64,
        clock: EpochClock,
    ) -> Self {
        Self {
            tracker,
            rings: DashMap::new(),
            windows: DashMap::new(),
            cluster_size,
            window_secs,
            safety_multiplier: DEFAULT_SAFETY_MULTIPLIER,
            clock,
        }
    }

    fn effective_imps(&self, deal_id: &str) -> u64 {
        let persisted = self.tracker.total_impressions(deal_id);
        let buffered = self
            .rings
            .get(deal_id)
            .map(|r| r.total() as u64)
            .unwrap_or(0);
        persisted + buffered
    }

    fn reserve(&self, deal_id: &str) {
        self.rings
            .entry(deal_id.to_owned())
            .or_insert_with(|| ReservationRing::new(DEFAULT_BUCKET_SECS, self.clock.clone()))
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
        let now_epoch = (self.clock)();
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
        let now_epoch = (self.clock)();
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
                let delivered = self.effective_imps(&deal.id);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::pipeline::ortb::direct::pacing::reservation::system_epoch_clock;
    use crate::core::models::common::Status;
    use crate::core::models::deal::{DealOwner, DealPricing, DealTargeting, DemandPolicy};
    use crate::core::models::targeting::CommonTargeting;
    use chrono::{DateTime, TimeZone, Utc};

    struct StubDealTracker {
        initial_imps: u64,
    }

    impl DealImpressionTracker for StubDealTracker {
        fn total_impressions(&self, _deal_id: &str) -> u64 {
            self.initial_imps
        }
        fn record_impression(&self, _deal_id: &str) {}
    }

    /// Tracker that reflects the true cumulative impression count set by the test.
    /// Simulates what the billing pipeline does: periodically flush
    /// counts to the persistent store so the pacer sees accurate totals.
    struct SyncDealTracker {
        total: AtomicU64,
    }

    impl SyncDealTracker {
        fn new() -> Self {
            Self {
                total: AtomicU64::new(0),
            }
        }
        fn set_total(&self, imps: u64) {
            self.total.store(imps, Relaxed);
        }
    }

    impl DealImpressionTracker for SyncDealTracker {
        fn total_impressions(&self, _deal_id: &str) -> u64 {
            self.total.load(Relaxed)
        }
        fn record_impression(&self, _deal_id: &str) {}
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

    fn make_deal(
        goal: Option<DeliveryGoal>,
        pacing: Option<DealPacing>,
        end_date: Option<DateTime<Utc>>,
    ) -> Deal {
        Deal {
            status: Status::Active,
            id: "d1".into(),
            name: "test deal".into(),
            policy: DemandPolicy::Direct {
                company_ids: vec!["co1".into()],
            },
            owner: DealOwner::Platform,
            pricing: DealPricing::Inherit,
            targeting: DealTargeting {
                common: CommonTargeting::default(),
            },
            start_date: Some(Utc::now() - chrono::Duration::hours(1)),
            end_date,
            delivery_goal: goal,
            pacing,
            takes_priority: false,
        }
    }

    fn make_deal_with_dates(
        goal: Option<DeliveryGoal>,
        pacing: Option<DealPacing>,
        start_date: Option<DateTime<Utc>>,
        end_date: Option<DateTime<Utc>>,
    ) -> Deal {
        Deal {
            status: Status::Active,
            id: "d1".into(),
            name: "test deal".into(),
            policy: DemandPolicy::Direct {
                company_ids: vec!["co1".into()],
            },
            owner: DealOwner::Platform,
            pricing: DealPricing::Inherit,
            targeting: DealTargeting {
                common: CommonTargeting::default(),
            },
            start_date,
            end_date,
            delivery_goal: goal,
            pacing,
            takes_priority: false,
        }
    }

    fn make_pacer(initial_imps: u64) -> EvenDealPacer {
        let tracker = Arc::new(StubDealTracker { initial_imps });
        EvenDealPacer::new(tracker, Box::new(|| 1), 60, system_epoch_clock())
    }

    fn make_pacer_with_clock(initial_imps: u64, clock: EpochClock) -> EvenDealPacer {
        let tracker = Arc::new(StubDealTracker { initial_imps });
        EvenDealPacer::new(tracker, Box::new(|| 1), 60, clock)
    }

    /// Deals with no delivery goal bypass pacing entirely.
    #[test]
    fn no_goal_always_passes() {
        let pacer = make_pacer(0);
        let deal = make_deal(None, None, None);
        assert!(pacer.passes(&deal));
    }

    /// Fast pacing with a Total goal passes bids until the impression limit.
    #[test]
    fn total_fast_passes_until_limit() {
        let pacer = make_pacer(0);
        let deal = make_deal(Some(DeliveryGoal::Total(5)), Some(DealPacing::Fast), None);
        // Should pass several times
        assert!(pacer.passes(&deal));
        assert!(pacer.passes(&deal));
    }

    /// A deal already at its Total impression limit is rejected immediately.
    #[test]
    fn total_fast_at_limit_rejects() {
        let pacer = make_pacer(100);
        let deal = make_deal(Some(DeliveryGoal::Total(100)), Some(DealPacing::Fast), None);
        assert!(!pacer.passes(&deal));
    }

    /// Even pacing with a Total goal and end_date rate-limits within a window.
    #[test]
    fn total_even_with_end_date_rate_limited() {
        let pacer = make_pacer(0);
        let deal = make_deal(
            Some(DeliveryGoal::Total(1000)),
            Some(DealPacing::Even),
            Some(Utc::now() + chrono::Duration::hours(1)),
        );
        // First pass should succeed
        assert!(pacer.passes(&deal));
        // Eventually should reject within window
        let mut passes = 0;
        for _ in 0..500 {
            if pacer.passes(&deal) {
                passes += 1;
            } else {
                break;
            }
        }
        assert!(passes < 500, "should eventually rate limit");
    }

    /// Daily even pacing rate-limits to daily_limit / windows_per_day per window.
    #[test]
    fn daily_even_rate_limited() {
        let pacer = make_pacer(0);
        // 10_000 daily / 1440 windows / 1 node ≈ 6.9 per window
        let deal = make_deal(
            Some(DeliveryGoal::Daily(10_000)),
            Some(DealPacing::Even),
            None,
        );
        let mut passes = 0;
        for _ in 0..500 {
            if pacer.passes(&deal) {
                passes += 1;
            } else {
                break;
            }
        }
        assert!(passes >= 1, "should pass at least once");
        assert!(passes < 500, "should eventually rate limit");
    }

    /// After exhausting a daily-even window, advancing the fake clock past
    /// window_secs resets the counter and allows impressions again.
    #[test]
    fn even_daily_window_resets_after_advance() {
        let start_epoch: u64 = 1_700_000_000;
        let (clock, epoch) = fake_clock(start_epoch);
        let pacer = make_pacer_with_clock(0, clock);
        let window_secs = 60u64;

        // 10_000 daily / 1440 windows / 1 node ≈ 6.9 per window
        let deal = make_deal(
            Some(DeliveryGoal::Daily(10_000)),
            Some(DealPacing::Even),
            None,
        );

        // Exhaust current window
        let mut passes = 0;
        for _ in 0..500 {
            if pacer.passes(&deal) {
                passes += 1;
            } else {
                break;
            }
        }
        assert!(passes >= 1, "should pass at least once");
        assert!(passes < 500, "should eventually rate limit");

        // Window exhausted
        assert!(!pacer.passes(&deal));

        // Advance past window_secs → new window
        epoch.store(start_epoch + window_secs, Relaxed);
        assert!(pacer.passes(&deal), "should pass after window reset");
    }

    /// As remaining flight time shrinks, the adaptive rate increases (catch-up),
    /// allowing more impressions per window when behind schedule.
    #[test]
    fn even_total_flight_adapts_as_time_passes() {
        let start_epoch: u64 = 1_700_000_000;
        let (clock, epoch) = fake_clock(start_epoch);
        let pacer = make_pacer_with_clock(0, clock);

        // 2h flight: start_epoch-3600 to start_epoch+3600, limit=1000
        let deal = make_deal_with_dates(
            Some(DeliveryGoal::Total(1000)),
            Some(DealPacing::Even),
            Some(epoch_to_dt(start_epoch - 3600)),
            Some(epoch_to_dt(start_epoch + 3600)),
        );

        // Count passes in early window
        let mut passes_early = 0;
        for _ in 0..500 {
            if pacer.passes(&deal) {
                passes_early += 1;
            } else {
                break;
            }
        }

        // Jump to 600s before end — remaining time shrinks, adaptive rate increases
        // (tracker always returns 0, so remaining_imps stays at 1000)
        epoch.store(start_epoch + 3600 - 600, Relaxed);
        let mut passes_late = 0;
        for _ in 0..500 {
            if pacer.passes(&deal) {
                passes_late += 1;
            } else {
                break;
            }
        }

        // Adaptive rate should be higher when less time remains (but clamped by safety)
        assert!(
            passes_late > passes_early,
            "adaptive rate should increase when behind schedule: early={passes_early}, late={passes_late}"
        );
    }

    /// Buffered ring impressions are fully evicted after SLOTS × bucket_width
    /// seconds elapse, ensuring stale reservations don't block future delivery.
    #[test]
    fn ring_eviction_across_windows() {
        let start_epoch: u64 = 1_700_000_000;
        let (clock, epoch) = fake_clock(start_epoch);
        let pacer = make_pacer_with_clock(0, clock);

        let deal = make_deal(
            Some(DeliveryGoal::Total(100_000)),
            Some(DealPacing::Fast),
            None,
        );

        // Reserve some impressions
        for _ in 0..5 {
            pacer.passes(&deal);
        }

        // Ring should have buffered impressions
        let buffered_before = pacer.rings.get("d1").map(|r| r.total() as u64).unwrap_or(0);
        assert!(buffered_before > 0, "should have buffered imps");

        // Advance past SLOTS × DEFAULT_BUCKET_SECS → ring fully evicted
        epoch.store(start_epoch + DEFAULT_BUCKET_SECS * 6, Relaxed);
        let buffered_after = pacer.rings.get("d1").map(|r| r.total() as u64).unwrap_or(0);
        assert_eq!(buffered_after, 0, "ring should be fully evicted");
    }

    // ---- Delivery accuracy, ceiling, & overshoot tests ----

    /// Simulates an entire 2-hour deal flight with even pacing and Total goal.
    /// Steps through every window, delivers impressions until the pacer throttles,
    /// then flushes the cumulative count to the tracker (simulating billing).
    ///
    /// Verifies total delivery lands within tolerance of the impression goal:
    /// the adaptive algorithm should deliver most of the goal without overshooting.
    #[test]
    fn even_total_delivers_within_tolerance() {
        let start_epoch: u64 = 1_700_000_000;
        let flight_secs: u64 = 7200; // 2 hours
        let window_secs: u64 = 60;
        let goal: u64 = 1000;
        let num_windows = flight_secs / window_secs; // 120

        let (clock, epoch) = fake_clock(start_epoch);
        let tracker = Arc::new(SyncDealTracker::new());
        let pacer = EvenDealPacer::new(
            tracker.clone() as Arc<dyn DealImpressionTracker>,
            Box::new(|| 1),
            window_secs,
            clock,
        );

        let deal = make_deal_with_dates(
            Some(DeliveryGoal::Total(goal)),
            Some(DealPacing::Even),
            Some(epoch_to_dt(start_epoch)),
            Some(epoch_to_dt(start_epoch + flight_secs)),
        );

        let mut total_delivered: u64 = 0;

        for w in 0..num_windows {
            epoch.store(start_epoch + w * window_secs, Relaxed);

            for _ in 0..1000 {
                if pacer.passes(&deal) {
                    total_delivered += 1;
                }
            }

            // Simulate billing flush
            tracker.set_total(total_delivered);
        }

        assert!(
            total_delivered >= (goal as f64 * 0.90) as u64,
            "should deliver at least 90% of goal: delivered {total_delivered}, goal {goal}"
        );
        assert!(
            total_delivered <= (goal as f64 * 1.05) as u64,
            "should not overshoot by more than 5%: delivered {total_delivered}, goal {goal}"
        );
    }

    /// Simulates uneven supply: some windows have plenty of bid requests,
    /// others have none. The adaptive algorithm should ramp up its rate
    /// in windows that do have supply to compensate for dry windows,
    /// delivering a reasonable fraction of the goal despite gaps.
    #[test]
    fn even_total_adapts_to_uneven_supply() {
        let start_epoch: u64 = 1_700_000_000;
        let flight_secs: u64 = 7200;
        let window_secs: u64 = 60;
        let goal: u64 = 1000;
        let num_windows = flight_secs / window_secs;

        let (clock, epoch) = fake_clock(start_epoch);
        let tracker = Arc::new(SyncDealTracker::new());
        let pacer = EvenDealPacer::new(
            tracker.clone() as Arc<dyn DealImpressionTracker>,
            Box::new(|| 1),
            window_secs,
            clock,
        );

        let deal = make_deal_with_dates(
            Some(DeliveryGoal::Total(goal)),
            Some(DealPacing::Even),
            Some(epoch_to_dt(start_epoch)),
            Some(epoch_to_dt(start_epoch + flight_secs)),
        );

        let mut total_delivered: u64 = 0;

        for w in 0..num_windows {
            epoch.store(start_epoch + w * window_secs, Relaxed);

            // Even windows: plenty of supply. Odd windows: zero supply.
            let supply = if w % 2 == 0 { 500 } else { 0 };

            for _ in 0..supply {
                if pacer.passes(&deal) {
                    total_delivered += 1;
                }
            }

            tracker.set_total(total_delivered);
        }

        // With half the windows dry, the adaptive rate should catch up
        // in the windows that have supply. Expect at least 70% delivery.
        assert!(
            total_delivered >= (goal as f64 * 0.70) as u64,
            "should deliver at least 70% with uneven supply: delivered {total_delivered}"
        );
        assert!(
            total_delivered <= (goal as f64 * 1.05) as u64,
            "should not overshoot: delivered {total_delivered}, goal {goal}"
        );
    }

    /// Verifies the safety multiplier caps the catch-up rate.
    /// When a deal is far behind schedule, the adaptive rate would spike
    /// to flood remaining impressions. The safety_multiplier (3×) clamps
    /// the per-window target so no single window delivers more than
    /// 3× the flat even rate — preventing burst flooding.
    #[test]
    fn safety_cap_limits_catchup_rate() {
        let start_epoch: u64 = 1_700_000_000;
        let flight_secs: u64 = 7200; // 2 hours
        let window_secs: u64 = 60;
        let goal: u64 = 1200; // 1200 imps across 120 windows → flat rate = 10/window

        let (clock, epoch) = fake_clock(start_epoch);
        let tracker = Arc::new(SyncDealTracker::new());
        let pacer = EvenDealPacer::new(
            tracker.clone() as Arc<dyn DealImpressionTracker>,
            Box::new(|| 1),
            window_secs,
            clock,
        );

        let deal = make_deal_with_dates(
            Some(DeliveryGoal::Total(goal)),
            Some(DealPacing::Even),
            Some(epoch_to_dt(start_epoch)),
            Some(epoch_to_dt(start_epoch + flight_secs)),
        );

        // Deliver nothing for the first 100 windows — fall far behind
        for w in 0..100 {
            epoch.store(start_epoch + w * window_secs, Relaxed);
        }

        // Now jump to window 100 (20 windows remaining, 1200 imps remaining).
        // Uncapped adaptive rate = 1200 / 20 = 60/window.
        // Flat rate = 1200 / 120 = 10/window.
        // Safety cap = 10 × 3 = 30/window.
        // So the pacer should allow ~30, not 60.
        epoch.store(start_epoch + 100 * window_secs, Relaxed);

        let mut window_imps = 0u64;
        for _ in 0..200 {
            if pacer.passes(&deal) {
                window_imps += 1;
            }
        }

        let flat_rate = goal as f64 / (flight_secs / window_secs) as f64; // 10
        let max_allowed = (flat_rate * DEFAULT_SAFETY_MULTIPLIER).ceil() as u64; // 30

        assert!(
            window_imps <= max_allowed + 1, // +1 for rounding
            "safety cap should limit to ~{max_allowed}/window, got {window_imps}"
        );
        assert!(
            window_imps > flat_rate as u64,
            "should allow above flat rate for catch-up: flat={flat_rate}, got {window_imps}"
        );
    }

    /// Verifies the hard impression cap prevents overshoot in single-threaded use.
    /// When the tracker reports delivery near the limit, the pacer should allow
    /// only enough impressions to reach exactly the goal, not beyond.
    #[test]
    fn hard_cap_prevents_overshoot() {
        let start_epoch: u64 = 1_700_000_000;
        let (clock, _) = fake_clock(start_epoch);

        // Tracker reports 995 of 1000 already delivered
        let tracker = Arc::new(StubDealTracker { initial_imps: 995 });
        let pacer = EvenDealPacer::new(tracker, Box::new(|| 1), 60, clock);

        let deal = make_deal_with_dates(
            Some(DeliveryGoal::Total(1000)),
            Some(DealPacing::Fast),
            Some(epoch_to_dt(start_epoch - 3600)),
            None,
        );

        // Try many bids — should allow exactly 5 (995 + 5 = 1000, then >= limit)
        let mut total_allowed = 0u64;
        for _ in 0..1000 {
            if pacer.passes(&deal) {
                total_allowed += 1;
            }
        }

        assert_eq!(total_allowed, 5, "should allow exactly 5 imps to reach cap");
    }
}
