use super::bucket::{AdaptiveBucket, TrackerStaleness};
use super::reservation::{DEFAULT_BUCKET_SECS, EpochClock, FineClock, ReservationRing};
use super::traits::{DealImpressionTracker, DealPacer};
use crate::core::models::common::DeliveryState;
use crate::core::models::deal::{Deal, DealPacing, DeliveryGoal};
use dashmap::DashMap;
use std::sync::Arc;
use tracing::warn;

/// Rate is clamped to at most this many times the flat even rate,
/// preventing burst flooding when catching up.
const DEFAULT_SAFETY_MULTIPLIER: f64 = 3.0;

const SECS_PER_DAY: f64 = 86400.0;

pub struct EvenDealPacer {
    tracker: Arc<dyn DealImpressionTracker>,
    /// Reuses ReservationRing — reserve(1.0) per imp, total() = imp count
    rings: DashMap<String, ReservationRing>,
    buckets: DashMap<String, AdaptiveBucket>,
    staleness: DashMap<String, TrackerStaleness>,
    cluster_size: Box<dyn Fn() -> usize + Send + Sync>,
    safety_multiplier: f64,
    clock: EpochClock,
    fine_clock: FineClock,
}

impl EvenDealPacer {
    pub fn new(
        tracker: Arc<dyn DealImpressionTracker>,
        cluster_size: Box<dyn Fn() -> usize + Send + Sync>,
        clock: EpochClock,
        fine_clock: FineClock,
    ) -> Self {
        Self {
            tracker,
            rings: DashMap::new(),
            buckets: DashMap::new(),
            staleness: DashMap::new(),
            cluster_size,
            safety_multiplier: DEFAULT_SAFETY_MULTIPLIER,
            clock,
            fine_clock,
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
        if let Some(ring) = self.rings.get_mut(deal_id) {
            ring.reserve(1.0);
        } else {
            let ring = ReservationRing::new(DEFAULT_BUCKET_SECS, self.clock.clone());
            ring.reserve(1.0);
            self.rings.insert(deal_id.to_owned(), ring);
        }
    }

    /// Even pacing for Total goal with end_date:
    /// adaptive rate from remaining imps / remaining time,
    /// capped by safety multiplier × flat rate.
    fn even_total_flight(&self, deal: &Deal, limit: u64, delivered: u64, end_epoch: u64) -> bool {
        let now_secs = (self.fine_clock)();
        let remaining_imps = (limit - delivered) as f64;
        let remaining_secs = (end_epoch as f64 - now_secs).max(1.0);
        let nodes = (self.cluster_size)().max(1) as f64;

        let adaptive_rate = remaining_imps / remaining_secs / nodes;

        // Flat rate: what even delivery would be across the full flight
        let total_secs = (end_epoch as f64
            - deal
                .start_date
                .map(|s| s.timestamp() as f64)
                .unwrap_or(now_secs))
        .max(1.0);
        let flat_rate = limit as f64 / total_secs / nodes;

        let rate = adaptive_rate.min(flat_rate * self.safety_multiplier);
        self.try_bucket(&deal.id, rate, now_secs)
    }

    /// Even pacing for Total goal without end_date:
    /// no time horizon to spread across, so just hard cap.
    fn even_total_no_end(&self, deal: &Deal) -> bool {
        self.reserve(&deal.id);
        true
    }

    /// Even pacing for Daily goal:
    /// adaptive catch-up using remaining / seconds_to_midnight / nodes,
    /// capped by safety multiplier × flat daily rate.
    fn even_daily(&self, deal: &Deal, daily_limit: u64) -> bool {
        let now_secs = (self.fine_clock)();
        let nodes = (self.cluster_size)().max(1) as f64;
        let now_epoch = (self.clock)() as i64;
        let remaining_secs =
            (SECS_PER_DAY as i64 - (now_epoch % SECS_PER_DAY as i64)).max(1) as f64;

        let delivered = self.effective_imps(&deal.id) as f64;
        let remaining = (daily_limit as f64 - delivered).max(0.0);

        let flat_rate = daily_limit as f64 / SECS_PER_DAY / nodes;
        let adaptive_rate = remaining / remaining_secs / nodes;
        let rate = adaptive_rate.min(flat_rate * self.safety_multiplier);
        self.try_bucket(&deal.id, rate, now_secs)
    }

    /// Try to acquire one impression from the deal's token bucket.
    /// Uses `get_mut` on the common path to avoid String clones.
    fn try_bucket(&self, deal_id: &str, rate: f64, now_secs: f64) -> bool {
        let max_burst = AdaptiveBucket::max_burst(rate, 1.0);

        let acquired = if let Some(mut bucket) = self.buckets.get_mut(deal_id) {
            bucket.try_acquire(1.0, rate, now_secs, max_burst)
        } else {
            self.buckets
                .insert(deal_id.to_owned(), AdaptiveBucket::new(now_secs, 1.0));
            if let Some(mut bucket) = self.buckets.get_mut(deal_id) {
                bucket.try_acquire(1.0, rate, now_secs, max_burst)
            } else {
                false
            }
        };

        if acquired {
            self.reserve(deal_id);
        }
        acquired
    }
}

impl DealPacer for EvenDealPacer {
    fn passes(&self, deal: &Deal) -> bool {
        let goal = match &deal.delivery_goal {
            Some(goal) => goal,
            None => return true,
        };

        // Staleness guard: halt if tracker hasn't updated despite active bidding.
        // Use the goal-appropriate metric so daily deals don't false-trigger
        // after midnight when total stops changing.
        let tracker_imps = match goal {
            DeliveryGoal::Total(_) => self.tracker.total_impressions(&deal.id) as f64,
            DeliveryGoal::Daily(_) => self.tracker.daily_impressions(&deal.id) as f64,
        };
        let now_secs = (self.fine_clock)();
        if let Some(mut entry) = self.staleness.get_mut(&deal.id) {
            if entry.check(tracker_imps, now_secs) {
                warn!(
                    deal = %deal.id,
                    tracker_imps,
                    "Tracker stale — halting deal bids"
                );
                return false;
            }
        } else {
            self.staleness.insert(
                deal.id.clone(),
                TrackerStaleness::new(tracker_imps, now_secs),
            );
        }

        let pass = match goal {
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
                // Hard cap: today's delivered count (tracker + buffered ring)
                let daily_delivered = self.tracker.daily_impressions(&deal.id)
                    + self
                        .rings
                        .get(&deal.id)
                        .map(|r| r.total() as u64)
                        .unwrap_or(0);
                if daily_delivered >= *daily_limit {
                    return false;
                }

                match deal.pacing {
                    Some(DealPacing::Even) => self.even_daily(deal, *daily_limit),
                    Some(DealPacing::Fast) | None => {
                        self.reserve(&deal.id);
                        true
                    }
                }
            }
        };

        if pass {
            if let Some(mut entry) = self.staleness.get_mut(&deal.id) {
                entry.record_bid();
            }
        }

        pass
    }

    fn record_impression(&self, deal_id: &str) {
        self.tracker.record_impression(deal_id);
    }
}

impl EvenDealPacer {
    /// Read-only impression check for the 60s rebuild loop.
    /// Returns a DeliveryState based on current impressions vs delivery goal.
    pub fn impression_state(&self, deal: &Deal) -> DeliveryState {
        let goal = match &deal.delivery_goal {
            Some(goal) => goal,
            None => return DeliveryState::Delivering,
        };

        match goal {
            DeliveryGoal::Total(limit) => {
                if self.effective_imps(&deal.id) >= *limit {
                    DeliveryState::TotalBudgetExhausted
                } else {
                    DeliveryState::Delivering
                }
            }
            DeliveryGoal::Daily(daily_limit) => {
                let daily_delivered = self.tracker.daily_impressions(&deal.id)
                    + self
                        .rings
                        .get(&deal.id)
                        .map(|r| r.total() as u64)
                        .unwrap_or(0);
                if daily_delivered >= *daily_limit {
                    DeliveryState::DailyBudgetExhausted
                } else {
                    DeliveryState::Delivering
                }
            }
        }
    }

    /// Evict pacing state for a deal that is no longer active.
    pub fn evict(&self, id: &str) {
        self.rings.remove(id);
        self.buckets.remove(id);
        self.staleness.remove(id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::pipeline::ortb::direct::pacing::reservation::{
        system_epoch_clock, system_fine_clock,
    };
    use crate::core::models::common::Status;
    use crate::core::models::deal::{DealOwner, DealPricing, DealTargeting, DemandPolicy};
    use crate::core::models::targeting::CommonTargeting;
    use chrono::{DateTime, TimeZone, Utc};
    use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

    struct StubDealTracker {
        initial_imps: u64,
    }

    impl DealImpressionTracker for StubDealTracker {
        fn total_impressions(&self, _deal_id: &str) -> u64 {
            self.initial_imps
        }
        fn daily_impressions(&self, _deal_id: &str) -> u64 {
            self.initial_imps
        }
        fn record_impression(&self, _deal_id: &str) {}
    }

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
        fn daily_impressions(&self, _deal_id: &str) -> u64 {
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

    fn fake_fine_clock(start_micros: u64) -> (FineClock, Arc<AtomicU64>) {
        let epoch = Arc::new(AtomicU64::new(start_micros));
        let e = epoch.clone();
        let clock: FineClock = Arc::new(move || e.load(Relaxed) as f64 / 1_000_000.0);
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
                buyer_ids: vec!["co1".into()],
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
                buyer_ids: vec!["co1".into()],
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
        EvenDealPacer::new(
            tracker,
            Box::new(|| 1),
            system_epoch_clock(),
            system_fine_clock(),
        )
    }

    fn make_pacer_with_clocks(
        initial_imps: u64,
        clock: EpochClock,
        fine_clock: FineClock,
    ) -> EvenDealPacer {
        let tracker = Arc::new(StubDealTracker { initial_imps });
        EvenDealPacer::new(tracker, Box::new(|| 1), clock, fine_clock)
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

    /// Even pacing with a Total goal and end_date rate-limits via bucket.
    #[test]
    fn total_even_with_end_date_rate_limited() {
        let pacer = make_pacer(0);
        let deal = make_deal(
            Some(DeliveryGoal::Total(1000)),
            Some(DealPacing::Even),
            Some(Utc::now() + chrono::Duration::hours(1)),
        );
        assert!(pacer.passes(&deal));
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

    /// Daily even pacing rate-limits via bucket.
    #[test]
    fn daily_even_rate_limited() {
        let pacer = make_pacer(0);
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

    /// After draining bucket tokens, advancing the fine clock refills them
    /// and allows impressions again (continuous refill).
    #[test]
    fn even_daily_tokens_refill_after_advance() {
        let start_epoch: u64 = 1_700_000_000;
        let (clock, coarse_epoch) = fake_clock(start_epoch);
        let (fine_clock, fine_epoch) = fake_fine_clock(start_epoch * 1_000_000);
        let pacer = make_pacer_with_clocks(0, clock, fine_clock);

        let deal = make_deal(
            Some(DeliveryGoal::Daily(10_000)),
            Some(DealPacing::Even),
            None,
        );

        // Drain bucket
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

        // Bucket drained
        assert!(!pacer.passes(&deal));

        // Advance by 30 seconds → tokens refill
        coarse_epoch.store(start_epoch + 30, Relaxed);
        fine_epoch.store((start_epoch + 30) * 1_000_000, Relaxed);
        assert!(
            pacer.passes(&deal),
            "should pass after time advance refills tokens"
        );
    }

    /// As remaining flight time shrinks, the adaptive rate increases (catch-up).
    #[test]
    fn even_total_flight_adapts_as_time_passes() {
        let start_epoch: u64 = 1_700_000_000;
        let (clock, coarse_epoch) = fake_clock(start_epoch);
        let (fine_clock, fine_epoch) = fake_fine_clock(start_epoch * 1_000_000);
        let pacer = make_pacer_with_clocks(0, clock, fine_clock);

        let deal = make_deal_with_dates(
            Some(DeliveryGoal::Total(1000)),
            Some(DealPacing::Even),
            Some(epoch_to_dt(start_epoch - 3600)),
            Some(epoch_to_dt(start_epoch + 3600)),
        );

        let mut passes_early = 0;
        for _ in 0..500 {
            if pacer.passes(&deal) {
                passes_early += 1;
            } else {
                break;
            }
        }

        // Jump to 600s before end
        let late_epoch = start_epoch + 3600 - 600;
        coarse_epoch.store(late_epoch, Relaxed);
        fine_epoch.store(late_epoch * 1_000_000, Relaxed);

        let mut passes_late = 0;
        for _ in 0..500 {
            if pacer.passes(&deal) {
                passes_late += 1;
            } else {
                break;
            }
        }

        assert!(
            passes_late > passes_early,
            "adaptive rate should increase when behind schedule: early={passes_early}, late={passes_late}"
        );
    }

    /// Buffered ring impressions are fully evicted after SLOTS × bucket_width.
    #[test]
    fn ring_eviction_across_windows() {
        let start_epoch: u64 = 1_700_000_000;
        let (clock, coarse_epoch) = fake_clock(start_epoch);
        let (fine_clock, _) = fake_fine_clock(start_epoch * 1_000_000);
        let pacer = make_pacer_with_clocks(0, clock, fine_clock);

        let deal = make_deal(
            Some(DeliveryGoal::Total(100_000)),
            Some(DealPacing::Fast),
            None,
        );

        for _ in 0..5 {
            pacer.passes(&deal);
        }

        let buffered_before = pacer.rings.get("d1").map(|r| r.total() as u64).unwrap_or(0);
        assert!(buffered_before > 0, "should have buffered imps");

        coarse_epoch.store(start_epoch + DEFAULT_BUCKET_SECS * 6, Relaxed);
        let buffered_after = pacer.rings.get("d1").map(|r| r.total() as u64).unwrap_or(0);
        assert_eq!(buffered_after, 0, "ring should be fully evicted");
    }

    // ---- Delivery accuracy, ceiling, & overshoot tests ----

    /// Simulates an entire 2-hour deal flight with even pacing and Total goal.
    #[test]
    fn even_total_delivers_within_tolerance() {
        let start_epoch: u64 = 1_700_000_000;
        let flight_secs: u64 = 7200;
        let step_secs: u64 = 5;
        let goal: u64 = 1000;
        let num_steps = flight_secs / step_secs;

        let (clock, coarse_epoch) = fake_clock(start_epoch);
        let (fine_clock, fine_epoch) = fake_fine_clock(start_epoch * 1_000_000);
        let tracker = Arc::new(SyncDealTracker::new());
        let pacer = EvenDealPacer::new(
            tracker.clone() as Arc<dyn DealImpressionTracker>,
            Box::new(|| 1),
            clock,
            fine_clock,
        );

        let deal = make_deal_with_dates(
            Some(DeliveryGoal::Total(goal)),
            Some(DealPacing::Even),
            Some(epoch_to_dt(start_epoch)),
            Some(epoch_to_dt(start_epoch + flight_secs)),
        );

        let mut total_delivered: u64 = 0;

        for w in 0..num_steps {
            let t = start_epoch + w * step_secs;
            coarse_epoch.store(t, Relaxed);
            fine_epoch.store(t * 1_000_000, Relaxed);

            for _ in 0..1000 {
                if pacer.passes(&deal) {
                    total_delivered += 1;
                }
            }

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

    /// Simulates uneven supply: adaptive rate catches up in supply windows.
    #[test]
    fn even_total_adapts_to_uneven_supply() {
        let start_epoch: u64 = 1_700_000_000;
        let flight_secs: u64 = 7200;
        let step_secs: u64 = 5;
        let goal: u64 = 1000;
        let num_steps = flight_secs / step_secs;

        let (clock, coarse_epoch) = fake_clock(start_epoch);
        let (fine_clock, fine_epoch) = fake_fine_clock(start_epoch * 1_000_000);
        let tracker = Arc::new(SyncDealTracker::new());
        let pacer = EvenDealPacer::new(
            tracker.clone() as Arc<dyn DealImpressionTracker>,
            Box::new(|| 1),
            clock,
            fine_clock,
        );

        let deal = make_deal_with_dates(
            Some(DeliveryGoal::Total(goal)),
            Some(DealPacing::Even),
            Some(epoch_to_dt(start_epoch)),
            Some(epoch_to_dt(start_epoch + flight_secs)),
        );

        let mut total_delivered: u64 = 0;

        for w in 0..num_steps {
            let t = start_epoch + w * step_secs;
            coarse_epoch.store(t, Relaxed);
            fine_epoch.store(t * 1_000_000, Relaxed);

            let supply = if w % 2 == 0 { 500 } else { 0 };

            for _ in 0..supply {
                if pacer.passes(&deal) {
                    total_delivered += 1;
                }
            }

            tracker.set_total(total_delivered);
        }

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
    /// With rate-based bucket: initial tokens = max_burst = rate * BURST_SECS.
    /// The safety-capped rate limits how many tokens are initially available.
    #[test]
    fn safety_cap_limits_catchup_rate() {
        let start_epoch: u64 = 1_700_000_000;
        let flight_secs: u64 = 7200;
        let goal: u64 = 1200;

        let (clock, coarse_epoch) = fake_clock(start_epoch);
        let (fine_clock, fine_epoch) = fake_fine_clock(start_epoch * 1_000_000);
        let tracker = Arc::new(SyncDealTracker::new());
        let pacer = EvenDealPacer::new(
            tracker.clone() as Arc<dyn DealImpressionTracker>,
            Box::new(|| 1),
            clock,
            fine_clock,
        );

        let deal = make_deal_with_dates(
            Some(DeliveryGoal::Total(goal)),
            Some(DealPacing::Even),
            Some(epoch_to_dt(start_epoch)),
            Some(epoch_to_dt(start_epoch + flight_secs)),
        );

        // Jump to window 100 (1200s remaining out of 7200s)
        let late_epoch = start_epoch + 6000;
        coarse_epoch.store(late_epoch, Relaxed);
        fine_epoch.store(late_epoch * 1_000_000, Relaxed);

        let mut imps = 0u64;
        for _ in 0..200 {
            if pacer.passes(&deal) {
                imps += 1;
            }
        }

        // flat_rate = 1200 / 7200 = 0.1667 imps/sec
        // safety-capped rate = flat_rate * 3 = 0.5 imps/sec
        // max_burst = 0.5 * 5.0 = 2.5, floored at 1.0
        // So initial tokens are at most 2.5 → we can acquire at most ~2 imps instantly
        // This verifies the safety cap prevents burst flooding
        let flat_rate_per_sec = goal as f64 / flight_secs as f64;
        let capped_rate = flat_rate_per_sec * DEFAULT_SAFETY_MULTIPLIER;
        let max_burst_tokens = AdaptiveBucket::max_burst(capped_rate, 1.0);

        assert!(
            imps <= max_burst_tokens.ceil() as u64 + 1,
            "safety cap should limit initial burst: max_burst={max_burst_tokens}, got {imps}"
        );
        assert!(imps >= 1, "should allow at least 1 imp");
    }

    /// Verifies the hard impression cap prevents overshoot.
    #[test]
    fn hard_cap_prevents_overshoot() {
        let start_epoch: u64 = 1_700_000_000;
        let (clock, _) = fake_clock(start_epoch);
        let (fine_clock, _) = fake_fine_clock(start_epoch * 1_000_000);

        let tracker = Arc::new(StubDealTracker { initial_imps: 995 });
        let pacer = EvenDealPacer::new(tracker, Box::new(|| 1), clock, fine_clock);

        let deal = make_deal_with_dates(
            Some(DeliveryGoal::Total(1000)),
            Some(DealPacing::Fast),
            Some(epoch_to_dt(start_epoch - 3600)),
            None,
        );

        let mut total_allowed = 0u64;
        for _ in 0..1000 {
            if pacer.passes(&deal) {
                total_allowed += 1;
            }
        }

        assert_eq!(total_allowed, 5, "should allow exactly 5 imps to reach cap");
    }
}
