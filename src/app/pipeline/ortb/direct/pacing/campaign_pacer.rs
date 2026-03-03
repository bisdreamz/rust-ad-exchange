use super::bucket::{AdaptiveBucket, TrackerStaleness};
use super::reservation::{DEFAULT_BUCKET_SECS, EpochClock, FineClock, ReservationRing};
use super::{SpendPacer, SpendTracker};
use crate::core::cluster::ClusterDiscovery;
use crate::core::models::campaign::{BudgetType, Campaign, CampaignPacing};
use dashmap::DashMap;
use std::sync::Arc;
use tracing::{trace, warn};

const SECS_PER_DAY: u64 = 86400;

/// Rate is clamped to at most this many times the flat even rate,
/// preventing burst flooding when catching up near end-of-flight
/// or end-of-day.
const SAFETY_MULTIPLIER: f64 = 3.0;

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
    buckets: DashMap<String, AdaptiveBucket>,
    staleness: DashMap<String, TrackerStaleness>,
    cluster: Arc<dyn ClusterDiscovery>,
    clock: EpochClock,
    fine_clock: FineClock,
}

impl CampaignSpendPacer {
    pub fn new(
        tracker: Arc<dyn SpendTracker>,
        cluster: Arc<dyn ClusterDiscovery>,
        clock: EpochClock,
        fine_clock: FineClock,
    ) -> Self {
        Self {
            tracker,
            rings: DashMap::new(),
            buckets: DashMap::new(),
            staleness: DashMap::new(),
            cluster,
            clock,
            fine_clock,
        }
    }

    /// Evict pacing state for a campaign that is no longer active.
    pub fn evict(&self, id: &str) {
        self.rings.remove(id);
        self.buckets.remove(id);
        self.staleness.remove(id);
    }

    fn reserve(&self, campaign_id: &str, price: f64) {
        if let Some(ring) = self.rings.get_mut(campaign_id) {
            ring.reserve(price);
        } else {
            let ring = ReservationRing::new(DEFAULT_BUCKET_SECS, self.clock.clone());
            ring.reserve(price);
            self.rings.insert(campaign_id.to_owned(), ring);
        }
    }

    /// Compute the token-bucket refill rate in **CPM per second**.
    ///
    /// `spent_cpm` is the total CPM sum spent so far (tracker + ring).
    /// The budget (real dollars) is multiplied by 1000 to convert to
    /// CPM-space, keeping all hot-path arithmetic in a single unit.
    fn compute_rate(&self, campaign: &Campaign, spent_cpm: f64, tz_offset: Option<i8>) -> f64 {
        let budget_cpm = campaign.budget * 1000.0;
        let remaining_cpm = (budget_cpm - spent_cpm).max(0.0);
        let now_epoch = (self.clock)() as i64;
        let nodes = self.cluster.cluster_size().max(1) as f64;

        let (remaining_secs, flat_rate) = match campaign.budget_type {
            BudgetType::Total => {
                let remaining_secs = (campaign.end_date.timestamp() - now_epoch).max(1) as f64;
                let total_secs =
                    (campaign.end_date.timestamp() - campaign.start_date.timestamp()).max(1) as f64;
                let flat_rate = budget_cpm / total_secs / nodes;
                (remaining_secs, flat_rate)
            }
            BudgetType::Daily => {
                let remaining_secs =
                    (SECS_PER_DAY as i64 - (now_epoch % SECS_PER_DAY as i64)).max(1) as f64;
                let flat_rate = budget_cpm / SECS_PER_DAY as f64 / nodes;
                (remaining_secs, flat_rate)
            }
        };

        let adaptive_rate = remaining_cpm / remaining_secs / nodes;
        let rate = adaptive_rate.min(flat_rate * SAFETY_MULTIPLIER);

        match tz_offset {
            Some(tz) => rate * daypart_weight(now_epoch, tz),
            None => rate,
        }
    }

    fn rate_check(
        &self,
        campaign: &Campaign,
        spent: f64,
        price: f64,
        tz_offset: Option<i8>,
    ) -> bool {
        let now_secs = (self.fine_clock)();
        let rate = self.compute_rate(campaign, spent, tz_offset);
        let max_burst = AdaptiveBucket::max_burst(rate, price);

        let acquired = if let Some(mut bucket) = self.buckets.get_mut(&campaign.id) {
            bucket.try_acquire(price, rate, now_secs, max_burst)
        } else {
            // Cold start — seed with a single bid's worth
            self.buckets
                .insert(campaign.id.clone(), AdaptiveBucket::new(now_secs, price));
            // The seeded token covers this first bid
            if let Some(mut bucket) = self.buckets.get_mut(&campaign.id) {
                bucket.try_acquire(price, rate, now_secs, max_burst)
            } else {
                false
            }
        };

        if acquired {
            self.reserve(&campaign.id, price);
        }
        acquired
    }
}

impl SpendPacer for CampaignSpendPacer {
    /// `price` is a **CPM value** (e.g. $5.0 CPM). The pacer works entirely
    /// in CPM-space: tracker returns CPM sums, ring stores CPM values,
    /// bucket tokens are CPM. The only dollar conversion is the budget
    /// comparison: `spent_cpm + price_cpm > budget_dollars * 1000`.
    fn passes(&self, campaign: &Campaign, price: f64) -> bool {
        // Read tracker once — returns CPM sum
        let tracker_cpm = match campaign.budget_type {
            BudgetType::Total => self.tracker.total_spend(&campaign.id),
            BudgetType::Daily => self.tracker.daily_spend(&campaign.id),
        };
        let buffered_cpm = self
            .rings
            .get(&campaign.id)
            .map(|r| r.total())
            .unwrap_or(0.0);
        let spent_cpm = tracker_cpm + buffered_cpm;

        // Hard budget cap — convert CPM sum to dollars for comparison
        if (spent_cpm + price) / 1000.0 > campaign.budget {
            trace!(
                campaign = %campaign.id,
                spent_dollars = spent_cpm / 1000.0,
                price_cpm = price,
                budget = campaign.budget,
                "Budget cap reached"
            );
            return false;
        }

        // Staleness guard: if tracker CPM sum hasn't changed despite ongoing
        // bid approvals, Firestore may be down — halt to prevent untracked spend.
        let now_secs = (self.fine_clock)();
        if let Some(mut entry) = self.staleness.get_mut(&campaign.id) {
            if entry.check(tracker_cpm, now_secs) {
                warn!(
                    campaign = %campaign.id,
                    tracker_cpm,
                    bids_untracked = entry.bids_since_change,
                    "Tracker stale — halting bids to prevent untracked spend"
                );
                return false;
            }
        } else {
            self.staleness.insert(
                campaign.id.clone(),
                TrackerStaleness::new(tracker_cpm, now_secs),
            );
        }

        // price is CPM — ring, bucket, and rate all operate in CPM-space
        let pass = match &campaign.pacing {
            CampaignPacing::Fast => {
                self.reserve(&campaign.id, price);
                true
            }
            CampaignPacing::Even => self.rate_check(campaign, spent_cpm, price, None),
            CampaignPacing::WeightedEven { tz_offset } => {
                self.rate_check(campaign, spent_cpm, price, Some(*tz_offset))
            }
        };

        if pass {
            if let Some(mut entry) = self.staleness.get_mut(&campaign.id) {
                entry.record_bid();
            }
        } else {
            trace!(
                campaign = %campaign.id,
                pacing = ?campaign.pacing,
                spent_dollars = spent_cpm / 1000.0,
                price_cpm = price,
                "Rate pacing limit reached"
            );
        }

        pass
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::pipeline::ortb::direct::pacing::reservation::{
        system_epoch_clock, system_fine_clock,
    };
    use crate::core::models::campaign::{CampaignTargeting, PricingStrategy};
    use crate::core::models::common::Status;
    use async_trait::async_trait;
    use chrono::{DateTime, TimeZone, Utc};
    use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

    /// Precision multiplier matching the real tracker implementations.
    const MICROS: f64 = 1_000_000.0;

    /// Stub tracker returning a fixed CPM sum.
    struct StubTracker {
        /// Fixed CPM sum returned by total/daily_spend.
        initial_cpm: f64,
    }

    impl SpendTracker for StubTracker {
        fn total_spend(&self, _campaign_id: &str) -> f64 {
            self.initial_cpm
        }
        fn daily_spend(&self, _campaign_id: &str) -> f64 {
            self.initial_cpm
        }
        fn record_spend(&self, _campaign_id: &str, _amount: f64) {}
    }

    /// Tracker whose CPM sum can be set externally via `set_cpm_total`.
    struct SyncTracker {
        cpm_micros: AtomicU64,
    }

    impl SyncTracker {
        fn new() -> Self {
            Self {
                cpm_micros: AtomicU64::new(0),
            }
        }
        /// Set the CPM sum the tracker returns (e.g. after N impressions
        /// at $X CPM, this is N * X).
        fn set_cpm_total(&self, cpm_sum: f64) {
            self.cpm_micros.store((cpm_sum * MICROS) as u64, Relaxed);
        }
    }

    impl SpendTracker for SyncTracker {
        fn total_spend(&self, _campaign_id: &str) -> f64 {
            self.cpm_micros.load(Relaxed) as f64 / MICROS
        }
        fn daily_spend(&self, _campaign_id: &str) -> f64 {
            self.cpm_micros.load(Relaxed) as f64 / MICROS
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

    fn fake_fine_clock(start_micros: u64) -> (FineClock, Arc<AtomicU64>) {
        let epoch = Arc::new(AtomicU64::new(start_micros));
        let e = epoch.clone();
        let clock: FineClock = Arc::new(move || e.load(Relaxed) as f64 / 1_000_000.0);
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
            buyer_id: "co1".into(),
            id: "c1".into(),
            start_date: start,
            end_date: end,
            name: "test".into(),
            pacing,
            budget,
            budget_type: BudgetType::Total,
            strategy: PricingStrategy::FixedPrice(5.0),
            advertiser_id: "adv1".into(),
            targeting: CampaignTargeting::default(),
        }
    }

    fn campaign(pacing: CampaignPacing, budget: f64) -> Campaign {
        Campaign {
            status: Status::Active,
            buyer_id: "co1".into(),
            id: "c1".into(),
            start_date: Utc::now() - chrono::Duration::hours(1),
            end_date: Utc::now() + chrono::Duration::hours(1),
            name: "test".into(),
            pacing,
            budget,
            budget_type: BudgetType::Total,
            strategy: PricingStrategy::FixedPrice(5.0),
            advertiser_id: "adv1".into(),
            targeting: CampaignTargeting::default(),
        }
    }

    /// Fast pacing passes bids until the ring CPM sum + tracker reaches the budget cap.
    /// Budget $10, price $5 CPM → per-imp cost $0.005, allows 2000 impressions.
    /// Ring accumulates CPM: 2000 × 5.0 = 10000 CPM → $10 real spend.
    #[test]
    fn fast_passes_until_budget_gone() {
        let tracker = Arc::new(StubTracker { initial_cpm: 0.0 });
        let cluster = Arc::new(StubCluster(1));
        let pacer =
            CampaignSpendPacer::new(tracker, cluster, system_epoch_clock(), system_fine_clock());

        // budget $10, $5 CPM → 10 * 1000 / 5 = 2000 impressions
        let c = campaign(CampaignPacing::Fast, 10.0);
        let mut passed = 0u32;
        for _ in 0..3000 {
            if pacer.passes(&c, 5.0) {
                passed += 1;
            }
        }
        assert_eq!(passed, 2000, "should allow exactly 2000 impressions");
    }

    /// Even pacing allows the first bid (bucket seeded with price).
    #[test]
    fn even_within_window_passes() {
        let tracker = Arc::new(StubTracker { initial_cpm: 0.0 });
        let cluster = Arc::new(StubCluster(1));
        let pacer =
            CampaignSpendPacer::new(tracker, cluster, system_epoch_clock(), system_fine_clock());

        let c = campaign(CampaignPacing::Even, 1000.0);
        assert!(pacer.passes(&c, 5.0));
    }

    /// Even pacing rejects once tokens are drained by sustained rapid bidding.
    /// Rate is in CPM/sec, bucket tokens are CPM. First bid seeded, then drains.
    #[test]
    fn even_bucket_drains_and_rejects() {
        let start_epoch: u64 = 1_700_000_000;
        let (clock, _) = fake_clock(start_epoch);
        let (fine_clock, _) = fake_fine_clock(start_epoch * 1_000_000);

        let tracker = Arc::new(StubTracker { initial_cpm: 0.0 });
        let cluster = Arc::new(StubCluster(1));
        let pacer = CampaignSpendPacer::new(tracker, cluster, clock, fine_clock);

        let c = campaign_with_dates(
            CampaignPacing::Even,
            1000.0,
            epoch_to_dt(start_epoch - 3600),
            epoch_to_dt(start_epoch + 3600),
        );

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
    }

    /// Hard budget cap rejects any bid that would exceed lifetime budget.
    /// Tracker returns CPM sum: budget $1000, tracker CPM 999_995 → $999.995 spent.
    /// Next $5 CPM bid → ($999_995 + $5) / 1000 = $1000 > $1000 budget → rejected.
    #[test]
    fn budget_exceeded_rejected() {
        // CPM sum of 999_995.0 → $999.995 real spend. Next $5 CPM → exactly $1000.
        let tracker = Arc::new(StubTracker {
            initial_cpm: 999_995.0,
        });
        let cluster = Arc::new(StubCluster(1));
        let pacer =
            CampaignSpendPacer::new(tracker, cluster, system_epoch_clock(), system_fine_clock());

        let c = campaign(CampaignPacing::Fast, 1000.0);
        // (999_995 + 5) / 1000 = 1000.0, which is NOT > 1000.0, so this passes
        assert!(pacer.passes(&c, 5.0));
        // Now ring has 5.0 CPM → total = 1000000 / 1000 = 1000.0 + next = 1000.005 > 1000 → rejected
        assert!(!pacer.passes(&c, 5.0));
    }

    /// After draining bucket tokens, advancing the fine clock refills them
    /// and allows bids again (continuous refill, not window reset).
    #[test]
    fn even_tokens_refill_after_time_advance() {
        let start_epoch: u64 = 1_700_000_000;
        let (clock, coarse_epoch) = fake_clock(start_epoch);
        let (fine_clock, fine_epoch) = fake_fine_clock(start_epoch * 1_000_000);

        let tracker = Arc::new(StubTracker { initial_cpm: 0.0 });
        let cluster = Arc::new(StubCluster(1));
        let pacer = CampaignSpendPacer::new(tracker, cluster, clock, fine_clock);

        let c = campaign_with_dates(
            CampaignPacing::Even,
            1000.0,
            epoch_to_dt(start_epoch - 3600),
            epoch_to_dt(start_epoch + 3600),
        );

        // Drain the bucket
        let mut passes = 0;
        for _ in 0..5000 {
            if pacer.passes(&c, 5.0) {
                passes += 1;
            } else {
                break;
            }
        }
        assert!(passes >= 1, "should pass at least once");

        // Bucket drained — next bid fails
        assert!(!pacer.passes(&c, 5.0));

        // Advance both clocks by 30 seconds → tokens refill
        coarse_epoch.store(start_epoch + 30, Relaxed);
        fine_epoch.store((start_epoch + 30) * 1_000_000, Relaxed);
        assert!(
            pacer.passes(&c, 5.0),
            "should pass after time advance refills tokens"
        );
    }

    /// As remaining flight time shrinks, the rate increases,
    /// allowing more tokens per second. Rate is in CPM/sec.
    #[test]
    fn budget_target_adapts_over_time() {
        let start_epoch: u64 = 1_700_000_000;
        let (clock, coarse_epoch) = fake_clock(start_epoch);
        let (fine_clock, fine_epoch) = fake_fine_clock(start_epoch * 1_000_000);

        let tracker = Arc::new(StubTracker { initial_cpm: 0.0 });
        let cluster = Arc::new(StubCluster(1));
        let pacer = CampaignSpendPacer::new(tracker, cluster, clock, fine_clock);

        let c = campaign_with_dates(
            CampaignPacing::Even,
            1000.0,
            epoch_to_dt(start_epoch - 3600),
            epoch_to_dt(start_epoch + 3600),
        );

        // Count passes at start (3600s remaining)
        // rate_cpm = 1000*1000 / 7200 / 1 = 138.9 CPM/sec
        // max_burst = 138.9 * 5 = 694.4, initial tokens = 1.0 (price)
        let mut passes_early = 0;
        for _ in 0..2000 {
            if pacer.passes(&c, 1.0) {
                passes_early += 1;
            } else {
                break;
            }
        }

        // Jump to 600s before end — rate increases (remaining time shrinks)
        // Bucket refills from elapsed time at the new (higher) rate
        let late_epoch = start_epoch + 3600 - 600;
        coarse_epoch.store(late_epoch, Relaxed);
        fine_epoch.store(late_epoch * 1_000_000, Relaxed);

        let mut passes_late = 0;
        for _ in 0..2000 {
            if pacer.passes(&c, 1.0) {
                passes_late += 1;
            } else {
                break;
            }
        }

        assert!(
            passes_late > passes_early,
            "rate should increase as remaining time shrinks: early={passes_early}, late={passes_late}"
        );
    }

    // ---- Delivery accuracy & overshoot tests ----

    /// Runs a full flight simulation. Steps through time, offers unlimited
    /// supply each step (breaks on first rejection since the bucket won't
    /// refill until time advances). Returns total CPM sum.
    fn simulate_flight(
        budget: f64,
        bid_price_cpm: f64,
        flight_secs: u64,
        step_secs: u64,
        supply_fn: impl Fn(u64) -> bool,
    ) -> f64 {
        let start_epoch: u64 = 1_700_000_000;
        let num_steps = flight_secs / step_secs;

        let (clock, coarse_epoch) = fake_clock(start_epoch);
        let (fine_clock, fine_epoch) = fake_fine_clock(start_epoch * 1_000_000);
        let tracker = Arc::new(SyncTracker::new());
        let cluster = Arc::new(StubCluster(1));
        let pacer = CampaignSpendPacer::new(tracker.clone(), cluster, clock, fine_clock);

        let c = campaign_with_dates(
            CampaignPacing::Even,
            budget,
            epoch_to_dt(start_epoch),
            epoch_to_dt(start_epoch + flight_secs),
        );

        let mut cpm_sum = 0.0;

        for w in 0..num_steps {
            let t = start_epoch + w * step_secs;
            coarse_epoch.store(t, Relaxed);
            fine_epoch.store(t * 1_000_000, Relaxed);

            if !supply_fn(w) {
                continue;
            }

            // Drain the bucket — break on first rejection since tokens
            // won't refill until the next time step.
            loop {
                if pacer.passes(&c, bid_price_cpm) {
                    cpm_sum += bid_price_cpm;
                } else {
                    break;
                }
            }

            tracker.set_cpm_total(cpm_sum);
        }

        cpm_sum
    }

    /// Simulates an entire 2-hour campaign flight with even pacing.
    /// Budget $1000, $1 CPM → expect ~1,000,000 impressions.
    #[test]
    fn even_delivers_full_budget_over_flight() {
        let budget = 1000.0;
        let cpm_sum = simulate_flight(budget, 1.0, 7200, 5, |_| true);
        let real_spend = cpm_sum / 1000.0;

        assert!(
            real_spend >= budget * 0.90,
            "should deliver at least 90% of budget: spent ${real_spend:.2}, budget ${budget}"
        );
        assert!(
            real_spend <= budget * 1.05,
            "should not overshoot by more than 5%: spent ${real_spend:.2}, budget ${budget}"
        );
    }

    /// Same delivery test with a larger budget ($50k) and higher CPM ($10).
    #[test]
    fn even_delivers_large_budget_over_flight() {
        let budget = 50_000.0;
        let cpm_sum = simulate_flight(budget, 10.0, 7200, 5, |_| true);
        let real_spend = cpm_sum / 1000.0;

        assert!(
            real_spend >= budget * 0.90,
            "should deliver at least 90%: spent ${real_spend:.2}, budget ${budget}"
        );
        assert!(
            real_spend <= budget * 1.05,
            "should not overshoot by more than 5%: spent ${real_spend:.2}, budget ${budget}"
        );
    }

    /// Simulates uneven supply: some intervals have many bids, others
    /// have none. The adaptive rate catches up when supply returns.
    #[test]
    fn even_adapts_to_uneven_supply() {
        let budget = 1000.0;
        let cpm_sum = simulate_flight(budget, 1.0, 7200, 5, |w| w % 2 == 0);
        let real_spend = cpm_sum / 1000.0;

        assert!(
            real_spend >= budget * 0.70,
            "should deliver at least 70% of budget with uneven supply: spent ${real_spend:.2}"
        );
        assert!(
            real_spend <= budget * 1.05,
            "should not overshoot: spent ${real_spend:.2}, budget ${budget}"
        );
    }

    /// Verifies the hard budget cap prevents overshoot in single-threaded use.
    /// Budget $1000, tracker CPM sum = 999_000 (= $999 real spend).
    /// At $1 CPM, each bid costs $0.001 real spend. Remaining = $1.00 → 1000 bids.
    #[test]
    fn hard_cap_prevents_overshoot() {
        let start_epoch: u64 = 1_700_000_000;
        let (clock, _) = fake_clock(start_epoch);
        let (fine_clock, _) = fake_fine_clock(start_epoch * 1_000_000);

        // CPM sum 999_000 → $999 real spend. $1 remaining.
        let tracker = Arc::new(StubTracker {
            initial_cpm: 999_000.0,
        });
        let cluster = Arc::new(StubCluster(1));
        let pacer = CampaignSpendPacer::new(tracker, cluster, clock, fine_clock);

        let c = campaign_with_dates(
            CampaignPacing::Fast,
            1000.0,
            epoch_to_dt(start_epoch - 3600),
            epoch_to_dt(start_epoch + 3600),
        );

        // At $1 CPM, each bid adds 1.0 to CPM sum → $0.001 real spend.
        // $1 remaining / $0.001 per imp = 1000 impressions.
        let mut total_allowed = 0u32;
        for _ in 0..2000 {
            if pacer.passes(&c, 1.0) {
                total_allowed += 1;
            }
        }

        assert_eq!(
            total_allowed, 1000,
            "should allow exactly 1000 bids to reach budget cap"
        );
    }
}
