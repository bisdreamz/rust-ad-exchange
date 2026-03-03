/// How long (seconds) a tracker value can remain unchanged while bids are
/// being approved before we consider the tracker stale.
///
/// Set to 5 minutes (300s). The billing pipeline flushes spend to Firestore
/// roughly once per minute, and Firestore snapshot listeners propagate
/// changes back to all nodes within seconds. Under normal operation the
/// tracker value should update well within this window. 5x the flush
/// cadence gives margin for GC pauses, transient network hiccups, and
/// Firestore replication lag — while still catching a downed Firestore
/// before significant untracked spend accumulates.
const STALENESS_TIMEOUT_SECS: f64 = 300.0;

/// How long (seconds) after staleness triggers before the guard
/// auto-resets and allows bids again. Gives the system a retry
/// window — if the tracker is still broken, staleness will
/// re-trigger after another round of MIN_BIDS + TIMEOUT.
const STALENESS_RESET_SECS: f64 = 600.0;

/// Minimum number of approved bids before staleness can trigger.
///
/// Set high (10k) because low-fill campaigns may legitimately
/// approve thousands of bids with zero wins — the tracker
/// correctly stays at 0 and should not be flagged as stale.
/// At 10k approved bids, even a 0.1% fill rate produces ~10
/// wins, enough for multiple billing flush cycles.
const STALENESS_MIN_BIDS: u32 = 10_000;

/// Seconds of rate to accumulate as burst capacity.
/// 5s gives a small initial token pool on cold start; the safety
/// multiplier (3× flat_rate) already handles catch-up after idle.
const BURST_SECS: f64 = 5.0;

pub struct AdaptiveBucket {
    tokens: f64,
    last_refill: f64,
}

impl AdaptiveBucket {
    pub fn new(now_secs: f64, initial_tokens: f64) -> Self {
        Self {
            tokens: initial_tokens,
            last_refill: now_secs,
        }
    }

    /// Refill based on elapsed time, attempt to deduct `cost`.
    /// `rate`: units per second (dollars/sec or imps/sec).
    /// `max_burst`: cap on accumulated tokens (prevents dump after idle).
    pub fn try_acquire(&mut self, cost: f64, rate: f64, now_secs: f64, max_burst: f64) -> bool {
        let elapsed = (now_secs - self.last_refill).max(0.0);
        self.last_refill = now_secs;
        self.tokens = (self.tokens + elapsed * rate).min(max_burst);

        if self.tokens >= cost {
            self.tokens -= cost;
            true
        } else {
            false
        }
    }

    /// Compute max_burst from rate. Floor at `min_cost` so a single
    /// acquire can always pass if the rate supports it.
    pub fn max_burst(rate: f64, min_cost: f64) -> f64 {
        (rate * BURST_SECS).max(min_cost)
    }
}

/// Detects when an external tracker value (spend or impressions) stops
/// updating despite this node continuing to approve bids.
///
/// # Problem
///
/// Pacers rely on tracker values (from Firestore listeners) to know how
/// much budget has been consumed. If the listener silently disconnects or
/// Firestore goes down, the tracker returns stale data. The pacer sees
/// "plenty of budget remaining" and keeps approving bids — spending money
/// that will never be tracked until the connection recovers.
///
/// # How it works
///
/// Each call to [`check()`] compares the current tracker value against the
/// last known value:
/// - **Value changed** → tracker is healthy, reset counters.
/// - **Value unchanged + enough bids + enough time** → tracker is stale,
///   return `true` to signal the pacer should halt.
///
/// # Self-healing
///
/// The moment the tracker value changes (Firestore reconnects, flush
/// arrives), `check()` resets and bidding resumes automatically. No
/// manual intervention or restart required.
///
/// # Usage
///
/// Used by both `CampaignSpendPacer` (tracks spend in dollars) and
/// `EvenDealPacer` (tracks impressions cast to f64). Each entity gets
/// its own `TrackerStaleness` instance in a `DashMap<String, _>`.
pub struct TrackerStaleness {
    /// Last tracker value we observed (spend dollars or impression count).
    last_value: f64,
    /// Timestamp (epoch seconds, f64) when `last_value` last changed.
    last_change_secs: f64,
    /// Number of bids approved since the last observed tracker change.
    pub(crate) bids_since_change: u32,
}

impl TrackerStaleness {
    pub fn new(value: f64, now_secs: f64) -> Self {
        Self {
            last_value: value,
            last_change_secs: now_secs,
            bids_since_change: 0,
        }
    }

    /// Feed the latest tracker value and return `true` if the tracker
    /// appears stale (unchanged for too long despite active bidding).
    ///
    /// Call this once per `passes()` invocation, *before* the pacing
    /// decision. If it returns `true`, the caller should reject the bid
    /// and log a warning.
    ///
    /// Auto-resets after `STALENESS_RESET_SECS` beyond the trigger
    /// point, allowing bids to flow again. If the tracker is still
    /// broken, staleness will re-trigger after another round of
    /// `MIN_BIDS` + `TIMEOUT`.
    pub fn check(&mut self, value: f64, now_secs: f64) -> bool {
        if (value - self.last_value).abs() > f64::EPSILON {
            self.last_value = value;
            self.last_change_secs = now_secs;
            self.bids_since_change = 0;
            return false;
        }

        let elapsed = now_secs - self.last_change_secs;

        if self.bids_since_change >= STALENESS_MIN_BIDS && elapsed > STALENESS_TIMEOUT_SECS {
            // Stale — but auto-reset after the reset window so the
            // system retries. If still broken it'll re-trigger.
            if elapsed > STALENESS_TIMEOUT_SECS + STALENESS_RESET_SECS {
                self.last_change_secs = now_secs;
                self.bids_since_change = 0;
                return false;
            }
            return true;
        }

        false
    }

    /// Record that a bid was approved. Call after the pacing decision
    /// returns `true` so that the bid count reflects real approvals,
    /// not just queries.
    pub fn record_bid(&mut self) {
        self.bids_since_change += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_refill() {
        let mut b = AdaptiveBucket::new(0.0, 0.0);
        // 2 seconds elapsed at rate 5.0/s → 10.0 tokens added
        let ok = b.try_acquire(0.0, 5.0, 2.0, 100.0);
        assert!(ok);
        assert!((b.tokens - 10.0).abs() < 1e-9);
    }

    #[test]
    fn acquire_deducts_and_returns_true() {
        let mut b = AdaptiveBucket::new(0.0, 10.0);
        assert!(b.try_acquire(3.0, 0.0, 0.0, 10.0));
        assert!((b.tokens - 7.0).abs() < 1e-9);
    }

    #[test]
    fn insufficient_tokens_returns_false() {
        let mut b = AdaptiveBucket::new(0.0, 2.0);
        assert!(!b.try_acquire(5.0, 0.0, 0.0, 10.0));
        // Tokens unchanged on failure
        assert!((b.tokens - 2.0).abs() < 1e-9);
    }

    #[test]
    fn burst_cap_limits_accumulation() {
        let mut b = AdaptiveBucket::new(0.0, 0.0);
        // 1000 seconds elapsed at rate 10.0 → would be 10000, capped at 50
        let max_burst = AdaptiveBucket::max_burst(10.0, 1.0); // 50.0
        b.try_acquire(0.0, 10.0, 1000.0, max_burst);
        assert!((b.tokens - 50.0).abs() < 1e-9);
    }

    #[test]
    fn zero_elapsed_no_refill() {
        let mut b = AdaptiveBucket::new(5.0, 3.0);
        b.try_acquire(0.0, 100.0, 5.0, 1000.0);
        assert!((b.tokens - 3.0).abs() < 1e-9);
    }

    #[test]
    fn sub_second_refill() {
        let mut b = AdaptiveBucket::new(0.0, 0.0);
        // 0.5s at rate 2.0/s → 1.0 token
        b.try_acquire(0.0, 2.0, 0.5, 100.0);
        assert!((b.tokens - 1.0).abs() < 1e-9);
    }

    #[test]
    fn drain_and_recover() {
        let mut b = AdaptiveBucket::new(0.0, 5.0);
        // Drain all tokens
        assert!(b.try_acquire(5.0, 0.0, 0.0, 10.0));
        assert!(!b.try_acquire(1.0, 0.0, 0.0, 10.0));
        // Refill after 1 second at rate 3.0
        assert!(b.try_acquire(2.0, 3.0, 1.0, 10.0));
        assert!((b.tokens - 1.0).abs() < 1e-9); // 0 + 3 - 2 = 1
    }

    #[test]
    fn idle_period_capped_at_max_burst() {
        let mut b = AdaptiveBucket::new(0.0, 0.0);
        let max_burst = AdaptiveBucket::max_burst(2.0, 1.0); // 10.0
        // Idle for 1 hour
        b.try_acquire(0.0, 2.0, 3600.0, max_burst);
        assert!((b.tokens - max_burst).abs() < 1e-9);
    }

    #[test]
    fn initial_tokens_immediately_usable() {
        let mut b = AdaptiveBucket::new(0.0, 7.0);
        assert!(b.try_acquire(7.0, 0.0, 0.0, 10.0));
        assert!((b.tokens).abs() < 1e-9);
    }

    #[test]
    fn max_burst_floor_at_min_cost() {
        // rate * BURST_SECS = 0.01 * 5 = 0.05, which is < min_cost of 1.0
        let mb = AdaptiveBucket::max_burst(0.01, 1.0);
        assert!((mb - 1.0).abs() < 1e-9);
    }

    // ---- TrackerStaleness tests ----

    #[test]
    fn value_change_resets() {
        let mut ts = TrackerStaleness::new(0.0, 0.0);
        // Record enough bids and time to trigger staleness
        for _ in 0..20 {
            ts.record_bid();
        }
        // Value changes → not stale, counters reset
        assert!(!ts.check(5.0, 500.0));
        assert_eq!(ts.bids_since_change, 0);
        assert!((ts.last_value - 5.0).abs() < f64::EPSILON);
    }

    #[test]
    fn stale_after_timeout() {
        let mut ts = TrackerStaleness::new(10.0, 0.0);
        for _ in 0..STALENESS_MIN_BIDS {
            ts.record_bid();
        }
        // Enough bids + past timeout → stale
        assert!(ts.check(10.0, STALENESS_TIMEOUT_SECS + 1.0));
    }

    #[test]
    fn below_min_bids_not_stale() {
        let mut ts = TrackerStaleness::new(10.0, 0.0);
        for _ in 0..STALENESS_MIN_BIDS - 1 {
            ts.record_bid();
        }
        // Under min bids even after timeout → not stale
        assert!(!ts.check(10.0, STALENESS_TIMEOUT_SECS + 1.0));
    }

    #[test]
    fn below_timeout_not_stale() {
        let mut ts = TrackerStaleness::new(10.0, 0.0);
        for _ in 0..STALENESS_MIN_BIDS {
            ts.record_bid();
        }
        // Enough bids but within timeout → not stale
        assert!(!ts.check(10.0, STALENESS_TIMEOUT_SECS - 1.0));
    }

    #[test]
    fn recovers_when_value_changes() {
        let mut ts = TrackerStaleness::new(10.0, 0.0);
        for _ in 0..STALENESS_MIN_BIDS {
            ts.record_bid();
        }
        // Confirm it's stale
        assert!(ts.check(10.0, STALENESS_TIMEOUT_SECS + 1.0));

        // Value changes → recovers
        assert!(!ts.check(15.0, STALENESS_TIMEOUT_SECS + 2.0));
        // And subsequent checks with new value are also not stale
        assert!(!ts.check(15.0, STALENESS_TIMEOUT_SECS + 3.0));
    }

    #[test]
    fn auto_resets_after_reset_window() {
        let mut ts = TrackerStaleness::new(10.0, 0.0);
        for _ in 0..STALENESS_MIN_BIDS {
            ts.record_bid();
        }

        // Stale at timeout
        assert!(ts.check(10.0, STALENESS_TIMEOUT_SECS + 1.0));

        // Still stale within the reset window
        assert!(ts.check(10.0, STALENESS_TIMEOUT_SECS + STALENESS_RESET_SECS - 1.0));

        // Auto-resets after timeout + reset window
        assert!(!ts.check(10.0, STALENESS_TIMEOUT_SECS + STALENESS_RESET_SECS + 1.0));
        assert_eq!(ts.bids_since_change, 0);

        // If still broken, needs another full round to re-trigger
        for _ in 0..STALENESS_MIN_BIDS {
            ts.record_bid();
        }
        let reset_time = STALENESS_TIMEOUT_SECS + STALENESS_RESET_SECS + 1.0;
        assert!(ts.check(10.0, reset_time + STALENESS_TIMEOUT_SECS + 1.0));
    }
}
