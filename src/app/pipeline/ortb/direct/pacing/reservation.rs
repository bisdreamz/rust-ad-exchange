use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

const SLOTS: usize = 5;
pub(super) const DEFAULT_BUCKET_SECS: u64 = 60;
const MICROS_PER_DOLLAR: f64 = 1_000_000.0;

/// Closure returning the current epoch seconds.
/// `Arc` so rings inside DashMap entries can share the parent pacer's clock.
pub type EpochClock = Arc<dyn Fn() -> u64 + Send + Sync>;

pub fn system_epoch_clock() -> EpochClock {
    Arc::new(|| {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock before epoch")
            .as_secs()
    })
}

/// Fixed-size circular buffer of bid value per time slot.
/// Old slots are zeroed on access when time advances.
/// All operations are lock-free using relaxed atomics.
pub struct ReservationRing {
    slots: [AtomicU64; SLOTS],
    bucket_width: u64,
    /// The epoch-index (now / bucket_width) when we last wrote.
    /// Used to detect time advancement and evict stale slots.
    cursor: AtomicU64,
    clock: EpochClock,
}

impl ReservationRing {
    pub fn new(bucket_width: u64, clock: EpochClock) -> Self {
        let now = (clock)();
        Self {
            slots: std::array::from_fn(|_| AtomicU64::new(0)),
            bucket_width,
            cursor: AtomicU64::new(now / bucket_width),
            clock,
        }
    }

    /// Reserve bid value in the current time slot.
    pub fn reserve(&self, dollars: f64) {
        self.advance();
        let idx = self.slot_index();
        self.slots[idx].fetch_add((dollars * MICROS_PER_DOLLAR) as u64, Relaxed);
    }

    /// Total non-stale reserved value in dollars.
    pub fn total(&self) -> f64 {
        self.advance();
        let sum: u64 = self.slots.iter().map(|s| s.load(Relaxed)).sum();
        sum as f64 / MICROS_PER_DOLLAR
    }

    /// Advance the cursor and zero any stale slots since last access.
    fn advance(&self) {
        let current = self.epoch_index();
        let prev = self.cursor.load(Relaxed);

        if current <= prev {
            return;
        }

        // Try to claim the advance. Loser of the race skips — winner
        // already zeroed the slots.
        if self
            .cursor
            .compare_exchange(prev, current, Relaxed, Relaxed)
            .is_err()
        {
            return;
        }

        let gap = (current - prev) as usize;

        if gap >= SLOTS {
            // Entire ring is stale, zero everything
            for slot in &self.slots {
                slot.store(0, Relaxed);
            }
        } else {
            // Zero only the slots that were skipped
            for offset in 1..=gap {
                let idx = (prev as usize + offset) % SLOTS;
                self.slots[idx].store(0, Relaxed);
            }
        }
    }

    fn slot_index(&self) -> usize {
        self.epoch_index() as usize % SLOTS
    }

    fn epoch_index(&self) -> u64 {
        (self.clock)() / self.bucket_width
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fake_clock(start_epoch: u64) -> (EpochClock, Arc<AtomicU64>) {
        let epoch = Arc::new(AtomicU64::new(start_epoch));
        let e = epoch.clone();
        let clock: EpochClock = Arc::new(move || e.load(Relaxed));
        (clock, epoch)
    }

    /// A freshly created ring has no reservations.
    #[test]
    fn new_ring_has_zero_total() {
        let ring = ReservationRing::new(DEFAULT_BUCKET_SECS, system_epoch_clock());
        assert_eq!(ring.total(), 0.0);
    }

    /// Multiple reserves within the same bucket sum correctly via micros roundtrip.
    #[test]
    fn reserve_and_total_roundtrip() {
        let ring = ReservationRing::new(DEFAULT_BUCKET_SECS, system_epoch_clock());
        ring.reserve(1.50);
        ring.reserve(2.25);
        let total = ring.total();
        assert!((total - 3.75).abs() < 0.01, "expected ~3.75, got {total}");
    }

    /// Many small reserves accumulate without drift from micros conversion.
    #[test]
    fn multiple_reserves_accumulate() {
        let ring = ReservationRing::new(DEFAULT_BUCKET_SECS, system_epoch_clock());
        for _ in 0..10 {
            ring.reserve(1.0);
        }
        let total = ring.total();
        assert!((total - 10.0).abs() < 0.01, "expected ~10.0, got {total}");
    }

    /// Advancing past SLOTS × bucket_width zeroes the entire ring (full eviction).
    #[test]
    fn advance_evicts_all_stale_slots() {
        let (clock, epoch) = fake_clock(1_000_000);
        let ring = ReservationRing::new(DEFAULT_BUCKET_SECS, clock);

        ring.reserve(5.0);
        assert!((ring.total() - 5.0).abs() < 0.01);

        // Advance past SLOTS × bucket_width → entire ring is stale
        epoch.store(1_000_000 + DEFAULT_BUCKET_SECS * SLOTS as u64, Relaxed);
        assert_eq!(ring.total(), 0.0);
    }

    /// A small time advance (< SLOTS buckets) preserves recent data;
    /// a larger advance (>= SLOTS) evicts everything.
    #[test]
    fn partial_advance_preserves_recent() {
        let (clock, epoch) = fake_clock(1_000_000);
        let ring = ReservationRing::new(DEFAULT_BUCKET_SECS, clock);

        ring.reserve(3.0);

        // Advance 2 buckets — old slot survives (gap < SLOTS)
        epoch.store(1_000_000 + DEFAULT_BUCKET_SECS * 2, Relaxed);
        assert!(
            (ring.total() - 3.0).abs() < 0.01,
            "should survive partial advance"
        );

        // Advance beyond SLOTS — all stale now
        epoch.store(
            1_000_000 + DEFAULT_BUCKET_SECS * (SLOTS as u64 + 1),
            Relaxed,
        );
        assert_eq!(ring.total(), 0.0);
    }

    /// Reservations spread across consecutive time buckets all contribute to total().
    #[test]
    fn reserves_across_buckets_accumulate() {
        let (clock, epoch) = fake_clock(1_000_000);
        let ring = ReservationRing::new(DEFAULT_BUCKET_SECS, clock);

        // Reserve in 3 consecutive buckets
        ring.reserve(1.0);

        epoch.store(1_000_000 + DEFAULT_BUCKET_SECS, Relaxed);
        ring.reserve(2.0);

        epoch.store(1_000_000 + DEFAULT_BUCKET_SECS * 2, Relaxed);
        ring.reserve(3.0);

        let total = ring.total();
        assert!((total - 6.0).abs() < 0.01, "expected ~6.0, got {total}");
    }
}
