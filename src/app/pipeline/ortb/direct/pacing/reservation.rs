use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

const SLOTS: usize = 5;
const DEFAULT_BUCKET_SECS: u64 = 60;
const MICROS_PER_DOLLAR: f64 = 1_000_000.0;

/// Fixed-size circular buffer of bid value per time slot.
/// Old slots are zeroed on access when time advances.
/// All operations are lock-free using relaxed atomics.
pub struct BidReservationRing {
    slots: [AtomicU64; SLOTS],
    bucket_width: u64,
    /// The epoch-index (now / bucket_width) when we last wrote.
    /// Used to detect time advancement and evict stale slots.
    cursor: AtomicU64,
}

impl BidReservationRing {
    pub fn new(bucket_width: u64) -> Self {
        Self {
            slots: std::array::from_fn(|_| AtomicU64::new(0)),
            bucket_width,
            cursor: AtomicU64::new(Self::epoch_index(bucket_width)),
        }
    }

    pub fn default() -> Self {
        Self::new(DEFAULT_BUCKET_SECS)
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
        let current = Self::epoch_index(self.bucket_width);
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
        Self::epoch_index(self.bucket_width) as usize % SLOTS
    }

    fn epoch_index(bucket_width: u64) -> u64 {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock before epoch")
            .as_secs();
        now / bucket_width
    }
}
