use ahash::AHashMap;
use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use std::sync::atomic::{AtomicU32, Ordering::Relaxed};
use tracing::{debug, warn};

const SECS_PER_DAY: u64 = 86400;

fn current_day_number() -> u32 {
    (std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        / SECS_PER_DAY) as u32
}

/// Returns true if the given timestamp falls on today (UTC).
pub fn is_bucket_today(dt: &DateTime<Utc>) -> bool {
    dt.date_naive() == Utc::now().date_naive()
}

/// Daily-bucketed map with automatic midnight reset.
///
/// Stores `String → u64` entries that represent today's counters
/// (spend micros, impression counts, etc.). On first read after
/// midnight UTC, all entries are cleared atomically — Firestore
/// listener events for the new day repopulate.
pub struct DailyMap {
    data: RwLock<AHashMap<String, u64>>,
    /// UTC day number when entries were last valid.
    day: AtomicU32,
}

impl DailyMap {
    pub fn new() -> Self {
        Self {
            data: RwLock::new(AHashMap::new()),
            day: AtomicU32::new(current_day_number()),
        }
    }

    /// If we've crossed midnight, clear all entries.
    /// Uses compare-exchange so only one thread performs the clear.
    pub fn maybe_reset(&self) {
        let today = current_day_number();
        let stored = self.day.load(Relaxed);
        if today != stored
            && self
                .day
                .compare_exchange(stored, today, Relaxed, Relaxed)
                .is_ok()
        {
            self.data.write().clear();
        }
    }

    /// Read a value, resetting first if the day changed.
    pub fn get(&self, key: &str) -> u64 {
        self.maybe_reset();
        self.data.read().get(key).copied().unwrap_or(0)
    }

    /// Bulk-load from an iterator of `(key, value)` pairs.
    /// Replaces all existing entries.
    pub fn load(&self, entries: impl Iterator<Item = (String, u64)>, label: &str) {
        let mut map = AHashMap::new();
        for (key, value) in entries {
            *map.entry(key).or_default() += value;
        }
        debug!("loaded daily {} for {} entries", label, map.len());
        *self.data.write() = map;
    }

    /// Insert or replace a single entry.
    pub fn insert(&self, key: String, value: u64) {
        self.data.write().insert(key, value);
    }

    /// Remove entries whose key is contained in `doc_id`.
    /// Returns the number of entries removed.
    pub fn remove_matching(&self, doc_id: &str) -> usize {
        let mut data = self.data.write();
        let before = data.len();
        data.retain(|k, _| !doc_id.contains(k));
        let removed = before - data.len();
        if removed == 0 {
            warn!("daily doc removed but no matching entry found: {}", doc_id);
        } else {
            debug!("daily doc removed: {}, cleaned {} entries", doc_id, removed);
        }
        removed
    }
}
