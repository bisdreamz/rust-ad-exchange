use anyhow::{anyhow, Error};
use ordered_float::NotNan;
use parking_lot::{Mutex, RwLock};
use std::collections::BTreeMap;
use std::ops::{Div, Mul};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::time::Instant;

fn calculate_effective_qps(duration_ms: u64, requests: u64) -> u32 {
    let multiplier = 1000.0.div(duration_ms as f32);
    let reqs_float = requests as f32;

    reqs_float.mul(multiplier) as u32
}

/// The live tick we are accumulating activity for
pub struct ActiveTick {
    /// Birth time of this tick
    timestamp: Instant,
    /// Total count of requests (increments) happened within tick period
    requests: AtomicU64,
    /// Map containing the ordered (descending) key buckets -> increments
    map: RwLock<BTreeMap<NotNan<f32>, u64>>,
}

impl ActiveTick {
    pub fn new() -> Self {
        ActiveTick {
            timestamp: Instant::now(),
            requests: AtomicU64::new(0),
            map: RwLock::new(BTreeMap::new()),
        }
    }

    /// Increment the counter for the provided key, which is expected
    /// to already have any desired bucketing applied
    pub fn increment(&self, bucket_key: NotNan<f32>) {
        let mut map = self.map.write();

        *map.entry(bucket_key).or_insert(0) += 1;

        self.requests.fetch_add(1, Ordering::Release);
    }
}

/// Represents a target threshold result containing
/// the associated historical QPS and its threshold value
pub struct QpsThreshold {
    /// The average QPS extracted from the associated tick, either up-sampled
    /// if the tick interval is subsecond or averaged if tick duration > 1s.
    /// This is inclusive of all requests which *meets or exceeds* the threshold
    pub avg_qps: u32,
    /// The bucket threshold value associated which indicates avg_qps of
    /// volume for any prediction threshold at or above this value
    pub threshold: f32,
}

/// A completed historical tick, mapping a histogram of
/// QPS details to their associated thresholds
pub struct Tick {
    /// Exact recorded tick duration during its lifetime,
    /// which may drift from desired timing
    duration: Duration,
    /// Total request count recorded during this tick
    requests: u64,
    /// The map keeping the raw histogram info of bucket prediction
    /// value -> requests at that bucket
    map: BTreeMap<NotNan<f32>, u64>,
}

impl Tick {
    fn from(active_tick: ActiveTick) -> Self {
        Tick {
            duration: Instant::now().duration_since(active_tick.timestamp),
            requests: active_tick.requests.load(Ordering::Acquire),
            map: active_tick.map.into_inner(),
        }
    }

    /// The actual elapsed tick duration
    pub fn duration(&self) -> &Duration {
        &self.duration
    }

    /// The total requests recorded within the tick
    pub fn total_requests(&self) -> u64 {
        self.requests
    }

    /// Calculates the effective average QPS across the tick duration.
    /// Stabilizes for a per-second rage, e.g. subsecond ticks are
    /// calculated as their per-second effective rate, and ticks
    /// longer than a second are averaged into a per-second result
    pub fn effective_qps(&self) -> u32 {
        calculate_effective_qps(self.duration.as_millis() as u64, self.requests)
    }

    /// Retrieve the ['QpsThreshold'] which is the highest bucket threshold
    /// which meets or exceeds the qps target
    ///
    /// # Arguments
    /// * 'target_qps' - The desired QPS target to locate a threshold for
    ///
    /// Returns none if no QPS limit provided (0) or if no
    /// activity data was recorded during the associated tick
    pub fn threshold(&self, target_qps: u32) -> Option<QpsThreshold> {
        if target_qps == 0 || self.map.is_empty() || self.requests == 0 {
            return None;
        }

        let tick_duration_ms = self.duration.as_millis() as u64;

        let mut total_threshold_reqs = 0;
        // Search top down, we often want the highest most valuable slice of inventory
        for (bucket, bucket_requests) in self.map.iter().rev() {
            total_threshold_reqs += bucket_requests;
            let threshold_qps = calculate_effective_qps(tick_duration_ms, total_threshold_reqs);

            if threshold_qps >= target_qps {
                let threshold_bucket = bucket.into_inner();

                return Some(QpsThreshold {
                    avg_qps: threshold_qps,
                    threshold: threshold_bucket,
                });
            }
        }

        None
    }
}

/// Histogram responsible for recording ad requests indexed by their
/// associated traffic shaping prediction value. The histogram is used
/// to derive the threshold value which would saturate a QPS budget
/// with the most valuable inventory, which is recorded and cycled
/// in an online fashion at a standard tick cadence e.g.1s.
/// Also allows an understanding of request value distribution.
pub struct QpsHistogram {
    /// Bucket width by which each value key should be rounded to
    bucket_width: f32,
    /// The active tick
    tick: Mutex<ActiveTick>,
}

impl QpsHistogram {
    /// Create a new QpsHistogram with the provided bucket width
    ///
    /// # Arguments
    /// * 'bucket_width' - The precision of rounding to be applied
    /// to the value keys. E.g. a value of 0.05 will round 1.1.23 to 1.25
    /// The finer the precision the greater the accuracy of thresholding
    /// however the more entries to maintain with added internal overhead
    pub fn new(bucket_width: f32) -> Self {
        Self {
            bucket_width,
            tick: Mutex::new(ActiveTick::new()),
        }
    }

    /// Record a request for the associated prediction value
    ///
    /// Returns 'Error' if the provided value is NaN
    pub fn record_request(&self, prediction_value: f32) -> Result<(), Error> {
        let bucket_value = (prediction_value / self.bucket_width).round() * self.bucket_width;
        let not_nan_bucket = NotNan::new(bucket_value)
            .map_err(|_| anyhow!("Cannot recored NaN prediction value"))?;

        let tick = self.tick.lock();

        tick.increment(not_nan_bucket);

        Ok(())
    }

    /// Cycles the active tick and begins a new tick, returning a completed ['Tick'] which
    /// can be used to access prior tick details such as QPS or target thresholds
    pub fn cycle(&self) -> Tick {
        let old_active_tick = std::mem::replace(&mut *self.tick.lock(), ActiveTick::new());

        Tick::from(old_active_tick)
    }
}
