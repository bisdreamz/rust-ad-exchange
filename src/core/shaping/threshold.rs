use anyhow::{Error, anyhow};
use ordered_float::NotNan;
use parking_lot::{Mutex, RwLock};
use std::collections::BTreeMap;
use std::ops::{Div, Mul};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::time::Instant;
use tracing::{ warn};

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
    /// This is inclusive of all requests which meet or exceed the threshold.
    /// May be 0 if no traffic meets the minimum threshold requirement.
    pub avg_qps: u32,
    /// The bucket threshold value which indicates the minimum metric value
    /// required for traffic to qualify. Traffic with metric >= threshold
    /// contributes to avg_qps.
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
    /// that meets or exceeds the target QPS. Iterates buckets from highest
    /// to lowest value, accumulating requests until the target QPS is reached.
    ///
    /// # Arguments
    /// * 'target_qps' - The desired QPS target to locate a threshold for. If 0,
    /// returns all traffic meeting or exceeding min_threshold.
    /// * 'min_threshold' - Optional minimum threshold to enforce. Traffic below
    /// this value is excluded even if needed to reach target_qps.
    ///
    /// # Returns
    /// * `Some(QpsThreshold)` - Threshold found with associated QPS
    /// * `None` - If no data recorded, both params are 0/None, or all traffic
    /// is below min_threshold
    pub fn threshold(&self, target_qps: u32, min_threshold: Option<f32>) -> Option<QpsThreshold> {
        if target_qps == 0 && min_threshold.is_none() {
            warn!("Target qps was 0 and no min_threshold? cant threshold that!");

            return None;
        }

        if self.map.is_empty() || self.requests == 0 {
            // no activity right now it appears
            return None;
        }

        let tick_duration_ms = self.duration.as_millis() as u64;

        let min_threshold = min_threshold.unwrap_or(0.0);
        let mut total_threshold_reqs = 0;
        let mut threshold_value = 0.0;

        let mut final_threshold_qps = 0;

        // Iterate buckets from highest to lowest value to select the most valuable inventory
        for (bucket, bucket_requests) in self.map.iter().rev() {
            // Stop if we've hit the minimum threshold floor
            if bucket.into_inner() < min_threshold {
                if final_threshold_qps > 0 {
                    // We accumulated some traffic above min_threshold, return it
                    return Some(QpsThreshold {
                        avg_qps: final_threshold_qps,
                        threshold: min_threshold,
                    });
                }

                // All traffic is below min_threshold, nothing qualifies
                return None;
            }

            // Accumulate requests from this bucket
            total_threshold_reqs += bucket_requests;

            // Calculate effective QPS from accumulated requests
            final_threshold_qps = calculate_effective_qps(tick_duration_ms, total_threshold_reqs);
            threshold_value = bucket.into_inner();

            // Check if we've met the target QPS
            if target_qps > 0 && final_threshold_qps >= target_qps {
                return Some(QpsThreshold {
                    avg_qps: final_threshold_qps,
                    threshold: threshold_value,
                });
            }
        }

        // Fallback: couldn't reach target_qps but have some valid traffic above min_threshold
        // Return all traffic we found, even though it's below target
        if final_threshold_qps > 0 {
            return Some(QpsThreshold {
                avg_qps: final_threshold_qps,
                threshold: threshold_value,
            });
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

    /// Record a request for the associated prediction value. Values are bucketed
    /// using floor rounding to ensure consistency with threshold comparison logic.
    ///
    /// # Bucketing
    /// Uses `.floor()` to round down to bucket boundaries. For example, with
    /// bucket_width = 0.01, a value of 0.238 becomes bucket 0.23. This ensures
    /// that the >= threshold comparison works correctly: if 0.238 is bucketed
    /// as 0.23, it will only contribute to thresholds of 0.23 or lower, matching
    /// the actual comparison result of `0.238 >= 0.23` (true) vs `0.238 >= 0.24` (false).
    ///
    /// Returns 'Error' if the provided value is NaN
    pub fn record_request(&self, prediction_value: f32) -> Result<(), Error> {
        // Floor bucketing: (0.238 / 0.01).floor() * 0.01 = 23.floor() * 0.01 = 0.23
        let bucket_value = (prediction_value / self.bucket_width).floor() * self.bucket_width;

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
