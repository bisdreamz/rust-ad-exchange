use crate::core::models::shaping::Metric;
use crate::core::shaping::tree::serializers;
use arc_swap::ArcSwap;
use logictree::{Feature, PredictionHandler};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use std::ops::{Div, Mul};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

#[derive(Clone, Default, Serialize, Deserialize, Debug)]
pub(crate) struct HandlerState {
    /// Count of top level bidrequests actually sent to bidder
    pub auctions: u64,
    /// Sum of total impressions available across 'requests' sent
    pub bids: u64,
    /// Sum of bid CPM value (the bid based RPM counterpart)
    pub bids_value: f64,
    /// Count of unique impressions billed
    pub impressions: u64,
    /// CPM sum of billed gross revenue as charged to demand
    pub rev_gross: f64,
    /// CPM sum of billed cost revenue as paid to publishers
    pub rev_cost: f64,
}

impl HandlerState {
    /// Increment state counters by the amounts provided in the training input
    fn add(&mut self, input: &RtbTrainingInput) {
        self.auctions += input.auctions as u64;
        self.bids += input.bids as u64;
        self.bids_value += input.bid_value as f64;
        self.impressions += input.impressions as u64;
        self.rev_gross += input.rev_gross as f64;
        self.rev_cost += input.rev_cost as f64;
    }
}

#[derive(Clone, Default, Serialize, Deserialize, Debug)]
pub struct RtbPredictionOutput {
    state: HandlerState,
}

impl RtbPredictionOutput {
    fn new(state: HandlerState) -> Self {
        RtbPredictionOutput { state }
    }

    /// Convenience method for returning the
    /// calculated metric from this prediction node
    /// for the provided target metric KPI
    pub fn metric_value(&self, metric: &Metric) -> f32 {
        match metric {
            Metric::BidRate => self.bid_rate_percent(),
            Metric::Bvpm => self.bid_value_per_mille(),
            Metric::FillRate => self.fill_rate_percent(),
            Metric::Rpm => self.rev_per_million_auctions(),
        }
    }

    /// The effective revenue in whole dollars generated
    /// per million auctions sent to the bidder, e.g. ($)8.25
    pub fn rev_per_million_auctions(&self) -> f32 {
        if self.state.auctions == 0 || self.state.impressions == 0 {
            return 0.0;
        }

        // by 1000 because revenue already in raw CPM sum, not actual dollars
        let cpm_auction_factor = 1_000.0 / self.state.auctions as f64;

        self.state.rev_gross.mul(cpm_auction_factor) as f32
    }

    pub fn bid_value_per_mille(&self) -> f32 {
        if self.state.bids == 0 || self.state.auctions == 0 {
            return 0.0;
        }

        // by 1000 because bid value already in raw CPM sum, not actual dollars
        let cpm_auction_factor = 1_000.0 / self.state.auctions as f64;

        self.state.bids_value.mul(cpm_auction_factor) as f32
    }

    /// The effective fill rate as defined by impressions
    /// billed divided by auctions sent, e.g. 0.055(%)
    pub fn fill_rate_percent(&self) -> f32 {
        if self.state.auctions == 0 {
            return 0.0;
        }

        let imps_billed_float = self.state.impressions as f32;

        imps_billed_float.div(self.state.auctions as f32)
    }

    /// The effective bid rate as defined by bids
    /// received divided by auctions sent, e.g. 0.34(%)
    pub fn bid_rate_percent(&self) -> f32 {
        if self.state.auctions == 0 {
            return 0.0;
        }

        let bids_float = self.state.bids as f32;

        bids_float.div(self.state.auctions as f32)
    }
}

pub struct RtbTrainingInput {
    pub auctions: u32,
    pub bids: u32,
    pub impressions: u32,
    pub bid_value: f32,
    pub rev_gross: f32,
    pub rev_cost: f32,
}

#[derive(Serialize, Deserialize)]
pub struct RtbPredictionHandler {
    min_auctions: u32,
    segment_ttl_secs: u32,
    #[serde(with = "serializers::atomic_u64_serde")]
    last_active: AtomicU64,
    #[serde(with = "serializers::rwlock_serde")]
    state: RwLock<HandlerState>,
    // this can be updated dynamically
    // which we need to sort picking
    // prediction branch for multi value features
    // THIS WONT SUPPORT saving and loading
    // from a file! The current usage doesnt do
    // this anyway but note this limitation!
    #[serde(skip, default = "deserialized_metric_unsupported")]
    metric: Arc<ArcSwap<Metric>>,
}

fn deserialized_metric_unsupported() -> Arc<ArcSwap<Metric>> {
    panic!(
        "Deserialized RtbPredictionHandler does not support metric swap and thus loading a saved model unsupported now"
    );
}

impl RtbPredictionHandler {
    /// Create a prediction handler which stores metrics observed
    /// for a particular ad segment, and calculates its performance,
    /// such that any target KPI can be produced for shaping.
    /// # Arguments
    /// * `min_decision_auctions` - Min number of auctions seen
    /// before a tree node decides its confident enough to make
    /// a prediction, usually 500-1000 is good.
    /// * `segment_ttl_secs` - Seconds before we evict a segment
    /// from memory for inactivity to prevent memory leaks. Suggest
    /// 10-30 mins or so.
    /// * `metric` - The metric we are optimizing toward, which
    /// can be updated/swapped by the owner of this pred handler.
    /// This determines sort order and priority when choosing a branch
    /// for prediction when multiple values are present, e.g.
    /// if a req has banner, video present - itll likely use
    /// the video metrics to value this request if it has
    /// higher average value
    pub fn new(
        min_decision_auctions: u32,
        segment_ttl_secs: u32,
        metric: Arc<ArcSwap<Metric>>,
    ) -> RtbPredictionHandler {
        Self {
            min_auctions: min_decision_auctions,
            segment_ttl_secs,
            last_active: AtomicU64::new(rtb::common::utils::epoch_timestamp()),
            state: RwLock::new(HandlerState::default()),
            metric,
        }
    }
}

impl PredictionHandler<RtbTrainingInput, RtbPredictionOutput> for RtbPredictionHandler {
    fn train(&self, input: &RtbTrainingInput, _next_feature: Option<&Feature>) -> () {
        self.last_active
            .store(rtb::common::utils::epoch_timestamp(), Ordering::Relaxed);

        self.state.write().add(input);
    }

    fn predict(&self) -> RtbPredictionOutput {
        RtbPredictionOutput::new(self.state.read().clone())
    }

    fn should_prune(&self) -> bool {
        let active_delta_ms = rtb::common::utils::epoch_timestamp()
            .saturating_sub(self.last_active.load(Ordering::Relaxed));
        let active_delta_secs = Duration::from_millis(active_delta_ms).as_secs();

        active_delta_secs >= self.segment_ttl_secs as u64
    }

    fn new_instance(&self) -> Self
    where
        Self: Sized,
    {
        RtbPredictionHandler::new(
            self.min_auctions,
            self.segment_ttl_secs,
            self.metric.clone(),
        )
    }

    fn resolve(
        &self,
        predictions: SmallVec<[(RtbPredictionOutput, usize); 1]>,
    ) -> Option<(RtbPredictionOutput, usize)> {
        let metric = self.metric.load();
        let mut best_branch = None;
        let mut highest_metric = 0.0;

        for (prediction, pred_depth) in predictions {
            if prediction.state.auctions < self.min_auctions as u64 {
                continue;
            }

            if best_branch.is_none() {
                highest_metric = prediction.metric_value(&metric);
                best_branch = Some((prediction, pred_depth));

                continue;
            }

            let pred_metric_val = prediction.metric_value(&metric);
            if pred_metric_val > highest_metric {
                highest_metric = pred_metric_val;
                best_branch = Some((prediction, pred_depth));
            }
        }

        best_branch
    }
}
