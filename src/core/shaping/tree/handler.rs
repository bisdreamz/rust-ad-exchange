use crate::core::shaping::tree::serializers;
use logictree::{Feature, PredictionHandler};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::ops::{Div, Mul};
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

    fn merge(&mut self, state: &HandlerState) {
        self.auctions += state.auctions;
        self.bids += state.bids;
        self.bids_value += state.bids_value;
        self.impressions += state.impressions;
        self.rev_gross += state.rev_gross;
        self.rev_cost += state.rev_cost;
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

    /// The effective revenue in whole dollars generated
    /// per million auctions sent to the bidder, e.g. ($)8.25
    pub fn rev_per_mille(&self) -> f32 {
        if self.state.auctions == 0 || self.state.impressions == 0 {
            return 0.0;
        }

        // by 1000 because revenue already in raw CPM sum, not actual dollars
        let cpm_auction_factor = 1_000.0 / self.state.auctions as f64;

        self.state.rev_gross.mul(cpm_auction_factor) as f32
    }

    pub fn bid_value_per_mille(&self) -> f32 {
        if self.state.auctions == 0 || self.state.impressions == 0 {
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
}

impl RtbPredictionHandler {
    pub fn new(min_decision_auctions: u32, segment_ttl_secs: u32) -> RtbPredictionHandler {
        Self {
            min_auctions: min_decision_auctions,
            segment_ttl_secs,
            last_active: AtomicU64::new(rtb::common::utils::epoch_timestamp()),
            state: RwLock::new(HandlerState::default()),
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
        RtbPredictionHandler::new(self.min_auctions, self.segment_ttl_secs)
    }

    fn resolve(
        &self,
        predictions: Vec<(RtbPredictionOutput, usize)>,
    ) -> Option<RtbPredictionOutput> {
        let mut aggregate_state = HandlerState::default();

        for (prediction, _) in predictions {
            aggregate_state.merge(&prediction.state);
        }

        if aggregate_state.auctions < self.min_auctions as u64 {
            return None;
        }

        Some(RtbPredictionOutput::new(aggregate_state))
    }
}
