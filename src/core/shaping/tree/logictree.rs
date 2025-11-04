use crate::core::models::shaping::{Metric, ShapingFeature};
use crate::core::shaping::threshold::QpsHistogram;
use crate::core::shaping::tree::handler::{
    RtbPredictionHandler, RtbPredictionOutput, RtbTrainingInput,
};
use crate::core::shaping::{tree, utils};
use anyhow::{anyhow, bail, Error};
use arc_swap::ArcSwap;
use log::info;
use logictree::{Feature, LogicTree};
use rtb::bid_response::Bid;
use rtb::BidRequest;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;
use strum::{AsRefStr, Display, EnumString};
use tokio::time::Instant;
use tracing::{debug, trace, warn};


fn task_cycle_threshold(histogram: &QpsHistogram, state_dest: &ArcSwap<ThresholdState>, target_passing_qps: u32) {
    trace!("Cycling threshold counters");
    let tick = histogram.cycle();

    let state = match tick.threshold(target_passing_qps) {
        Some(threshold) => {
            ThresholdState {
                threshold: threshold.threshold,
                qps_passing: threshold.avg_qps,
                qps_avail: tick.effective_qps()
            }
        },
        None => {
            ThresholdState {
                threshold: 0.0,
                qps_passing: 0,
                qps_avail: tick.effective_qps()
            }
        }
    };

    info!("Updating state: {:?}", &state);

    state_dest.store(Arc::new(state));
}

fn task_tree_prune(tree: &LogicTree<RtbTrainingInput, RtbPredictionOutput, RtbPredictionHandler>) {
    let start = Instant::now();

    if tree.prune() {
        warn!("Whole decision tree pruned - endpoint must be inactive!")
    }

    debug!("Completed tree prune in {} ms", start.elapsed().as_millis());
}

#[derive(Debug, Default)]
struct ThresholdState {
    pub threshold: f32,
    pub qps_avail: u32,
    pub qps_passing: u32
}

#[derive(Debug, AsRefStr, EnumString, Display)]
pub enum ShapingDecision {
    PassedMetric,
    PassedExploratory,
    PassedBoost,
    Blocked
}

#[derive(Debug)]
pub struct ShapingResult {
    pub decision: ShapingDecision,
    pub metric_value: f32,
    pub metric_target: f32,
    pub pred_depth: u32
}

/// A Summarizing, online learning traffic shaping implementation
/// built atop the ['LogicTree']
///
/// Learns in realtime and makes decisions based on summary parent segment nodes
/// such until it observes enough auctions to make more specific predictions
pub struct TreeShaper {
    /// The target ['Metric'] (KPI) we are optimizing towwards
    metric: Metric,
    /// The ['ShapingFeature'] set in order
    features: Vec<ShapingFeature>,
    /// The ['QpsHistogram'] which tracks available QPS and shaping value correlations
    histogram: Arc<QpsHistogram>,
    /// The ['LogicTree'] powering the training and decisioning states for ad segments
    tree: Arc<LogicTree<RtbTrainingInput, RtbPredictionOutput, RtbPredictionHandler>>,
    /// Keeps state about the current passing qps, available qps, and threshold target
    /// from the histogram cycle
    state: Arc<ArcSwap<ThresholdState>>,
    qps_explore_percent: u32,
    qps_limit: u32,
}

impl TreeShaper {
    pub fn new(
        features: &Vec<ShapingFeature>,
        metric: &Metric,
        min_decision_auctions: u32,
        segment_ttl: &Duration,
        explore_percent: u32,
        qps_limit: u32
    ) -> Self {
        let str_features = features.iter().map(|f| f.to_string()).collect();
        let first_pred_handler =
            RtbPredictionHandler::new(min_decision_auctions, segment_ttl.as_secs() as u32);

        // xTODO schedule threshold cycle
        // xTODO schedule pruning
        // xTODO logic to calculate "boost" from free QPS budget percentage with floor of exploratory
        // TODO task? methods? to record auction, bid, impression events

        let histogram = Arc::new(QpsHistogram::new(0.01));
        let tree = Arc::new(LogicTree::new(str_features, first_pred_handler));
        let state = Arc::new(ArcSwap::new(Arc::new(ThresholdState::default())));

        let histogram_clone = histogram.clone();
        let state_clone = state.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            loop {
                interval.tick().await;
                let passing_qps_budget = tree::utils::qps_budget_passing(explore_percent, qps_limit);
                task_cycle_threshold(&histogram_clone, &state_clone, passing_qps_budget);
            }
        });

        let tree_clone = tree.clone();
        let segment_ttl_clone = segment_ttl.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(segment_ttl_clone);
            loop {
                interval.tick().await;
                task_tree_prune(&tree_clone);
            }
        });

        /*
            Notes
            1) Check if passing threshold -> immediate ALLOW if so
            2) Check and allow exploratory percent, gives failing and immature segments
                the opportunity for exposure and reperformance
            3) Block if FAILED prediction *and* failed exploratory check
            4) Wait, what is our boost criteria now?

            block from boost if has literal 0s in bids OR if full depth and
            failing threshold

            if no boost quota available and its gotten here.. block it!

            can I use auctions < MIN_AUCTIONS * multiplier?
            but then whats purpose of the min auctions, it might be
            a full depth prediction..
        */

        /*
        need utility to manage QPS share of a percentage or absolute amount of QPS
        percent -> exploratory -> 5% = QPS Budget * 0.05 = what percentage of available?
        absolute -> boost -> QPS budget - exploratory (qps budget * 0.05) - passing = what percentage of available?

        need to be able to update it every cycle with the new available + passing count
         */

        TreeShaper {
            metric: metric.clone(),
            histogram,
            tree,
            features: features.clone(),
            state,
            qps_explore_percent: explore_percent,
            qps_limit
        }
    }

    fn extract_features(&self, req: &BidRequest, bid: &Option<&Bid>) -> Vec<Feature> {
        self.features
            .iter()
            .map(|f| utils::extract_shaping_feature(f, req, bid))
            .collect()
    }

    pub fn passes_shaping(&self, req: &BidRequest) -> Result<ShapingResult, Error> {
        let req_features = self.extract_features(req, &None);
        let prediction_opt = match self.tree.predict(&req_features) {
            Ok(prediction_opt) => prediction_opt,
            Err(e) => {
                bail!("Prediction failed: {:?}", e);
            }
        };

        let state = self.state.load();

        let prediction = match prediction_opt {
            Some(prediction) => prediction,
            None => {
                // this is only expected for extremely brannd new shaping instances
                debug!("No prediction data at all yet for: {:?}", req_features);

                self.record_auction(req)?;

                return Ok(ShapingResult {
                    decision: ShapingDecision::PassedExploratory,
                    metric_value: 0.0,
                    metric_target: state.threshold,
                    pred_depth: 0,
                });
            }
        };

        let metric_value = match self.metric {
            Metric::BidRate => prediction.value.bid_rate_percent(),
            Metric::FillRate => prediction.value.fill_rate_percent(),
            Metric::Rpm => prediction.value.rev_per_mille(),
        };

        let passed = metric_value >= state.threshold;

        debug!(
            "Features: {:?}\t Prediction: {:?}\t Threshold: {}\tMetric: {}\tPassed={}",
            req_features, prediction, state.threshold, metric_value, passed
        );

        if passed {
            return Ok(ShapingResult {
                decision: ShapingDecision::PassedMetric,
                metric_value,
                metric_target: state.threshold,
                pred_depth: prediction.depth as u32,
            })
        }

        if tree::utils::qps_exploratory_passes(self.qps_explore_percent, self.qps_limit, state.qps_avail) {
            debug!("Request failed shaping but passed exploratory");

            return Ok(ShapingResult {
                decision: ShapingDecision::PassedExploratory,
                metric_value,
                metric_target: state.threshold,
                pred_depth: prediction.depth as u32,
            })
        }

        if prediction.full_depth || metric_value == 0.0 {
            debug!("Skipping boost check for request - full depth failure or 0 KPI activity");

            return Ok(ShapingResult {
                decision: ShapingDecision::Blocked,
                metric_value,
                metric_target: state.threshold,
                pred_depth: prediction.depth as u32,
            })
        }

        if tree::utils::qps_boost_passes(state.qps_passing, self.qps_limit, state.qps_avail) {
            debug!("Request passing through boost");

            return Ok(ShapingResult {
                decision: ShapingDecision::PassedBoost,
                metric_value,
                metric_target: state.threshold,
                pred_depth: prediction.depth as u32,
            })
        }

        debug!("Request failed shaping, exploratory, and boost - blocking!");

        Ok(ShapingResult {
            decision: ShapingDecision::Blocked,
            metric_value,
            metric_target: state.threshold,
            pred_depth: prediction.depth as u32,
        })
    }

    pub fn record_auction(&self, req: &BidRequest) -> Result<(), Error> {
        let features = self.extract_features(req, &None);

        self.histogram.record_request(1.0).expect("TODO: panic message");

        self.tree.train(&features, &RtbTrainingInput {
            auctions: 1,
            bids: 0,
            impressions: 0,
            rev_gross: 0.0,
            rev_cost: 0.0,
        }).map_err(|e| anyhow!(e))
    }
}
