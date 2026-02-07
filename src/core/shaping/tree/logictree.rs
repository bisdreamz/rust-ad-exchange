use crate::core::models::shaping::{Metric, ShapingFeature};
use crate::core::shaping::threshold::QpsHistogram;
use crate::core::shaping::tree::handler::{
    RtbPredictionHandler, RtbPredictionOutput, RtbTrainingInput,
};
use crate::core::shaping::{tree, utils};
use anyhow::{Error, anyhow, bail, format_err};
use arc_swap::ArcSwap;
use logictree::{Feature, LogicTree};
use rtb::BidRequest;
use rtb::bid_response::Bid;
use std::sync::Arc;
use std::time::Duration;
use strum::{AsRefStr, Display, EnumString};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tracing::{debug, info, trace, warn};

fn task_cycle_threshold(
    histogram: &QpsHistogram,
    state_dest: &ArcSwap<ThresholdState>,
    target_passing_qps: u32,
    min_threshold: f32,
) {
    let tick = histogram.cycle();

    let min_threshold_opt = Some(min_threshold);

    let state = match tick.threshold(target_passing_qps, min_threshold_opt) {
        Some(threshold) => ThresholdState {
            threshold: threshold.threshold,
            qps_passing: threshold.avg_qps,
            qps_avail: tick.effective_qps(),
        },
        None => {
            // this happens when no data yet or nothing satisfies the
            // target criteria (qps or min threshold)
            ThresholdState {
                threshold: 0.0,
                qps_passing: tick.effective_qps(),
                qps_avail: tick.effective_qps(),
            }
        }
    };

    trace!(
        "Tick duration {}ms requests {} state: {:?}",
        tick.duration().as_millis(),
        tick.total_requests(),
        &state
    );

    state_dest.store(Arc::new(state));
}

fn task_tree_prune(tree: &LogicTree<RtbTrainingInput, RtbPredictionOutput, RtbPredictionHandler>) {
    let start = Instant::now();

    if tree.prune() {
        warn!("Whole decision tree pruned - endpoint must be inactive!")
    }

    debug!("Completed tree prune in {} ms", start.elapsed().as_millis());
}

fn encode_feature_string(features: &Vec<Feature>) -> Result<String, Error> {
    serde_json::to_string(&features).map_err(|e| anyhow!("Failed to encode feature array: {}", e))
}

fn decode_feature_string(feature_string: &String) -> Result<Vec<Feature>, Error> {
    serde_json::from_str::<Vec<Feature>>(feature_string)
        .map_err(|e| anyhow!("Failed to decode feature array: {}", e))
}

#[derive(Debug, Default)]
struct ThresholdState {
    pub threshold: f32,
    pub qps_avail: u32,
    pub qps_passing: u32,
}

#[derive(Debug, AsRefStr, EnumString, Display)]
pub enum ShapingDecision {
    PassedMetric,
    PassedExploratory,
    PassedBoost,
    Blocked,
}

#[derive(Debug)]
pub struct ShapingResult {
    pub decision: ShapingDecision,
    pub metric_value: f32,
    pub metric_target: f32,
    pub pred_depth: u32,
    pub features: Vec<Feature>,
}

struct DynamicConfig {
    metric: Metric,
    explore_percent: u32,
    qps_limit: u32,
    min_threshold: f32,
}

/// A Summarizing, online learning traffic shaping implementation
/// built atop the ['LogicTree']
///
/// Learns in realtime and makes decisions based on summary parent segment nodes
/// such until it observes enough auctions to make more specific predictions
pub struct TreeShaper {
    /// The ['ShapingFeature'] set in order
    features: Vec<ShapingFeature>,
    /// The ['QpsHistogram'] which tracks available QPS and shaping value correlations,
    /// required for dynamic thresholding when a bidder has a QPS limit enforced
    histogram: Arc<QpsHistogram>,
    /// The ['LogicTree'] powering the training and decisioning states for ad segments
    tree: Arc<LogicTree<RtbTrainingInput, RtbPredictionOutput, RtbPredictionHandler>>,
    /// Keeps state about the current passing qps, available qps, and threshold target
    /// from the histogram cycle
    state: Arc<ArcSwap<ThresholdState>>,
    /// Dynamically updatable config (metric, explore_percent, qps_limit, min_threshold)
    config: Arc<ArcSwap<DynamicConfig>>,
    task_handles: Vec<JoinHandle<()>>,
}

impl TreeShaper {
    pub fn new(
        features: &Vec<ShapingFeature>,
        metric: &Metric,
        min_decision_auctions: u32,
        segment_ttl: &Duration,
        explore_percent: u32,
        qps_limit: u32,
        min_threshold: f32,
    ) -> Self {
        let str_features = features.iter().map(|f| f.to_string()).collect();
        let first_pred_handler =
            RtbPredictionHandler::new(min_decision_auctions, segment_ttl.as_secs() as u32);

        let tree = Arc::new(LogicTree::new(str_features, first_pred_handler));
        let state = Arc::new(ArcSwap::new(Arc::new(ThresholdState::default())));
        let config = Arc::new(ArcSwap::new(Arc::new(DynamicConfig {
            metric: metric.clone(),
            explore_percent,
            qps_limit,
            min_threshold,
        })));

        info!("Starting dynamic thresholding, QPS limit {}", qps_limit);

        let histogram = Arc::new(QpsHistogram::new(0.01));

        let h1 = {
            let histogram = histogram.clone();
            let state = state.clone();
            let config = config.clone();
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_secs(1));
                loop {
                    interval.tick().await;
                    let cfg = config.load();
                    let avail_qps = state.load().qps_avail;
                    let passing_qps_budget = tree::utils::qps_budget_passing(
                        cfg.explore_percent,
                        cfg.qps_limit,
                        avail_qps,
                    );

                    if passing_qps_budget == 0 && avail_qps > 0 {
                        warn!(
                            "Passing qps budget became zero but have avail QPS! This should not happen unless inactive!"
                        );
                    }

                    task_cycle_threshold(&histogram, &state, passing_qps_budget, cfg.min_threshold);
                }
            })
        };

        let h2 = {
            let tree = tree.clone();
            let segment_ttl = *segment_ttl;
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(segment_ttl);
                loop {
                    interval.tick().await;
                    task_tree_prune(&tree);
                }
            })
        };

        TreeShaper {
            histogram,
            tree,
            features: features.clone(),
            state,
            config,
            task_handles: vec![h1, h2],
        }
    }

    pub fn features(&self) -> &[ShapingFeature] {
        &self.features
    }

    pub fn update_config(
        &self,
        metric: Metric,
        explore_percent: u32,
        qps_limit: u32,
        min_threshold: f32,
    ) {
        self.config.store(Arc::new(DynamicConfig {
            metric,
            explore_percent,
            qps_limit,
            min_threshold,
        }));
    }

    fn extract_features(&self, req: &BidRequest, bid: Option<&Bid>) -> Vec<Feature> {
        self.features
            .iter()
            .map(|f| utils::extract_shaping_feature(f, req, bid))
            .collect()
    }

    pub fn passes_shaping(&self, req: &BidRequest) -> Result<ShapingResult, Error> {
        let req_features = self.extract_features(req, None);
        let prediction_opt = match self.tree.predict(&req_features) {
            Ok(prediction_opt) => prediction_opt,
            Err(e) => {
                bail!("Prediction failed: {:?}", e);
            }
        };

        let state = self.state.load();
        // We will continuously offset this avail pool by the amounts
        // consumed at certain code paths to ensure later QPS shares
        // are properly adjusted, e.g. we dont calculate a boost
        // passing with the *total* qps, when in fact it only sees
        // evaluations for (avail - passing - exploratory)
        let mut avail_qps_pool = state.qps_avail;

        let prediction = match prediction_opt {
            Some(prediction) => prediction,
            None => {
                // this is only expected for extremely brand new shaping instances
                // that dont have data even at the root level
                debug!("No prediction data at all yet for: {:?}", req_features);

                self.histogram
                    .record_request(0.0)
                    .map_err(|e| format_err!("Histogram err recording QPS value: {:?}", e))?;

                return Ok(ShapingResult {
                    decision: ShapingDecision::PassedExploratory,
                    metric_value: 0.0,
                    metric_target: state.threshold,
                    pred_depth: 0,
                    features: req_features,
                });
            }
        };

        let cfg = self.config.load();

        let metric_value = match cfg.metric {
            Metric::BidRate => prediction.value.bid_rate_percent(),
            Metric::Bvpm => prediction.value.bid_value_per_mille(),
            Metric::FillRate => prediction.value.fill_rate_percent(),
            Metric::Rpm => prediction.value.rev_per_mille(),
        };

        // records the *available* request and its value in the histogram so it sees all
        self.histogram
            .record_request(metric_value)
            .map_err(|e| format_err!("Histogram err recording QPS value: {:?}", e))?;

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
                features: req_features,
            });
        }

        avail_qps_pool = avail_qps_pool.saturating_sub(state.qps_passing);
        if avail_qps_pool == 0 && state.threshold > 0.0 {
            // how is this possible? this says that all qps passing = qps avail
            // but it didnt pass the check above?

            warn!(
                "Passing QPS ({}) is >= available ({}) but failed metric?!",
                state.qps_passing, state.qps_avail
            );
        }

        let qps_exploratory = tree::utils::qps_budget_exploratory(
            cfg.explore_percent,
            cfg.qps_limit,
            state.qps_avail, // exploratory is percent of endpoint qps limit OR all avail
        );
        if tree::utils::qps_passes_percentage(qps_exploratory, avail_qps_pool) {
            debug!("Request failed shaping but passed exploratory");

            self.record_auction(req)?;

            return Ok(ShapingResult {
                decision: ShapingDecision::PassedExploratory,
                metric_value,
                metric_target: state.threshold,
                pred_depth: prediction.depth as u32,
                features: req_features,
            });
        }

        avail_qps_pool = avail_qps_pool.saturating_sub(qps_exploratory);
        if avail_qps_pool == 0 {
            debug!(
                "Request failed shaping. Full QPS allocation used by passing and exploratory, blocking boost"
            );

            return Ok(ShapingResult {
                decision: ShapingDecision::Blocked,
                metric_value,
                metric_target: state.threshold,
                pred_depth: prediction.depth as u32,
                features: req_features,
            });
        }

        // need min auctions check to filter on 0.0
        // or do we? keeping 0.0 means accelerate premature but
        // promising segments
        if prediction.full_depth
        /*|| metric_value == 0.0 */
        {
            debug!("Skipping boost check for request - full depth failure or 0 KPI activity");

            return Ok(ShapingResult {
                decision: ShapingDecision::Blocked,
                metric_value,
                metric_target: state.threshold,
                pred_depth: prediction.depth as u32,
                features: req_features,
            });
        }

        let qps_boost = tree::utils::qps_budget_boost(
            state.qps_passing,
            qps_exploratory,
            cfg.qps_limit,
            avail_qps_pool,
        );
        if tree::utils::qps_passes_percentage(qps_boost, avail_qps_pool) {
            debug!("Request passing through boost");

            return Ok(ShapingResult {
                decision: ShapingDecision::PassedBoost,
                metric_value,
                metric_target: state.threshold,
                pred_depth: prediction.depth as u32,
                features: req_features,
            });
        }

        avail_qps_pool = avail_qps_pool.saturating_sub(qps_boost);

        debug!(
            "Request failed shaping, exploratory, and boost - blocking! Had {} unallocated QPS",
            avail_qps_pool
        );

        Ok(ShapingResult {
            decision: ShapingDecision::Blocked,
            metric_value,
            metric_target: state.threshold,
            pred_depth: prediction.depth as u32,
            features: req_features,
        })
    }

    pub fn record_auction(&self, req: &BidRequest) -> Result<(), Error> {
        let features = self.extract_features(req, None);

        self.tree
            .train(
                &features,
                &RtbTrainingInput {
                    auctions: 1,
                    bids: 0,
                    impressions: 0,
                    bid_value: 0.0,
                    rev_gross: 0.0,
                    rev_cost: 0.0,
                },
            )
            .map_err(|e| anyhow!("Failed recording tree auction: {}", e))
    }

    pub fn record_bid(&self, req: &BidRequest, bid: &Bid) -> Result<String, Error> {
        let features = self.extract_features(req, Some(bid));

        self.tree
            .train(
                &features,
                &RtbTrainingInput {
                    auctions: 0,
                    bids: 1,
                    bid_value: bid.price as f32,
                    impressions: 0,
                    rev_gross: 0.0,
                    rev_cost: 0.0,
                },
            )
            .map_err(|e| anyhow!("Failed recording tree bid: {}", e))?;

        Ok(encode_feature_string(&features)?)
    }

    pub fn record_impression(
        &self,
        bid_feature_key: &String,
        cpm_gross: f64,
        cpm_cost: f64,
    ) -> Result<(), Error> {
        let features = decode_feature_string(&bid_feature_key)?;

        self.tree
            .train(
                &features,
                &RtbTrainingInput {
                    auctions: 0,
                    bids: 0,
                    bid_value: 0.0,
                    impressions: 1,
                    rev_gross: cpm_gross as f32,
                    rev_cost: cpm_cost as f32,
                },
            )
            .map_err(|e| anyhow!("Failed recording tree impression: {}", e))
    }
}

impl Drop for TreeShaper {
    fn drop(&mut self) {
        for handle in &self.task_handles {
            handle.abort();
        }
    }
}
