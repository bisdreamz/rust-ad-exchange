use crate::core::cluster::ClusterDiscovery;
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
            qps_boost_eligible: tick.effective_boost_eligible_qps(),
        },
        None => {
            // this happens when no data yet or nothing satisfies the
            // target criteria (qps or min threshold)
            ThresholdState {
                threshold: 0.0,
                qps_passing: 0,
                qps_boost_eligible: tick.effective_boost_eligible_qps(),
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
    /// Current calculated threshold which produces the
    /// associated passing qps
    pub threshold: f32,
    /// Total measured QPS available last second
    /// to this endpoint shaper on this particular node
    pub qps_avail: u32,
    /// Total QPS passing the target metric as measured
    /// by this endpoint shaper on this node
    pub qps_passing: u32,
    /// Total QPS that was recorded as being boost eligible
    pub qps_boost_eligible: u32,
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
    pub raw_prediction: Option<RtbPredictionOutput>,
}

struct DynamicConfig {
    metric: Metric,
    explore_percent: u32,
    qps_limit: u32,
    min_threshold: f32,
}

/// Derive the node-local QPS share from the cluster-wide endpoint limit.
/// Returns 0 when no limit is configured (unlimited).
fn local_qps_limit(limit: u32, cluster_size: usize) -> u32 {
    match limit {
        0 => 0,
        limit => (limit / cluster_size.max(1) as u32).max(1),
    }
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
    /// So we can swap metric directly to update
    /// the prediction handler when sorting
    /// multi value branches for highest value
    handler_metric: Arc<ArcSwap<Metric>>,
    cluster: Arc<dyn ClusterDiscovery>,
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
        cluster: Arc<dyn ClusterDiscovery>,
    ) -> Self {
        let arc_metric = Arc::new(ArcSwap::from_pointee(metric.clone()));

        let str_features = features.iter().map(|f| f.to_string()).collect();
        let first_pred_handler = RtbPredictionHandler::new(
            min_decision_auctions,
            segment_ttl.as_secs() as u32,
            arc_metric.clone(),
        );

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
            let cluster = cluster.clone();

            tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_secs(1));
                loop {
                    interval.tick().await;
                    let cfg = config.load();
                    let local_qps = local_qps_limit(cfg.qps_limit, cluster.cluster_size());
                    let avail_qps = state.load().qps_avail;
                    let effective_avail_qps =
                        tree::utils::calc_effective_avail_pool(local_qps, avail_qps);
                    let passing_qps_budget =
                        tree::utils::qps_budget_passing(cfg.explore_percent, effective_avail_qps);

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
            handler_metric: arc_metric,
            cluster,
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
        let old_metric = self.handler_metric.load_full();
        if old_metric.as_ref() != &metric {
            info!(
                "Updating metric from {} to {} for endpoint",
                old_metric, metric
            );
            self.handler_metric.store(Arc::new(metric.clone()));
        }

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
        let cfg = self.config.load();
        let local_qps_limit = local_qps_limit(cfg.qps_limit, self.cluster.cluster_size());

        // for some metrics like exploratory we want to calculate
        // off of the "effective" qps that is
        // min(local endpoint limit, actually available)
        let effective_qps_limit =
            tree::utils::calc_effective_avail_pool(local_qps_limit, state.qps_avail);

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
                    raw_prediction: None,
                });
            }
        };

        let metric_value = prediction.value.metric_value(&self.config.load().metric);

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
                raw_prediction: Some(prediction.value),
            });
        }

        // calculate what is remaining available after consumed by passing
        let avail_after_passing = state.qps_avail.saturating_sub(state.qps_passing);

        // inbound = 10,000
        // limit = 1000
        // passing = 950
        // exploratory_qps = percent(exploratory_perc, effective_limit) = 50
        // actual passing check = .. passes_percent(exploratory_qps, avail_after_passing)
        let qps_exploratory =
            tree::utils::qps_budget_exploratory(cfg.explore_percent, effective_qps_limit);

        if tree::utils::qps_passes_percentage(qps_exploratory, avail_after_passing) {
            debug!("Request failed shaping but passed exploratory");

            return Ok(ShapingResult {
                decision: ShapingDecision::PassedExploratory,
                metric_value,
                metric_target: state.threshold,
                pred_depth: prediction.depth as u32,
                features: req_features,
                raw_prediction: Some(prediction.value),
            });
        }

        // boost is the "free space" we have within our
        // endpoint QPS limit, unused currently by
        // exploratory or passing volume. we will
        // allow us to borrow up to the endpoint
        // limit to accelerate shaping learnings on new
        // or underobserved ad segments
        let free_budget_after_passing_exploratory = effective_qps_limit
            .saturating_sub(qps_exploratory)
            .saturating_sub(state.qps_passing);

        if free_budget_after_passing_exploratory == 0 {
            debug!(
                "Request failed shaping. Full QPS allocation used by passing and exploratory, blocking boost"
            );

            return Ok(ShapingResult {
                decision: ShapingDecision::Blocked,
                metric_value,
                metric_target: state.threshold,
                pred_depth: prediction.depth as u32,
                features: req_features,
                raw_prediction: Some(prediction.value),
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
                raw_prediction: Some(prediction.value),
            });
        }

        // we keep counters specific to boost eligible
        // reqs so we can calculate their passing
        // percentages properly since we only want
        // to count the under-trained inventory here
        self.histogram.record_boost_eligible_request();

        // now we have the qps of boost, calculate its passing
        // against the total effective *available* qps pool
        // from the publisher remaining, which we
        // will observe at this code path
        if tree::utils::qps_passes_percentage(
            free_budget_after_passing_exploratory,
            state.qps_boost_eligible,
        ) {
            debug!("Request passing through boost");

            return Ok(ShapingResult {
                decision: ShapingDecision::PassedBoost,
                metric_value,
                metric_target: state.threshold,
                pred_depth: prediction.depth as u32,
                features: req_features,
                raw_prediction: Some(prediction.value),
            });
        }

        debug!(
            "Request failed shaping, exploratory, and boost - blocking! Had {} boost QPS",
            state.qps_boost_eligible
        );

        Ok(ShapingResult {
            decision: ShapingDecision::Blocked,
            metric_value,
            metric_target: state.threshold,
            pred_depth: prediction.depth as u32,
            features: req_features,
            raw_prediction: Some(prediction.value),
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
            .map_err(|e| anyhow!("Failed recording tree impression: {}", e))?;

        match self
            .tree
            .predict(&features)
            .map_err(|e| anyhow!("Failed debug predict on tree impression: {}", e))?
        {
            Some(prediction) => {
                debug!(
                    "Pred result after imp train:: {:?}\t Prediction: {:?} rev gross {}",
                    features, prediction, cpm_gross
                );
            }
            None => {
                warn!(
                    "No pred result yet after train, features: {:?} cpm gross {}",
                    features, cpm_gross
                );
            }
        }

        Ok(())
    }
}

impl Drop for TreeShaper {
    fn drop(&mut self) {
        for handle in &self.task_handles {
            handle.abort();
        }
    }
}
