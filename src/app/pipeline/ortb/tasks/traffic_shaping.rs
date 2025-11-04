use crate::app::pipeline::ortb::AuctionContext;
use crate::app::pipeline::ortb::context::{BidderCallout, BidderContext, CalloutSkipReason};
use crate::core::managers::BidderManager;
use crate::core::models::shaping::TrafficShaping;
use crate::core::shaping::tree::{ShapingDecision, TreeShaper};
use anyhow::Error;
use async_trait::async_trait;
use pipeline::AsyncTask;
use rtb::{BidRequest, child_span_info};
use std::collections::HashMap;
use std::time::Duration;
use tracing::{Instrument, debug, info, warn};

pub struct TrafficShapingTask {
    endpoints: HashMap<String, Option<TreeShaper>>,
}

impl TrafficShapingTask {
    pub fn new(manager: &BidderManager) -> Self {
        let mut shape_map = HashMap::new();

        for (bidder, endpoints) in manager.bidders() {
            for endpoint in endpoints {
                if endpoint.enabled == false {
                    debug!("Skipping endpoint {} because it is not enabled", endpoint.name);
                    continue;
                }

                let shaping_opt = match &endpoint.shaping {
                    TrafficShaping::None => {
                        info!(
                            "Bidder {} endpoint {} disabled shaping",
                            bidder.name, endpoint.name
                        );
                        None
                    }
                    TrafficShaping::Tree {
                        control_percent,
                        metric,
                        features,
                    } => {
                        info!(
                            "Bidder {} endpoint {} shaping ctrl {}% metric {} features {:?}",
                            bidder.name, endpoint.name, control_percent, metric, features
                        );

                        Some(TreeShaper::new(
                            &features,
                            &metric,
                            1000,
                            &Duration::from_secs(10 * 60),
                            control_percent.clone(),
                            endpoint.qps as u32
                        ))
                    }
                };

                shape_map.insert(endpoint.name.clone(), shaping_opt);
            }
        }

        Self {
            endpoints: shape_map,
        }
    }

    fn evaluate_record_endpoint(
        &self,
        bidder_context: &BidderContext,
        callout: &BidderCallout,
        req: &BidRequest,
    ) {
        let shaper_opt = match self.endpoints.get(&callout.endpoint.name) {
            Some(shaper) => shaper,
            None => {
                // all should have a Some/None entry
                warn!(
                    "No shaping settings registered for endpoint {}! Skipping!",
                    callout.endpoint.name
                );
                return;
            }
        };

        let shaper = match shaper_opt {
            Some(shaper) => shaper,
            None => {
                debug!(
                    "Shaping disabled for endpoint {}, skipping",
                    callout.endpoint.name
                );
                return;
            }
        };

        match shaper.passes_shaping(req) {
            Ok(res) => {
                debug!(
                    "Bidder {} endpoint {} shaping result: {:?}",
                    bidder_context.bidder.name, callout.endpoint.name, res
                );

                if matches!(res.decision, ShapingDecision::Blocked) {
                    callout
                        .skip_reason
                        .set(CalloutSkipReason::TrafficShaping)
                        .unwrap_or_else(|_| {
                            warn!("Failed to set skip reason, already exists on ctx")
                        });
                }
            }
            Err(err) => {
                warn!(
                    "Shaping failed for endpoint {}: {}",
                    callout.endpoint.name, err
                );
            }
        }
    }

    async fn run0(&self, context: &AuctionContext) -> Result<(), Error> {
        let bidders = context.bidders.lock().await;
        let bid_request = context.req.read();

        for bidder_context in bidders.iter() {
            for callout in &bidder_context.callouts {
                if callout.skip_reason.get().is_some() {
                    continue;
                }

                self.evaluate_record_endpoint(bidder_context, callout, &bid_request);
            }
        }

        Ok(())
    }
}

#[async_trait]
impl AsyncTask<AuctionContext, Error> for TrafficShapingTask {
    async fn run(&self, context: &AuctionContext) -> Result<(), Error> {
        let span = child_span_info!("traffic_shaping_task");

        self.run0(context).instrument(span).await
    }
}
