use crate::app::pipeline::ortb::context::{BidderCallout, CalloutSkipReason};
use crate::app::pipeline::ortb::AuctionContext;
use crate::core::cluster::ClusterDiscovery;
use crate::core::managers::DemandManager;
use crate::core::spec::nobidreasons;
use anyhow::{anyhow, bail, Error};
use arc_swap::ArcSwap;
use async_trait::async_trait;
use governor::{DefaultDirectRateLimiter, Quota, RateLimiter};
use pipeline::AsyncTask;
use rtb::child_span_info;
use rtb::common::bidresponsestate::BidResponseState;
use std::collections::HashMap;
use std::num::NonZeroU32;
use std::sync::Arc;
use tracing::{debug, trace, warn, Instrument, Span};

/// Responsible for enforcing QPS limits per endpoint,
/// for callouts which do not already have a skip_reason assigned
pub struct QpslimiterTask {
    endpoints: Arc<ArcSwap<HashMap<String, Option<DefaultDirectRateLimiter>>>>,
}

impl QpslimiterTask {
    pub fn new(bidder_manager: Arc<DemandManager>, cluster: Arc<dyn ClusterDiscovery>) -> Self {
        let endpoint_limiters = Self::rebuild_limiters_map(&bidder_manager, cluster.cluster_size());

        let endpoints = Arc::new(ArcSwap::from_pointee(endpoint_limiters));

        let endpoints_clone = endpoints.clone();
        let bidder_manager_clone = bidder_manager.clone();

        cluster.on_change(Box::new(move |cluster_sz| {
            debug!(
                "Cluster size changed to {}, rebuilding QPS limiters map",
                cluster_sz
            );

            let new_limiters =
                QpslimiterTask::rebuild_limiters_map(&bidder_manager_clone, cluster_sz);

            endpoints_clone.store(Arc::new(new_limiters));
        }));

        Self { endpoints }
    }

    fn rebuild_limiters_map(
        bidder_manager: &DemandManager,
        cluster_sz: usize,
    ) -> HashMap<String, Option<DefaultDirectRateLimiter>> {
        let mut endpoints_limiters = HashMap::new();

        assert!(cluster_sz > 0, "Cluster cannot be empty!");

        bidder_manager
            .bidders_endpoints()
            .iter()
            .for_each(|(_, endpoints)| {
                for endpoint in endpoints {
                    if !endpoint.enabled {
                        continue;
                    }

                    let rl = if endpoint.qps < 1 {
                        debug!("Endpoint {} QPS limit: None", endpoint.name);
                        None
                    } else {
                        let local_qps = (endpoint.qps / cluster_sz).max(1);

                        debug!(
                            "Endpoint {} QPS limit: {} ({} effective locally)",
                            endpoint.name, endpoint.qps, local_qps
                        );
                        Some(RateLimiter::direct(Quota::per_second(
                            NonZeroU32::new(local_qps as u32).unwrap(),
                        )))
                    };

                    endpoints_limiters.insert(endpoint.name.clone(), rl);
                }
            });

        endpoints_limiters
    }

    fn should_block(&self, callout: &BidderCallout) -> bool {
        let span = child_span_info!(
            "qps_limiter_endpoint_should_block",
            endpoint_name = &callout.endpoint.name,
            qps_passed = tracing::field::Empty,
        )
        .entered();

        let endpoints = self.endpoints.load();

        let rl_opt = match endpoints.get(&callout.endpoint.name) {
            Some(rl_opt) => rl_opt,
            None => {
                warn!(
                    "No QPS limiter entry for endpoint {}, panic! Blocking request!",
                    &callout.endpoint.name
                );
                return true;
            }
        };

        let rl = match rl_opt {
            Some(rl) => rl,
            None => {
                trace!(
                    "No QPS limit for endpoint {}, allowing request through",
                    &callout.endpoint.name
                );
                span.record("qps_passed", true);
                return false;
            }
        };

        match rl.check().is_err() {
            false => {
                debug!("Endpoint passed QPS limiter");
                span.record("qps_passed", false);
                false
            }
            true => {
                debug!("Endpoint failed QPS limiter");
                span.record("qps_passed", true);
                true
            }
        }
    }

    async fn run0(&self, context: &AuctionContext) -> Result<(), Error> {
        let span = Span::current();

        let bidder_contexts = context.bidders.lock().await;

        let mut total_callouts = 0;
        let mut callouts_passed = 0;
        for bidder_context in bidder_contexts.iter() {
            for callout in bidder_context.callouts.iter() {
                if callout.skip_reason.get().is_some() {
                    continue; // someone already blocked this guy
                }

                total_callouts += 1;

                if self.should_block(callout) {
                    debug!(
                        "Endpoint {} throttled by QPS limiter",
                        callout.endpoint.name
                    );
                    callout
                        .skip_reason
                        .set(CalloutSkipReason::QpsLimit)
                        .unwrap_or_else(|_| warn!("Failed assigning skip reason on context!"));
                    continue;
                }

                callouts_passed += 1;

                debug!("Endpoint {} passed QPS limiter", &callout.endpoint.name);
            }
        }

        span.record("bidders_count", bidder_contexts.len() as u64);
        span.record("possible_callouts", total_callouts);
        span.record("callouts_passed", callouts_passed);

        if callouts_passed == 0 {
            let brs = BidResponseState::NoBidReason {
                reqid: context.req.read().id.clone(),
                nbr: nobidreasons::THROTTLED_BUYER_QPS,
                desc: "Demand QPS Saturated".into(),
            };

            context
                .res
                .set(brs)
                .map_err(|_| anyhow!("Failed assigning no buyer qps reason on context!"))?;

            bail!("No endpoints survived prior filtering or QPS filtering, bailing");
        }

        Ok(())
    }
}

#[async_trait]
impl AsyncTask<AuctionContext, Error> for QpslimiterTask {
    async fn run(&self, context: &AuctionContext) -> Result<(), Error> {
        let span = child_span_info!(
            "qps_limiter_task",
            bidders_count = tracing::field::Empty,
            possible_callouts = tracing::field::Empty,
            callouts_passed = tracing::field::Empty,
        );

        self.run0(context).instrument(span).await
    }
}
