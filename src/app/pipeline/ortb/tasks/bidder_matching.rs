use crate::app::pipeline::ortb::context::{BidderCallout, BidderContext};
use crate::app::pipeline::ortb::AuctionContext;
use crate::core::managers::bidders::BidderManager;
use crate::core::models::bidder::{Bidder, Endpoint};
use anyhow::{bail, Error};
use async_trait::async_trait;
use pipeline::AsyncTask;
use rtb::child_span_info;
use rtb::common::bidresponsestate::BidResponseState;
use rtb::BidRequest;
use std::sync::{Arc, OnceLock};
use tracing::log::debug;
use tracing::{Instrument, Span};

fn matches_endpoint(bidder: &Bidder, endpoint: &Endpoint, req: &BidRequest) -> bool {
    let span = child_span_info!(
            "bidder_endpoint_matching_task",
            bidder_name = tracing::field::Empty,
            endpoint_name = tracing::field::Empty,
            bidder_endpoint_filter_reason = tracing::field::Empty
        )
        .entered();

    span.record("bidder_name", &bidder.name);
    span.record("endpoint_name", &endpoint.name);

    if !endpoint.enabled {
        span.record("bidder_endpoint_filter_reason", "endpoint_disabled");

        return false;
    }

    let targeting = &endpoint.targeting;
    let device = &req.device.as_ref().expect("No device");
    let geo = &device.geo.as_ref().expect("No geo");

    let geo_match = targeting.geos.is_empty()
        || targeting.geos.first().unwrap() == "*"
        || targeting.geos.contains(&geo.country.to_uppercase());

    if !geo_match {
        span.record("bidder_endpoint_filter_reason", "geo_country");

        return false;
    }

    let mut format_match = false;

    for imp in &req.imp {
        let imp_match = (imp.banner.is_some() && targeting.banner)
            || (imp.video.is_some() && targeting.video)
            || (imp.native.is_some() && targeting.native);

        format_match = imp_match;
        break;
    }

    if !format_match {
        span.record("bidder_endpoint_filter_reason", "ad_format");

        return false;
    }

    true
}

fn get_filtered_matching(
    bidders: &Vec<(Arc<Bidder>, Vec<Arc<Endpoint>>)>,
    req: &BidRequest,
) -> Vec<(Arc<Bidder>, Vec<Arc<Endpoint>>)> {
    let mut matches = Vec::with_capacity(bidders.len());

    for (bidder, endpoints) in bidders {
        let mut bidder_matches = Vec::with_capacity(endpoints.len());

        for endpoint in endpoints {
            if matches_endpoint(bidder, endpoint, req) {
                bidder_matches.push(endpoint.clone());
            }
        }

        if !bidder_matches.is_empty() {
            matches.push((bidder.clone(), bidder_matches));
        }
    }

    matches
}

pub struct BidderMatchingTask {
    manager: Arc<BidderManager>,
}

impl BidderMatchingTask {
    pub fn new(manager: Arc<BidderManager>) -> Self {
        Self { manager }
    }

    async fn run0(&self, context: &AuctionContext) -> Result<(), Error> {
        let mut bidder_contexts = Vec::new();
        let span = Span::current();

        let matches =
            get_filtered_matching(self.manager.bidders().as_ref(), &*context.req.read());

        if !span.is_disabled() {
            span.record("bidder_matches_count", matches.len());
            span.record(
                "endpoints_matches_count",
                matches.iter().map(|(_, e)| e.len()).sum::<usize>(),
            );
            span.record("matches", tracing::field::debug(&matches));
        }

        if matches.is_empty() {
            let msg = "No matching bidders";

            let brs = BidResponseState::NoBidReason {
                reqid: context.req.read().id.clone(),
                nbr: rtb::spec::nobidreason::BLOCKED_PUB_OR_SITE,
                desc: Some(msg),
            };

            context.res.set(brs).expect("Shouldnt have brs");

            bail!(msg);
        }

        debug!("Found {} matching pretargeting bidders", matches.len());

        for (bidder, endpoints) in matches {
            let mut callouts = Vec::with_capacity(endpoints.len());

            for endpoint in endpoints {
                callouts.push(BidderCallout {
                    endpoint: endpoint.clone(),
                    req: context.req.read().clone(),
                    response: OnceLock::new(),
                });
            }

            bidder_contexts.push(BidderContext { bidder, callouts });
        }

        *context.bidders.lock().await = bidder_contexts;

        Ok(())
    }
}

#[async_trait]
impl AsyncTask<AuctionContext, Error> for BidderMatchingTask {
    async fn run(&self, context: &AuctionContext) -> Result<(), Error> {
        let span = child_span_info!(
                "bidder_matching_task",
                bidder_matches_count = tracing::field::Empty,
                endpoints_matches_count = tracing::field::Empty,
                matches = tracing::field::Empty
            );

        self.run0(context).instrument(span).await
    }
}
