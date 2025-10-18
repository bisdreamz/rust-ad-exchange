use std::sync::Arc;
use anyhow::{bail, Error};
use parking_lot::Mutex;
use pipeline::BlockingTask;
use rtb::BidRequest;
use rtb::common::bidresponsestate::BidResponseState;
use crate::app::pipeline::ortb::AuctionContext;
use crate::app::pipeline::ortb::context::BidderContext;
use crate::core::managers::bidders::BidderManager;
use crate::core::models::bidder::{Bidder, Endpoint};

pub struct BidderMatchingTask {
    manager: Arc<BidderManager>
}

impl BidderMatchingTask {
    pub fn new(manager: Arc<BidderManager>) -> Self {
        Self { manager }
    }

    fn matches_endpoint(_bidder: &Bidder, endpoint: &Endpoint, req: &BidRequest) -> bool {
        if !endpoint.enabled {
            return false;
        }

        let targeting = &endpoint.targeting;
        let device = &req.device.as_ref().expect("No device");
        let geo = &device.geo.as_ref().expect("No geo");

        let geo_match = targeting.geos.is_empty() ||
                targeting.geos.first().unwrap() == "*" ||
                targeting.geos.contains(&geo.country.to_uppercase());

        if !geo_match {
            return false;
        }

        let mut format_match = false;

        for imp in &req.imp {
            let imp_match = (imp.banner.is_some() && targeting.banner) ||
                (imp.video.is_some() && targeting.video) ||
                    (imp.native.is_some() && targeting.native);

            format_match = imp_match;
            break;
        }

        format_match
    }

    fn get_filtered_matching(bidders: &Vec<Bidder>, req: &BidRequest) -> Vec<Bidder> {
        let mut matches = bidders.clone();

        for bidder in &mut matches {
            let mut endpoints = std::mem::take(&mut bidder.endpoints);
            endpoints.retain(|endpoint| {
                Self::matches_endpoint(bidder, endpoint, req)
            });
            bidder.endpoints = endpoints;
        }

        matches.retain(|bidder| !bidder.endpoints.is_empty());

        matches
    }
}

impl BlockingTask<AuctionContext, Error> for BidderMatchingTask {
    fn run(&self, context: &AuctionContext) -> Result<(), Error> {
        let matches = Self::get_filtered_matching(self.manager.bidders().as_ref(),
            &*context.req.read());

        if matches.is_empty() {
            let msg = "No matching bidders";

            let brs = BidResponseState::NoBidReason {
                reqid: context.req.read().id.clone(),
                nbr: rtb::spec::nobidreason::BLOCKED_PUB_OR_SITE,
                desc: Some(msg)
            };

            context.res.set(brs).expect("Shouldnt have brs");

            bail!(msg);
        }

        println!("Found {} matching pretargeting bidders", matches.len());

        let mut bidder_contexts = Vec::with_capacity(matches.len());

        for bidder in matches {
            bidder_contexts.push(BidderContext {
                bidder,
                reqs: Mutex::new(vec![context.req.read().clone()])
            })
        }

        *context.bidders.lock() = bidder_contexts;

        Ok(())
    }
}