use crate::app::pipeline::ortb::AuctionContext;
use crate::app::pipeline::ortb::context::{BidderCallout, BidderContext};
use crate::core::managers::DemandManager;
use crate::core::models::bidder::{Bidder, Endpoint};
use crate::core::spec::nobidreasons;
use anyhow::{Error, bail};
use async_trait::async_trait;
use pipeline::AsyncTask;
use rtb::BidRequest;
use rtb::bid_request::DistributionchannelOneof;
use rtb::child_span_info;
use rtb::common::bidresponsestate::BidResponseState;
use rtb::spec::adcom::devicetype;
use std::sync::Arc;
use tracing::debug;
use tracing::{Instrument, Span};

fn matches_endpoint(
    context: &AuctionContext,
    bidder: &Bidder,
    endpoint: &Endpoint,
    req: &BidRequest,
) -> bool {
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
    let device = match req.device.as_ref() {
        Some(device) => device,
        None => {
            span.record("bidder_endpoint_filter_reason", "missing_device");
            return false;
        }
    };
    let geo = match device.geo.as_ref() {
        Some(geo) => geo,
        None => {
            span.record("bidder_endpoint_filter_reason", "missing_geo");
            return false;
        }
    };

    let geo_match = targeting.geos.is_empty()
        || targeting.geos.first().unwrap() == "*"
        || targeting.geos.contains(&geo.country.to_uppercase());

    if !geo_match {
        span.record("bidder_endpoint_filter_reason", "geo_country");

        return false;
    }

    let format_match = req.imp.iter().any(|imp| {
        let f = &targeting.formats;

        (imp.banner.is_some() && f.banner)
            || (imp.video.is_some() && f.video)
            || (imp.native.is_some() && f.native)
            || (imp.audio.is_some() && f.audio)
    });

    if !format_match {
        span.record("bidder_endpoint_filter_reason", "ad_format");

        return false;
    }

    let channel = match &req.distributionchannel_oneof {
        Some(channel) => channel,
        None => return false,
    };

    let channel_match = match channel {
        DistributionchannelOneof::Site(_) => targeting.channels.site,
        DistributionchannelOneof::App(_) => targeting.channels.app,
        DistributionchannelOneof::Dooh(_) => targeting.channels.dooh,
    };

    if !channel_match {
        span.record("bidder_endpoint_filter_reason", "channel_type");

        return false;
    }

    match device.devicetype as u32 {
        devicetype::MOBILE_TABLET_GENERAL | devicetype::PHONE | devicetype::TABLET => {
            if !targeting.devices.mobile {
                span.record("bidder_endpoint_filter_reason", "device_type");

                return false;
            }
        }
        devicetype::PERSONAL_COMPUTER => {
            if !targeting.devices.desktop {
                span.record("bidder_endpoint_filter_reason", "device_type");

                return false;
            }
        }
        devicetype::CONNECTED_TV | devicetype::CONNECTED_DEVICE | devicetype::SET_TOP_BOX => {
            if !targeting.devices.ctv {
                span.record("bidder_endpoint_filter_reason", "device_type");

                return false;
            }
        }
        devicetype::DOOH => {
            if !targeting.devices.dooh {
                span.record("bidder_endpoint_filter_reason", "device_type");

                return false;
            }
        }
        _ => {
            span.record("bidder_endpoint_filter_reason", "unknown_device_type");

            return false;
        }
    }

    if !targeting.pubs.is_empty() && !targeting.pubs.contains(&context.pubid) {
        span.record("bidder_endpoint_filter_reason", "publisher_id");

        return false;
    }

    // if a company is supply & demand, dont send them
    // their own supply
    if context.pubid == bidder.id {
        span.record("bidder_endpoint_filter_reason", "self_publisher_id");

        return false;
    }

    true
}

fn get_filtered_matching(
    context: &AuctionContext,
    bidders: &Vec<(Arc<Bidder>, Vec<Arc<Endpoint>>)>,
) -> Vec<(Arc<Bidder>, Vec<Arc<Endpoint>>)> {
    let mut matches = Vec::with_capacity(bidders.len());

    let req = context.req.read();

    for (bidder, endpoints) in bidders {
        let mut bidder_matches = Vec::with_capacity(endpoints.len());

        for endpoint in endpoints {
            if matches_endpoint(context, bidder, endpoint, &req) {
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
    manager: Arc<DemandManager>,
}

impl BidderMatchingTask {
    pub fn new(manager: Arc<DemandManager>) -> Self {
        Self { manager }
    }

    async fn run0(&self, context: &AuctionContext) -> Result<(), Error> {
        let mut bidder_contexts = Vec::new();
        let span = Span::current();

        let matches = get_filtered_matching(context, self.manager.bidders_endpoints().as_ref());

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
                reqid: context.original_auction_id.clone(),
                nbr: nobidreasons::NO_BUYERS_PREMATCHED,
                desc: Some(msg),
            };

            context.res.set(brs).expect("Shouldnt have brs");

            bail!(msg);
        }

        debug!("Found {} matching pretargeting bidders", matches.len());

        for (bidder, endpoints) in matches {
            let mut callouts = Vec::with_capacity(endpoints.len());

            for endpoint in endpoints {
                let callout = BidderCallout {
                    endpoint: endpoint.clone(),
                    req: context.req.read().clone(),
                    ..Default::default()
                };

                callouts.push(callout);
            }

            bidder_contexts.push(BidderContext {
                bidder,
                callouts,
                ..Default::default()
            });
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
