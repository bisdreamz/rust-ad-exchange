use crate::app::pipeline::ortb::AuctionContext;
use crate::app::pipeline::ortb::context::{BidderCallout, BidderContext};
use crate::app::pipeline::ortb::targeting::matches_targeting;
use crate::core::managers::deals::BidderDeals;
use crate::core::managers::{DealManager, DemandManager};
use crate::core::models::bidder::{Bidder, Endpoint};
use crate::core::models::deal::{Deal, DealPricing, DemandPolicy};
use crate::core::models::placement::FillPolicy;
use crate::core::spec::nobidreasons;
use anyhow::{Error, bail};
use async_trait::async_trait;
use opentelemetry::metrics::Counter;
use opentelemetry::{KeyValue, global};
use pipeline::AsyncTask;
use rtb::BidRequest;
use rtb::bid_request::DistributionchannelOneof;
use rtb::bid_request::{Deal as RtbDeal, Pmp};
use rtb::child_span_info;
use rtb::spec::adcom::devicetype;
use std::sync::{Arc, LazyLock};
use tracing::debug;
use tracing::{Instrument, Span};

use crate::app::pipeline::ortb::direct::pacing::DealPacer;

static COUNTER_RTB_DEALS_INJECTED: LazyLock<Counter<u64>> = LazyLock::new(|| {
    global::meter("rex:demand:matching")
        .u64_counter("matching.rtb_deals_injected")
        .with_description("Total PMP deals injected across all bidders/imps")
        .with_unit("1")
        .build()
});

static COUNTER_BIDDERS_SKIPPED_DEALS_ONLY: LazyLock<Counter<u64>> = LazyLock::new(|| {
    global::meter("rex:demand:matching")
        .u64_counter("matching.bidders_skipped_deals_only")
        .with_description("Bidders filtered out by deals-only fill policy")
        .with_unit("1")
        .build()
});

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

    if !targeting.os.is_empty() {
        let os_match = context
            .device
            .get()
            .map(|d| targeting.os.contains(&d.os))
            .unwrap_or(false);

        if !os_match {
            span.record("bidder_endpoint_filter_reason", "os_type");
            return false;
        }
    }

    if !targeting.pubs.is_empty() && !targeting.pubs.contains(&context.publisher.id) {
        span.record("bidder_endpoint_filter_reason", "publisher_id");

        return false;
    }

    // if a company is supply & demand, dont send them
    // their own supply
    if context.publisher.id == bidder.id {
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

/// Evaluates RTB deals for a single imp. Checks private deals first;
/// if any private deal matches, only those are returned (private auction).
/// Otherwise returns matching open deals.
///
/// Returns (matched_deals, is_private_auction).
fn eval_deals_for_imp(
    bidder_deals: &BidderDeals,
    deal_pacer: &dyn DealPacer,
    ctx: &AuctionContext,
    imp: &rtb::bid_request::Imp,
) -> (Vec<Arc<Deal>>, bool) {
    // Check private deals first
    let private_matches: Vec<Arc<Deal>> = bidder_deals
        .private
        .iter()
        .filter(|d| matches_targeting(&d.targeting.common, ctx, imp) && deal_pacer.passes(d))
        .cloned()
        .collect();

    if !private_matches.is_empty() {
        return (private_matches, true);
    }

    // Fall back to open deals
    let open_matches: Vec<Arc<Deal>> = bidder_deals
        .open
        .iter()
        .filter(|d| matches_targeting(&d.targeting.common, ctx, imp) && deal_pacer.passes(d))
        .cloned()
        .collect();

    (open_matches, false)
}

/// Builds the RTB PMP object from matched deals and injects it onto the imp.
fn inject_pmp(imp: &mut rtb::bid_request::Imp, deals: &[Arc<Deal>], is_private: bool) {
    let rtb_deals: Vec<RtbDeal> = deals
        .iter()
        .map(|d| {
            let (bidfloor, at) = match &d.pricing {
                DealPricing::Inherit => (0.0, 1), // inherit imp floor, first price
                DealPricing::Floor(f) => (*f, 1), // floor price, first price
                DealPricing::Fixed(f) => (*f, 3), // fixed/agreed price
            };

            let wseat = match &d.policy {
                DemandPolicy::Rtb { wdsps, .. } => wdsps
                    .iter()
                    .flat_map(|(_, seats)| seats.iter().cloned())
                    .collect(),
                _ => vec![],
            };

            RtbDeal {
                id: d.id.clone(),
                bidfloor,
                at,
                wseat,
                ..Default::default()
            }
        })
        .collect();

    imp.pmp = Some(Pmp {
        private_auction: is_private,
        deals: rtb_deals,
        ..Default::default()
    });
}

pub struct BidderMatchingTask {
    manager: Arc<DemandManager>,
    deal_manager: Arc<DealManager>,
    deal_pacer: Arc<dyn DealPacer>,
}

impl BidderMatchingTask {
    pub fn new(
        manager: Arc<DemandManager>,
        deal_manager: Arc<DealManager>,
        deal_pacer: Arc<dyn DealPacer>,
    ) -> Self {
        Self {
            manager,
            deal_manager,
            deal_pacer,
        }
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
            debug!("No matching bidders");

            let _ = context
                .rtb_nbr
                .set((nobidreasons::NO_BUYERS_PREMATCHED, "No matching bidders"));
            bail!("No matching bidders");
        }

        debug!("Found {} matching pretargeting bidders", matches.len());

        let deals_only_rtb = context
            .placement
            .as_ref()
            .map(|p| matches!(p.fill_policy, FillPolicy::DirectAndRtbDeals))
            .unwrap_or(false);

        let mut rtb_deals_injected_count: u64 = 0;
        let mut bidders_skipped_deals_only: u64 = 0;

        for (bidder, endpoints) in matches {
            let bidder_deals = self.deal_manager.rtb_deals_for_bidder(&bidder.id);
            let mut callouts = Vec::with_capacity(endpoints.len());

            for endpoint in endpoints {
                let mut req = context.req.read().clone();
                let mut any_imp_has_deal = false;

                if let Some(ref deals) = bidder_deals {
                    for imp in req.imp.iter_mut() {
                        let (matched_deals, is_private) =
                            eval_deals_for_imp(deals, &*self.deal_pacer, context, imp);
                        if !matched_deals.is_empty() {
                            rtb_deals_injected_count += matched_deals.len() as u64;
                            inject_pmp(imp, &matched_deals, is_private);
                            any_imp_has_deal = true;
                        }
                    }
                }

                if deals_only_rtb && !any_imp_has_deal {
                    bidders_skipped_deals_only += 1;
                    continue; // skip this endpoint — no deals under deals-only policy
                }

                callouts.push(BidderCallout {
                    endpoint: endpoint.clone(),
                    req,
                    ..Default::default()
                });
            }

            if !callouts.is_empty() {
                bidder_contexts.push(BidderContext {
                    bidder,
                    callouts,
                    ..Default::default()
                });
            }
        }

        // Tracing + metrics
        if !span.is_disabled() {
            span.record("rtb_deals_injected_count", rtb_deals_injected_count);
            span.record("bidders_skipped_deals_only", bidders_skipped_deals_only);
        }

        let pub_id = context.publisher.id.clone();

        if rtb_deals_injected_count > 0 {
            COUNTER_RTB_DEALS_INJECTED.add(
                rtb_deals_injected_count,
                &[KeyValue::new("pub_id", pub_id.clone())],
            );
        }

        if bidders_skipped_deals_only > 0 {
            COUNTER_BIDDERS_SKIPPED_DEALS_ONLY.add(
                bidders_skipped_deals_only,
                &[KeyValue::new("pub_id", pub_id)],
            );
        }

        context.bidders.lock().await.extend(bidder_contexts);

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
            matches = tracing::field::Empty,
            rtb_deals_injected_count = tracing::field::Empty,
            bidders_skipped_deals_only = tracing::field::Empty
        );

        self.run0(context).instrument(span).await
    }
}
