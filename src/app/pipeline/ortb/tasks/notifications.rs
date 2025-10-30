use crate::app::pipeline::ortb::AuctionContext;
use crate::app::pipeline::ortb::context::{BidContext, BidResponseContext, BidderResponseState};
use crate::core::events;
use crate::core::events::DataUrl;
use crate::core::events::billing::{BillingEvent, BillingEventBuilder, EventSource};
use crate::core::models::bidder::{Bidder, Endpoint};
use anyhow::{Error, bail};
use async_trait::async_trait;
use log::debug;
use pipeline::AsyncTask;
use rtb::bid_response::Bid;
use rtb::child_span_info;
use rtb::utils::detect_ad_format;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{Instrument, warn};

fn build_billing_event(
    event_id: &String,
    bidder: &Bidder,
    endpoint: &Endpoint,
    bid: &Bid,
    source: EventSource,
) -> Result<BillingEvent, Error> {
    let ad_format = match detect_ad_format(bid) {
        Some(ad_format) => ad_format,
        None => bail!("Could not detect ad format when building billing event"),
    };

    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64;

    BillingEventBuilder::default()
        .bid_timestamp(timestamp)
        .auction_event_id(event_id.clone())
        .bid_event_id(bid.id.to_string()) // TODO this needs to be a real uuid from bid_context
        .bidder_id(bidder.name.clone())
        .endpoint_id(endpoint.name.clone())
        .cpm_cost(bid.price as f32)
        .cpm_gross(bid.price as f32)
        .pub_id("123".to_string())
        .event_source(source.clone())
        .bid_ad_format(ad_format)
        .build()
        .map_err(Error::from)
}

fn inject_burl(bid_context: &mut BidContext, base_url: &DataUrl) {
    let burl = match base_url.clone().add_string("source", "burl") {
        Ok(burl) => burl,
        Err(e) => {
            warn!("Failed to build burl, have to skip valid bids!: {}", e);
            bid_context.filter_reason.replace((
                rtb::spec::openrtb::lossreason::INTERNAL_ERROR,
                "Cant build burl".to_string(),
            ));
            return;
        }
    };

    // shit, need to cache partner burl here
    warn!("Cant inject burl on bid yet! Need a partner burl cache impl");
}

fn inject_beacon(bid_context: &mut BidContext, beacon_url: &DataUrl) {
    let beacon_url = match beacon_url.url(true) {
        Ok(beacon_url) => beacon_url,
        Err(e) => {
            warn!("Failed to build burl, have to skip valid bids!: {}", e);
            bid_context.filter_reason.replace((
                rtb::spec::openrtb::lossreason::INTERNAL_ERROR,
                "Cant build beacon url".to_string(),
            ));
            return;
        }
    };

    match events::injectors::adm::inject_adm_beacon(beacon_url, &mut bid_context.bid) {
        Ok(_) => debug!("Successfully injected billing beacon"),
        Err(e) => warn!("Failed to inject billing beacon: {}", e),
    }
}

fn inject_adm_beacon(
    event_id: &String,
    bid_context: &mut BidContext,
    event_domain: &String,
    billing_path: &String,
    bidder: &Bidder,
    endpoint: &Endpoint,
) {
    let billing_event_result = build_billing_event(
        event_id,
        bidder,
        endpoint,
        &bid_context.bid,
        EventSource::Adm,
    );

    let billing_event = match billing_event_result {
        Ok(event) => event,
        Err(e) => {
            warn!(
                "Failed to build billing event, have to skip valid bids!: {}",
                e
            );
            bid_context.filter_reason.replace((
                rtb::spec::openrtb::lossreason::INTERNAL_ERROR,
                "Cant build notice url".to_string(),
            ));
            return;
        }
    };

    let mut beacon_url = match DataUrl::new(event_domain, billing_path) {
        Ok(beacon_url) => beacon_url,
        Err(e) => {
            warn!(
                "Failed to build billing URL, have to skip valid bids!: {}",
                e
            );
            bid_context.filter_reason.replace((
                rtb::spec::openrtb::lossreason::INTERNAL_ERROR,
                "Cant build notice url".to_string(),
            ));
            return;
        }
    };

    match billing_event.write_to(&mut beacon_url) {
        Ok(_) => debug!("Successfully injected billing event params into url"),
        Err(e) => {
            warn!(
                "Failed to build billing URL, have to skip valid bids!: {}",
                e
            );
            bid_context.filter_reason.replace((
                rtb::spec::openrtb::lossreason::INTERNAL_ERROR,
                "Cant build notice url".to_string(),
            ));
            return;
        }
    };

    beacon_url.finalize();

    inject_beacon(bid_context, &beacon_url);
}

fn inject_event_handlers(
    event_id: &String,
    event_domain: &String,
    billing_path: &String,
    bidder: &Bidder,
    endpoint: &Endpoint,
    bid_response: &mut BidResponseContext,
) {
    for seat_context in bid_response.seatbids.iter_mut() {
        for bid_context in seat_context.bids.iter_mut() {
            if bid_context.filter_reason.is_some() {
                continue;
            }

            inject_adm_beacon(
                event_id,
                bid_context,
                event_domain,
                billing_path,
                bidder,
                endpoint,
            );
        }
    }
}

/// Responsible for injecting billing events into the bid response,
/// e.g. burl, adm beacon, vast impression.. depending on pub config and ad type
pub struct NotificationsUrlInjectionTask {
    /// The public domain (less proto) events should arrive to e.g. events.server.com
    event_domain: String,
    /// The path billing events (burl or adm) should arrive at e.g. /billing or /burl
    billing_path: String,
}

impl NotificationsUrlInjectionTask {
    /// Construct a task which is responsible for injecting events into
    /// the bid response. E.g. burl, lurl, or adm beacons
    ///
    /// # Arguments
    /// * 'event_domain' - The public domain (less proto) events should arrive to e.g. events.server.com
    /// * 'billing_path' - The path billing events (burl or adm) should arrive at e.g. /billing or /burl
    pub fn new(event_domain: String, billing_path: String) -> Self {
        NotificationsUrlInjectionTask {
            event_domain,
            billing_path,
        }
    }
}

// TODO per-pub configs here for notification types
impl NotificationsUrlInjectionTask {
    async fn run0(&self, context: &AuctionContext) -> Result<(), Error> {
        let mut bidders = context.bidders.lock().await;

        for bidder_context in bidders.iter_mut() {
            for callout in bidder_context.callouts.iter_mut() {
                let res_opt = callout.response.get_mut();

                if res_opt.is_none() {
                    continue;
                }

                let res = res_opt.unwrap();

                let bid_response_context = match &mut res.state {
                    BidderResponseState::Bid(bid_response) => bid_response,
                    _ => continue,
                };

                debug!(
                    "Injecting notification handlers for bid from {}:{}",
                    bidder_context.bidder.name, callout.endpoint.name
                );

                inject_event_handlers(
                    &context.event_id,
                    &self.event_domain,
                    &self.billing_path,
                    &bidder_context.bidder,
                    &callout.endpoint,
                    bid_response_context,
                );
            }
        }

        Ok(())
    }
}

#[async_trait]
impl AsyncTask<AuctionContext, Error> for NotificationsUrlInjectionTask {
    async fn run(&self, context: &AuctionContext) -> Result<(), Error> {
        let span = child_span_info!("notifications_injection_task");

        self.run0(context).instrument(span).await
    }
}
