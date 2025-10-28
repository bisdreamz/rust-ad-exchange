use crate::app::pipeline::ortb::AuctionContext;
use crate::app::pipeline::ortb::context::{
    BidContext, BidResponseContext, BidderCallout, BidderContext, BidderResponseState,
};
use crate::core::events;
use crate::core::events::DataUrl;
use crate::core::models::bidder::{Bidder, Endpoint};
use anyhow::Error;
use async_trait::async_trait;
use log::debug;
use pipeline::AsyncTask;
use rtb::{BidResponse, child_span_info};
use tracing::{Instrument, warn};

fn build_base_url(
    reqid: &String,
    domain: String,
    path: String,
    bidder: &Bidder,
    endpoint: &Endpoint,
) -> Result<DataUrl, Error> {
    let mut data_url = DataUrl::new(domain.as_str(), path.as_str())?;

    data_url.add_string("cb", reqid)?;
    data_url.add_string("buyer_id", &bidder.name)?;
    data_url.add_string("buyer_ep", &endpoint.name)?;
    // data_url.add_string("gp", )
    // TODO publisher details data_url.add_string("buyer_ep", &callout.endpoint.name)?;

    Ok(data_url)
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

    let mut bid = &bid_context.bid;
    // shit, need to cache partner burl here
    warn!("Cant inject burl on bid yet! Need a partner burl cache impl");
}

fn inject_beacon(bid_context: &mut BidContext, base_url: &DataUrl) {
    let mut beacon_data_url = base_url.clone();

    if let Err(e) = beacon_data_url.add_string("source", "beacon") {
        warn!("Failed to build burl, have to skip valid bids!: {}", e);
        bid_context.filter_reason.replace((
            rtb::spec::openrtb::lossreason::INTERNAL_ERROR,
            "Cant build beacon url".to_string(),
        ));
        return;
    }

    beacon_data_url.finalize();

    let beacon_url = match beacon_data_url.url(true) {
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

fn inject_event_handlers(
    reqid: &String,
    bidder: &Bidder,
    endpoint: &Endpoint,
    bid_response: &mut BidResponseContext,
) {
    for seat_context in bid_response.seatbids.iter_mut() {
        for bid_context in seat_context.bids.iter_mut() {
            if bid_context.filter_reason.is_some() {
                continue;
            }

            let base_url_result = build_base_url(
                reqid,
                "localhost".to_string(),
                "/bill".to_string(),
                bidder,
                endpoint,
            );

            if let Err(e) = base_url_result {
                warn!(
                    "Failed to build base notification URL, have to skip valid bids!: {}",
                    e
                );
                bid_context.filter_reason.replace((
                    rtb::spec::openrtb::lossreason::INTERNAL_ERROR,
                    "Cant build notice url".to_string(),
                ));
                continue;
            }

            let base_url = base_url_result.unwrap();

            inject_beacon(bid_context, &base_url);
        }
    }
}

/// Responsible for injecting billing notifications into the bid response,
/// e.g. burl, adm beacon, vast impression.. depending on pub config and ad type
pub struct NotificationsUrlInjectionTask;

// TODO per-pub configs here for notification types
impl NotificationsUrlInjectionTask {
    async fn run0(&self, context: &AuctionContext) -> Result<(), Error> {
        let mut bidders = context.bidders.lock().await;
        let req = context.req.read();

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
                    &req.id,
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
