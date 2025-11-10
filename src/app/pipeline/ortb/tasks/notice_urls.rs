use crate::app::pipeline::ortb::context::{BidContext, BidResponseContext, BidderResponseState};
use crate::app::pipeline::ortb::AuctionContext;
use crate::core::events::billing::{BillingEvent, BillingEventBuilder};
use crate::core::models::bidder::{Bidder, Endpoint};
use anyhow::{anyhow, bail, Error};
use async_trait::async_trait;
use log::debug;
use pipeline::AsyncTask;
use rtb::child_span_info;
use rtb::common::bidresponsestate::BidResponseState;
use rtb::common::DataUrl;
use rtb::utils::detect_ad_format;
use tracing::{warn, Instrument};

fn build_billing_event(
    event_id: &String,
    bidder: &Bidder,
    endpoint: &Endpoint,
    bid_context: &BidContext,
) -> Result<BillingEvent, Error> {
    let ad_format = match detect_ad_format(&bid_context.bid) {
        Some(ad_format) => ad_format,
        None => bail!("Could not detect ad format when building billing event"),
    };

    let timestamp = rtb::common::utils::epoch_timestamp();

    let cpm_cost = match bid_context.reduced_bid_price {
        Some(price) => price,
        None => bail!("CPM cost has not ben assigned to bid context! What is our margin?")
    };

    BillingEventBuilder::default()
        .bid_timestamp(timestamp)
        .auction_event_id(event_id.clone())
        .bid_event_id(bid_context.bid_event_id.clone()) // TODO this needs to be a real uuid from bid_context
        .bidder_id(bidder.name.clone())
        .endpoint_id(endpoint.name.clone())
        .cpm_cost(cpm_cost)
        .cpm_gross(bid_context.original_bid_price)
        .pub_id("123".to_string())
        .event_source(None)
        .bid_ad_format(ad_format)
        .build()
        .map_err(Error::from)
}

fn build_event_url(
    event_id: &String,
    bid_context: &mut BidContext,
    event_domain: &String,
    billing_path: &String,
    bidder: &Bidder,
    endpoint: &Endpoint,
) -> Result<DataUrl, Error> {
    let billing_event_result = build_billing_event(
        event_id,
        bidder,
        endpoint,
        &bid_context
    );

    let billing_event = match billing_event_result {
        Ok(event) => event,
        Err(e) => {
            bid_context.filter_reason.replace((
                rtb::spec::openrtb::lossreason::INTERNAL_ERROR,
                "Cant build notice url".to_string(),
            ));

            bail!(
                "Failed to build billing event, have to skip valid bids!: {}",
                e
            );
        }
    };

    let mut beacon_url = match DataUrl::new(event_domain, billing_path) {
        Ok(beacon_url) => beacon_url,
        Err(e) => {
            bail!(
                "Failed to build billing URL, have to skip valid bids!: {}",
                e
            );
        }
    };

    match billing_event.write_to(&mut beacon_url) {
        Ok(_) => debug!("Successfully injected billing event params into url"),
        Err(e) => {
            bail!(
                "Failed to build billing URL, have to skip valid bids!: {}",
                e
            );
        }
    };

    Ok(beacon_url)
}

/// Injects billing URLs
/// Returns true if ALL bids present *failed* processing
fn attach_event_handler_urls(
    event_id: &String,
    event_domain: &String,
    billing_path: &String,
    bidder: &Bidder,
    endpoint: &Endpoint,
    bid_response: &mut BidResponseContext,
) -> bool {
    let mut total = 0;
    let mut errs = 0;

    for seat_context in bid_response.seatbids.iter_mut() {
        for bid_context in seat_context.bids.iter_mut() {
            if bid_context.filter_reason.is_some() {
                continue;
            }

            total += 1;

            let billing_data_url = match build_event_url(
                event_id,
                bid_context,
                event_domain,
                billing_path,
                bidder,
                endpoint,
            ) {
                Ok(billing_data_url) => billing_data_url,
                Err(e) => {
                    errs += 1;
                    warn!("Failed to build billing url for bid, invalidating!: {}", e);

                    bid_context.filter_reason.replace((
                        rtb::spec::openrtb::lossreason::INTERNAL_ERROR,
                        "Cant build notice url".to_string(),
                    ));

                    continue;
                }
            };

            bid_context
                .notifications
                .billing
                .set(billing_data_url)
                .unwrap_or_else(|_| warn!("Failed to attach billing url, already exists?!"));
        }
    }

    errs > 0 && total == errs
}

/// Responsible for injecting notification ['DataUrl] urls into the bid callout
/// context, so that other tasks have an opportunity to attach metadata freely
/// and retrieve it later post call as needed, e.g. shaping details
pub struct NotificationsUrlCreationTask {
    /// The public domain (less proto) events should arrive to e.g. events.server.com
    event_domain: String,
    /// The path billing events (burl or adm) should arrive at e.g. /billing or /burl
    billing_path: String,
}

impl NotificationsUrlCreationTask {
    /// Construct a task which is responsible for attaching notification urls into
    /// the bid callout context, *not* the ad markup or response url field directly!
    /// This enables other tasks to attach data as needed, until a later task
    /// takes these urls and injects them into the final responses
    ///
    /// # Arguments
    /// * 'event_domain' - The public domain (less proto) events should arrive to e.g. events.server.com
    /// * 'billing_path' - The path billing events (burl or adm) should arrive at e.g. /billing or /burl
    pub fn new(event_domain: String, billing_path: String) -> Self {
        NotificationsUrlCreationTask {
            event_domain,
            billing_path,
        }
    }
}

impl NotificationsUrlCreationTask {
    async fn run0(&self, context: &AuctionContext) -> Result<(), Error> {
        let mut bidders = context.bidders.lock().await;

        let mut total = 0;
        let mut errs = 0;

        for bidder_context in bidders.iter_mut() {
            for callout in bidder_context.callouts.iter_mut() {
                let res = match callout.response.get_mut() {
                    Some(res) => res,
                    None => continue,
                };

                let bid_response_context = match &mut res.state {
                    BidderResponseState::Bid(bid_response) => bid_response,
                    _ => continue,
                };

                // we have a bid here, total = total count of valid bids
                total += 1;

                debug!(
                    "Attaching notification handlers for bid from {}:{}",
                    bidder_context.bidder.name, callout.endpoint.name
                );

                if attach_event_handler_urls(
                    &context.event_id,
                    &self.event_domain,
                    &self.billing_path,
                    &bidder_context.bidder,
                    &callout.endpoint,
                    bid_response_context,
                ) {
                    errs += 1;
                }
            }
        }

        if errs > 0 && total == errs {
            let brs = BidResponseState::NoBidReason {
                reqid: context.req.read().id.clone(),
                nbr: rtb::spec::openrtb::nobidreason::TECHNICAL_ERROR,
                desc: Some("Had bids but all failed event url creation"),
            };

            context
                .res
                .set(brs)
                .map_err(|_| anyhow!("Failed to attach failed billing event creation on on ctx"))?;

            bail!("Every bid received failed create internal billing event url");
        }

        Ok(())
    }
}

#[async_trait]
impl AsyncTask<AuctionContext, Error> for NotificationsUrlCreationTask {
    async fn run(&self, context: &AuctionContext) -> Result<(), Error> {
        let span = child_span_info!("notifications_creation_task");

        self.run0(context).instrument(span).await
    }
}
