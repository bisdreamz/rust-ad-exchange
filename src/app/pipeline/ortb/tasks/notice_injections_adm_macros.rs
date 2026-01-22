use crate::app::pipeline::ortb::AuctionContext;
use crate::app::pipeline::ortb::context::{BidContext, BidResponseContext, BidderResponseState};
use crate::core::demand::notifications::{DemandNotificationsCache, NoticeUrls};
use crate::core::events;
use crate::core::events::{billing, macros};
use anyhow::{Error, anyhow, bail};
use async_trait::async_trait;
use pipeline::AsyncTask;
use rtb::bid_response::SeatBid;
use rtb::common::DataUrl;
use rtb::common::bidresponsestate::BidResponseState;
use rtb::utils::adm::process_replace_adm;
use rtb::{BidRequest, BidResponse, child_span_info};
use std::sync::Arc;
use tracing::{Instrument, debug, trace, warn};

/// Convenience method for retrieving the burl from the context.notifications.burl
/// base ['DataUrl'] and cloning it so it may be used in the adm, burl, or wherever.
/// If its missing, record a filter reason on the bid since we would fail to record imp
fn get_ctx_burl_clone(bid_context: &mut BidContext) -> Result<DataUrl, Error> {
    match bid_context.notifications.billing.get() {
        Some(beacon_url) => Ok(beacon_url.clone_unfinalized()),
        None => bail!("No billing url found on context for bid! Blocking bid!"),
    }
}

fn inject_adm_macros(
    req: &BidRequest,
    res: &BidResponse,
    seat: &SeatBid,
    bid_context: &mut BidContext,
) -> Result<(), Error> {
    if let Err(e) = process_replace_adm(&mut bid_context.bid, |adm, bid| {
        macros::fill_predelivery_macros(adm.to_string(), req, res, seat, bid)
    }) {
        bail!("Failed to replace macros in bid adm: {}", e);
    }

    Ok(())
}

fn inject_adm_beacon(bid_context: &mut BidContext) -> Result<(), Error> {
    let mut beacon_url = get_ctx_burl_clone(bid_context)?;

    if let Err(e) = beacon_url.add_string(billing::FIELD_EVENT_SOURCE, "adm") {
        bail!("Failed to add adm source param to beacon url: {}", e);
    }

    beacon_url.finalize();

    let beacon_url_string = match beacon_url.url(true) {
        Ok(url) => url,
        Err(e) => bail!("Failed to finalize beacon billing url: {}", e),
    };

    match events::injectors::adm::inject_adm_beacon(beacon_url_string, &mut bid_context.bid) {
        Ok(_) => debug!("Successfully injected billing beacon(s)"),
        Err(e) => warn!("Failed to inject billing beacon: {}", e),
    }

    Ok(())
}

/// Injects our own billing url. If the DSP bid includes a burl,
/// we will complete the macros in it and return the resulting demand burl for caching
fn inject_swap_burl(
    req: &BidRequest,
    res: &BidResponse,
    seat: &SeatBid,
    bid_context: &mut BidContext,
) -> Result<Option<String>, Error> {
    if bid_context.filter_reason.is_some() {
        bail!("Skipping injecting burl, bid already rejected");
    }

    let mut beacon_url = get_ctx_burl_clone(bid_context)?;

    if let Err(e) = beacon_url.add_string(billing::FIELD_EVENT_SOURCE, "burl") {
        bail!("Failed to record burl source param on data url: {}", e);
    }

    beacon_url.finalize();

    let final_burl = match beacon_url.url(true) {
        Ok(url) => url,
        Err(e) => bail!("Failed to inject swap bid burl: {}", e),
    };

    // get original demand burl
    let demand_burl = bid_context.bid.burl.clone();

    // attach ours now
    bid_context.bid.burl = final_burl;

    if demand_burl.trim().is_empty() {
        return Ok(None);
    }

    let updated_demand_burl =
        macros::fill_predelivery_macros(demand_burl.clone(), req, res, seat, &bid_context.bid);

    debug!(
        "Demand burl before {} after macros {}",
        demand_burl, updated_demand_burl
    );

    Ok(Some(updated_demand_burl))
}

/// Responsible for injecting and *caching* billing events from notifications context into the bid response,
/// e.g. burl, adm beacon, vast impression.. depending on pub config and ad type. This
/// swaps the demand partner event URLs (if any provided) with ours and caches the partners'
pub struct NotificationsUrlInjectionTask {
    demand_url_cache: Arc<DemandNotificationsCache>,
}

// TODO per-pub configs here for notification types
impl NotificationsUrlInjectionTask {
    /// Construct a new NotificationsUrlInjectionTask which uses the
    /// provided ['DemandNotificationsCache'] to cache demand
    /// burls and related url notification handlers
    pub fn new(demand_url_cache: Arc<DemandNotificationsCache>) -> Self {
        Self { demand_url_cache }
    }

    fn inject_bid_event_handlers(
        &self,
        req: &BidRequest,
        res: &BidResponse,
        seat: &SeatBid,
        bid_context: &mut BidContext,
    ) -> Result<(), Error> {
        // Finalize urls since they can no longer be modified, ensure safety
        if let Some(base_url) = bid_context.notifications.billing.get_mut() {
            base_url.finalize();
        }

        inject_adm_beacon(bid_context)?;
        inject_adm_macros(req, res, &seat, bid_context)?;

        let demand_burl_opt = inject_swap_burl(req, res, seat, bid_context)?;

        assert!(
            !bid_context.bid_event_id.is_empty(),
            "Bid event id should never be empty"
        );

        self.demand_url_cache.cache(
            &bid_context.bid_event_id,
            NoticeUrls {
                burl: demand_burl_opt,
                lurl: None,
            },
        );

        trace!("Injected our burl and cached partners burl, too");

        Ok(())
    }

    /// Returns true if *all* bids present were invalidated due to some error
    fn inject_event_handlers(
        &self,
        bid_request: &BidRequest,
        bid_response_context: &mut BidResponseContext,
    ) -> bool {
        let mut total = 0;
        let mut errs = 0;

        for seat_context in bid_response_context.seatbids.iter_mut() {
            for bid_context in seat_context.bids.iter_mut() {
                if bid_context.filter_reason.is_some() {
                    continue;
                }

                total += 1;

                match self.inject_bid_event_handlers(
                    bid_request,
                    &bid_response_context.response,
                    &seat_context.seat,
                    bid_context,
                ) {
                    Ok(()) => debug!("Successfully injected all bid macros and event handlers"),
                    Err(e) => {
                        errs += 1;
                        warn!("Failed to inject bid macros or events, blocking bid: {}", e);

                        let _ = bid_context.filter_reason.replace((
                            rtb::spec::openrtb::nobidreason::TECHNICAL_ERROR,
                            e.to_string(),
                        ));
                    }
                }
            }
        }

        errs > 0 && total == errs
    }

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

                // important - use actual request sent to bidder
                // to fill macros
                let req = &callout.req;

                let bid_response_context = match &mut res.state {
                    BidderResponseState::Bid(bid_response) => bid_response,
                    _ => continue,
                };

                total += 1;

                debug!(
                    "Injecting notification handlers for bid from {}:{}",
                    bidder_context.bidder.name, callout.endpoint.name
                );

                if self.inject_event_handlers(&req, bid_response_context) {
                    errs += 1;
                }
            }
        }

        if errs > 0 && total == errs {
            let brs = BidResponseState::NoBidReason {
                reqid: context.original_auction_id.clone(),
                nbr: rtb::spec::openrtb::nobidreason::TECHNICAL_ERROR,
                desc: Some("Had bids but all failed post processing"),
            };

            context
                .res
                .set(brs)
                .map_err(|_| anyhow!("Failed to attach failed adm processing reason on ctx"))?;

            bail!("Every bid received failed to inject bid macros and event handlers");
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
