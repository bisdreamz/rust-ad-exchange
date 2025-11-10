use crate::app::pipeline::constants;
use crate::app::pipeline::ortb::AuctionContext;
use crate::app::pipeline::ortb::context::{BidContext, BidderCallout, BidderResponseState};
use crate::core::shaping::tree::TreeShaper;
use anyhow::Error;
use async_trait::async_trait;
use pipeline::AsyncTask;
use rtb::{BidRequest, child_span_info};
use tracing::{Instrument, debug, warn};

fn record_shaping_key(req: &BidRequest, bid_context: &mut BidContext, shaper: &TreeShaper) {
    let shaping_key = match shaper.record_bid(req, &bid_context.bid) {
        Ok(key) => key,
        Err(e) => {
            warn!("Failed training shaping bid! {}", e);
            return;
        }
    };

    let billing_url = match bid_context.notifications.billing.get_mut() {
        Some(billing_url) => billing_url,
        None => {
            warn!("No billing URL on bid context! Cant attach shaping info!");
            return;
        }
    };

    if let Err(e) = billing_url.add_string(constants::URL_SHAPING_KEY_PARAM, shaping_key.as_str()) {
        warn!("Failed to add shaping key to billing url! {}", e);
    }

    debug!("Added shaping key to dataurl: {}", shaping_key);
}

fn record(callout: &mut BidderCallout) {
    let shaper = match callout.shaping.get() {
        Some(shaper) => shaper,
        None => return,
    };

    let req = &callout.req;

    match shaper.record_auction(req) {
        Ok(_) => debug!(
            "Trained shaping auction successfully for {}",
            &callout.endpoint.name
        ),
        Err(e) => {
            warn!("Failed training auction event! {}", e);
            return;
        }
    }

    let bidder_response = match callout.response.get_mut() {
        Some(bid_response) => bid_response,
        None => return,
    };

    let bid_res_context = match &mut bidder_response.state {
        BidderResponseState::Bid(res_context) => res_context,
        _ => return,
    };

    for seat_context in bid_res_context.seatbids.iter_mut() {
        for bid_context in seat_context.bids.iter_mut() {
            record_shaping_key(req, bid_context, shaper);
        }
    }
}

pub struct RecordShapingTrainingTask;

impl RecordShapingTrainingTask {
    async fn run0(&self, context: &AuctionContext) -> Result<(), Error> {
        let mut bidders = context.bidders.lock().await;

        for bidder_context in bidders.iter_mut() {
            for callout in bidder_context.callouts.iter_mut() {
                if callout.skip_reason.get().is_some() {
                    continue;
                }

                record(callout);
            }
        }

        Ok(())
    }
}

#[async_trait]
impl AsyncTask<AuctionContext, Error> for RecordShapingTrainingTask {
    async fn run(&self, context: &AuctionContext) -> Result<(), Error> {
        let span = child_span_info!("record_shaping_training_task");

        self.run0(context).instrument(span).await
    }
}
