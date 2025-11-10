use crate::app::pipeline::ortb::context::{BidderCallout, BidderResponseState};
use crate::app::pipeline::ortb::AuctionContext;
use crate::core::demand::takerate;
use anyhow::{anyhow, Error};
use async_trait::async_trait;
use pipeline::AsyncTask;
use rtb::child_span_info;
use tracing::{debug, warn, Instrument};

fn apply_margin(callout: &mut BidderCallout, take_rate: u32) {
    let res = match callout.response.get_mut() {
        Some(res) => res,
        None => return,
    };

    let bid_res_context = match &mut res.state {
        BidderResponseState::Bid(res_context) => res_context,
        _ => return,
    };

    for seat_context in bid_res_context.seatbids.iter_mut() {
        for bid_context in seat_context.bids.iter_mut() {
            if bid_context.reduced_bid_price.is_some() {
                // might change later but safety for now
                warn!("Already have reduced price margin on bid!");
                continue;
            }

            let bid = &mut bid_context.bid;

            if bid.price != bid_context.original_bid_price {
                warn!("Bid object price does not equal recorded original bid price?! Overwriting margin!");
            }

            let reduced_bid = takerate::markdown_bid(bid.price, take_rate);

            bid_context.reduced_bid_price.replace(reduced_bid);
            bid.price = reduced_bid;

            debug!("Applied margin of {}% bid ${} -> ${}",
                take_rate, bid_context.original_bid_price, bid.price);
        }
    }
}

pub struct BidMarginTask;

impl BidMarginTask {
    async fn run0(&self, context: &AuctionContext) -> Result<(), Error> {
        let publisher = context.publisher.get()
            .ok_or_else(|| anyhow!("No publisher associated with margin context!"))?;

        for bidder_context in context.bidders.lock().await.iter_mut() {
            for callout in bidder_context.callouts.iter_mut() {
                if callout.skip_reason.get().is_some() {
                    continue;
                }

                apply_margin(callout, publisher.margin);
            }
        }

        Ok(())
    }
}

#[async_trait]
impl AsyncTask<AuctionContext, Error> for BidMarginTask {
    async fn run(&self, context: &AuctionContext) -> Result<(), Error> {
        let span = child_span_info!("bid_margin_task");

        self.run0(context).instrument(span).await
    }
}