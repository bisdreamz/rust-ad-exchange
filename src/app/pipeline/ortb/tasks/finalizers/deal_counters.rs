use crate::app::pipeline::ortb::AuctionContext;
use crate::app::pipeline::ortb::context::BidderResponseState;
use crate::core::firestore::counters::deal::{DealCounterStore, DealCounters};
use anyhow::Error;
use async_trait::async_trait;
use pipeline::AsyncTask;
use rtb::child_span_info;
use std::sync::Arc;
use tracing::Instrument;

/// Records auction-phase bid counts for deals.
///
/// Iterates the unified `bidders` list after merge and counts a bid for every
/// bid response (direct or RTB) that has a deal attributed to it. Impressions
/// and spend are recorded separately in the billing pipeline when the billing
/// event fires.
pub struct DealBidCountersTask {
    store: Arc<DealCounterStore>,
}

impl DealBidCountersTask {
    pub fn new(store: Arc<DealCounterStore>) -> Self {
        Self { store }
    }

    async fn run0(&self, ctx: &AuctionContext) -> Result<(), Error> {
        let bidders = ctx.bidders.lock().await;

        for bidder_ctx in bidders.iter() {
            for callout in &bidder_ctx.callouts {
                let response = match callout.response.get() {
                    Some(r) => r,
                    None => continue,
                };
                let bid_response = match &response.state {
                    BidderResponseState::Bid(br) => br,
                    _ => continue,
                };

                for seat in &bid_response.seatbids {
                    for bid_ctx in &seat.bids {
                        let deal = match bid_ctx.deal.get() {
                            Some(d) => d,
                            None => continue,
                        };

                        let mut counters = DealCounters::default();
                        counters.bid();
                        self.store.merge(&deal.id, &counters);
                    }
                }
            }
        }

        Ok(())
    }
}

#[async_trait]
impl AsyncTask<AuctionContext, Error> for DealBidCountersTask {
    async fn run(&self, ctx: &AuctionContext) -> Result<(), Error> {
        let span = child_span_info!("deal_bid_counters_task");
        self.run0(ctx).instrument(span).await
    }
}
