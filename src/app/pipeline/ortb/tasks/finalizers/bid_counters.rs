use crate::app::pipeline::ortb::AuctionContext;
use crate::app::pipeline::ortb::context::BidderResponseState;
use anyhow::Error;
use async_trait::async_trait;
use opentelemetry::metrics::Counter;
use opentelemetry::{KeyValue, global};
use pipeline::AsyncTask;
use rtb::child_span_info;
use std::sync::LazyLock;
use tracing::Instrument;

/// Counts all bids received across both direct campaign and RTB demand sources,
/// post-merge, so both paths are attributed uniformly.
///
/// RTB: bidder_id, no creative_id (too high cardinality).
/// Direct: buyer_id, campaign_id, campaign_name, creative_id.
static COUNTER_AUCTION_BIDS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    global::meter("rex:auction:bids")
        .u64_counter("auction.bids")
        .with_description("Total bids received post-merge from all demand sources")
        .with_unit("1")
        .build()
});

pub struct AuctionBidCountersTask;

impl AuctionBidCountersTask {
    async fn run0(&self, ctx: &AuctionContext) -> Result<(), Error> {
        let bidders = ctx.bidders.lock().await;

        for bidder_ctx in bidders.iter() {
            for callout in &bidder_ctx.callouts {
                let bid_response = match callout.response.get() {
                    Some(r) => match &r.state {
                        BidderResponseState::Bid(br) => br,
                        _ => continue,
                    },
                    None => continue,
                };

                for seat in &bid_response.seatbids {
                    for bid_ctx in &seat.bids {
                        let filtered = bid_ctx.filter_reason.is_some();

                        let attrs = if let Some(direct) = bid_ctx.direct.get() {
                            vec![
                                KeyValue::new("pub_id", ctx.publisher.id.clone()),
                                KeyValue::new("pub_name", ctx.publisher.name.clone()),
                                KeyValue::new("demand_type", "direct"),
                                KeyValue::new("buyer_id", direct.buyer.id.clone()),
                                KeyValue::new("buyer_name", direct.buyer.buyer_name.clone()),
                                KeyValue::new("campaign_id", direct.campaign.id.clone()),
                                KeyValue::new("campaign_name", direct.campaign.name.clone()),
                                KeyValue::new("creative_id", direct.creative.id.clone()),
                                KeyValue::new("filtered", filtered),
                            ]
                        } else {
                            vec![
                                KeyValue::new("pub_id", ctx.publisher.id.clone()),
                                KeyValue::new("pub_name", ctx.publisher.name.clone()),
                                KeyValue::new("demand_type", "rtb"),
                                KeyValue::new("bidder_id", bidder_ctx.bidder.id.clone()),
                                KeyValue::new("bidder_name", bidder_ctx.bidder.name.clone()),
                                KeyValue::new("filtered", filtered),
                            ]
                        };

                        COUNTER_AUCTION_BIDS.add(1, &attrs);
                    }
                }
            }
        }

        Ok(())
    }
}

#[async_trait]
impl AsyncTask<AuctionContext, Error> for AuctionBidCountersTask {
    async fn run(&self, ctx: &AuctionContext) -> Result<(), Error> {
        let span = child_span_info!("auction_bid_counters_task");
        self.run0(ctx).instrument(span).await
    }
}
