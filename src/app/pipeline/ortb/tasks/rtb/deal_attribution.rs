use crate::app::pipeline::ortb::AuctionContext;
use crate::app::pipeline::ortb::context::BidderResponseState;
use crate::core::managers::DealManager;
use anyhow::Error;
use async_trait::async_trait;
use opentelemetry::metrics::Counter;
use opentelemetry::{KeyValue, global};
use pipeline::AsyncTask;
use rtb::child_span_info;
use std::sync::{Arc, LazyLock};
use tracing::{Instrument, Span};

static COUNTER_DEALS_ATTRIBUTED: LazyLock<Counter<u64>> = LazyLock::new(|| {
    global::meter("rex:demand:deals")
        .u64_counter("deals.rtb_attributed")
        .with_description("RTB bids successfully matched to a known deal")
        .with_unit("1")
        .build()
});

static COUNTER_DEALS_UNRECOGNIZED: LazyLock<Counter<u64>> = LazyLock::new(|| {
    global::meter("rex:demand:deals")
        .u64_counter("deals.rtb_unrecognized")
        .with_description("RTB bids with dealid that didn't match any known deal")
        .with_unit("1")
        .build()
});

/// Runs after BidderCalloutsTask. For each RTB bid with a non-empty
/// `bid.dealid`, looks up the deal and sets `bid_context.deal` via
/// `OnceLock::set()` for downstream attribution (counters, settlement).
pub struct RtbDealAttributionTask {
    deal_manager: Arc<DealManager>,
}

impl RtbDealAttributionTask {
    pub fn new(deal_manager: Arc<DealManager>) -> Self {
        Self { deal_manager }
    }

    async fn run0(&self, context: &AuctionContext) -> Result<(), Error> {
        let bidders = context.bidders.lock().await;
        let mut deals_attributed: u64 = 0;
        let mut deals_unrecognized: u64 = 0;

        for bidder_ctx in bidders.iter() {
            for callout in &bidder_ctx.callouts {
                let Some(response) = callout.response.get() else {
                    continue;
                };

                let BidderResponseState::Bid(bid_response) = &response.state else {
                    continue;
                };

                for seat_ctx in &bid_response.seatbids {
                    for bid_ctx in &seat_ctx.bids {
                        if bid_ctx.bid.dealid.is_empty() {
                            continue;
                        }

                        if let Some(deal) = self.deal_manager.get(&bid_ctx.bid.dealid) {
                            let _ = bid_ctx.deal.set(deal);
                            deals_attributed += 1;
                        } else {
                            deals_unrecognized += 1;
                        }
                    }
                }
            }
        }

        let pub_id = context.publisher.id.clone();

        if deals_attributed > 0 {
            COUNTER_DEALS_ATTRIBUTED
                .add(deals_attributed, &[KeyValue::new("pub_id", pub_id.clone())]);
        }

        if deals_unrecognized > 0 {
            COUNTER_DEALS_UNRECOGNIZED.add(deals_unrecognized, &[KeyValue::new("pub_id", pub_id)]);
        }

        let span = Span::current();
        span.record("deals_attributed", deals_attributed);
        span.record("deals_unrecognized", deals_unrecognized);

        Ok(())
    }
}

#[async_trait]
impl AsyncTask<AuctionContext, Error> for RtbDealAttributionTask {
    async fn run(&self, context: &AuctionContext) -> Result<(), Error> {
        let span = child_span_info!(
            "rtb_deal_attribution_task",
            deals_attributed = tracing::field::Empty,
            deals_unrecognized = tracing::field::Empty
        );

        self.run0(context).instrument(span).await
    }
}
