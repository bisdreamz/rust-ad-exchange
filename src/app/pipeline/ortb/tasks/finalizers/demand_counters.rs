use crate::app::pipeline::ortb::AuctionContext;
use crate::app::pipeline::ortb::context::{
    BidderCallout, BidderContext, BidderResponseState, CalloutSkipReason, IdentityContext,
};
use crate::core::firestore::counters::demand::{DemandCounterStore, DemandCounters};
use crate::core::firestore::counters::publisher::{PublisherCounterStore, PublisherCounters};
use anyhow::{Error, anyhow};
use async_trait::async_trait;
use pipeline::AsyncTask;
use rtb::bid_request::{Device, DistributionchannelOneof};
use rtb::{BidRequest, child_span_info};
use smallvec::SmallVec;
use std::sync::Arc;
use tracing::Instrument;

fn get_auction_stats(bidder_contexts: &Vec<BidderContext>) -> (u32, u32) {
    let mut auctions = 0u32;
    let mut bids = 0u32;

    for bidder_context in bidder_contexts {
        for callout in &bidder_context.callouts {
            if callout.skip_reason.get().is_none() {
                auctions += 1;

                if let Some(response) = callout.response.get().as_ref() {
                    if matches!(response.state, BidderResponseState::Bid(_)) {
                        bids += 1;
                    }
                }
            }
        }
    }

    (auctions, bids)
}

fn build_endpoint_counters(bidder_callout: &BidderCallout) -> Result<DemandCounters, Error> {
    let mut counters = DemandCounters::default();

    counters.request_matched();

    match bidder_callout.skip_reason.get() {
        None => {
            counters.auction();

            if let Some(response) = bidder_callout.response.get().as_ref() {
                match response.state {
                    BidderResponseState::Timeout => counters.timeout(),
                    BidderResponseState::Error(_) => counters.error(),
                    BidderResponseState::Unknown(_, _) => counters.error(),
                    BidderResponseState::NoBid(_) => {}
                    BidderResponseState::Bid(_) => counters.bid(),
                }
            }
        }
        Some(skip_reason) => match skip_reason {
            CalloutSkipReason::TrafficShaping => counters.request_shaping_blocked(),
            CalloutSkipReason::QpsLimit => counters.request_qps_limited(),
            CalloutSkipReason::EndpointRotation => {}
        },
    }

    Ok(counters)
}

/// Responsible for merging the recorded counters activity
/// after an auction, and merging with the respective counter
/// stores. This always runs, since we may have a pub req
/// that errs or halts half way through (e.g. blocked) but
/// we still need to record that activity
pub struct DemandCountersTask {
    store: Arc<DemandCounterStore>,
}

impl DemandCountersTask {
    pub fn new(store: Arc<DemandCounterStore>) -> Self {
        DemandCountersTask { store }
    }

    async fn run0(&self, context: &AuctionContext) -> Result<(), Error> {
        let bidders = context.bidders.lock().await;

        for bidder_context in bidders.iter() {
            let bidder_id = bidder_context.bidder.id.as_str();
            let bidder_name = bidder_context.bidder.name.as_str();

            let mut bidder_had_any_matches = false;
            let mut bidder_auctions = 0;
            let mut bidder_bids = 0;
            let mut bidder_timeouts = 0;
            let mut bidder_errors = 0;

            for bidder_callout in bidder_context.callouts.iter() {
                let counters = build_endpoint_counters(bidder_callout)?;

                self.store.merge_endpoint(
                    bidder_id,
                    bidder_name,
                    bidder_callout.endpoint.name.as_str(),
                    &counters,
                );

                bidder_had_any_matches |= counters.requests_matched > 0;
                bidder_auctions += counters.auctions;
                bidder_bids += counters.bids;
                bidder_timeouts += counters.timeouts;
                bidder_errors += counters.errors;
            }

            let bidder_counters = DemandCounters {
                requests_matched: bidder_had_any_matches as u64,
                auctions: bidder_auctions,
                bids: bidder_bids,
                timeouts: bidder_timeouts,
                errors: bidder_errors,
                ..Default::default()
            };

            self.store
                .merge_bidder(bidder_id, bidder_name, &bidder_counters);
        }

        Ok(())
    }
}

#[async_trait]
impl AsyncTask<AuctionContext, Error> for DemandCountersTask {
    async fn run(&self, context: &AuctionContext) -> Result<(), Error> {
        let span = child_span_info!("demand_counters_task");

        self.run0(context).instrument(span).await
    }
}
