use crate::app::pipeline::ortb::AuctionContext;
use crate::app::pipeline::ortb::context::{
    BidResponseContext, BidderCallout, BidderContext, BidderResponse, BidderResponseState,
};
use crate::core::demand::client::{DemandClient, DemandResponse};
use anyhow::Error;
use async_trait::async_trait;
use futures_util::future::join_all;
use opentelemetry::metrics::{Counter, Histogram};
use opentelemetry::{KeyValue, global};
use pipeline::AsyncTask;
use reqwest::StatusCode;
use rtb::{BidResponse, child_span_info};
use std::sync::{LazyLock, OnceLock};
use std::time::{Duration, Instant};
use tokio::time::timeout;
use tracing::{Instrument, debug, warn};

static COUNTER_BIDDER_CALLOUTS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    global::meter("rex:demand:callouts")
        .u64_counter("callouts.count")
        .with_description("Count of separate auctions sent to bidders")
        .with_unit("1")
        .build()
});

static COUNTER_BIDDER_LATENCY: LazyLock<Histogram<f64>> = LazyLock::new(|| {
    global::meter("rex:demand:callouts")
        .f64_histogram("callouts.latency")
        .with_description("Response latency of bidder callouts")
        .with_unit("s")
        .build()
});

static COUNTER_BIDDER_BIDS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    global::meter("rex:demand:callouts")
        .u64_counter("callouts.bids.count")
        .with_description("Total count of individual bids received")
        .with_unit("1")
        .build()
});

static COUNTER_BIDDER_BIDS_PER_AUCTION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    global::meter("rex:demand:callouts")
        .u64_histogram("callouts.bids.per_action")
        .with_description("Histogram of bids returned per unique auction")
        .with_unit("1")
        .build()
});

static COUNTER_BIDDER_CPM: LazyLock<Histogram<f64>> = LazyLock::new(|| {
    global::meter("rex:demand:callouts")
        .f64_histogram("callouts.cpm")
        .with_description("Response CPM of bids received")
        .with_unit("{USD}")
        .build()
});

async fn get_call_result_or_record_err(
    context: &BidderCallout,
    context_response: &OnceLock<BidderResponse>,
    callout_result: impl Future<Output = Result<DemandResponse, Error>>,
    start: &Instant,
) -> Option<DemandResponse> {
    match callout_result.await {
        Ok(res) => Some(res),
        Err(e) => {
            let set = context_response.set(BidderResponse {
                latency: start.elapsed(),
                state: BidderResponseState::Error(format!(
                    "Request error to {}: {}",
                    context.endpoint.url, e
                )),
            });

            if let Err(_) = set {
                warn!("Tried to assign res error condition but response state exists");
            }

            None
        }
    }
}

async fn record_204(
    context: &BidderCallout,
    context_response: &OnceLock<BidderResponse>,
    start: &Instant,
) {
    let set = context_response.set(BidderResponse {
        latency: start.elapsed(),
        state: BidderResponseState::NoBid(None),
    });

    if let Err(_) = set {
        return warn!("Tried to assign res nobid condition but response state exists");
    }

    debug!("204 received from {}", context.endpoint.name);
}

async fn record_200_nobid(
    context: &BidderCallout,
    context_response: &OnceLock<BidderResponse>,
    bid_response: &BidResponse,
    start: &Instant,
) {
    debug!(
        "Received no bid (nbr) {} from {}",
        bid_response.nbr, context.endpoint.name
    );

    let nbr = if bid_response.nbr > 0 {
        Some(bid_response.nbr as u32)
    } else {
        None
    };

    let set = context_response.set(BidderResponse {
        latency: start.elapsed(),
        state: BidderResponseState::NoBid(nbr),
    });

    if let Err(_) = set {
        warn!("Tried to assign res nobid (empty seatbid) but response state exists");
    }

    return;
}

async fn record_unexpected_status(
    context: &BidderCallout,
    context_response: &OnceLock<BidderResponse>,
    res: &DemandResponse,
    start: &Instant,
) {
    let set = context_response.set(BidderResponse {
        latency: start.elapsed(),
        state: BidderResponseState::Unknown(res.status_code, res.status_message.clone()),
    });

    if let Err(_) = set {
        return warn!("Tried to assign res unknown http condition but response state exists");
    }

    debug!(
        "Unexpected status {} received from {}",
        res.status_code, context.endpoint.name
    );
}

async fn record_bids(
    context_response: &OnceLock<BidderResponse>,
    bid_response: BidResponse,
    start: &Instant,
) {
    debug!("Received bids! Recording");

    let set = context_response.set(BidderResponse {
        latency: start.elapsed(),
        state: BidderResponseState::Bid(BidResponseContext::from(bid_response)),
    });

    if let Err(_) = set {
        warn!("Tried to assign res valid bid condition but response state exists");
    }
}

async fn record_bid_response_state(
    context: &BidderCallout,
    callout_result: impl Future<Output = Result<DemandResponse, Error>>,
) {
    let start = Instant::now();

    let context_response = &context.response;

    let res = match get_call_result_or_record_err(context, context_response, callout_result, &start)
        .await
    {
        Some(res) => res,
        None => return,
    };

    if res.status_code == StatusCode::NO_CONTENT.as_u16() as u32 {
        return record_204(context, context_response, &start).await;
    }

    if res.status_code != StatusCode::OK.as_u16() as u32 {
        return record_unexpected_status(context, context_response, &res, &start).await;
    }

    if res.response.is_none() {
        return warn!("Received 200 but empty body from {}", context.endpoint.name);
    }

    let bid_response = res.response.unwrap();

    if bid_response.nbr > 0 || bid_response.seatbid.is_empty() {
        return record_200_nobid(context, context_response, &bid_response, &start).await;
    }

    record_bids(context_response, bid_response, &start).await;
}

fn record_counter_bid_price_metrics(response: &BidderResponse, attrs: &[KeyValue]) {
    let bid_response = match &response.state {
        BidderResponseState::Bid(bid_response) => bid_response,
        _ => return,
    };

    let mut bid_count = 0;
    for seat in bid_response.response.seatbid.iter() {
        for bid in seat.bid.iter() {
            bid_count += 1;

            COUNTER_BIDDER_BIDS.add(1, attrs);
            COUNTER_BIDDER_CPM.record(bid.price, attrs);
        }
    }

    COUNTER_BIDDER_BIDS_PER_AUCTION.record(bid_count, attrs);
}

fn record_counter_metrics(context: &AuctionContext, bidders: &Vec<BidderContext>) {
    let publisher = context.publisher.get().unwrap();

    for bidder_context in bidders {
        for callout in &bidder_context.callouts {
            let mut attrs = vec![
                KeyValue::new("pub_id", publisher.id.clone()),
                KeyValue::new("pub_name", publisher.name.clone()),
                KeyValue::new("bidder_id", bidder_context.bidder.name.clone()),
                KeyValue::new("bidder_endpoint_id", callout.endpoint.name.clone()),
                KeyValue::new(
                    "bidder_endpoint_proto",
                    callout.endpoint.protocol.to_string(),
                ),
                KeyValue::new(
                    "bidder_endpoint_encoding",
                    callout.endpoint.encoding.to_string(),
                ),
            ];

            let outcome = match callout.response.get() {
                Some(outcome) => {
                    record_counter_bid_price_metrics(outcome, &attrs);

                    attrs.push(KeyValue::new(
                        "auction_outcome",
                        outcome.state.as_ref().to_string(),
                    ));

                    outcome
                }
                None => {
                    warn!("No response state for auction! Cant record metrics!");
                    continue;
                }
            };

            COUNTER_BIDDER_CALLOUTS.add(1, &attrs);
            COUNTER_BIDDER_LATENCY.record(outcome.latency.as_secs_f64(), &attrs);
        }
    }
}

pub struct BidderCalloutsTask {
    client: DemandClient,
}

impl BidderCalloutsTask {
    pub fn new(client: DemandClient) -> Self {
        Self { client }
    }

    fn send_bidder_callouts(&self, bidders: &Vec<BidderContext>) -> Vec<impl Future<Output = ()>> {
        let mut futs = Vec::with_capacity(bidders.len());

        for bidder_context in bidders.iter() {
            let callouts = &bidder_context.callouts;
            if callouts.is_empty() {
                debug!("Bidder entry but empty callouts list?");

                continue;
            }

            for callout in callouts.iter() {
                let endpoint = &callout.endpoint;

                let res_fut = self.client.send_request(
                    bidder_context.bidder.clone(),
                    endpoint.clone(),
                    &callout.req,
                );
                let handled_fut = record_bid_response_state(callout, res_fut);

                futs.push(handled_fut);
            }
        }

        futs
    }

    fn record_timeouts(&self, bidders: &Vec<BidderContext>) {
        for bidder in bidders.iter() {
            for callout in bidder.callouts.iter() {
                if callout.response.get().is_none() {
                    let br = BidderResponse {
                        latency: Duration::from_secs(1),
                        state: BidderResponseState::Timeout,
                    };

                    if let Err(_) = callout.response.set(br) {
                        warn!("Tried to assign res timeout, but response state exists");
                    }

                    debug!("Marked request to {} as timeout", &callout.endpoint.name);
                }

                let bidder_response = callout.response.get().unwrap();

                debug!(
                    "Result state {:?} latency {} from {}",
                    bidder_response.state,
                    bidder_response.latency.as_millis(),
                    &callout.endpoint.name
                );
            }
        }
    }

    async fn send_all(&self, context: &AuctionContext) -> Result<(), Error> {
        let bidders = context.bidders.lock().await;

        debug!("Have {} bidders for callouts", bidders.len());

        let futs = self.send_bidder_callouts(&bidders);

        // todo enforce tmax mins and adjustments earlier
        let mut tmax = context.req.read().tmax.min(700).max(50);
        tmax -= 10;

        match timeout(Duration::from_millis(tmax as u64), join_all(futs)).await {
            Ok(_) => debug!("All bidders responded within timax"),
            Err(_) => debug!("At least one bidder held auction open until end of tmax"),
        }

        self.record_timeouts(&bidders);

        record_counter_metrics(context, &bidders);

        Ok(())
    }
}

#[async_trait]
impl AsyncTask<AuctionContext, Error> for BidderCalloutsTask {
    async fn run(&self, context: &AuctionContext) -> Result<(), Error> {
        let span = child_span_info!("bidder_callouts_task", bidders = tracing::field::Empty);

        if context.req.read().test {
            warn!("request.test is true, skipping live bidder auctions");
            return Ok(());
        }

        // TODO simplify span details
        if !span.is_disabled() {
            span.record("bidders", tracing::field::debug(&context.bidders));
        }

        self.send_all(context).instrument(span).await
    }
}
