use crate::app::pipeline::ortb::AuctionContext;
use anyhow::Error;
use async_trait::async_trait;
use pipeline::AsyncTask;
use rtb::child_span_info;
use tracing::{info, warn};

pub struct BidderCalloutsTask;

#[async_trait]
impl AsyncTask<AuctionContext, Error> for BidderCalloutsTask {
    async fn run(&self, context: &AuctionContext) -> Result<(), Error> {
        let span =
            child_span_info!("bidder_callouts_task", bidders = tracing::field::Empty).entered();
        let bidders = context.bidders.lock();

        // TODO simplify the entries here
        if !span.is_disabled() {
            span.record("bidders", tracing::field::debug(&bidders));
        }

        for bidder in bidders.iter() {
            let endpoints = &bidder.endpoints;
            if endpoints.is_empty() {
                continue; // wtf
            }

            if endpoints.len() > 1 {
                warn!("Multiple matching endpoints! Will only send to first one");
            }

            let endpoint = endpoints.first().unwrap();

            info!("Sending request to {}, poof..", &endpoint.name);
        }

        Ok(())
    }
}
