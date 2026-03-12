use crate::app::pipeline::adtag::context::AdtagContext;
use crate::app::pipeline::ortb::AuctionContext;
use anyhow::{Error, anyhow};
use async_trait::async_trait;
use pipeline::{AsyncTask, Pipeline};
use rtb::child_span_info;
use std::sync::Arc;
use tracing::{Instrument, debug};

pub struct RunAuctionTask {
    auction_pipeline: Arc<Pipeline<AuctionContext, Error>>,
}

impl RunAuctionTask {
    pub fn new(auction_pipeline: Arc<Pipeline<AuctionContext, Error>>) -> Self {
        Self { auction_pipeline }
    }
}

#[async_trait]
impl AsyncTask<AdtagContext, Error> for RunAuctionTask {
    async fn run(&self, ctx: &AdtagContext) -> Result<(), Error> {
        let auction_ctx = ctx
            .auction_ctx
            .get()
            .ok_or_else(|| anyhow!("auction_ctx not set"))?;

        let span = child_span_info!("run_auction_task");
        self.auction_pipeline
            .run(auction_ctx)
            .instrument(span)
            .await
            .map_err(|e| {
                debug!("auction pipeline aborted: {}", e);
                e
            })?;

        debug!("auction pipeline completed");
        Ok(())
    }
}
