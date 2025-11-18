use crate::app::pipeline::syncing::r#in::context::SyncInContext;
use crate::core::managers::BidderManager;
use anyhow::{Error, anyhow, bail};
use pipeline::BlockingTask;
use rtb::child_span_info;
use std::sync::Arc;

/// Task validates the partner id in the ['SyncInEvent']
/// and attached the demand partner to context. At
/// time of writing this *only expects partner id to be demand!*
pub struct ExtractDemandTask {
    bidder_manager: Arc<BidderManager>,
}

impl ExtractDemandTask {
    pub fn new(bidder_manager: Arc<BidderManager>) -> Self {
        Self { bidder_manager }
    }
}

impl BlockingTask<SyncInContext, Error> for ExtractDemandTask {
    fn run(&self, context: &SyncInContext) -> Result<(), Error> {
        let span = child_span_info!("extract_demand_task", partner_name = tracing::field::Empty);

        let event = context
            .event
            .get()
            .ok_or_else(|| anyhow!("No event on sync in context!"))?;

        // TODO add support for hosting from pubs too if ever needed? right now only demand support
        let bidder = match self.bidder_manager.bidder(&event.partner_id) {
            Some(bidder) => bidder,
            None => bail!(
                "No bidder found for bidder_id {} for sync in call",
                event.partner_id
            ),
        };

        span.record("partner_name", &bidder.name);

        context
            .bidder
            .set(bidder)
            .map_err(|_| anyhow!("Failed to set bidder for sync in call"))?;

        Ok(())
    }
}
