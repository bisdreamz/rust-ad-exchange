use crate::app::pipeline::ortb::AuctionContext;
use anyhow::Error;
use pipeline::BlockingTask;
use tracing::{info_span, trace};

/// Write the context event_id to the outgoing auction.id
pub struct AuctionIdTask;

impl BlockingTask<AuctionContext, anyhow::Error> for AuctionIdTask {
    fn run(&self, context: &AuctionContext) -> Result<(), Error> {
        let mut req = context.req.write();

        let _span = info_span!("auction_id_task", source_id = req.id);

        req.id = context.event_id.clone();

        trace!("Assigned event ID to auction ID {}", req.id);

        Ok(())
    }
}
