use crate::app::pipeline::ortb::AuctionContext;
use crate::app::pipeline::ortb::direct::bid;
use anyhow::Error;
use async_trait::async_trait;
use pipeline::AsyncTask;
use tracing::{Level, debug, enabled};

/// Drains staged direct campaign bids into `bidders` so shared
/// bid tasks (margin, notice URLs, settlement) operate on a
/// uniform list of all bid sources. After merge, staging is
/// empty — all downstream consumers (including campaign counters)
/// read from `bidders` to see the final post-processing state.
pub struct MergeDirectBidsTask;

#[async_trait]
impl AsyncTask<AuctionContext, Error> for MergeDirectBidsTask {
    async fn run(&self, ctx: &AuctionContext) -> Result<(), Error> {
        let staged: Vec<_> = {
            let mut staging = ctx.direct_bid_staging.lock().await;
            std::mem::take(&mut *staging)
        };

        let direct_count = staged.len();

        if !staged.is_empty() {
            let wrapped: Vec<_> = staged
                .into_iter()
                .map(bid::wrap_in_bidder_context)
                .collect();

            let mut bidders = ctx.bidders.lock().await;
            bidders.extend(wrapped);

            if enabled!(Level::DEBUG) {
                debug!(
                    direct = direct_count,
                    rtb = bidders.len() - direct_count,
                    "Bid sources merged"
                );
            }
        }

        Ok(())
    }
}
