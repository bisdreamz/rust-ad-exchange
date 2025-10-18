use crate::app::pipeline::ortb::AuctionContext;
use anyhow::Error;
use async_trait::async_trait;
use pipeline::AsyncTask;

pub struct BidderCalloutsTask;

#[async_trait]
impl AsyncTask<AuctionContext, Error> for BidderCalloutsTask {

    async fn run(&self, _context: &AuctionContext) -> Result<(), Error> {
        //let bidders = context.bidders.lock();

        Ok(())
    }
}