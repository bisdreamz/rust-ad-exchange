use pipeline::{Pipeline, PipelineBuilder};
use crate::app::core::enrichment::device::DeviceLookup;
use crate::app::pipeline::ortb::context::AuctionContext;
use crate::app::pipeline::ortb::tasks::device_lookup::DeviceLookupTask;
use crate::app::pipeline::ortb::tasks::validate::ValidateRequestTask;

pub fn build_auction_pipeline() -> Pipeline<AuctionContext, anyhow::Error> {
    let device_lookup = DeviceLookup::try_new()
        .expect("Failed loading device lookup data");

    PipelineBuilder::new()
        .with_blocking(Box::new(ValidateRequestTask))
        .with_blocking(Box::new(DeviceLookupTask::new(device_lookup)))
        .build()
        .expect("Auction pipeline should have tasks")
}