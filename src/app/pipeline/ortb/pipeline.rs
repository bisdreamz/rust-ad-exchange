use crate::app::context::StartupContext;
use crate::app::pipeline::ortb::tasks;
use crate::core::enrichment::device::DeviceLookup;
use anyhow::{bail, Error};
use pipeline::PipelineBuilder;
use std::num::NonZeroU32;
use std::sync::Arc;

pub fn build_rtb_pipeline(context: &StartupContext) -> Result<(), Error> {
    let device_lookup = DeviceLookup::try_new(NonZeroU32::new(250_000).unwrap())
        .expect("Failed loading device lookup data");

    let ip_risk_filter = context.ip_risk_filter.lock().unwrap()
        .take()
        .ok_or(anyhow::anyhow!("IP risk filter not set"))?;

    let bidder_manager = match context.bidder_manager.get() {
        Some(bidder_manager) => bidder_manager,
        None => bail!("No Bidder Manager?! Cant build rtb pipeline"),
    };

    let rtb_pipeline = PipelineBuilder::new()
        .with_blocking(Box::new(tasks::validate::ValidateRequestTask))
        .with_blocking(Box::new(tasks::ip_block::IpBlockTask::new(ip_risk_filter)))
        .with_blocking(Box::new(tasks::device_lookup::DeviceLookupTask::new(device_lookup)))
        .with_blocking(Box::new(tasks::bidder_matching::BidderMatchingTask::new(bidder_manager.clone())))
        .build()
        .expect("Auction pipeline should have tasks");

    context.rtb_pipeline.set(Arc::new(rtb_pipeline))
        .map_err(|_| anyhow::anyhow!("rtb_pipeline already assigned!"))
}