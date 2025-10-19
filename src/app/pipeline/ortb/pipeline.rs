use crate::app::context::StartupContext;
use crate::app::pipeline::ortb::{tasks, AuctionContext};
use anyhow::{bail, Error};
use pipeline::{Pipeline, PipelineBuilder};

/// Builds the pipeline for which an openrtb request flows through for auction handling.
/// This can be pre-fixed by other inbound adapters such as prebid, which should then
/// adapt to a bidrequest to be passed through this pipeline.
///
/// # Behavior
/// * Observability - Will create its own rtb_span (samples as configured per cfg) and auto attach
/// to parent span if any
/// * BidResponseState - Will always attach a ['BidResponseState'] to the context which
/// may represent a detailed nobid or a valid bid response
/// * Flow - If early timeout, blocked IP etc, pipeline will attach associated context 'res'
/// and return an error, aborting the remainder of the pipeline.
/// TODO parent pipeline to capture resulting context and log to db
pub fn build_rtb_pipeline(context: &StartupContext) -> Result<Pipeline<AuctionContext, Error>, Error> {
    let ip_risk_filter = context
        .ip_risk_filter
        .lock()
        .unwrap()
        .take()
        .ok_or(anyhow::anyhow!("IP risk filter not set"))?;

    let device_lookup = context
        .device_lookup
        .lock()
        .unwrap()
        .take()
        .ok_or(anyhow::anyhow!("Device lookup not set"))?;

    let bidder_manager = match context.bidder_manager.get() {
        Some(bidder_manager) => bidder_manager,
        None => bail!("No Bidder Manager?! Cant build rtb pipeline"),
    };

    let rtb_pipeline = PipelineBuilder::new()
        .with_blocking(Box::new(tasks::ValidateRequestTask))
        .with_blocking(Box::new(tasks::IpBlockTask::new(ip_risk_filter)))
        .with_blocking(Box::new(tasks::DeviceLookupTask::new(device_lookup)))
        .with_blocking(Box::new(tasks::BidderMatchingTask::new(
            bidder_manager.clone(),
        )))
        .with_async(Box::new(tasks::BidderCalloutsTask))
        .build()
        .expect("Auction pipeline should have tasks");

    Ok(rtb_pipeline)
}
