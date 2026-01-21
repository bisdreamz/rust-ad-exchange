use crate::app::context::StartupContext;
use crate::app::pipeline::ortb::tasks::{
    BidSettlementTask, FloorsMarkupTask, IdentityDemandTask, NotificationsUrlCreationTask,
    NotificationsUrlInjectionTask, RecordShapingTrainingTask,
};
use crate::app::pipeline::ortb::{AuctionContext, tasks};
use crate::core::demand::client::DemandClient;
use anyhow::{Error, anyhow, bail};
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
pub fn build_auction_pipeline(
    context: &StartupContext,
) -> Result<Pipeline<AuctionContext, Error>, Error> {
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

    let demand_url_cache = context
        .demand_url_cache
        .get()
        .ok_or_else(|| anyhow::anyhow!("Demand url cache not set"))?;

    let cluster_manager = context
        .cluster_manager
        .get()
        .ok_or(anyhow!("Cluster manager not set"))?;

    let bidder_manager = match context.bidder_manager.get() {
        Some(bidder_manager) => bidder_manager,
        None => bail!("No Bidder Manager?! Cant build rtb pipeline"),
    };

    let pub_manager = match context.pub_manager.get() {
        Some(pub_manager) => pub_manager,
        None => bail!("No publisher manager?! Cant build rtb pipeline"),
    };

    let shaping_manager = match context.shaping_manager.get() {
        Some(shaping_manager) => shaping_manager,
        None => bail!("No shaping manager?! Cant build rtb pipeline"),
    };

    let sync_store = context
        .sync_store
        .get()
        .ok_or_else(|| anyhow!("No sync store created on context!"))?;

    let config = context.config.get().ok_or(anyhow!(
        "RTB pipeline config not set when configuring rtb pipeline"
    ))?;

    let demand_client =
        DemandClient::new().or_else(|e| bail!("RTB pipeline client failed: {}", e))?;

    let events_config = &config.notifications;

    let rtb_pipeline = PipelineBuilder::new()
        .with_blocking(Box::new(tasks::PubLookupTask::new(pub_manager.clone())))
        .with_blocking(Box::new(tasks::ValidateRequestTask))
        .with_blocking(Box::new(tasks::SchainHopsGlobalFilter::new(
            config.schain_limit,
        )))
        .with_blocking(Box::new(tasks::IpBlockTask::new(ip_risk_filter)))
        .with_blocking(Box::new(tasks::DeviceLookupTask::new(device_lookup)))
        .with_blocking(Box::new(tasks::SchainAppendTask::new(
            config.schain.clone(),
        )))
        .with_blocking(Box::new(tasks::LocalIdentityTask))
        .with_async(Box::new(tasks::BidderMatchingTask::new(
            bidder_manager.clone(),
        )))
        .with_async(Box::new(FloorsMarkupTask))
        .with_async(Box::new(IdentityDemandTask::new(sync_store.clone())))
        .with_async(Box::new(tasks::MultiImpBreakoutTask))
        .with_async(Box::new(tasks::TrafficShapingTask::new(
            shaping_manager.clone(),
        )))
        .with_async(Box::new(tasks::QpslimiterTask::new(
            bidder_manager.clone(),
            cluster_manager.clone(),
        )))
        .with_async(Box::new(tasks::BidderCalloutsTask::new(demand_client)))
        .with_async(Box::new(tasks::TestBidderTask))
        .with_async(Box::new(tasks::BidMarginTask))
        .with_async(Box::new(NotificationsUrlCreationTask::new(
            events_config.domain.clone(),
            events_config.billing_path.clone(),
        )))
        .with_async(Box::new(RecordShapingTrainingTask))
        .with_async(Box::new(NotificationsUrlInjectionTask::new(
            demand_url_cache.clone(),
        )))
        .with_async(Box::new(BidSettlementTask))
        .build()
        .expect("Auction pipeline should have tasks");

    Ok(rtb_pipeline)
}
