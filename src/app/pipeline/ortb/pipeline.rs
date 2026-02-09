use crate::app::context::StartupContext;
use crate::app::pipeline::ortb::tasks::finalizers::{DemandCountersTask, PubCountersTask};
use crate::app::pipeline::ortb::{AuctionContext, tasks};
use crate::core::demand::client::DemandClient;
use anyhow::{Error, anyhow, bail};
use async_trait::async_trait;
use pipeline::{AsyncTask, Pipeline, PipelineBuilder};
use rtb::child_span_info;
use tracing::Instrument;

/// Build the pipeline which handles RTB auctions,
/// which may optionally be wrapped by an
/// upstream pipeline which generates auctions,
/// e.g. a vast or prebid handler.
fn build_rtb_pipeline(context: &StartupContext) -> Result<Pipeline<AuctionContext, Error>, Error> {
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
        .with_blocking(Box::new(tasks::auction::PubLookupTask::new(
            pub_manager.clone(),
        )))
        .with_blocking(Box::new(tasks::auction::ValidateRequestTask))
        .with_blocking(Box::new(tasks::auction::SchainHopsGlobalFilter::new(
            config.schain_limit,
        )))
        .with_blocking(Box::new(tasks::auction::JunkFilterTask))
        .with_blocking(Box::new(tasks::auction::IpBlockTask::new(ip_risk_filter)))
        .with_blocking(Box::new(tasks::auction::DeviceLookupTask::new(
            device_lookup,
        )))
        .with_blocking(Box::new(tasks::auction::AuctionIdTask))
        .with_blocking(Box::new(tasks::auction::SchainAppendTask::new(
            config.schain.clone(),
        )))
        .with_blocking(Box::new(tasks::auction::LocalIdentityTask))
        .with_async(Box::new(tasks::auction::BidderMatchingTask::new(
            bidder_manager.clone(),
        )))
        .with_async(Box::new(tasks::auction::FloorsMarkupTask))
        .with_async(Box::new(tasks::auction::IdentityDemandTask::new(
            sync_store.clone(),
        )))
        .with_async(Box::new(tasks::auction::MultiImpBreakoutTask))
        .with_async(Box::new(tasks::auction::TrafficShapingTask::new(
            shaping_manager.clone(),
        )))
        .with_async(Box::new(tasks::auction::QpslimiterTask::new(
            bidder_manager.clone(),
            cluster_manager.clone(),
        )))
        .with_async(Box::new(tasks::auction::BidderCalloutsTask::new(
            demand_client,
        )))
        .with_async(Box::new(tasks::auction::TestBidderTask))
        .with_async(Box::new(tasks::auction::BidMarginTask))
        .with_async(Box::new(tasks::auction::NotificationsUrlCreationTask::new(
            events_config.domain.clone(),
            events_config.billing_path.clone(),
        )))
        .with_async(Box::new(tasks::auction::RecordShapingTrainingTask))
        .with_async(Box::new(
            tasks::auction::NotificationsUrlInjectionTask::new(demand_url_cache.clone()),
        ))
        .with_async(Box::new(tasks::auction::BidSettlementTask))
        .build()
        .expect("Auction pipeline should have tasks");

    Ok(rtb_pipeline)
}

/// Builds the pipeline which handles final tasks after an auction,
/// which must always run regardless of whether the full
/// rtb pipeline ran or not, e.g. even if we didnt match any
/// bidders, we may still want to run tasks to
/// persist the events to our db
fn build_finalizers_pipeline(
    context: &StartupContext,
) -> Result<Option<Pipeline<AuctionContext, Error>>, Error> {
    let mut pipeline_builder = PipelineBuilder::new();

    let pub_store_opt = context
        .counters_pub_store
        .get()
        .ok_or_else(|| anyhow!("No publisher counter store option set on context"))?;
    let demand_store_opt = context
        .counters_demand_store
        .get()
        .ok_or_else(|| anyhow!("No demand counter store option set on context"))?;

    if pub_store_opt.is_some() != demand_store_opt.is_some() {
        bail!("Both publisher and demand counter stores must be set or unset together");
    }

    if pub_store_opt.is_some() && demand_store_opt.is_some() {
        let pub_store = pub_store_opt.as_ref().unwrap();
        let demand_store = demand_store_opt.as_ref().unwrap();

        pipeline_builder.add_async(Box::new(PubCountersTask::new(pub_store.clone())));

        pipeline_builder.add_async(Box::new(DemandCountersTask::new(demand_store.clone())));
    }

    Ok(pipeline_builder.build())
}

/// Represents the RTB pipeline and the following
/// finalizer tasks which must always run,
/// regardless of whether the RTB pipeline completed
pub struct AuctionAndFinalizersPipelineTask {
    rtb_pipeline: Pipeline<AuctionContext, Error>,
    finalizers_pipeline: Option<Pipeline<AuctionContext, Error>>,
}

impl AuctionAndFinalizersPipelineTask {
    pub fn new(
        rtb_pipeline: Pipeline<AuctionContext, Error>,
        finalizers_pipeline: Option<Pipeline<AuctionContext, Error>>,
    ) -> Self {
        AuctionAndFinalizersPipelineTask {
            rtb_pipeline,
            finalizers_pipeline,
        }
    }
    async fn run0(&self, context: &AuctionContext) -> Result<(), Error> {
        // Run the auction pipeline, which may or may not
        // complete entirely e.g. auction is blocked for
        // bad request values
        let auction_pipeline_res = self.rtb_pipeline.run(context).await;

        // For local dev we may not have database persistence
        // configured, so its opt. In prod however we always
        // want to ensure we increment request counters
        // and push data to the database for all activity
        if let Some(finalizers_pipeline) = &self.finalizers_pipeline {
            // Bail here if some finalizer fails, which likely means
            // we were unable to store activity in the database..
            // we consider these finalizers non negotiable
            finalizers_pipeline.run(context).await?;
        }

        // Okay, finalizer tasks succeeded - return whatever
        // our auction result was
        auction_pipeline_res
    }
}

#[async_trait]
impl AsyncTask<AuctionContext, Error> for AuctionAndFinalizersPipelineTask {
    async fn run(&self, context: &AuctionContext) -> Result<(), Error> {
        let span = child_span_info!("auction_pipeline");

        self.run0(context).instrument(span).await
    }
}

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
pub fn build_auction_pipeline(
    context: &StartupContext,
) -> Result<Pipeline<AuctionContext, Error>, Error> {
    let rtb_pipeline = build_rtb_pipeline(context)?;
    let finalizers_pipeline_opt = build_finalizers_pipeline(context)?;

    let auction_pipeline = PipelineBuilder::new()
        .with_async(Box::new(AuctionAndFinalizersPipelineTask::new(
            rtb_pipeline,
            finalizers_pipeline_opt,
        )))
        .build()
        .expect("Auction pipeline should have had RTB tasks");

    Ok(auction_pipeline)
}
