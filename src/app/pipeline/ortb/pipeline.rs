use crate::app::context::StartupContext;
use crate::app::pipeline::ortb::tasks::finalizers::{
    CampaignCountersTask, DemandCountersTask, PubCountersTask,
};
use crate::app::pipeline::ortb::{AuctionContext, tasks};
use crate::core::demand::client::DemandClient;
use crate::core::models::placement::FillPolicy;
use anyhow::{Error, anyhow, bail};
use async_trait::async_trait;
use pipeline::{AsyncTask, Pipeline, PipelineBuilder};
use rtb::child_span_info;
use tracing::Instrument;

// ---------------------------------------------------------------------------
// Enrichment — always runs, resolves pub/device/identity/filters
// ---------------------------------------------------------------------------

fn build_enrichment_pipeline(
    context: &StartupContext,
) -> Result<Pipeline<AuctionContext, Error>, Error> {
    let ip_risk_filter = context
        .ip_risk_filter
        .lock()
        .unwrap()
        .take()
        .ok_or(anyhow!("IP risk filter not set"))?;

    let device_lookup = context
        .device_lookup
        .lock()
        .unwrap()
        .take()
        .ok_or(anyhow!("Device lookup not set"))?;

    let pub_manager = context
        .pub_manager
        .get()
        .ok_or(anyhow!("No publisher manager"))?;

    let config = context
        .config
        .get()
        .ok_or(anyhow!("Config not set when building enrichment pipeline"))?;

    let pipeline = PipelineBuilder::new()
        .with_blocking(Box::new(tasks::enrichment::PubLookupTask::new(
            pub_manager.clone(),
        )))
        .with_blocking(Box::new(tasks::enrichment::ValidateRequestTask))
        .with_blocking(Box::new(tasks::enrichment::SchainHopsGlobalFilter::new(
            config.schain_limit,
        )))
        .with_blocking(Box::new(tasks::enrichment::JunkFilterTask))
        .with_blocking(Box::new(tasks::enrichment::IpBlockTask::new(
            ip_risk_filter,
        )))
        .with_blocking(Box::new(tasks::enrichment::DeviceLookupTask::new(
            device_lookup,
        )))
        .with_blocking(Box::new(tasks::enrichment::AuctionIdTask))
        .with_blocking(Box::new(tasks::enrichment::TmaxOffsetTask))
        .with_blocking(Box::new(tasks::enrichment::SchainAppendTask::new(
            config.schain.clone(),
        )))
        .with_blocking(Box::new(tasks::enrichment::LocalIdentityTask))
        .build()
        .expect("Enrichment pipeline should have tasks");

    Ok(pipeline)
}

// ---------------------------------------------------------------------------
// RTB sub-pipeline — bidder matching, callouts, post-processing
// ---------------------------------------------------------------------------

fn build_rtb_sub_pipeline(
    context: &StartupContext,
) -> Result<Pipeline<AuctionContext, Error>, Error> {
    let demand_url_cache = context
        .demand_url_cache
        .get()
        .ok_or_else(|| anyhow!("Demand url cache not set"))?;

    let cluster_manager = context
        .cluster_manager
        .get()
        .ok_or(anyhow!("Cluster manager not set"))?;

    let bidder_manager = context
        .bidder_manager
        .get()
        .ok_or(anyhow!("No bidder manager"))?;

    let shaping_manager = context
        .shaping_manager
        .get()
        .ok_or(anyhow!("No shaping manager"))?;

    let sync_store = context
        .sync_store
        .get()
        .ok_or_else(|| anyhow!("No sync store"))?;

    let config = context
        .config
        .get()
        .ok_or(anyhow!("Config not set when building RTB sub-pipeline"))?;

    let demand_client =
        DemandClient::new().or_else(|e| bail!("RTB pipeline client failed: {}", e))?;

    let events_config = &config.notifications;

    let pipeline = PipelineBuilder::new()
        .with_async(Box::new(tasks::rtb::BidderMatchingTask::new(
            bidder_manager.clone(),
        )))
        .with_async(Box::new(tasks::rtb::FloorsMarkupTask))
        .with_async(Box::new(tasks::rtb::IdentityDemandTask::new(
            sync_store.clone(),
        )))
        .with_async(Box::new(tasks::rtb::MultiImpBreakoutTask))
        .with_async(Box::new(tasks::rtb::TrafficShapingTask::new(
            shaping_manager.clone(),
        )))
        .with_async(Box::new(tasks::rtb::QpslimiterTask::new(
            bidder_manager.clone(),
            cluster_manager.clone(),
        )))
        .with_async(Box::new(tasks::rtb::BidderCalloutsTask::new(demand_client)))
        .with_async(Box::new(tasks::rtb::TestBidderTask))
        .with_async(Box::new(tasks::rtb::BidMarginTask))
        .with_async(Box::new(tasks::rtb::NotificationsUrlCreationTask::new(
            events_config.domain.clone(),
            events_config.billing_path.clone(),
        )))
        .with_async(Box::new(tasks::rtb::RecordShapingTrainingTask))
        .with_async(Box::new(tasks::rtb::NotificationsUrlInjectionTask::new(
            demand_url_cache.clone(),
        )))
        .build()
        .expect("RTB sub-pipeline should have tasks");

    Ok(pipeline)
}

// ---------------------------------------------------------------------------
// Conditional RTB task — skips RTB when direct fill satisfies placement policy
// ---------------------------------------------------------------------------

struct ConditionalRtbTask {
    rtb_pipeline: Pipeline<AuctionContext, Error>,
}

impl ConditionalRtbTask {
    fn new(rtb_pipeline: Pipeline<AuctionContext, Error>) -> Self {
        Self { rtb_pipeline }
    }
}

#[async_trait]
impl AsyncTask<AuctionContext, Error> for ConditionalRtbTask {
    async fn run(&self, ctx: &AuctionContext) -> Result<(), Error> {
        let fill_policy = ctx
            .placement
            .get()
            .map(|p| &p.fill_policy)
            .unwrap_or(&FillPolicy::HighestPrice);

        let direct_filled = !ctx.bidders.lock().await.is_empty();

        let run_rtb = match fill_policy {
            FillPolicy::DealsOnly | FillPolicy::DirectOnly => false,
            FillPolicy::DirectAndRtbDeals => !direct_filled,
            FillPolicy::RtbBackfill | FillPolicy::HighestPrice => true,
        };

        if run_rtb {
            self.rtb_pipeline.run(ctx).await?;
        }

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Finalizers — always runs, persists counters
// ---------------------------------------------------------------------------

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

    // Campaign counters — only if campaign store + buyer/advertiser managers loaded
    let campaign_store_opt = context
        .counters_campaign_store
        .get()
        .ok_or_else(|| anyhow!("No campaign counter store option set on context"))?;

    if let Some(campaign_store) = campaign_store_opt {
        if let (Some(buyer_mgr), Some(adv_mgr)) = (
            context.buyer_manager.get(),
            context.advertiser_manager.get(),
        ) {
            pipeline_builder.add_async(Box::new(CampaignCountersTask::new(
                campaign_store.clone(),
                buyer_mgr.clone(),
                adv_mgr.clone(),
            )));
        }
    }

    Ok(pipeline_builder.build())
}

// ---------------------------------------------------------------------------
// Top-level auction orchestrator — enrichment → direct → conditional RTB
//   → settlement → finalizers
// ---------------------------------------------------------------------------

struct AuctionOrchestratorTask {
    enrichment_pipeline: Pipeline<AuctionContext, Error>,
    direct_task: Option<Box<dyn AsyncTask<AuctionContext, Error>>>,
    conditional_rtb: ConditionalRtbTask,
    finalizers_pipeline: Option<Pipeline<AuctionContext, Error>>,
}

impl AuctionOrchestratorTask {
    async fn run0(&self, ctx: &AuctionContext) -> Result<(), Error> {
        // Phase 1: Enrichment — pub lookup, validation, device, identity, etc.
        // Errors here abort the auction (bad request, blocked IP, etc.)
        self.enrichment_pipeline.run(ctx).await?;

        // Phase 2: Direct campaign matching (no-ops if no campaigns loaded)
        if let Some(direct_task) = &self.direct_task {
            // Direct matching never errors — misses are silent
            let _ = direct_task.run(ctx).await;
        }

        // Phase 3: RTB — conditionally runs based on fill policy + direct outcome
        let rtb_res = self.conditional_rtb.run(ctx).await;

        // Phase 4: Settlement — picks winner from direct + RTB bids
        // Runs regardless of RTB result so direct bids can still settle
        let settlement_res = tasks::settlement::BidSettlementTask.run(ctx).await;

        // Phase 5: Finalizers — always run, persist counters
        if let Some(finalizers) = &self.finalizers_pipeline {
            finalizers.run(ctx).await?;
        }

        // Return earliest error from RTB or settlement
        rtb_res?;
        settlement_res
    }
}

#[async_trait]
impl AsyncTask<AuctionContext, Error> for AuctionOrchestratorTask {
    async fn run(&self, ctx: &AuctionContext) -> Result<(), Error> {
        let span = child_span_info!("auction_pipeline");
        self.run0(ctx).instrument(span).await
    }
}

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

/// Builds the pipeline for which an openrtb request flows through for auction handling.
/// This can be pre-fixed by other inbound adapters such as prebid, which should then
/// adapt to a bidrequest to be passed through this pipeline.
///
/// # Phases
/// 1. **Enrichment** — pub lookup, validation, device, identity (always runs)
/// 2. **Direct campaign matching** — matches campaigns + deals per imp (always runs, no-ops if empty)
/// 3. **Conditional RTB** — bidder matching, callouts, post-processing (skipped when direct fills)
/// 4. **Settlement** — picks winner from direct + RTB bids (always runs)
/// 5. **Finalizers** — persists counters (always runs)
pub fn build_auction_pipeline(
    context: &StartupContext,
) -> Result<Pipeline<AuctionContext, Error>, Error> {
    let enrichment_pipeline = build_enrichment_pipeline(context)?;
    let rtb_sub_pipeline = build_rtb_sub_pipeline(context)?;
    let finalizers_pipeline = build_finalizers_pipeline(context)?;

    // Direct campaign task — optional, only if managers were loaded
    let direct_task: Option<Box<dyn AsyncTask<AuctionContext, Error>>> = match (
        context.campaign_manager.get(),
        context.creative_manager.get(),
        context.deal_manager.get(),
        context.spend_pacer.get(),
        context.deal_pacer.get(),
    ) {
        (Some(cm), Some(crm), Some(dm), Some(sp), Some(dp)) => {
            Some(Box::new(tasks::direct::DirectCampaignMatchingTask::new(
                cm.clone(),
                crm.clone(),
                dm.clone(),
                sp.clone(),
                dp.clone(),
            )))
        }
        _ => None,
    };

    let orchestrator = AuctionOrchestratorTask {
        enrichment_pipeline,
        direct_task,
        conditional_rtb: ConditionalRtbTask::new(rtb_sub_pipeline),
        finalizers_pipeline,
    };

    let auction_pipeline = PipelineBuilder::new()
        .with_async(Box::new(orchestrator))
        .build()
        .expect("Auction pipeline should have orchestrator task");

    Ok(auction_pipeline)
}
