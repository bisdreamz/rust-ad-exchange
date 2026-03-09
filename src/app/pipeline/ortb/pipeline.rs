use crate::app::context::StartupContext;
use crate::app::pipeline::ortb::tasks::finalizers::{
    CampaignCountersTask, DealBidCountersTask, DemandCountersTask, PubCountersTask,
};
use crate::app::pipeline::ortb::{AuctionContext, tasks};
use crate::core::demand::client::DemandClient;
use crate::core::models::placement::FillPolicy;
use anyhow::{Error, anyhow, bail};
use async_trait::async_trait;
use pipeline::{AsyncTask, Pipeline, PipelineBuilder};
use rtb::child_span_info;
use tracing::{Instrument, debug};

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

    let config = context
        .config
        .get()
        .ok_or(anyhow!("Config not set when building enrichment pipeline"))?;

    let mut builder = PipelineBuilder::new()
        .with_blocking(Box::new(tasks::enrichment::PublisherEnabledCheckTask))
        .with_blocking(Box::new(tasks::enrichment::ValidateRequestTask))
        .with_blocking(Box::new(tasks::enrichment::SchainHopsGlobalFilter::new(
            config.schain_limit,
        )))
        .with_blocking(Box::new(tasks::enrichment::JunkFilterTask));

    if !config.skip_ip_block {
        builder = builder.with_blocking(Box::new(tasks::enrichment::IpBlockTask::new(
            ip_risk_filter,
        )));
    }

    let pipeline = builder
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
// RTB sub-pipeline — bidder matching, callouts, deal attribution
// ---------------------------------------------------------------------------

fn build_rtb_sub_pipeline(
    context: &StartupContext,
) -> Result<Pipeline<AuctionContext, Error>, Error> {
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

    let demand_client =
        DemandClient::new().or_else(|e| bail!("RTB pipeline client failed: {}", e))?;

    let deal_manager = context
        .deal_manager
        .get()
        .ok_or(anyhow!("No deal manager"))?;

    let deal_pacer = context.deal_pacer.get().ok_or(anyhow!("No deal pacer"))?;

    let pipeline = PipelineBuilder::new()
        .with_async(Box::new(tasks::rtb::BidderMatchingTask::new(
            bidder_manager.clone(),
            deal_manager.clone(),
            deal_pacer.clone(),
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
        .with_async(Box::new(tasks::rtb::RtbDealAttributionTask::new(
            deal_manager.clone(),
        )))
        .build()
        .expect("RTB sub-pipeline should have tasks");

    Ok(pipeline)
}

// ---------------------------------------------------------------------------
// Shared bid pipeline — runs after merge on ALL bids (direct + RTB) uniformly
// as both direct campaign bids and rtb bids will share the context.res
// bid settlement object
// ---------------------------------------------------------------------------

fn build_shared_bid_pipeline(
    context: &StartupContext,
) -> Result<Pipeline<AuctionContext, Error>, Error> {
    let demand_url_cache = context
        .demand_url_cache
        .get()
        .ok_or_else(|| anyhow!("Demand url cache not set"))?;

    let config = context
        .config
        .get()
        .ok_or(anyhow!("Config not set when building shared bid pipeline"))?;

    let events_config = &config.notifications;

    let pipeline = PipelineBuilder::new()
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
        .expect("Shared bid pipeline should have tasks");

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
            .as_ref()
            .map(|p| &p.fill_policy)
            .unwrap_or(&FillPolicy::HighestPrice);

        let run_rtb = match fill_policy {
            FillPolicy::DealsOnly | FillPolicy::DirectOnly => false,
            // RTB always runs — deal restriction is enforced by BidderMatchingTask
            // (skips non-deal bidders) and settlement (filters non-deal bids)
            FillPolicy::DirectAndRtbDeals => true,
            FillPolicy::RtbBackfill | FillPolicy::HighestPrice => true,
        };

        if run_rtb {
            debug!("Running RTB sub-pipeline");
            self.rtb_pipeline.run(ctx).await?;
        } else {
            debug!(fill_policy = ?fill_policy, "RTB skipped");
        }

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Finalizers — always runs, persists counters and any database stats
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

    // Campaign counters — only if campaign store + advertiser manager loaded
    let campaign_store_opt = context
        .counters_campaign_store
        .get()
        .ok_or_else(|| anyhow!("No campaign counter store option set on context"))?;

    if let Some(campaign_store) = campaign_store_opt {
        if let Some(adv_mgr) = context.advertiser_manager.get() {
            pipeline_builder.add_async(Box::new(CampaignCountersTask::new(
                campaign_store.clone(),
                adv_mgr.clone(),
            )));
        }
    }

    let deal_store_opt = context
        .counters_deal_store
        .get()
        .ok_or_else(|| anyhow!("No deal counter store option set on context"))?;

    if let Some(deal_store) = deal_store_opt {
        pipeline_builder.add_async(Box::new(DealBidCountersTask::new(deal_store.clone())));
    }

    Ok(pipeline_builder.build())
}

// ---------------------------------------------------------------------------
// Top-level auction orchestrator
//   enrichment → direct → [test bidder] → conditional RTB → merge → shared bids → settlement → finalizers
// ---------------------------------------------------------------------------

struct AuctionOrchestratorTask {
    enrichment_pipeline: Pipeline<AuctionContext, Error>,
    direct_task: Option<Box<dyn AsyncTask<AuctionContext, Error>>>,
    resolve_macros_task: Option<tasks::direct::ResolveDirectCreativeMacrosTask>,
    conditional_rtb: ConditionalRtbTask,
    merge_task: tasks::direct::MergeDirectBidsTask,
    shared_pipeline: Pipeline<AuctionContext, Error>,
    finalizers_pipeline: Option<Pipeline<AuctionContext, Error>>,
}

impl AuctionOrchestratorTask {
    async fn run0(&self, ctx: &AuctionContext) -> Result<(), Error> {
        // Phase 1: Enrichment — pub lookup, validation, device, identity, etc.
        // Errors here mean the request is blocked — skip auction phases.
        let auction_res = match self.enrichment_pipeline.run(ctx).await {
            Ok(()) => self.run_auction(ctx).await,
            err => err,
        };

        // Finalizers ALWAYS run — counters and events must be persisted
        // regardless of whether the auction succeeded, failed, or was blocked.
        if let Some(finalizers) = &self.finalizers_pipeline {
            finalizers.run(ctx).await?;
        }

        auction_res
    }

    async fn run_auction(&self, ctx: &AuctionContext) -> Result<(), Error> {
        // Phase 2: Direct campaign matching → staging
        if let Some(direct_task) = &self.direct_task {
            // Direct matching never errors — misses are silent
            let _ = direct_task.run(ctx).await;
        }

        // Phase 2a: Resolve creative macros in staged direct bids
        if let Some(resolve_task) = &self.resolve_macros_task {
            let _ = resolve_task.run(ctx).await;
        }

        // Phase 2b: Test bidder — injects synthetic bids as direct bids
        // when force_bid is set and no real direct bids were produced.
        if ctx.direct_bid_staging.lock().await.is_empty() {
            let _ = tasks::rtb::TestBidderTask.run(ctx).await;
        }

        // Phase 3: RTB — conditionally runs, writes to bidders + rtb_nbr.
        // Errors are only surfaced if no direct bids were produced,
        // since direct bids alone can satisfy the auction.
        let rtb_err = self.conditional_rtb.run(ctx).await.err();

        // Phase 4: Merge direct staging into bidders
        let merge_res = self.merge_task.run(ctx).await;

        // Phase 5: Shared bid tasks (margin, notice URLs, shaping, injection)
        let shared_res = self.shared_pipeline.run(ctx).await;

        // Phase 6: Settlement
        let settlement_res = tasks::settlement::BidSettlementTask.run(ctx).await;

        // Surface RTB error if no bids were produced from any source
        if let Some(e) = rtb_err {
            if ctx.bidders.lock().await.is_empty() {
                return Err(e);
            }
        }

        merge_res.and(shared_res).and(settlement_res)
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
/// adapt to a bidrequest to be passed through this pipeline. RTB pipeline
/// is skipped for requests with a ['Placement'] present and a policy which
/// disallows RTB *or* prioritizes direct campaign bids over RTB demand.
///
/// # Phases
/// 1. **Enrichment** — pub lookup, validation, device, identity (always runs)
/// 2. **Direct campaign matching** — matches campaigns + deals per imp → staging
/// 3. **Conditional RTB** — bidder matching, callouts → bidders + rtb_nbr
/// 4. **Merge** — moves staged direct bids into bidders
/// 5. **Shared bid tasks** — margin, notice URLs, shaping, injection (all bids uniform)
/// 6. **Settlement** — picks winner from unified bidders list
/// 7. **Finalizers** — persists counters (always runs)
pub fn build_auction_pipeline(
    context: &StartupContext,
) -> Result<Pipeline<AuctionContext, Error>, Error> {
    let enrichment_pipeline = build_enrichment_pipeline(context)?;
    let rtb_sub_pipeline = build_rtb_sub_pipeline(context)?;
    let shared_pipeline = build_shared_bid_pipeline(context)?;
    let finalizers_pipeline = build_finalizers_pipeline(context)?;

    // Direct campaign task — optional, only if managers were loaded
    let direct_task: Option<Box<dyn AsyncTask<AuctionContext, Error>>> = match (
        context.campaign_manager.get(),
        context.creative_manager.get(),
        context.deal_manager.get(),
        context.spend_pacer.get(),
        context.deal_pacer.get(),
        context.buyer_manager.get(),
    ) {
        (Some(cm), Some(crm), Some(dm), Some(sp), Some(dp), Some(bm)) => {
            Some(Box::new(tasks::direct::DirectCampaignMatchingTask::new(
                cm.clone(),
                crm.clone(),
                dm.clone(),
                sp.clone(),
                dp.clone(),
                bm.clone(),
            )))
        }
        _ => None,
    };

    // Resolve creative macros — optional, needs cdn_domain + advertiser_manager
    let resolve_macros_task = match (context.cdn_base.get(), context.advertiser_manager.get()) {
        (Some(cdn_domain), Some(adv_mgr)) => {
            Some(tasks::direct::ResolveDirectCreativeMacrosTask::new(
                cdn_domain.clone(),
                adv_mgr.clone(),
            ))
        }
        _ => None,
    };

    let orchestrator = AuctionOrchestratorTask {
        enrichment_pipeline,
        direct_task,
        resolve_macros_task,
        conditional_rtb: ConditionalRtbTask::new(rtb_sub_pipeline),
        merge_task: tasks::direct::MergeDirectBidsTask,
        shared_pipeline,
        finalizers_pipeline,
    };

    let auction_pipeline = PipelineBuilder::new()
        .with_async(Box::new(orchestrator))
        .build()
        .expect("Auction pipeline should have orchestrator task");

    Ok(auction_pipeline)
}
