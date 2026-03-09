use crate::app::context::StartupContext;
use crate::app::pipeline::events::billing::context::BillingEventContext;
use crate::app::pipeline::events::billing::tasks::{
    BailIfExpiredTask, CacheNoticeUrlsValidationTask, ExtractBillingEventTask, FireDemandBurlTask,
    MarkIfExpiredTask, ParseDataUrlTask, RecordBillingMetricsTask,
    RecordCampaignBillingCountersTask, RecordDealBillingCountersTask,
    RecordDemandBillingCountersTask, RecordPacingTask, RecordPubBillingCountersTask,
    RecordShapingEventsTask,
};
use anyhow::{Error, anyhow, bail};
use pipeline::{Pipeline, PipelineBuilder};

pub fn build_event_pipeline(
    context: &StartupContext,
) -> Result<Pipeline<BillingEventContext, Error>, Error> {
    let shaping_manager = context
        .shaping_manager
        .get()
        .ok_or_else(|| anyhow!("No shaping manager?! Cant build event pipeline"))?;

    let demand_url_cache = context
        .demand_url_cache
        .get()
        .ok_or_else(|| anyhow!("No demand url cache set! Cant build event pipeline"))?
        .clone();

    let pub_store_opt = context
        .counters_pub_store
        .get()
        .ok_or_else(|| anyhow!("No publisher counter store option set on context"))?;

    let demand_store_opt = context
        .counters_demand_store
        .get()
        .ok_or_else(|| anyhow!("No demand counter store option set on context"))?;

    let campaign_store_opt = context
        .counters_campaign_store
        .get()
        .ok_or_else(|| anyhow!("No campaign counter store option set on context"))?;

    let mut builder = PipelineBuilder::new()
        .with_blocking(Box::new(ParseDataUrlTask))
        .with_blocking(Box::new(ExtractBillingEventTask))
        .with_blocking(Box::new(CacheNoticeUrlsValidationTask::new(
            demand_url_cache,
        )))
        .with_blocking(Box::new(MarkIfExpiredTask))
        .with_blocking(Box::new(RecordBillingMetricsTask))
        .with_blocking(Box::new(BailIfExpiredTask));

    if let Some(pub_store) = pub_store_opt {
        let pub_manager = context
            .pub_manager
            .get()
            .ok_or_else(|| anyhow!("Pub store set but no pub manager!"))?;

        builder.add_blocking(Box::new(RecordPubBillingCountersTask::new(
            pub_store.clone(),
            pub_manager.clone(),
        )));
    }

    if let Some(demand_store) = demand_store_opt {
        let demand_manager = context
            .bidder_manager
            .get()
            .ok_or_else(|| anyhow!("Demand store set but no demand manager!"))?;

        builder.add_blocking(Box::new(RecordDemandBillingCountersTask::new(
            demand_store.clone(),
            demand_manager.clone(),
        )));
    }

    // Pacing updates — always run regardless of Firestore
    let spend_tracker = context
        .spend_tracker
        .get()
        .ok_or_else(|| anyhow!("No spend tracker on context!"))?;

    let deal_pacer = context
        .deal_pacer
        .get()
        .ok_or_else(|| anyhow!("No deal pacer on context!"))?;

    builder.add_blocking(Box::new(RecordPacingTask::new(
        spend_tracker.clone(),
        deal_pacer.clone(),
    )));

    // Deal counters — platform-wide, runs for both direct and RTB bids
    if let Some(deal_store) = context
        .counters_deal_store
        .get()
        .ok_or_else(|| anyhow!("No deal counter store option set on context"))?
    {
        builder.add_blocking(Box::new(RecordDealBillingCountersTask::new(
            deal_store.clone(),
        )));
    }

    // Campaign counters — only for direct bids
    if let Some(campaign_store) = campaign_store_opt {
        let pub_store = pub_store_opt
            .clone()
            .ok_or_else(|| anyhow!("Campaign store set but no pub counter store!"))?;

        let advertiser_manager = context
            .advertiser_manager
            .get()
            .ok_or_else(|| anyhow!("Campaign store set but no advertiser manager!"))?;

        let pub_manager = context
            .pub_manager
            .get()
            .ok_or_else(|| anyhow!("Campaign store set but no pub manager!"))?;

        builder.add_blocking(Box::new(RecordCampaignBillingCountersTask::new(
            campaign_store.clone(),
            pub_store,
            advertiser_manager.clone(),
            pub_manager.clone(),
        )));
    }

    builder
        .add_blocking(Box::new(RecordShapingEventsTask::new(
            shaping_manager.clone(),
        )))
        .add_async(Box::new(FireDemandBurlTask));

    match builder.build() {
        Some(pipeline) => Ok(pipeline),
        None => bail!("Failed to build event pipeline"),
    }
}
