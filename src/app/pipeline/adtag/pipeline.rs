use crate::app::lifecycle::context::StartupContext;
use crate::app::pipeline::adtag::AdtagContext;
use crate::app::pipeline::adtag::tasks::build_auction::BuildAuctionTask;
use crate::app::pipeline::adtag::tasks::build_response::BuildResponseTask;
use crate::app::pipeline::adtag::tasks::resolve_placement::ResolvePlacementTask;
use crate::app::pipeline::adtag::tasks::run_auction::RunAuctionTask;
use anyhow::{Error, anyhow};
use pipeline::{Pipeline, PipelineBuilder};

pub fn build_adtag_pipeline(
    context: &StartupContext,
) -> Result<Pipeline<AdtagContext, Error>, Error> {
    let placement_manager = context
        .placement_manager
        .get()
        .ok_or_else(|| anyhow!("Placement manager not set"))?
        .clone();

    let property_manager = context
        .property_manager
        .get()
        .ok_or_else(|| anyhow!("Property manager not set"))?
        .clone();

    let pub_manager = context
        .pub_manager
        .get()
        .ok_or_else(|| anyhow!("Publisher manager not set"))?
        .clone();

    let auction_pipeline = context
        .auction_pipeline
        .get()
        .ok_or_else(|| anyhow!("Auction pipeline not set — build RTB pipeline first"))?
        .clone();

    let cookie_domain = context
        .config
        .get()
        .ok_or_else(|| anyhow!("Config not set when building adtag pipeline"))?
        .cookie_domain
        .clone();

    let pipeline = PipelineBuilder::new()
        .with_blocking(Box::new(ResolvePlacementTask::new(
            placement_manager,
            property_manager,
            pub_manager,
        )))
        .with_blocking(Box::new(BuildAuctionTask))
        .with_async(Box::new(RunAuctionTask::new(auction_pipeline)))
        .with_async(Box::new(BuildResponseTask::new(cookie_domain)))
        .build()
        .expect("Adtag pipeline should have tasks");

    Ok(pipeline)
}
