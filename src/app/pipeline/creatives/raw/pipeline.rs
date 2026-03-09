use crate::app::context::StartupContext;
use crate::app::pipeline::creatives::raw::context::RawCreativeContext;
use crate::app::pipeline::creatives::raw::tasks::{ResolveCdnTask, ResolveCreativeTask};
use anyhow::{Error, anyhow, bail};
use pipeline::{Pipeline, PipelineBuilder};

pub fn build_raw_creative_pipeline(
    context: &StartupContext,
    cdn_base: String,
) -> Result<Pipeline<RawCreativeContext, Error>, Error> {
    let creative_manager = context
        .creative_manager
        .get()
        .ok_or_else(|| anyhow!("No creative manager on context"))?
        .clone();

    let pipeline = PipelineBuilder::new()
        .with_blocking(Box::new(ResolveCreativeTask::new(creative_manager)))
        .with_blocking(Box::new(ResolveCdnTask::new(cdn_base)))
        .build();

    match pipeline {
        Some(p) => Ok(p),
        None => bail!("Failed to build raw creative pipeline"),
    }
}
