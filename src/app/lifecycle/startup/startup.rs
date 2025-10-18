use crate::app::lifecycle::context::StartupContext;
use crate::app::lifecycle::startup::tasks::config_load::ConfigLoadTask;
use crate::app::startup::tasks::bidder_manager_load::BidderManagerLoadTask;
use crate::app::startup::tasks::ip_risk_load::IpRiskLoadTask;
use crate::app::startup::tasks::rtb_pipeline::BuildRtbPipelineTask;
use crate::app::startup::tasks::start_server::StartServerTask;
use crate::core::config_manager::ConfigManager;
use pipeline::{Pipeline, PipelineBuilder};
use std::path::PathBuf;
use std::sync::Arc;

pub fn build_start_pipeline(cfg_path: PathBuf) -> Pipeline<StartupContext, anyhow::Error> {
    let cfg_manager = Arc::new(ConfigManager::new(cfg_path));

    PipelineBuilder::new()
        .with_blocking(Box::new(ConfigLoadTask::new(cfg_manager.clone())))
        .with_blocking(Box::new(BidderManagerLoadTask::new(cfg_manager.clone())))
        .with_async(Box::new(IpRiskLoadTask))
        .with_blocking(Box::new(BuildRtbPipelineTask))
        .with_async(Box::new(StartServerTask))
        .build()
        .expect("Startup pipeline should have tasks!")
}