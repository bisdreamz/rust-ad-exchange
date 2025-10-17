use std::path::PathBuf;
use crate::app::lifecycle::context::StartupContext;
use pipeline::{Pipeline, PipelineBuilder};
use crate::app::core::config_manager::ConfigManager;
use crate::app::lifecycle::startup::tasks::config_load::ConfigLoadTask;
use crate::app::pipeline::ortb::build_auction_pipeline;
use crate::app::startup::tasks::start_server::StartServerTask;

pub fn build_start_pipeline(cfg_path: PathBuf) -> Pipeline<StartupContext, anyhow::Error> {
    let cfg_manager = ConfigManager::new(cfg_path);

    let rtb_pipeline = build_auction_pipeline();

    PipelineBuilder::new()
        .with_blocking(Box::new(ConfigLoadTask::new(cfg_manager)))
        .with_async(Box::new(StartServerTask::new(rtb_pipeline)))
        .build()
        .expect("Startup pipeline should have tasks!")
}