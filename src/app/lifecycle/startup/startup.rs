use crate::app::lifecycle::context::StartupContext;
use crate::app::lifecycle::startup::tasks::config_load::ConfigLoadTask;
use crate::app::span::WrappedPipelineTask;
use crate::app::startup::tasks::bidders_load::BidderManagerLoadTask;
use crate::app::startup::tasks::cluster::ClusterDiscoveryTask;
use crate::app::startup::tasks::firestore::FirestoreTask;
use crate::app::startup::tasks::demand_url_cache::DemandUrlCacheStartTask;
use crate::app::startup::tasks::device_load::DeviceLookupLoadTask;
use crate::app::startup::tasks::event_pipeline::BuildEventPipelineTask;
use crate::app::startup::tasks::ip_risk_load::IpRiskLoadTask;
use crate::app::startup::tasks::observability::ConfigureObservabilityTask;
use crate::app::startup::tasks::pubs_load::PubsManagerLoadTask;
use crate::app::startup::tasks::rtb_pipeline::BuildRtbPipelineTask;
use crate::app::startup::tasks::shapers_load::ShapersManagerLoadTask;
use crate::app::startup::tasks::start_server::StartServerTask;
use crate::app::startup::tasks::sync_pipelines::BuildSyncPipelinesTask;
use crate::app::startup::tasks::sync_store_init::SyncStoreInitTask;
use crate::core::config_manager::ConfigManager;
use pipeline::{Pipeline, PipelineBuilder};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tracing::{Span, info_span};

/// Builds the graceful ordering of startup tasks required for a successful startup.
/// Configures logging, builds the request pipelines, all that good stuff
pub fn build_start_pipeline(cfg_path: PathBuf) -> Pipeline<StartupContext, anyhow::Error> {
    let cfg_manager = Arc::new(ConfigManager::new(cfg_path));

    // gotta load the old mbr, we our config and observability wired
    let boot_loader = PipelineBuilder::new()
        .with_blocking(Box::new(ConfigLoadTask::new(cfg_manager.clone())))
        .with_blocking(Box::new(ConfigureObservabilityTask))
        .build()
        .expect("Bootloader should have tasks!");

    // now logging is configured, we can start our span. if done earlier, it would be dropped
    // NOTE - Tasks here can use the #[instrument] attribute without concern since we want to
    // log them during startup/shutdown and dont need to filter those
    let start_pipeline = PipelineBuilder::new()
        .with_async(Box::new(ClusterDiscoveryTask))
        .with_async(Box::new(FirestoreTask))
        .with_async(Box::new(BidderManagerLoadTask::new(cfg_manager.clone())))
        .with_blocking(Box::new(ShapersManagerLoadTask))
        .with_async(Box::new(PubsManagerLoadTask::new(cfg_manager.clone())))
        .with_async(Box::new(IpRiskLoadTask))
        .with_async(Box::new(DeviceLookupLoadTask))
        .with_blocking(Box::new(DemandUrlCacheStartTask::new(cfg_manager.clone())))
        .with_async(Box::new(SyncStoreInitTask::new(Duration::from_hours(
            24 * 7,
        ))))
        .with_blocking(Box::new(BuildRtbPipelineTask))
        .with_blocking(Box::new(BuildEventPipelineTask))
        .with_blocking(Box::new(BuildSyncPipelinesTask))
        .with_async(Box::new(StartServerTask))
        .build()
        .expect("Startup pipeline should have tasks!");

    let nop_bootloader_pipeline = WrappedPipelineTask::new(boot_loader, || Span::none());

    let observed_startup_pipeline =
        WrappedPipelineTask::new(start_pipeline, || info_span!("start_pipeline"));

    PipelineBuilder::new()
        .with_async(Box::new(nop_bootloader_pipeline))
        .with_async(Box::new(observed_startup_pipeline))
        .build()
        .expect("Pipeline should have tasks!")
}
