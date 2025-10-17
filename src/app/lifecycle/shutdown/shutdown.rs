use crate::app::lifecycle::context::StartupContext;
use crate::app::lifecycle::shutdown::tasks::stop_server::StopServerTask;
use pipeline::{Pipeline, PipelineBuilder};

/// Builds the shutdown pipeline, which takes the resulting `StartupContext`
/// which is responsible for attaching anything which may need shutdown
pub fn build_shutdown_pipeline() -> Pipeline<StartupContext, anyhow::Error> {
    PipelineBuilder::new()
        .with_async(Box::new(StopServerTask))
        .build()
        .expect("Shutdown pipeline should have tasks!")
}