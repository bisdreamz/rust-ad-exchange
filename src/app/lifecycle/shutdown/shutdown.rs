use crate::app::lifecycle::context::StartupContext;
use crate::app::lifecycle::shutdown::tasks::stop_server::StopServerTask;
use pipeline::{Pipeline, PipelineBuilder};
use tracing::info_span;
use crate::app::shutdown::tasks::observability::ObservabilityShutdownTask;
use crate::app::span::WrappedPipelineTask;

/// Builds the shutdown pipeline, which takes the resulting `StartupContext`
/// which is responsible for attaching anything which may need shutdown
pub fn build_shutdown_pipeline() -> Pipeline<StartupContext, anyhow::Error> {
    let shutdown_pipeline = PipelineBuilder::new()
        .with_async(Box::new(StopServerTask))
        .with_blocking(Box::new(ObservabilityShutdownTask))
        .build()
        .expect("Shutdown pipeline should have tasks!");

    let observed_pipeline = WrappedPipelineTask::new(shutdown_pipeline,
                                                     || info_span!("shutdown_pipeline"));

    PipelineBuilder::new()
        .with_async(Box::new(observed_pipeline))
        .build()
        .expect("Shutdown pipeline should have tasks!")
}
