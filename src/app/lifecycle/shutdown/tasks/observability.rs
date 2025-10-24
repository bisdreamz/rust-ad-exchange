use crate::app::context::StartupContext;
use crate::core::observability;
use anyhow::{Context, Error};
use async_trait::async_trait;
use pipeline::AsyncTask;
use tracing::instrument;

pub struct ObservabilityShutdownTask;

#[async_trait]
impl AsyncTask<StartupContext, Error> for ObservabilityShutdownTask {
    #[instrument(skip_all, name = "observability_shutdown_task")]
    async fn run(&self, context: &StartupContext) -> Result<(), Error> {
        if let Some(handles) = context.observability.get() {
            observability::shutdown(handles)
                .await
                .context("Failed to shutdown observability")?;
        }

        Ok(())
    }
}
