use crate::app::context::StartupContext;
use crate::core::observability;
use anyhow::{Error, bail};
use pipeline::BlockingTask;
use tracing::{info, instrument};

pub struct ObservabilityShutdownTask;

impl BlockingTask<StartupContext, Error> for ObservabilityShutdownTask {
    #[instrument(skip_all, name = "observability_shutdown_task")]
    fn run(&self, context: &StartupContext) -> Result<(), Error> {
        if let Some(handles) = context.observability.get() {
            observability::shutdown(handles)
                .or_else(|_| bail!("Failed to observe shutdown task"))?;

            info!("Shut down observability");
        }

        Ok(())
    }
}
