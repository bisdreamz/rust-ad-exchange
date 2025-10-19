use crate::app::context::StartupContext;
use crate::core::observability;
use anyhow::{bail, Error};
use pipeline::BlockingTask;
use tracing::{info, instrument};

pub struct ObservabilityShutdownTask;

impl BlockingTask<StartupContext, Error> for ObservabilityShutdownTask {
    #[instrument(skip_all, name = "observability_shutdown_task")]
    fn run(&self, context: &StartupContext) -> Result<(), Error> {
        let observability_opt = context.observability.get();

        if observability_opt.is_some() {
            let provider = observability_opt.unwrap();

            observability::shutdown(provider)
                .or_else(|_| bail!("Failed to observe shutdown task"))?;

            info!("Shut down observability");
        }

        Ok(())
    }
}