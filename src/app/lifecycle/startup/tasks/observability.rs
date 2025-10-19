use crate::app::lifecycle::context::StartupContext;
use crate::core::observability;
use anyhow::Error;
use pipeline::BlockingTask;
use tracing::info;

pub struct ConfigureObservabilityTask;

impl BlockingTask<StartupContext, Error> for ConfigureObservabilityTask {
    fn run(&self, context: &StartupContext) -> Result<(), Error> {
        // Get config from context
        let config = context.config.get()
            .ok_or_else(|| anyhow::anyhow!("Config not loaded before observability initialization"))?;

        // a provider is returned if otel export is configured
        // but observability may still have valid logging etc
        if let Some(provider) = observability::init(&config.logging)? {
            context.observability.set(provider)
                .map_err(|_| anyhow::anyhow!("Observability context already initialized"))?
        }

        info!("Hello world! Observability configured");

        Ok(())
    }
}
