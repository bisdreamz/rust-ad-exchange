use crate::app::lifecycle::context::StartupContext;
use anyhow::Error;
use async_trait::async_trait;
use pipeline::AsyncTask;
use tracing::{info, instrument};

pub(crate) struct StopServerTask;

#[async_trait]
impl AsyncTask<StartupContext, anyhow::Error> for StopServerTask {
    #[instrument(skip_all, name = "server_shutdown_task")]
    async fn run(&self, context: &StartupContext) -> Result<(), Error> {
        if context.server.get().is_none() {
            return Ok(());
        }

        match context.server.get() {
            Some(server) => {
                info!("Closing listener gracefully..");
                server.stop().await;
                info!("Listener closed");
            }
            None => {
                info!("Skipping listener shutdown, was never started");
            }
        }

        Ok(())
    }
}
