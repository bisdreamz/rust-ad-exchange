use crate::app::context::StartupContext;
use anyhow::{Error, anyhow};
use async_trait::async_trait;
use pipeline::AsyncTask;
use tracing::{info, instrument};

pub struct FlushCountersTask;

#[async_trait]
impl AsyncTask<StartupContext, Error> for FlushCountersTask {
    #[instrument(skip_all, name = "flush_counters_task")]
    async fn run(&self, context: &StartupContext) -> Result<(), Error> {
        let counters_pub_store_opt = context
            .counters_pub_store
            .get()
            .ok_or_else(|| anyhow!("Failed to get publisher counter store from startup context"))?;

        if let Some(counters_pub_store) = counters_pub_store_opt {
            counters_pub_store.shutdown().await;

            info!("Flushed publisher counters");
        }

        let counters_demand_store_opt = context
            .counters_demand_store
            .get()
            .ok_or_else(|| anyhow!("Failed to get demand counter store from startup context"))?;

        if let Some(counters_demand_store) = counters_demand_store_opt {
            counters_demand_store.shutdown().await;

            info!("Flushed demand counters");
        }

        Ok(())
    }
}
