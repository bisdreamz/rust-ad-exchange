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

        let counters_campaign_store_opt = context
            .counters_campaign_store
            .get()
            .ok_or_else(|| anyhow!("Failed to get campaign counter store from startup context"))?;

        if let Some(counters_campaign_store) = counters_campaign_store_opt {
            counters_campaign_store.shutdown().await;

            info!("Flushed campaign counters");
        }

        let counters_deal_store_opt = context
            .counters_deal_store
            .get()
            .ok_or_else(|| anyhow!("Failed to get deal counter store from startup context"))?;

        if let Some(counters_deal_store) = counters_deal_store_opt {
            counters_deal_store.shutdown().await;

            info!("Flushed deal counters");
        }

        Ok(())
    }
}
