use crate::app::context::StartupContext;
use crate::core::filters::bot::IpRiskFilter;
use anyhow::{Error, anyhow, bail};
use async_trait::async_trait;
use pipeline::AsyncTask;
use std::time::Duration;
use tracing::{info, instrument};

pub struct IpRiskLoadTask;

#[async_trait]
impl AsyncTask<StartupContext, anyhow::Error> for IpRiskLoadTask {
    #[instrument(skip_all, name = "ip_risk_load_task")]
    async fn run(&self, context: &StartupContext) -> Result<(), Error> {
        info!("Ip Risk loading started..");

        let config_opt = context.config.get();
        if config_opt.is_none() {
            bail!("Missing config");
        }

        let cache_config = &config_opt.unwrap().caches;
        let cache_sz = cache_config.cache_ip_sz;

        if cache_sz < 10000 {
            bail!("Ip risk cache size way too small! Min 10k")
        }

        let ip_risk_filter = IpRiskFilter::try_new(cache_sz, Duration::from_secs(10 * 60)).await?;

        info!(
            "Ip Risk loading of {} ranges finished",
            ip_risk_filter.ranges()
        );

        let mut ip_lock = context
            .ip_risk_filter
            .lock()
            .map_err(|_| anyhow!("Failed to acquire ip risk filter lock during assignment"))?;

        if ip_lock.is_some() {
            bail!("Someone already assigned ip risk filter on context");
        }

        ip_lock.replace(ip_risk_filter);

        Ok(())
    }
}
