use std::time::Duration;
use crate::app::context::StartupContext;
use crate::core::filters::bot::IpRiskFilter;
use anyhow::{anyhow, bail, Error};
use async_trait::async_trait;
use pipeline::AsyncTask;

pub struct IpRiskLoadTask;

#[async_trait]
impl AsyncTask<StartupContext, anyhow::Error> for IpRiskLoadTask {
    async fn run(&self, context: &StartupContext) -> Result<(), Error> {
        println!("Ip Risk loading started");

        let config_opt = context.config.get();
        if (config_opt.is_none()) {
            bail!("Missing config");
        }

        let cache_config = &config_opt.unwrap().caches;
        let cache_sz = cache_config.cache_ip_sz;

        if cache_sz < 10000 {
            bail!("Ip risk cache size way too small! Min 10k")
        }

        let ip_risk_filter = IpRiskFilter::try_new(
            cache_sz,
            Duration::from_secs(10 * 60),
        ).await?;

        println!("Ip Risk loading of {} ranges finished", ip_risk_filter.ranges());

        let mut ip_lock = context.ip_risk_filter.lock()
            .map_err(|_| anyhow!("Failed to acquire ip risk filter lock during assignment"))?;

        if ip_lock.is_some() {
            bail!("Someone already assigned ip risk filter on context");
        }

        ip_lock.replace(ip_risk_filter);

        Ok(())
    }
}