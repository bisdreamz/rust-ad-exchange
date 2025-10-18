use std::num::NonZeroU32;
use std::time::Duration;
use crate::app::context::StartupContext;
use crate::core::filters::bot::IpRiskFilter;
use anyhow::{anyhow, bail, Error};
use async_trait::async_trait;
use pipeline::AsyncTask;
use crate::core::enrichment::device::DeviceLookup;

pub struct DeviceLookupLoadTask;

#[async_trait]
impl AsyncTask<StartupContext, anyhow::Error> for DeviceLookupLoadTask {
    async fn run(&self, context: &StartupContext) -> Result<(), Error> {
        println!("Device data load task started");

        let config_opt = context.config.get();
        if (config_opt.is_none()) {
            bail!("Missing config");
        }

        let cache_config = &config_opt.unwrap().caches;
        let cache_sz = cache_config.cache_ip_sz;

        if cache_sz < 10000 {
            bail!("Device cache size way too small! Min 10k")
        }

        let device_lookup = DeviceLookup::try_new(NonZeroU32::new(cache_sz as u32).unwrap())
            .map_err(|e| anyhow!("Failed to create device lookup: {}", e))?;

        let mut dev_lock = context.device_lookup.lock()
            .map_err(|_| anyhow!("Failed to acquire device lookup lock during assignment"))?;

        if dev_lock.is_some() {
            bail!("Someone already assigned device lookup on context");
        }

        dev_lock.replace(device_lookup);

        Ok(())
    }
}