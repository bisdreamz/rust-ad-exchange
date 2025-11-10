use crate::app::context::StartupContext;
use crate::core::config_manager::ConfigManager;
use crate::core::demand::notifications::DemandNotificationsCache;
use anyhow::{Error, anyhow};
use pipeline::BlockingTask;
use std::sync::Arc;
use tracing::info;

pub struct DemandUrlCacheStartTask {
    cfg_manager: Arc<ConfigManager>,
}

impl DemandUrlCacheStartTask {
    pub fn new(cfg_manager: Arc<ConfigManager>) -> Self {
        DemandUrlCacheStartTask { cfg_manager }
    }
}

impl BlockingTask<StartupContext, Error> for DemandUrlCacheStartTask {
    fn run(&self, context: &StartupContext) -> Result<(), Error> {
        let config = self.cfg_manager.get();
        let ttl = config.notifications.ttl;

        let cache = DemandNotificationsCache::new(ttl);

        context
            .demand_url_cache
            .set(Arc::new(cache))
            .map_err(|_| anyhow!("Failed to set demand cache for url cache on startup context"))?;

        info!("Started demand URL cache, ttl={}s", ttl.as_secs());

        Ok(())
    }
}
