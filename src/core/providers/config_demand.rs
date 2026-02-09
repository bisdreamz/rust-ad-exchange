use crate::app::config::BidderConfig;
use crate::core::config_manager::ConfigManager;
use crate::core::providers::{Provider, ProviderEvent};
use anyhow::Error;
use async_trait::async_trait;
use std::sync::Arc;

pub struct ConfigDemandProvider {
    config_manager: Arc<ConfigManager>,
}

impl ConfigDemandProvider {
    pub fn new(config_manager: Arc<ConfigManager>) -> Self {
        Self { config_manager }
    }
}

#[async_trait]
impl Provider<BidderConfig> for ConfigDemandProvider {
    async fn start(
        &self,
        _on_event: Box<dyn Fn(ProviderEvent<BidderConfig>) + Send + Sync>,
    ) -> Result<Vec<BidderConfig>, Error> {
        self.config_manager.get().bidders.clone()
            .ok_or_else(|| anyhow::anyhow!("bidders must be defined in config when Firestore is not configured"))
    }
}
