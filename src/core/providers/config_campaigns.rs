use crate::core::config_manager::ConfigManager;
use crate::core::models::campaign::Campaign;
use crate::core::providers::{Provider, ProviderEvent};
use anyhow::Error;
use async_trait::async_trait;
use std::sync::Arc;

pub struct ConfigCampaignProvider {
    config_manager: Arc<ConfigManager>,
}

impl ConfigCampaignProvider {
    pub fn new(config_manager: Arc<ConfigManager>) -> Self {
        Self { config_manager }
    }
}

#[async_trait]
impl Provider<Campaign> for ConfigCampaignProvider {
    async fn start(
        &self,
        _on_event: Box<dyn Fn(ProviderEvent<Campaign>) + Send + Sync>,
    ) -> Result<Vec<Campaign>, Error> {
        Ok(self
            .config_manager
            .get()
            .campaigns
            .clone()
            .unwrap_or_default())
    }
}
