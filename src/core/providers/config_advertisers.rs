use crate::core::config_manager::ConfigManager;
use crate::core::models::advertiser::Advertiser;
use crate::core::providers::{Provider, ProviderEvent};
use anyhow::Error;
use async_trait::async_trait;
use std::sync::Arc;

pub struct ConfigAdvertiserProvider {
    config_manager: Arc<ConfigManager>,
}

impl ConfigAdvertiserProvider {
    pub fn new(config_manager: Arc<ConfigManager>) -> Self {
        Self { config_manager }
    }
}

#[async_trait]
impl Provider<Advertiser> for ConfigAdvertiserProvider {
    async fn start(
        &self,
        _on_event: Box<dyn Fn(ProviderEvent<Advertiser>) + Send + Sync>,
    ) -> Result<Vec<Advertiser>, Error> {
        Ok(self
            .config_manager
            .get()
            .advertisers
            .clone()
            .unwrap_or_default())
    }
}
