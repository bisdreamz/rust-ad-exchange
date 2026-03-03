use crate::core::config_manager::ConfigManager;
use crate::core::models::creative::Creative;
use crate::core::providers::{Provider, ProviderEvent};
use anyhow::Error;
use async_trait::async_trait;
use std::sync::Arc;

pub struct ConfigCreativeProvider {
    config_manager: Arc<ConfigManager>,
}

impl ConfigCreativeProvider {
    pub fn new(config_manager: Arc<ConfigManager>) -> Self {
        Self { config_manager }
    }
}

#[async_trait]
impl Provider<Creative> for ConfigCreativeProvider {
    async fn start(
        &self,
        _on_event: Box<dyn Fn(ProviderEvent<Creative>) + Send + Sync>,
    ) -> Result<Vec<Creative>, Error> {
        Ok(self
            .config_manager
            .get()
            .creatives
            .clone()
            .unwrap_or_default())
    }
}
