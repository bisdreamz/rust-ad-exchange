use crate::core::config_manager::ConfigManager;
use crate::core::models::publisher::Publisher;
use crate::core::providers::{Provider, ProviderEvent};
use anyhow::Error;
use async_trait::async_trait;
use std::sync::Arc;

pub struct ConfigPublisherProvider {
    config_manager: Arc<ConfigManager>,
}

impl ConfigPublisherProvider {
    pub fn new(config_manager: Arc<ConfigManager>) -> Self {
        Self { config_manager }
    }
}

#[async_trait]
impl Provider<Publisher> for ConfigPublisherProvider {
    async fn start(
        &self,
        _on_event: Box<dyn Fn(ProviderEvent<Publisher>) + Send + Sync>,
    ) -> Result<Vec<Publisher>, Error> {
        Ok(self.config_manager.get().publishers.clone())
    }
}
