use crate::core::config_manager::ConfigManager;
use crate::core::models::deal::Deal;
use crate::core::providers::{Provider, ProviderEvent};
use anyhow::Error;
use async_trait::async_trait;
use std::sync::Arc;

pub struct ConfigDealProvider {
    config_manager: Arc<ConfigManager>,
}

impl ConfigDealProvider {
    pub fn new(config_manager: Arc<ConfigManager>) -> Self {
        Self { config_manager }
    }
}

#[async_trait]
impl Provider<Deal> for ConfigDealProvider {
    async fn start(
        &self,
        _on_event: Box<dyn Fn(ProviderEvent<Deal>) + Send + Sync>,
    ) -> Result<Vec<Deal>, Error> {
        self.config_manager.get().deals.clone().ok_or_else(|| {
            anyhow::anyhow!("deals must be defined in config when Firestore is not configured")
        })
    }
}
