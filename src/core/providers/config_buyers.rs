use crate::core::config_manager::ConfigManager;
use crate::core::models::buyer::Buyer;
use crate::core::providers::{Provider, ProviderEvent};
use anyhow::Error;
use async_trait::async_trait;
use std::sync::Arc;

pub struct ConfigBuyerProvider {
    config_manager: Arc<ConfigManager>,
}

impl ConfigBuyerProvider {
    pub fn new(config_manager: Arc<ConfigManager>) -> Self {
        Self { config_manager }
    }
}

#[async_trait]
impl Provider<Buyer> for ConfigBuyerProvider {
    async fn start(
        &self,
        _on_event: Box<dyn Fn(ProviderEvent<Buyer>) + Send + Sync>,
    ) -> Result<Vec<Buyer>, Error> {
        self.config_manager.get().buyers.clone().ok_or_else(|| {
            anyhow::anyhow!("buyers must be defined in config when Firestore is not configured")
        })
    }
}
