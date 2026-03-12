use crate::core::config_manager::ConfigManager;
use crate::core::models::property::Property;
use crate::core::providers::{Provider, ProviderEvent};
use anyhow::Error;
use async_trait::async_trait;
use std::sync::Arc;

pub struct ConfigPropertyProvider {
    config_manager: Arc<ConfigManager>,
}

impl ConfigPropertyProvider {
    pub fn new(config_manager: Arc<ConfigManager>) -> Self {
        Self { config_manager }
    }
}

#[async_trait]
impl Provider<Property> for ConfigPropertyProvider {
    async fn start(
        &self,
        _on_event: Box<dyn Fn(ProviderEvent<Property>) + Send + Sync>,
    ) -> Result<Vec<Property>, Error> {
        self.config_manager.get().properties.clone().ok_or_else(|| {
            anyhow::anyhow!("properties must be defined in config when Firestore is not configured")
        })
    }
}
