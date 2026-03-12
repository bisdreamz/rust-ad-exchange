use crate::core::config_manager::ConfigManager;
use crate::core::models::placement::Placement;
use crate::core::providers::{Provider, ProviderEvent};
use anyhow::Error;
use async_trait::async_trait;
use std::sync::Arc;

pub struct ConfigPlacementProvider {
    config_manager: Arc<ConfigManager>,
}

impl ConfigPlacementProvider {
    pub fn new(config_manager: Arc<ConfigManager>) -> Self {
        Self { config_manager }
    }
}

#[async_trait]
impl Provider<Placement> for ConfigPlacementProvider {
    async fn start(
        &self,
        _on_event: Box<dyn Fn(ProviderEvent<Placement>) + Send + Sync>,
    ) -> Result<Vec<Placement>, Error> {
        self.config_manager.get().placements.clone().ok_or_else(|| {
            anyhow::anyhow!("placements must be defined in config when Firestore is not configured")
        })
    }
}
