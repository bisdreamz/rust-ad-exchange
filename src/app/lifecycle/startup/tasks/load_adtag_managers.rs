use crate::app::context::StartupContext;
use crate::core::config_manager::ConfigManager;
use crate::core::managers::{PlacementManager, PropertyManager};
use crate::core::models::placement::Placement;
use crate::core::models::property::Property;
use crate::core::providers::{
    ConfigPlacementProvider, ConfigPropertyProvider, FirestoreProvider, Provider,
};
use anyhow::{Error, anyhow};
use async_trait::async_trait;
use pipeline::AsyncTask;
use std::sync::Arc;
use tracing::{info, instrument};

pub struct LoadAdtagManagersTask {
    config_manager: Arc<ConfigManager>,
}

impl LoadAdtagManagersTask {
    pub fn new(config_manager: Arc<ConfigManager>) -> Self {
        Self { config_manager }
    }
}

#[async_trait]
impl AsyncTask<StartupContext, Error> for LoadAdtagManagersTask {
    #[instrument(skip_all, name = "load_adtag_managers_task")]
    async fn run(&self, context: &StartupContext) -> Result<(), Error> {
        let firestore_opt = context
            .firestore
            .get()
            .ok_or_else(|| anyhow!("Firestore state not set"))?;

        let (placement_prov, property_prov): (
            Arc<dyn Provider<Placement>>,
            Arc<dyn Provider<Property>>,
        ) = match firestore_opt {
            Some(db) => {
                info!("Loading placements and properties from Firestore");
                (
                    Arc::new(FirestoreProvider::new(db.clone(), "placements")),
                    Arc::new(FirestoreProvider::new(db.clone(), "properties")),
                )
            }
            None => {
                info!("Loading placements and properties from config");
                (
                    Arc::new(ConfigPlacementProvider::new(self.config_manager.clone())),
                    Arc::new(ConfigPropertyProvider::new(self.config_manager.clone())),
                )
            }
        };

        let (placements, properties) = tokio::try_join!(
            PlacementManager::start(placement_prov),
            PropertyManager::start(property_prov),
        )?;

        context
            .placement_manager
            .set(placements)
            .map_err(|_| anyhow!("placement_manager already set on context"))?;
        context
            .property_manager
            .set(properties)
            .map_err(|_| anyhow!("property_manager already set on context"))?;

        info!("Placements and properties loaded");
        Ok(())
    }
}
