use crate::app::config::BidderConfig;
use crate::app::context::StartupContext;
use crate::core::config_manager::ConfigManager;
use crate::core::managers::DemandManager;
use crate::core::providers::{ConfigDemandProvider, FirestoreProvider, Provider};
use anyhow::{anyhow, Error};
use async_trait::async_trait;
use pipeline::AsyncTask;
use std::sync::Arc;
use tracing::{info, instrument};

pub struct BidderManagerLoadTask {
    config_manager: Arc<ConfigManager>,
}

impl BidderManagerLoadTask {
    pub fn new(config_manager: Arc<ConfigManager>) -> Self {
        BidderManagerLoadTask { config_manager }
    }
}

#[async_trait]
impl AsyncTask<StartupContext, Error> for BidderManagerLoadTask {
    #[instrument(skip_all, name = "bidder_manager_load_task")]
    async fn run(&self, context: &StartupContext) -> Result<(), Error> {
        let provider: Arc<dyn Provider<BidderConfig>> = match context.firestore.get() {
            None => return Err(anyhow!("Firestore task must run before bidder manager")),
            Some(None) => {
                info!("Loading bidders from config");
                Arc::new(ConfigDemandProvider::new(self.config_manager.clone()))
            }
            Some(Some(db)) => {
                info!("Loading bidders from Firestore");
                Arc::new(FirestoreProvider::new(db.clone(), "bidders"))
            }
        };

        let demand_manager = DemandManager::start(provider).await?;

        context
            .bidder_manager
            .set(demand_manager)
            .map_err(|_| anyhow!("Can't init bidder manager"))?;

        Ok(())
    }
}
