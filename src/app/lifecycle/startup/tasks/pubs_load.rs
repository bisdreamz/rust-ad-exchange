use crate::app::context::StartupContext;
use crate::core::config_manager::ConfigManager;
use crate::core::managers::PublisherManager;
use crate::core::models::publisher::Publisher;
use crate::core::providers::{ConfigPublisherProvider, FirestoreProvider, Provider};
use anyhow::{Error, anyhow};
use async_trait::async_trait;
use pipeline::AsyncTask;
use std::sync::Arc;
use tracing::{info, instrument};

pub struct PubsManagerLoadTask {
    config_manager: Arc<ConfigManager>,
}

impl PubsManagerLoadTask {
    pub fn new(config_manager: Arc<ConfigManager>) -> Self {
        PubsManagerLoadTask { config_manager }
    }
}

#[async_trait]
impl AsyncTask<StartupContext, Error> for PubsManagerLoadTask {
    #[instrument(skip_all, name = "pubs_manager_load_task")]
    async fn run(&self, context: &StartupContext) -> Result<(), Error> {
        let provider: Arc<dyn Provider<Publisher>> = match context.firestore.get() {
            None => return Err(anyhow!("Firestore task must run before publisher manager")),
            Some(None) => {
                info!("Loading publishers from config");
                Arc::new(ConfigPublisherProvider::new(self.config_manager.clone()))
            }
            Some(Some(db)) => {
                info!("Loading publishers from Firestore");
                Arc::new(FirestoreProvider::new(db.clone(), "publishers"))
            }
        };

        let pub_manager = PublisherManager::start(provider).await?;

        context
            .pub_manager
            .set(pub_manager)
            .map_err(|_| anyhow!("Can't init publisher manager"))?;

        Ok(())
    }
}
