use crate::app::context::StartupContext;
use crate::core::firestore::create_client;
use anyhow::{Error, anyhow};
use async_trait::async_trait;
use pipeline::AsyncTask;
use std::sync::Arc;
use tracing::{info, instrument};

pub struct FirestoreTask;

#[async_trait]
impl AsyncTask<StartupContext, Error> for FirestoreTask {
    #[instrument(skip_all, name = "firestore_task")]
    async fn run(&self, context: &StartupContext) -> Result<(), Error> {
        let config = context
            .config
            .get()
            .ok_or(anyhow!("Config not set on startup context"))?;

        let firestore = if let Some(fs_config) = &config.firestore {
            info!("Connecting to Firestore project: {}", fs_config.project_id);

            let client = create_client(fs_config).await.map_err(|e| {
                anyhow!("Failed to connect to Firestore: {}", e)
            })?;

            info!("Connected to Firestore");
            Some(Arc::new(client))
        } else {
            info!("Firestore not configured, using local config providers");
            None
        };

        context
            .firestore
            .set(firestore)
            .map_err(|_| anyhow!("Failed to set firestore on startup context"))?;

        Ok(())
    }
}
