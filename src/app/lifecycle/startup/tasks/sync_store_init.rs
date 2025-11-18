use crate::app::context::StartupContext;
use crate::core::usersync;
use anyhow::{Error, anyhow};
use async_trait::async_trait;
use pipeline::AsyncTask;
use rtb::child_span_info;
use std::sync::Arc;
use std::time::Duration;
use tracing::info;

/// Creates the user match store, currently only caches locally
pub struct SyncStoreInitTask {
    duration_ttl: Duration,
}

impl SyncStoreInitTask {
    pub fn new(duration_ttl: Duration) -> Self {
        Self { duration_ttl }
    }
}

#[async_trait]
impl AsyncTask<StartupContext, Error> for SyncStoreInitTask {
    async fn run(&self, context: &StartupContext) -> Result<(), Error> {
        let _span = child_span_info!("sync_store_init_task").entered();

        let sync_store = usersync::LocalStore::new(self.duration_ttl);

        context
            .sync_store
            .set(Arc::new(sync_store))
            .map_err(|_err| anyhow!("Failed to attach sync store to start context!"))?;

        info!(
            "Attached sync store to start context with TTL of {:?}",
            self.duration_ttl
        );

        Ok(())
    }
}
