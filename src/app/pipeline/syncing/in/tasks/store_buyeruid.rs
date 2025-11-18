use crate::app::pipeline::syncing::r#in::context::SyncInContext;
use crate::core::usersync::SyncStore;
use anyhow::{Error, anyhow};
use async_trait::async_trait;
use pipeline::AsyncTask;
use rtb::child_span_info;
use std::sync::Arc;
use tracing::{Instrument, debug};

/// Task stores the incoming buyeruid mapping to
/// the provided ['SyncStore']
pub struct StoreBuyeruidTask {
    store: Arc<dyn SyncStore>,
}

impl StoreBuyeruidTask {
    pub fn new(store: Arc<dyn SyncStore>) -> Self {
        Self { store }
    }

    async fn run0(&self, context: &SyncInContext) -> Result<(), Error> {
        let event = context
            .event
            .get()
            .ok_or_else(|| anyhow!("No event in context!"))?;

        match self
            .store
            .append(
                &event.local_uid,
                &event.partner_id,
                event.remote_uid.clone(),
            )
            .await
        {
            Some(_) => {
                debug!(
                    "Refreshed existing buyeruid {} from partner {} local uid {}",
                    event.remote_uid, event.partner_id, event.local_uid
                );
            }
            None => {
                debug!(
                    "Appended new buyeruid value for partner {} local uid {}",
                    event.partner_id, event.local_uid
                );
            }
        }

        Ok(())
    }
}

#[async_trait]
impl AsyncTask<SyncInContext, Error> for StoreBuyeruidTask {
    async fn run(&self, context: &SyncInContext) -> Result<(), Error> {
        let span = child_span_info!("store_buyeruid_task");

        self.run0(context).instrument(span).await
    }
}
