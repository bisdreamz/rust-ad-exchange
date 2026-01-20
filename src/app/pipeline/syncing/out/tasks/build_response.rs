use crate::app::pipeline::syncing::out::context::{SyncOutContext, SyncResponse};
use crate::core::managers::DemandManager;
use crate::core::models::sync::{SyncConfig, SyncKind};
use crate::core::usersync;
use anyhow::{bail, Error};
use pipeline::BlockingTask;
use std::sync::Arc;
use tracing::warn;

pub struct BuildSyncOutResponseTask {
    bidders: Arc<DemandManager>,
}

impl BuildSyncOutResponseTask {
    pub fn new(bidders: Arc<DemandManager>) -> Self {
        Self { bidders }
    }
}

impl BlockingTask<SyncOutContext, Error> for BuildSyncOutResponseTask {
    fn run(&self, context: &SyncOutContext) -> Result<(), Error> {
        let local_uid = match context.local_uid.get() {
            Some(local_uid) => local_uid,
            None => bail!("Local uid is not set! Cannot build sync response"),
        };

        let bidders = self.bidders.bidders();

        if bidders.is_empty() {
            warn!("Sync call but no bidders to sync to! Skipping");

            context
                .response
                .set(SyncResponse::NoContent)
                .unwrap_or_else(|_| {
                    warn!("Someone already assigned sync response on empty bidder skip")
                });

            bail!("No bidders matching to sync with");
        }

        let publisher = match context.publisher.get() {
            Some(publisher) => publisher.clone(),
            None => bail!("No publisher on context for sync response"),
        };

        let pub_sync = match &publisher.sync_url {
            Some(sync) => Some(SyncConfig {
                kind: SyncKind::Image,
                url: sync.clone(),
            }),
            None => None,
        };

        // TODO warn need to finish user sync uid macros!
        warn!("Must finish user sync uid macros!");

        let response_html =
            usersync::utils::generate_sync_iframe_html(&local_uid, bidders, pub_sync);
        if response_html.is_empty() {
            context
                .response
                .set(SyncResponse::NoContent)
                .unwrap_or_else(|_| {
                    warn!("Someone already assigned sync response on empty bidder skip")
                });

            bail!("Built sync html content empty, skipping response - no pub or demand syncs?");
        }

        let response = SyncResponse::Content(response_html);

        context.response.set(response).unwrap_or_else(|_| {
            warn!("Someone already assigned sync response on empty bidder skip")
        });

        Ok(())
    }
}
