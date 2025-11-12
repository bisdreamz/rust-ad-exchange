use std::sync::Arc;
use anyhow::{anyhow, bail, Error};
use pipeline::BlockingTask;
use rtb::child_span_info;
use tracing::{debug, warn};
use crate::app::pipeline::syncing::out::context::{SyncOutContext, SyncResponse};
use crate::core::managers::PublisherManager;
use crate::core::usersync;

pub struct ExtractLocalUidTask {
    pub_manager: Arc<PublisherManager>
}

impl ExtractLocalUidTask {
    pub fn new(pub_manager: Arc<PublisherManager>) -> Self {
        Self { pub_manager }
    }
}

impl BlockingTask<SyncOutContext, Error> for ExtractLocalUidTask {
    fn run(&self, context: &SyncOutContext) -> Result<(), Error> {
        let span = child_span_info!(
            "extract_local_uid_task",
            "pubid" = tracing::field::Empty,
            "local_uid" = tracing::field::Empty,
        );

        let pubid = &context.pubid;

        if pubid.trim().is_empty() {
            context.response.set(SyncResponse::Error("Empty pub id".into()))
                .unwrap_or_else(|_| warn!("Failed to attach missing pub id err to context!"));

            bail!("Empty pub id on sync request");
        }

        span.record("pubid", pubid.clone());

        let publisher = match self.pub_manager.get(pubid) {
            Some(publisher) => publisher,
            None => {
                context.response.set(SyncResponse::Error("Unrecognized pub id".into()))
                    .unwrap_or_else(|_| warn!("Failed to attach unknown pub id err to context!"));

                bail!("Unknown pub id {} on sync request", pubid);
            }
        };

        context.publisher.set(publisher.clone())
            .map_err(|_| anyhow!("Failed to publish unknown pub_id err to context"))?;

        let local_uid = match context.cookies.get(usersync::constants::CONST_REX_COOKIE_ID) {
            Some(uid) => {
                if usersync::utils::validate_local_id(uid) {
                    debug!("Found existing local uid on cookie: {}", uid);

                    uid.clone()
                } else {
                    let new_uid = usersync::utils::generate_local_id();

                    debug!("Found invalid uid cookie value {}, how?! Re-assigning to {}", uid, new_uid);

                    new_uid
                }
            },
            None => {
                let new_uid = usersync::utils::generate_local_id();

                debug!("No local uid found on user cookie yet, assigning {}", new_uid);

                new_uid
            }
        };

        context.local_id.set(local_uid)
            .map_err(|_| anyhow!("Failed to update local id on context?!"))?;

        Ok(())
    }
}