use crate::app::pipeline::syncing::out::context::{SyncOutContext, SyncResponse};
use crate::core::managers::PublisherManager;
use anyhow::{Error, anyhow, bail};
use pipeline::BlockingTask;
use rtb::child_span_info;
use std::sync::Arc;
use tracing::{debug, warn};

pub struct ExtractPublisherTask {
    pub_manager: Arc<PublisherManager>,
}

impl ExtractPublisherTask {
    pub fn new(pub_manager: Arc<PublisherManager>) -> Self {
        Self { pub_manager }
    }
}

impl BlockingTask<SyncOutContext, Error> for ExtractPublisherTask {
    fn run(&self, context: &SyncOutContext) -> Result<(), Error> {
        let span = child_span_info!("extract_publisher_task", "pubid" = tracing::field::Empty);

        let pubid = &context.pubid;

        if pubid.trim().is_empty() {
            context
                .response
                .set(SyncResponse::Error("Empty pub id".into()))
                .unwrap_or_else(|_| warn!("Failed to attach missing pub id err to context!"));

            bail!("Empty pub id on sync request");
        }

        span.record("pubid", pubid.clone());

        let publisher = match self.pub_manager.get(pubid) {
            Some(publisher) => publisher,
            None => {
                context
                    .response
                    .set(SyncResponse::Error("Unrecognized pub id".into()))
                    .unwrap_or_else(|_| warn!("Failed to attach unknown pub id err to context!"));

                bail!("Unknown pub id {} on sync request", pubid);
            }
        };

        context
            .publisher
            .set(publisher.clone())
            .map_err(|_| anyhow!("Failed to publish unknown pub_id err to context"))?;

        debug!(
            "Attached recognized publisher {} to context",
            publisher.name
        );

        Ok(())
    }
}
