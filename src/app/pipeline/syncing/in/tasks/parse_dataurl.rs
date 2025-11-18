use crate::app::pipeline::syncing::r#in::context::SyncInContext;
use anyhow::{Error, anyhow, bail};
use pipeline::BlockingTask;
use rtb::child_span_info;
use rtb::common::DataUrl;
use tracing::{debug, warn};

/// Task to parse the raw http req url into a ['DataUrl']
pub struct ParseUrlTask;

impl BlockingTask<SyncInContext, Error> for ParseUrlTask {
    fn run(&self, context: &SyncInContext) -> Result<(), Error> {
        let _span = child_span_info!("extract_event_task", raw_url = context.event_url);

        let data_url = match DataUrl::from(&context.event_url) {
            Ok(data_url) => data_url,
            Err(_) => {
                warn!(
                    "Received invalid url from inbound sync: {}",
                    context.event_url
                );

                bail!("Invalid sync in url");
            }
        };

        context
            .data_url
            .set(data_url)
            .map_err(|_| anyhow!("Could not set data url, already set?!"))?;

        debug!("Parsed data url from inbound sync: {}", context.event_url);

        Ok(())
    }
}
