use crate::app::pipeline::events::billing::context::BillingEventContext;
use anyhow::{Error, bail};
use tracing::debug;
use pipeline::BlockingTask;
use rtb::common::DataUrl;

/// Responsible for parsing the raw event url string into
/// a ['DataUrl'] by which we can extract rich types like price, etc
pub struct ParseDataUrlTask;

impl BlockingTask<BillingEventContext, Error> for ParseDataUrlTask {
    fn run(&self, context: &BillingEventContext) -> Result<(), Error> {
        let data_url = match DataUrl::from(&context.event_url) {
            Ok(data_url) => data_url,
            Err(err) => bail!("Failed to parse event url {}: {}", &context.event_url, err),
        };

        match context.data_url.set(data_url) {
            Ok(()) => debug!("Parsed data url: {:?}", &context.data_url),
            Err(_) => bail!("Failed to set data url on context, already set?"),
        }

        Ok(())
    }
}
