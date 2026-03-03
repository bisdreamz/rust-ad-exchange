use crate::app::pipeline::events::billing::context::BillingEventContext;
use anyhow::{Error, bail};
use pipeline::BlockingTask;
use rtb::child_span_info;
use rtb::common::DataUrl;
use tracing::debug;

/// Responsible for parsing the raw event url string into
/// a ['DataUrl'] by which we can extract rich types like price, etc
pub struct ParseDataUrlTask;

impl BlockingTask<BillingEventContext, Error> for ParseDataUrlTask {
    fn run(&self, context: &BillingEventContext) -> Result<(), Error> {
        let span = child_span_info!("parse_data_url_task", raw_url = tracing::field::Empty,);
        span.record("raw_url", context.event_url.as_str());

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
