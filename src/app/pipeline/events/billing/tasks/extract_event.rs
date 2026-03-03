use crate::app::pipeline::events::billing::context::BillingEventContext;
use crate::core::events::billing::BillingEvent;
use anyhow::{Error, bail};
use pipeline::BlockingTask;
use rtb::child_span_info;
use tracing::debug;

pub struct ExtractBillingEventTask;

impl BlockingTask<BillingEventContext, Error> for ExtractBillingEventTask {
    fn run(&self, context: &BillingEventContext) -> Result<(), Error> {
        let span = child_span_info!(
            "extract_billing_event_task",
            billing_event = tracing::field::Empty,
        );

        let data_url = match context.data_url.get() {
            Some(data_url) => data_url,
            None => bail!("data_url missing on billing event context!"),
        };

        let billing_event = BillingEvent::from(data_url)?;

        span.record("billing_event", format!("{:?}", &billing_event).as_str());

        match context.details.set(billing_event) {
            Ok(_) => debug!("Extracted and attached billing event!"),
            Err(_) => bail!("Failed to set billing event on context!"),
        };

        Ok(())
    }
}
