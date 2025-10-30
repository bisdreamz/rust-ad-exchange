use crate::app::pipeline::events::billing::context::BillingEventContext;
use crate::core::events::billing::BillingEvent;
use anyhow::{Error, bail};
use log::debug;
use pipeline::BlockingTask;

pub struct ExtractBillingEventTask;

impl BlockingTask<BillingEventContext, Error> for ExtractBillingEventTask {
    fn run(&self, context: &BillingEventContext) -> Result<(), Error> {
        let data_url = match context.data_url.get() {
            Some(data_url) => data_url,
            None => bail!("data_url missing on billing event context!"),
        };

        let billing_event = BillingEvent::from(data_url)?;

        debug!("Extracted billing event {:?}", &billing_event);

        match context.details.set(billing_event) {
            Ok(_) => debug!("Extracted and attached billing event!"),
            Err(_) => bail!("Failed to set billing event on context!"),
        };

        Ok(())
    }
}
