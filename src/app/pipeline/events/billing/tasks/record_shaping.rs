use crate::app::pipeline::constants;
use crate::app::pipeline::events::billing::context::BillingEventContext;
use crate::core::managers::ShaperManager;
use anyhow::{Error, anyhow};
use pipeline::BlockingTask;
use std::sync::Arc;
use tracing::{debug, warn};

pub struct RecordShapingEventsTask {
    shaper_manager: Arc<ShaperManager>,
}

impl RecordShapingEventsTask {
    pub fn new(shaper_manager: Arc<ShaperManager>) -> Self {
        Self { shaper_manager }
    }
}

impl BlockingTask<BillingEventContext, Error> for RecordShapingEventsTask {
    fn run(&self, context: &BillingEventContext) -> Result<(), Error> {
        let billing_url = context
            .data_url
            .get()
            .ok_or_else(|| anyhow!("No billing event on context!"))?;

        let billing_event = context
            .details
            .get()
            .ok_or_else(|| anyhow!("No billing event on context!"))?;

        let shaping_key_opt = billing_url
            .get_string(constants::URL_SHAPING_KEY_PARAM)
            .map_err(|e| anyhow!("Failed to read shaping key in data url: {}", e))?;

        let shaping_key = match shaping_key_opt {
            Some(shaping_key) => shaping_key,
            None => {
                debug!("No shaping key for bid event, skipping shaping training");
                return Ok(());
            }
        };

        debug!("Attempting to record shaping key: {shaping_key:?}");

        let shaper = match self
            .shaper_manager
            .shaper(&billing_event.bidder_id, &billing_event.endpoint_id)
        {
            Some(shaper) => shaper,
            None => {
                // likely, it was enabled but recently disabled
                warn!("Have shaping key but no shaper found while recording billing event!");
                return Ok(());
            }
        };

        shaper
            .record_impression(
                &shaping_key,
                billing_event.cpm_gross,
                billing_event.cpm_cost,
            )
            .map_err(|e| anyhow!("Failed to record billing event on shaper: {}", e))?;

        debug!(
            "Successfully recorded impression on traffic shaper for bidder {} endpoint {}",
            billing_event.bidder_id, billing_event.endpoint_id
        );

        Ok(())
    }
}
