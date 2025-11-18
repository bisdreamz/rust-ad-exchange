use crate::app::pipeline::events::billing::context::BillingEventContext;
use anyhow::{Error, bail};
use async_trait::async_trait;
use pipeline::AsyncTask;
use rtb::child_span_info;
use tracing::{Instrument, Span, debug, warn};

/// Responsible for firing the demand billing events. This task
/// expects them to be present and will bail if missing. also
/// bails if the call to a partner burl fails
pub struct FireDemandBurlTask;

impl FireDemandBurlTask {
    async fn run0(&self, context: &BillingEventContext) -> Result<(), Error> {
        let span = Span::current();

        let notice_urls = match context.demand_urls.get() {
            Some(urls) => urls,
            None => {
                warn!(
                    "No notice URLs on event context when firing BURL task. This should not happen!"
                );

                bail!("No notice URLs!");
            }
        };

        if notice_urls.burl.is_none() {
            debug!("No demand burl present on event, skipping burl fire");
            return Ok(());
        }

        let burl = notice_urls.burl.as_ref().unwrap();

        span.record("demand_burl", burl.as_str());

        match reqwest::get(burl).await {
            Ok(_) => {
                debug!("Successfully fetched burl fire: {}", burl);
                span.record("success", true);
            }
            Err(e) => {
                span.record("success", false);
                span.record("error", e.to_string());

                bail!("Failed to fetched burl fire {}: {}", burl, e);
            }
        }

        Ok(())
    }
}

#[async_trait]
impl AsyncTask<BillingEventContext, Error> for FireDemandBurlTask {
    async fn run(&self, context: &BillingEventContext) -> Result<(), Error> {
        let span = child_span_info!(
            "fire_demand_burl_task",
            demand_burl = tracing::field::Empty,
            success = tracing::field::Empty,
            error = tracing::field::Empty,
        );

        self.run0(context).instrument(span).await
    }
}
