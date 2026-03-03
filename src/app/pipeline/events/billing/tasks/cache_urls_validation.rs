use crate::app::pipeline::events::billing::context::BillingEventContext;
use crate::core::demand::notifications::DemandNotificationsCache;
use anyhow::{Error, anyhow};
use pipeline::BlockingTask;
use rtb::child_span_info;
use std::sync::Arc;
use tracing::debug;

/// Looks up the bid event ID in the ['DemandNotificationsCache']
/// which is responsible for validatin recognized billing events.
/// If an event is recognized, it means the event has not already
/// been received and the event is within the url cache expiry ttl
pub struct CacheNoticeUrlsValidationTask {
    cache: Arc<DemandNotificationsCache>,
}

impl CacheNoticeUrlsValidationTask {
    pub fn new(cache: Arc<DemandNotificationsCache>) -> Self {
        Self { cache }
    }
}

impl BlockingTask<BillingEventContext, Error> for CacheNoticeUrlsValidationTask {
    fn run(&self, context: &BillingEventContext) -> Result<(), Error> {
        let span = child_span_info!(
            "cache_notice_urls_validation_task",
            bid_event_id = tracing::field::Empty,
            result = tracing::field::Empty,
            is_direct = tracing::field::Empty,
        );

        let billing_event = context
            .details
            .get()
            .ok_or_else(|| anyhow!("No details on billing context!"))?;

        span.record("bid_event_id", billing_event.bid_event_id.as_str());

        match self.cache.get(&billing_event.bid_event_id) {
            Some(notice) => {
                span.record("result", "found");
                span.record("is_direct", notice.direct.is_some());

                debug!(
                    "Received valid billing event id {}",
                    billing_event.bid_event_id
                );

                context
                    .bid_notice
                    .set(notice)
                    .map_err(|_| anyhow!("bid_notice already set on context?!"))?;
            }
            None => {
                span.record("result", "expired_or_duplicate");

                debug!(
                    "Received expired or duplicate billing event id {}",
                    billing_event.bid_event_id
                );
            }
        }

        // We dont bail here because we want the pipeline to continue for the moment
        // so that we can record the raw success stats
        Ok(())
    }
}
