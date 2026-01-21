use crate::app::pipeline::ortb::{AuctionContext, telemetry};
use crate::core::managers::PublisherManager;
use crate::core::models::publisher::Publisher;
use crate::core::spec::nobidreasons;
use anyhow::{Error, anyhow, bail};
use tracing::debug;
use pipeline::BlockingTask;
use rtb::child_span_info;
use rtb::common::bidresponsestate::BidResponseState;
use std::sync::Arc;
use tracing::Span;

fn record_and_bail_if_empty(context: &AuctionContext) -> Result<(), Error> {
    let parent_span = Span::current();

    if context.pubid.trim().is_empty() {
        let brs = BidResponseState::NoBidReason {
            reqid: context.req.read().id.clone(),
            nbr: nobidreasons::UNKNOWN_SELLER,
            desc: Some("Empty pub id"),
        };

        context
            .res
            .set(brs)
            .map_err(|_| anyhow!("Failed to attach empty pubid reason on ctx"))?;

        parent_span.record(telemetry::SPAN_REQ_BLOCK_REASON, "pubid_arg_missing");

        bail!("Unknown pub id");
    }

    Ok(())
}

fn record_and_bail_if_disabled(
    context: &AuctionContext,
    publisher: &Publisher,
) -> Result<(), Error> {
    let parent_span = Span::current();

    if !publisher.enabled {
        let brs = BidResponseState::NoBidReason {
            reqid: context.req.read().id.clone(),
            nbr: nobidreasons::SELLER_DISABLED,
            desc: Some("Publisher account has been disabled"),
        };

        context
            .res
            .set(brs)
            .map_err(|_| anyhow!("Failed to attach disabled pub reason on ctx"))?;

        parent_span.record(telemetry::SPAN_REQ_BLOCK_REASON, "pubid_disabled");

        bail!("Disabled publisher {}", publisher.name);
    }

    Ok(())
}

pub struct PubLookupTask {
    manager: Arc<PublisherManager>,
}

impl PubLookupTask {
    pub fn new(manager: Arc<PublisherManager>) -> Self {
        Self { manager }
    }
    fn lookup_pub_or_bail(&self, context: &AuctionContext) -> Result<Arc<Publisher>, Error> {
        let parent_span = Span::current();
        let _span = child_span_info!("pub_lookup_task_lookup").entered();

        if let Some(publisher) = self.manager.get(&context.pubid) {
            return Ok(publisher);
        }

        let brs = BidResponseState::NoBidReason {
            reqid: context.req.read().id.clone(),
            nbr: nobidreasons::UNKNOWN_SELLER,
            desc: Some("Publisher id unrecognized"),
        };

        context
            .res
            .set(brs)
            .map_err(|_| anyhow!("Failed to attach unrecognized pub reason on ctx"))?;

        parent_span.record(telemetry::SPAN_REQ_BLOCK_REASON, "pubid_unknown");

        bail!("Unknown publisher id {}", &context.pubid);
    }
}

impl BlockingTask<AuctionContext, Error> for PubLookupTask {
    fn run(&self, context: &AuctionContext) -> Result<(), Error> {
        let span = child_span_info!("pub_lookup_task", pub_id = tracing::field::Empty).entered();

        record_and_bail_if_empty(context)?;

        span.record("pub_id", &context.pubid);

        let publisher = self.lookup_pub_or_bail(&context)?;

        record_and_bail_if_disabled(context, &publisher)?;

        context
            .publisher
            .set(publisher.clone())
            .map_err(|_| anyhow!("Publisher already set on ctx?"))?;

        debug!(
            "Pub lookup complete. Found seller ({}) {}",
            publisher.id, publisher.name
        );

        Ok(())
    }
}
