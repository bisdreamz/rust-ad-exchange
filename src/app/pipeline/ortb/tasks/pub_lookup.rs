use crate::app::pipeline::ortb::AuctionContext;
use crate::core::managers::PublisherManager;
use crate::core::models::publisher::Publisher;
use crate::core::spec::nobidreasons;
use anyhow::{Error, anyhow, bail};
use log::debug;
use pipeline::BlockingTask;
use rtb::child_span_info;
use rtb::common::bidresponsestate::BidResponseState;
use std::sync::Arc;

fn record_and_bail_if_empty(context: &AuctionContext) -> Result<(), Error> {
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

        bail!("Unknown pub id");
    }

    Ok(())
}

fn record_and_bail_if_disabled(
    context: &AuctionContext,
    publisher: &Publisher,
) -> Result<(), Error> {
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

        bail!("Unknown publisher id {}", &context.pubid);
    }
}

impl BlockingTask<AuctionContext, Error> for PubLookupTask {
    fn run(&self, context: &AuctionContext) -> Result<(), Error> {
        let _ = child_span_info!("pub_lookup_task", pub_id = tracing::field::Empty).entered();

        record_and_bail_if_empty(context)?;

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
