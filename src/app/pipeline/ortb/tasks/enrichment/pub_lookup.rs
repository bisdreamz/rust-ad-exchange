use crate::app::pipeline::ortb::context::PublisherBlockReason;
use crate::app::pipeline::ortb::{AuctionContext, telemetry};
use crate::core::models::common::Status;
use crate::core::spec::nobidreasons;
use anyhow::{Error, anyhow, bail};
use pipeline::BlockingTask;
use rtb::child_span_info;
use rtb::common::bidresponsestate::BidResponseState;
use tracing::Span;

/// Lightweight check that the already-resolved publisher is enabled and
/// the placement (if present) is active. Replaces the old `PubLookupTask`
/// which performed the actual lookup — handlers now resolve the publisher
/// before creating the `AuctionContext`.
pub struct PublisherEnabledCheckTask;

impl BlockingTask<AuctionContext, Error> for PublisherEnabledCheckTask {
    fn run(&self, context: &AuctionContext) -> Result<(), Error> {
        let publisher = &context.publisher;

        let _span = child_span_info!(
            "publisher_enabled_check_task",
            pub_id = %publisher.id,
            placement_id = context.placement.as_ref().map(|p| p.id.as_str()).unwrap_or("")
        )
        .entered();

        let parent_span = Span::current();

        // Check publisher is enabled
        if !publisher.enabled {
            let brs = BidResponseState::NoBidReason {
                reqid: context.original_auction_id.clone(),
                nbr: nobidreasons::SELLER_DISABLED,
                desc: Some("Publisher account has been disabled"),
            };

            context
                .block_reason
                .set(PublisherBlockReason::DisabledSeller)
                .map_err(|_| anyhow!("Failed to attach disabled pub reason on ctx"))?;

            context
                .res
                .set(brs)
                .map_err(|_| anyhow!("Failed to attach disabled pub reason on ctx"))?;

            parent_span.record(telemetry::SPAN_REQ_BLOCK_REASON, "pubid_disabled");

            bail!("Disabled publisher {}", publisher.name);
        }

        // Check placement is active if present
        if let Some(placement) = &context.placement {
            if placement.status != Status::Active {
                let brs = BidResponseState::NoBidReason {
                    reqid: context.original_auction_id.clone(),
                    nbr: nobidreasons::SELLER_DISABLED,
                    desc: Some("Placement is not active"),
                };

                context
                    .block_reason
                    .set(PublisherBlockReason::DisabledSeller)
                    .map_err(|_| anyhow!("Failed to attach placement inactive reason on ctx"))?;

                context
                    .res
                    .set(brs)
                    .map_err(|_| anyhow!("Failed to attach placement inactive reason on ctx"))?;

                parent_span.record(telemetry::SPAN_REQ_BLOCK_REASON, "placement_inactive");

                bail!("Placement {} is not active", placement.id);
            }
        }

        Ok(())
    }
}
