use crate::app::pipeline::adtag::context::{AdtagContext, KnownPage};
use crate::app::pipeline::ortb::PublisherBlockReason;
use crate::core::managers::{PlacementManager, PropertyManager, PublisherManager};
use crate::core::models::common::Status;
use anyhow::{Error, bail};
use pipeline::BlockingTask;
use rtb::child_span_info;
use std::sync::Arc;
use tracing::{Span, warn};
use url::Url;

pub struct ResolvePlacementTask {
    placements: Arc<PlacementManager>,
    properties: Arc<PropertyManager>,
    publishers: Arc<PublisherManager>,
}

impl ResolvePlacementTask {
    pub fn new(
        placements: Arc<PlacementManager>,
        properties: Arc<PropertyManager>,
        publishers: Arc<PublisherManager>,
    ) -> Self {
        Self {
            placements,
            properties,
            publishers,
        }
    }
}

impl BlockingTask<AdtagContext, Error> for ResolvePlacementTask {
    fn run(&self, ctx: &AdtagContext) -> Result<(), Error> {
        let parent_span = Span::current();

        let placement = match self.placements.get(&ctx.request.placement_id) {
            Some(p) => p,
            None => {
                ctx.block_reason
                    .set(PublisherBlockReason::UnknownPlacement)
                    .map_err(|_| anyhow::anyhow!("block_reason already set"))?;
                parent_span.record("block_reason", "unknown_placement");
                bail!("Unknown placement: {}", ctx.request.placement_id);
            }
        };

        if placement.status != Status::Active {
            ctx.block_reason
                .set(PublisherBlockReason::DisabledSeller)
                .map_err(|_| anyhow::anyhow!("block_reason already set"))?;
            parent_span.record("block_reason", "placement_inactive");
            bail!("Placement {} is not active", placement.id);
        }

        let property = match self.properties.get(&placement.property_id) {
            Some(p) => p,
            None => {
                ctx.block_reason
                    .set(PublisherBlockReason::UnknownProperty)
                    .map_err(|_| anyhow::anyhow!("block_reason already set"))?;
                parent_span.record("block_reason", "unknown_property");
                bail!("Unknown property: {}", placement.property_id);
            }
        };

        if property.status != Status::Active {
            ctx.block_reason
                .set(PublisherBlockReason::DisabledSeller)
                .map_err(|_| anyhow::anyhow!("block_reason already set"))?;
            parent_span.record("block_reason", "property_inactive");
            bail!("Property {} is not active", property.id);
        }

        let publisher = match self.publishers.get(&placement.pub_id) {
            Some(p) => p,
            None => {
                ctx.block_reason
                    .set(PublisherBlockReason::UnknownSeller)
                    .map_err(|_| anyhow::anyhow!("block_reason already set"))?;
                parent_span.record("block_reason", "unknown_publisher");
                bail!("Unknown publisher: {}", placement.pub_id);
            }
        };

        if !publisher.enabled {
            ctx.block_reason
                .set(PublisherBlockReason::DisabledSeller)
                .map_err(|_| anyhow::anyhow!("block_reason already set"))?;
            parent_span.record("block_reason", "publisher_disabled");
            bail!("Publisher {} is disabled", publisher.id);
        }

        let _span = child_span_info!(
            "resolve_placement_task",
            placement_name = %placement.name,
            property_id = %property.id,
            property_name = %property.name,
            pub_id = %publisher.id,
            pub_name = %publisher.name,
        )
        .entered();

        let known_page = ctx.request.page_url.as_ref().and_then(|raw_url| {
            let parsed = match Url::parse(raw_url) {
                Ok(u) => u,
                Err(e) => {
                    warn!(page_url = %raw_url, error = %e, "page_url unparseable — falling back to property domain");
                    return None;
                }
            };

            let host = match parsed.host_str() {
                Some(h) => h.to_string(),
                None => {
                    warn!(page_url = %raw_url, "page_url has no host — falling back to property domain");
                    return None;
                }
            };

            if host.ends_with(&property.domain) || host == property.domain {
                Some(KnownPage { page_url: raw_url.clone(), extracted_domain: host })
            } else {
                warn!(
                    host = %host,
                    property_domain = %property.domain,
                    "page_url host does not match property domain — falling back to property domain"
                );
                None
            }
        });

        ctx.placement
            .set(placement)
            .map_err(|_| anyhow::anyhow!("placement already set"))?;
        ctx.property
            .set(property)
            .map_err(|_| anyhow::anyhow!("property already set"))?;
        ctx.publisher
            .set(publisher)
            .map_err(|_| anyhow::anyhow!("publisher already set"))?;
        ctx.known_page
            .set(known_page)
            .map_err(|_| anyhow::anyhow!("known_page already set"))?;

        Ok(())
    }
}
