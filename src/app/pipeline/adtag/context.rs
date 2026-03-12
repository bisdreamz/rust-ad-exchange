use crate::app::pipeline::adtag::request::AdTagRequest;
use crate::app::pipeline::adtag::response::AdTagResponse;
use crate::app::pipeline::ortb::AuctionContext;
use crate::app::pipeline::ortb::{HttpRequestContext, PublisherBlockReason};
use crate::core::models::placement::Placement;
use crate::core::models::property::Property;
use crate::core::models::publisher::Publisher;
use std::sync::{Arc, OnceLock};

/// Validated page URL with its extracted domain.
/// Only set when the JS-supplied page_url passed domain validation against the property config.
#[derive(Debug)]
pub struct KnownPage {
    pub page_url: String,
    pub extracted_domain: String,
}

#[derive(Debug)]
pub struct AdtagContext {
    pub request: AdTagRequest,
    pub http: HttpRequestContext,

    /// Resolved by ResolveEntitiesTask
    pub placement: OnceLock<Arc<Placement>>,
    pub property: OnceLock<Arc<Property>>,
    pub publisher: OnceLock<Arc<Publisher>>,

    /// Set by ValidatePageUrlTask — None if page_url absent or failed domain validation
    pub known_page: OnceLock<Option<KnownPage>>,

    /// Full auction context after BuildAuctionTask runs the ortb pipeline
    pub auction_ctx: OnceLock<AuctionContext>,

    /// Final serializable response, set by BuildResponseTask
    pub response: OnceLock<AdTagResponse>,

    /// Set before bailing — handler maps this to the appropriate HTTP status code
    pub block_reason: OnceLock<PublisherBlockReason>,
}

impl AdtagContext {
    pub fn new(request: AdTagRequest, http: HttpRequestContext) -> Self {
        Self {
            request,
            http,
            placement: OnceLock::new(),
            property: OnceLock::new(),
            publisher: OnceLock::new(),
            known_page: OnceLock::new(),
            auction_ctx: OnceLock::new(),
            response: OnceLock::new(),
            block_reason: OnceLock::new(),
        }
    }
}
