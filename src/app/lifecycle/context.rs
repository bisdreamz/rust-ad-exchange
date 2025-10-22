use crate::app::config::RexConfig;
use crate::app::pipeline::ortb::AuctionContext;
use crate::core::enrichment::device::DeviceLookup;
use crate::core::filters::bot::IpRiskFilter;
use crate::core::managers::bidders::BidderManager;
use crate::core::observability::ObservabilityProviders;
use anyhow::Error;
use pipeline::Pipeline;
use rtb::server::Server;
use std::sync::{Arc, Mutex, OnceLock};

#[derive(Default)]
pub struct StartupContext {
    /// Local config options
    pub config: OnceLock<RexConfig>,
    /// Observability providers so they can be properly flushed at shutdown
    pub observability: OnceLock<ObservabilityProviders>,

    // Transient items that are assigned but taken ownership of later
    /// Ip bot filter, pipeline takes ownership of it later
    pub ip_risk_filter: Mutex<Option<IpRiskFilter>>,
    /// User agent lookup provider
    pub device_lookup: Mutex<Option<DeviceLookup>>,

    // Shared things and data providers
    /// Maintains updated list of bidders and endpoints
    pub bidder_manager: OnceLock<Arc<BidderManager>>,

    // Pipelines
    // TODO prefixing pipelines such as prebid which may then pass through rtb_pipeline
    /// The pipeline which defines the core of tasks a bidrequest will flow through for handling
    pub auction_pipeline: OnceLock<Arc<Pipeline<AuctionContext, Error>>>,

    /// The web server
    pub server: OnceLock<Server>,
}
