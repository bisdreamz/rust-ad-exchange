use crate::app::config::RexConfig;
use crate::app::pipeline::events::billing::context::BillingEventContext;
use crate::app::pipeline::ortb::AuctionContext;
use crate::app::pipeline::syncing::r#in::context::SyncInContext;
use crate::app::pipeline::syncing::out::context::SyncOutContext;
use crate::core::cluster::ClusterDiscovery;
use crate::core::demand::notifications::DemandNotificationsCache;
use crate::core::enrichment::device::DeviceLookup;
use crate::core::filters::bot::IpRiskFilter;
use crate::core::managers::{BidderManager, PublisherManager, ShaperManager};
use crate::core::observability::ObservabilityProviders;
use crate::core::usersync::SyncStore;
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
    /// Maintains list of publishers
    pub pub_manager: OnceLock<Arc<PublisherManager>>,
    /// Traffic shaping instances per endpoint
    pub shaping_manager: OnceLock<Arc<ShaperManager>>,
    /// Caches demand provided notification URLs like burl, lurl
    pub demand_url_cache: OnceLock<Arc<DemandNotificationsCache>>,
    /// The user sync store for partners which we host a match table
    pub sync_store: OnceLock<Arc<dyn SyncStore>>,
    /// Responsible for observing cluster sizing changes
    pub cluster_manager: OnceLock<Arc<dyn ClusterDiscovery>>,

    // Pipelines
    // TODO prefixing pipelines such as prebid which may then pass through rtb_pipeline
    /// The pipeline which defines the core of tasks a bidrequest will flow through for handling
    pub auction_pipeline: OnceLock<Arc<Pipeline<AuctionContext, Error>>>,
    /// The pipeline which handles billing event events, regardless of source (adm, burl..)
    pub event_pipeline: OnceLock<Arc<Pipeline<BillingEventContext, Error>>>,
    /// The pipeline which handles firing of our user sync pixel, which starts outbound demand sync
    pub sync_out_pipeline: OnceLock<Arc<Pipeline<SyncOutContext, Error>>>,
    /// The pipeline which accepts incoming partner syncs, where we receive & host partner buyeruid
    pub sync_in_pipeline: OnceLock<Arc<Pipeline<SyncInContext, Error>>>,
    /// The web server
    pub server: OnceLock<Server>,
}
