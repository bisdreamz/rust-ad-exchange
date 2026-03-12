use crate::app::config::RexConfig;
use crate::app::pipeline::adtag::AdtagContext;
use crate::app::pipeline::creatives::raw::RawCreativeContext;
use crate::app::pipeline::events::billing::context::BillingEventContext;
use crate::app::pipeline::ortb::AuctionContext;
use crate::app::pipeline::ortb::direct::pacing::{
    DealImpressionTracker, DealPacer, SpendPacer, SpendTracker,
};
use crate::app::pipeline::syncing::r#in::context::SyncInContext;
use crate::app::pipeline::syncing::out::context::SyncOutContext;
use crate::core::cluster::ClusterDiscovery;
use crate::core::demand::notifications::DemandNotificationsCache;
use crate::core::enrichment::device::DeviceLookup;
use crate::core::filters::bot::IpRiskFilter;
use crate::core::firestore::counters::campaign::CampaignCounterStore;
use crate::core::firestore::counters::deal::DealCounterStore;
use crate::core::firestore::counters::demand::DemandCounterStore;
use crate::core::firestore::counters::publisher::PublisherCounterStore;
use crate::core::managers::{
    AdvertiserManager, BuyerManager, CampaignManager, CreativeManager, DealManager, DemandManager,
    PlacementManager, PropertyManager, PublisherManager, ShaperManager,
};
use crate::core::observability::ObservabilityProviders;
use crate::core::usersync::SyncStore;
use anyhow::Error;
use firestore::FirestoreDb;
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
    pub bidder_manager: OnceLock<Arc<DemandManager>>,
    /// Maintains list of publishers
    pub pub_manager: OnceLock<Arc<PublisherManager>>,
    /// Maintains list of buyer profiles (direct campaign companies)
    pub buyer_manager: OnceLock<Arc<BuyerManager>>,
    /// Maintains list of advertisers (brands) owned by buyers
    pub advertiser_manager: OnceLock<Arc<AdvertiserManager>>,
    /// Maintains list of direct campaigns
    pub campaign_manager: OnceLock<Arc<CampaignManager>>,
    /// Maintains list of creatives for direct campaigns
    pub creative_manager: OnceLock<Arc<CreativeManager>>,
    /// Maintains list of deals (direct + RTB)
    pub deal_manager: OnceLock<Arc<DealManager>>,
    /// Traffic shaping instances per endpoint
    pub shaping_manager: OnceLock<Arc<ShaperManager>>,
    /// Caches demand provided notification URLs like burl, lurl
    pub demand_url_cache: OnceLock<Arc<DemandNotificationsCache>>,
    /// The user sync store for partners which we host a match table
    pub sync_store: OnceLock<Arc<dyn SyncStore>>,
    /// Responsible for observing cluster sizing changes
    pub cluster_manager: OnceLock<Arc<dyn ClusterDiscovery>>,
    /// Optional Firestore client, if configured. oncelock should
    /// always be set to catch accidential pipeline configurations
    /// leading to inactive database tasks
    pub firestore: OnceLock<Option<Arc<FirestoreDb>>>,
    /// Optional pub specific activity counters to persist to firestore
    pub counters_pub_store: OnceLock<Option<Arc<PublisherCounterStore>>>,
    /// Optional demand specific activity counters to persist to firestore
    pub counters_demand_store: OnceLock<Option<Arc<DemandCounterStore>>>,
    /// Optional campaign (direct) activity counters to persist to firestore
    pub counters_campaign_store: OnceLock<Option<Arc<CampaignCounterStore>>>,
    /// Optional deal impression counters to persist to firestore
    pub counters_deal_store: OnceLock<Option<Arc<DealCounterStore>>>,
    /// Campaign spend tracker for pacing decisions (Firestore or in-memory)
    pub spend_tracker: OnceLock<Arc<dyn SpendTracker>>,
    /// Deal impression tracker for pacing decisions (in-memory for now)
    pub deal_tracker: OnceLock<Arc<dyn DealImpressionTracker>>,
    /// Deal delivery pacer — wraps deal_tracker with windowed rate limiting
    pub deal_pacer: OnceLock<Arc<dyn DealPacer>>,
    /// Unified campaign spend pacer — reads campaign.pacing per call
    pub spend_pacer: OnceLock<Arc<dyn SpendPacer>>,

    /// Maintains updated list of publisher ad placements
    pub placement_manager: OnceLock<Arc<PlacementManager>>,
    /// Maintains updated list of publisher properties (sites/apps)
    pub property_manager: OnceLock<Arc<PropertyManager>>,

    // Pipelines
    // TODO prefixing pipelines such as prebid which may then pass through rtb_pipeline
    /// The pipeline which defines the core of tasks a bidrequest will flow through for handling
    pub auction_pipeline: OnceLock<Arc<Pipeline<AuctionContext, Error>>>,
    /// The pipeline which handles inbound ad tag requests — placement resolution, auction, response
    pub adtag_pipeline: OnceLock<Arc<Pipeline<AdtagContext, Error>>>,
    /// The pipeline which handles billing event events, regardless of source (adm, burl..)
    pub event_pipeline: OnceLock<Arc<Pipeline<BillingEventContext, Error>>>,
    /// The pipeline which handles firing of our user sync pixel, which starts outbound demand sync
    pub sync_out_pipeline: OnceLock<Arc<Pipeline<SyncOutContext, Error>>>,
    /// The pipeline which accepts incoming partner syncs, where we receive & host partner buyeruid
    pub sync_in_pipeline: OnceLock<Arc<Pipeline<SyncInContext, Error>>>,
    /// Protocol-relative CDN base URL for resolving ${CDN_DOMAIN} macros in creative content,
    /// e.g. "//ads.example.com".
    pub cdn_base: OnceLock<String>,
    /// Pipeline for serving raw creative content (HTML, VAST XML)
    pub raw_creative_pipeline: OnceLock<Arc<Pipeline<RawCreativeContext, Error>>>,
    /// The web server
    pub server: OnceLock<Server>,
}
