use crate::core::enrichment::device::DeviceInfo;
use crate::core::models::bidder::{Bidder, Endpoint};
use crate::core::models::buyer::Buyer;
use crate::core::models::campaign::Campaign;
use crate::core::models::creative::Creative;
use crate::core::models::deal::Deal;
use crate::core::models::placement::Placement;
use crate::core::models::publisher::Publisher;
use crate::core::shaping::tree::TreeShaper;
use derivative::Derivative;
use derive_builder::Builder;
use parking_lot::RwLock;
use rtb::bid_response::{Bid, SeatBid};
use rtb::common::DataUrl;
use rtb::common::bidresponsestate::BidResponseState;
use rtb::{BidRequest, BidResponse};
use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use strum::{AsRefStr, Display, EnumString};
use uuid::Uuid;

/// Write-once, type-keyed extension store. Behaves like a OnceLock per type —
/// once a type T has been set it is permanent and WILL NEVER be overwritten.
/// Calling `set` twice for the same type is a no-op that returns false.
/// All stored values must be Send + Sync.
#[allow(dead_code)]
#[derive(Default)]
pub struct Extensions {
    map: HashMap<TypeId, Box<dyn Any + Send + Sync>>,
}

impl std::fmt::Debug for Extensions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Extensions")
            .field("len", &self.map.len())
            .finish()
    }
}

#[allow(dead_code)]
impl Extensions {
    /// Stores val under type T. Returns false and drops val if T is already set.
    /// WILL NEVER overwrite an existing value — treat this exactly as OnceLock::set.
    pub fn set<T: Send + Sync + 'static>(&mut self, val: T) -> bool {
        if self.map.contains_key(&TypeId::of::<T>()) {
            return false;
        }
        self.map.insert(TypeId::of::<T>(), Box::new(val));
        true
    }

    /// Returns a shared reference to the stored T, or None if not yet set.
    pub fn get<T: 'static>(&self) -> Option<&T> {
        self.map
            .get(&TypeId::of::<T>())
            .and_then(|b| b.downcast_ref())
    }

    /// Returns a mutable reference into the stored T, or None if not yet set.
    /// Allows mutating the value's fields — does NOT allow replacing the value.
    pub fn get_mut<T: 'static>(&mut self) -> Option<&mut T> {
        self.map
            .get_mut(&TypeId::of::<T>())
            .and_then(|b| b.downcast_mut())
    }
}

/// Present on a [`BidContext`] when the bid originated from a
/// direct campaign rather than an RTB demand callout. Carries
/// the full objects so downstream logic (counters, billing,
/// settlement) can attribute and render without extra lookups.
/// Deal context (if any) lives on `BidContext.deal` for uniform
/// access across both direct and RTB bid paths.
#[derive(Debug, Clone)]
pub struct DirectCampaignContext {
    pub buyer: Arc<Buyer>,
    pub campaign: Arc<Campaign>,
    pub creative: Arc<Creative>,
}

#[derive(Debug, Clone, Default)]
pub struct BidContext {
    pub bid_event_id: String,
    pub bid: Bid,
    /// original gross demand bid price
    pub original_bid_price: f64,
    /// if this bid should be filtered for some reason e.g. blocked crid or below floor
    /// the associated loss code and description will be set. this indicates the bid is invalid!
    pub filter_reason: Option<(u32, String)>,
    /// reduced bid price after margin. None if not yet applied.
    pub reduced_bid_price: Option<f64>,
    /// Structured notification URLs which tasks may optionally attach metadata to,
    ///and retrieve later post adm pixel/burl/etc firing
    pub notifications: NoticeUrls,
    /// The deal this bid was matched through, if any.
    /// Set for both direct and RTB bids.
    pub deal: OnceLock<Arc<Deal>>,
    /// Set when this bid was synthesized from a direct campaign
    /// rather than received from an RTB callout. All downstream
    /// logic iterates bids uniformly — check this to distinguish
    /// source or access campaign/creative details
    pub direct: OnceLock<DirectCampaignContext>,
}

impl BidContext {
    pub fn from(bid: Bid) -> Self {
        BidContext {
            bid_event_id: Uuid::new_v4().to_string(),
            original_bid_price: bid.price,
            reduced_bid_price: None,
            filter_reason: None,
            bid,
            ..Default::default()
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct SeatBidContext {
    pub seat: SeatBid,
    pub bids: Vec<BidContext>,
}

#[derive(Debug, Clone, Default)]
pub struct BidResponseContext {
    /// The original response as it arrived from the bidder, less the seatbids and bids
    /// which ownership has been moved into their corresponding context objects
    pub response: BidResponse,
    pub seatbids: Vec<SeatBidContext>,
}

impl BidResponseContext {
    /// Builds a ['SeatBidContext'] which removes ownership of the bids into their own
    /// associated ['BidContext'] entries (leaves the direct seatbid.bid empty)
    pub fn from(mut response: BidResponse) -> Self {
        let mut seat_bid_contexts = Vec::with_capacity(response.seatbid.len());

        let response_seats = std::mem::take(&mut response.seatbid);

        for mut seat in response_seats {
            let seat_bids = std::mem::take(&mut seat.bid);
            let bid_contexts: Vec<_> = seat_bids.into_iter().map(|b| BidContext::from(b)).collect();

            seat_bid_contexts.push(SeatBidContext {
                seat,
                bids: bid_contexts,
            })
        }

        BidResponseContext {
            response,
            seatbids: seat_bid_contexts,
        }
    }
}

#[derive(Debug, Clone, Default, EnumString, AsRefStr)]
#[allow(unused)]
pub enum BidderResponseState {
    /// Bidder failed to respond within time
    #[default]
    Timeout,
    /// Error experienced sending request such as broken url, bad dns
    Error(String),
    /// Unknown or unexpected http status response (status code, reason message)
    Unknown(u32, String),
    /// No bid received as defined by http 204 of 200 with empty seatbid.
    /// Nbr present if provided in response.
    NoBid(Option<u32>),
    /// Valid bid received as defined by an http 200 with non empty seatbid
    Bid(BidResponseContext),
}

#[derive(Debug, Clone, Default, Builder)]
pub struct BidderResponse {
    pub state: BidderResponseState,
    pub latency: Duration,
}

#[derive(Debug, Clone, PartialEq, EnumString, AsRefStr, Display)]
pub enum CalloutSkipReason {
    TrafficShaping,
    QpsLimit,
    EndpointRotation,
}

/// The ['DataUrl'] notification events are sent to,
/// which enables tasks to attach and retrieve additional
/// meta data as needed
#[derive(Debug, Clone, Default)]
pub struct NoticeUrls {
    /// The url invoked upon billing event, which may be from the burl or adm pixel
    /// depending on publisher configuration
    pub billing: OnceLock<DataUrl>,
    // loss, nurl, etc when needed
}

#[derive(Derivative)]
#[derivative(Debug, Default)]
pub struct BidderCallout {
    /// The reason this bid request to the associated endpoint should be skipped, see
    /// ['CalloutSkipReason']
    pub skip_reason: OnceLock<CalloutSkipReason>,
    /// Bidder ['Endpoint'] this callout should be sent to
    pub endpoint: Arc<Endpoint>,
    /// The owned ['BidRequest'] to be sent during this callout, which can safely
    /// have any required partner specific adaptations made
    pub req: BidRequest,
    /// The ['BidderResponse'] if any after auction callout
    pub response: OnceLock<BidderResponse>,
    #[derivative(Debug = "ignore", Default(value = "OnceLock::new()"))]
    pub shaping: OnceLock<Arc<TreeShaper>>,
}

/// Bidder context
///
/// # Arguments
/// * `bidder` - Cloned bidder containing all top level bidder settings, as well
/// as the list of bidder endpoints. The bidder endpoints are intended to be
/// filtered and used during hosting an auction. Only one endpoint should
/// receive the associated requests.
/// * `req` - Cloned bidrequest(s) which is where bidder specific adapted
/// request are stored and safe to mutate, e.g. are margins or tagid changes.
/// Can be multiple requests in case of behavior such as imp request breakout.
#[derive(Debug, Default)]
pub struct BidderContext {
    pub bidder: Arc<Bidder>,
    pub callouts: Vec<BidderCallout>,
}

#[derive(Debug, Default)]
pub struct IdentityContext {
    /// Our web cookie id for this user, which has been extracted
    /// from cookie or provided to us in buyeruid from seller. If
    /// this is present, req.user.id will now be set to this val
    pub local_uid: OnceLock<String>,
    // later, ramp ids or.. whatever
}

#[derive(Debug, Clone, PartialEq, EnumString, AsRefStr, Display)]
pub enum PublisherBlockReason {
    UnknownSeller,
    DisabledSeller,
    IpInvalid,
    IpDatacenter,
    DeviceUnknown,
    DeviceBot,
    TooManyHops,
    BidsProcessingError,
    MissingAuctionId,
    MissingDevice,
    MissingDeviceDetails,
    MissingAppSite,
    MissingAppSiteDomain,
    TmaxTooLow,
}

/// IP, UA, client hints, referer, and cookies from the inbound HTTP request.
#[allow(dead_code)]
#[derive(Debug, Default)]
pub struct HttpRequestContext {
    /// Real client IP resolved from CF-Connecting-IP → X-Forwarded-For → socket
    pub ip: Option<IpAddr>,
    pub user_agent: Option<String>,
    pub sec_ch_ua: Option<String>,
    pub sec_ch_ua_mobile: Option<bool>,
    pub sec_ch_ua_platform: Option<String>,
    pub referer: Option<String>,
    pub cookies: HashMap<String, String>,
}

/// Top level auction context object which carries all context required
/// to fullfill a request pipeline
///
/// # Arguments
/// * `req` - The inbound [`BidRequest`] which is intended to use interior mutability
/// for required adaptations, for example during supplementing device data
/// * `res` - The final outbound [`BidResponseState`] representing the final
/// outcome result of processing. **This is FINAL** — once set, it IS the
/// response and prevents all subsequent tasks from overriding it.
/// Only enrichment/validation tasks (blocked publisher, bad request, etc.)
/// should set this to abort processing early. RTB demand tasks must NOT
/// set this; they use `rtb_nbr` instead.
/// * `bidders` - The list of [`BidderContext`] assigned by the bidder matching stage,
/// and optionally further modified by other stages, which contains the list of bidders
/// and bidder specific adapted request objects
#[derive(Debug)]
pub struct AuctionContext {
    /// The original auction ID sent from pub, which
    /// we need to set in our response
    pub original_auction_id: String,
    /// HTTP request context — IP, UA, client hints, cookies
    pub http: HttpRequestContext,
    pub device: OnceLock<DeviceInfo>,
    pub identity: OnceLock<IdentityContext>,
    /// The resolved ['Publisher'] for this request
    pub publisher: Arc<Publisher>,
    /// Resolved placement for this request. Set by upstream handlers that already
    /// performed the lookup (jstag, prebid, etc.) so pipeline tasks don't repeat it.
    pub placement: Option<Arc<Placement>>,
    /// Locally assigned but globally unique identifier for this auction
    pub event_id: String,
    /// Tag to describe the inbound source of this request, e.g. rtb, rtb_protobuf, prebid, etc
    pub source: String,
    pub req: RwLock<BidRequest>,
    /// The final outbound response. **This is FINAL** — once set, it IS the
    /// response and prevents all subsequent tasks from overriding it.
    /// Only enrichment/validation tasks (blocked publisher, bad request, etc.)
    /// should set this to abort processing early. RTB demand tasks must NOT
    /// set this; they use `rtb_nbr` instead.
    pub res: OnceLock<BidResponseState>,
    pub bidders: tokio::sync::Mutex<Vec<BidderContext>>,
    /// Staging area for direct campaign bid results.
    /// Populated by DirectCampaignMatchingTask, then moved into
    /// `bidders` by the merge task before shared bid processing.
    /// After merge, this is empty.
    pub direct_bid_staging: tokio::sync::Mutex<Vec<BidContext>>,
    /// RTB-specific no-bid reason, set by RTB tasks when no viable demand exists.
    /// If settlement finds zero bids from all sources, it uses this NBR code
    /// for the specific reason. If direct campaign bids exist, this is ignored.
    pub rtb_nbr: OnceLock<(u32, &'static str)>,
    /// Reason we may have blocked this publisher request
    /// from reaching auction, so we may persist these
    /// stats as individually reportable
    pub block_reason: OnceLock<PublisherBlockReason>,
    /// Write-once extension store — for attaching pipeline-extension data without
    /// modifying this struct. See [`Extensions`].
    #[allow(dead_code)]
    pub ext: Extensions,
}

impl AuctionContext {
    pub fn new(
        original_id: String,
        source: String,
        publisher: Arc<Publisher>,
        placement: Option<Arc<Placement>>,
        req: BidRequest,
        http: HttpRequestContext,
    ) -> AuctionContext {
        AuctionContext {
            original_auction_id: original_id,
            http,
            device: OnceLock::new(),
            identity: OnceLock::new(),
            publisher,
            placement,
            event_id: Uuid::new_v4().to_string(),
            source,
            req: RwLock::new(req),
            res: OnceLock::new(),
            bidders: tokio::sync::Mutex::new(Vec::new()),
            direct_bid_staging: tokio::sync::Mutex::new(Vec::new()),
            rtb_nbr: OnceLock::new(),
            block_reason: OnceLock::new(),
            ext: Extensions::default(),
        }
    }

    #[cfg(test)]
    pub fn test_default(pubid: &str) -> AuctionContext {
        let publisher = Arc::new(Publisher {
            id: pubid.to_string(),
            enabled: true,
            name: format!("test_{pubid}"),
            ..Default::default()
        });
        AuctionContext::new(
            String::new(),
            String::new(),
            publisher,
            None,
            BidRequest::default(),
            HttpRequestContext::default(),
        )
    }
}
