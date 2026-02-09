use crate::core::models::bidder::{Bidder, Endpoint};
use crate::core::models::publisher::Publisher;
use crate::core::shaping::tree::TreeShaper;
use derivative::Derivative;
use derive_builder::Builder;
use parking_lot::RwLock;
use rtb::bid_response::{Bid, SeatBid};
use rtb::common::DataUrl;
use rtb::common::bidresponsestate::BidResponseState;
use rtb::{BidRequest, BidResponse};
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use strum::{AsRefStr, Display, EnumString};
use uuid::Uuid;

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

/// Top level auction context object which carries all context required
/// to fullfill a request pipeline
///
/// # Arguments
/// * `req` - The inbound [`BidRequest`] which is intended to use interior mutability
/// for required adaptations, for example during supplementing device data
/// * `res` - The final outbound [`BidResponseState`] representing the final
/// outcome result of processing
/// * `bidders` - The list of [`BidderContext`] assigned by the bidder matching stage,
/// and optionally further modified by other stages, which contains the list of bidders
/// and bidder specific adapted request objects
#[derive(Debug, Default)]
pub struct AuctionContext {
    /// Raw pubid provided from the http path, pre-validation
    pub pubid: String,
    /// The original auction ID sent from pub, which
    /// we need to set in our response
    pub original_auction_id: String,
    /// Cookies present on inbound request, if any
    pub cookies: Option<HashMap<String, String>>,
    pub identity: OnceLock<IdentityContext>,
    /// The ['Publisher'] object if the pubid value has been recognized
    pub publisher: OnceLock<Arc<Publisher>>,
    /// Locally assigned but globally unique identifier for this auction
    pub event_id: String,
    /// Tag to describe the inbound source of this request, e.g. rtb, rtb_protobuf, prebid, etc
    pub source: String,
    pub req: RwLock<BidRequest>,
    pub res: OnceLock<BidResponseState>,
    pub bidders: tokio::sync::Mutex<Vec<BidderContext>>,
    /// Reason we may have blocked this publisher request
    /// from reaching auction, so we may persist these
    /// stats as individually reportable
    pub block_reason: OnceLock<PublisherBlockReason>,
}

impl AuctionContext {
    pub fn new(
        original_id: String,
        source: String,
        pubid: String,
        req: BidRequest,
        cookies: Option<HashMap<String, String>>,
    ) -> AuctionContext {
        AuctionContext {
            original_auction_id: original_id,
            pubid,
            cookies,
            event_id: Uuid::new_v4().to_string(),
            source,
            req: RwLock::new(req),
            bidders: tokio::sync::Mutex::new(Vec::new()),
            ..Default::default()
        }
    }
}
