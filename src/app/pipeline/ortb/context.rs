use crate::core::models::bidder::{Bidder, Endpoint};
use crate::core::models::publisher::Publisher;
use derive_builder::Builder;
use parking_lot::RwLock;
use rtb::bid_response::{Bid, SeatBid};
use rtb::common::bidresponsestate::BidResponseState;
use rtb::{BidRequest, BidResponse};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use strum::{AsRefStr, EnumString};
use uuid::Uuid;

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
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
}

impl BidContext {
    pub fn from(bid: Bid) -> Self {
        BidContext {
            bid_event_id: Uuid::new_v4().to_string(),
            original_bid_price: bid.price,
            reduced_bid_price: None,
            filter_reason: None,
            bid,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct SeatBidContext {
    pub seat: SeatBid,
    pub bids: Vec<BidContext>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
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

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, EnumString, AsRefStr)]
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

#[derive(Debug, Clone, Serialize, Deserialize, Default, Builder)]
pub struct BidderResponse {
    pub state: BidderResponseState,
    pub latency: Duration,
}

#[derive(Debug, Default)]
pub struct BidderCallout {
    pub endpoint: Arc<Endpoint>,
    pub req: BidRequest,
    pub response: OnceLock<BidderResponse>,
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
    /// The ['Publisher'] object if the pubid value has been recognized
    pub publisher: OnceLock<Arc<Publisher>>,
    /// Locally assigned but globally unique identifier for this auction
    pub event_id: String,
    /// Tag to describe the inbound source of this request, e.g. rtb, rtb_protobuf, prebid, etc
    pub source: String,
    pub req: RwLock<BidRequest>,
    pub res: OnceLock<BidResponseState>,
    pub bidders: tokio::sync::Mutex<Vec<BidderContext>>,
}

impl AuctionContext {
    pub fn new(source: String, pubid: String, req: BidRequest) -> AuctionContext {
        AuctionContext {
            pubid,
            publisher: OnceLock::new(),
            event_id: Uuid::new_v4().to_string(),
            source,
            req: RwLock::new(req),
            res: OnceLock::new(),
            bidders: tokio::sync::Mutex::new(Vec::new()),
        }
    }
}
