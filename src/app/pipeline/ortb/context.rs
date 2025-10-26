use crate::core::models::bidder::{Bidder, Endpoint};
use derive_builder::Builder;
use parking_lot::RwLock;
use rtb::common::bidresponsestate::BidResponseState;
use rtb::{BidRequest, BidResponse};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
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
    Bid(BidResponse),
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
    /// Tag to describe the inbound source of this request, e.g. rtb, rtb_protobuf, prebid, etc
    pub source: String,
    pub pubid: String,
    pub req: RwLock<BidRequest>,
    pub res: OnceLock<BidResponseState>,
    pub bidders: tokio::sync::Mutex<Vec<BidderContext>>,
}

impl AuctionContext {
    pub fn new(source: String, pubid: String, req: BidRequest) -> AuctionContext {
        AuctionContext {
            pubid,
            source,
            req: RwLock::new(req),
            res: OnceLock::new(),
            bidders: tokio::sync::Mutex::new(Vec::new()),
        }
    }
}
