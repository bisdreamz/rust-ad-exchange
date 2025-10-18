use crate::core::models::bidder::Bidder;
use parking_lot::{Mutex, RwLock};
use rtb::common::bidresponsestate::BidResponseState;
use rtb::BidRequest;
use std::sync::OnceLock;

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
pub struct BidderContext {
    pub bidder: Bidder,
    pub reqs: Mutex<Vec<BidRequest>>
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
pub struct AuctionContext {
    pub req: RwLock<BidRequest>,
    pub res: OnceLock<BidResponseState>,
    pub bidders: Mutex<Vec<BidderContext>>,
}

impl AuctionContext {
    pub fn new (req: BidRequest) -> AuctionContext {
        AuctionContext {
            req: RwLock::new(req),
            res: OnceLock::new(),
            bidders: Mutex::new(Vec::new()),
        }
    }
}