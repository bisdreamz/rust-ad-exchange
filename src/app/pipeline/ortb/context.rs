use parking_lot::RwLock;
use rtb::common::bidresponsestate::BidResponseState;
use rtb::BidRequest;
use std::sync::OnceLock;

pub struct AuctionContext {
    pub req: RwLock<BidRequest>,
    pub res: OnceLock<BidResponseState>
}

impl AuctionContext {
    pub fn new (req: BidRequest) -> AuctionContext {
        AuctionContext {
            req: RwLock::new(req),
            res: OnceLock::new(),
        }
    }
}