use crate::app::pipeline::ortb::context::{
    BidContext, BidResponseContext, BidderCallout, BidderContext, BidderResponse,
    BidderResponseState, DirectCampaignContext, SeatBidContext,
};
use crate::core::models::bidder::Bidder;
use crate::core::models::buyer::Buyer;
use crate::core::models::campaign::Campaign;
use crate::core::models::creative::Creative;
use crate::core::models::deal::Deal;
use rtb::BidResponse;
use rtb::bid_response::bid::AdmOneof;
use rtb::bid_response::{Bid, SeatBid};
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

/// Synthesizes a BidContext from a direct campaign win.
/// Sets the DirectCampaignContext so downstream tasks
/// can distinguish source and access campaign/creative details.
pub fn synthesize_bid(
    buyer: &Arc<Buyer>,
    campaign: &Arc<Campaign>,
    creative: &Arc<Creative>,
    deal: Option<Arc<Deal>>,
    price: f64,
    imp_id: &str,
) -> BidContext {
    let bid = Bid {
        id: Uuid::new_v4().to_string(),
        impid: imp_id.to_string(),
        price,
        adm_oneof: Some(AdmOneof::Adm(creative.content.clone())),
        crid: creative.id.clone(),
        ..Default::default()
    };

    let bid_ctx = BidContext::from(bid);

    let _ = bid_ctx.direct.set(DirectCampaignContext {
        buyer: Arc::clone(buyer),
        campaign: Arc::clone(campaign),
        creative: Arc::clone(creative),
    });

    if let Some(d) = deal {
        let _ = bid_ctx.deal.set(d);
    }

    bid_ctx
}

/// Wraps a single direct campaign BidContext into a full BidderContext
/// with a synthetic "direct" Bidder, so it flows through shared bid
/// tasks (margin, notice URLs, settlement) uniformly with RTB bids.
pub fn wrap_in_bidder_context(bid_ctx: BidContext) -> BidderContext {
    let buyer_id = bid_ctx
        .direct
        .get()
        .map(|d| d.buyer.id.as_str())
        .unwrap_or("unknown");

    let seat = format!("direct:{buyer_id}");

    let synthetic_bidder = Arc::new(Bidder {
        id: seat.clone(),
        name: "direct".to_string(),
        ..Default::default()
    });

    let seat_ctx = SeatBidContext {
        seat: SeatBid {
            seat,
            ..Default::default()
        },
        bids: vec![bid_ctx],
    };

    let response_ctx = BidResponseContext {
        response: BidResponse::default(),
        seatbids: vec![seat_ctx],
    };

    let callout = BidderCallout {
        response: std::sync::OnceLock::from(BidderResponse {
            state: BidderResponseState::Bid(response_ctx),
            latency: Duration::ZERO,
        }),
        ..Default::default()
    };

    BidderContext {
        bidder: synthetic_bidder,
        callouts: vec![callout],
    }
}
