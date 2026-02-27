use crate::app::pipeline::ortb::context::{
    BidContext, BidResponseContext, BidderCallout, BidderContext, BidderResponse,
    BidderResponseState, DirectCampaignContext, SeatBidContext,
};
use crate::core::models::bidder::Bidder;
use crate::core::models::campaign::Campaign;
use crate::core::models::creative::Creative;
use crate::core::models::deal::Deal;
use rtb::bid_response::bid::AdmOneof;
use rtb::bid_response::{Bid, SeatBid};
use std::sync::Arc;
use uuid::Uuid;

/// Synthesizes a BidContext from a direct campaign win.
/// Sets the DirectCampaignContext so downstream tasks
/// can distinguish source and access campaign/creative details.
pub fn synthesize_bid(
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
        campaign: Arc::clone(campaign),
        creative: Arc::clone(creative),
    });

    if let Some(d) = deal {
        let _ = bid_ctx.deal.set(d);
    }

    bid_ctx
}

/// Wraps a direct bid into the BidderContext structure expected
/// by ctx.bidders. Uses a synthetic "direct" bidder with a
/// pre-populated response so BidderCalloutsTask skips it.
pub fn wrap_in_bidder_context(bid_ctx: BidContext) -> BidderContext {
    let seatbid_ctx = SeatBidContext {
        seat: SeatBid {
            seat: "direct".to_string(),
            ..Default::default()
        },
        bids: vec![bid_ctx],
    };

    let response_ctx = BidResponseContext {
        response: Default::default(),
        seatbids: vec![seatbid_ctx],
    };

    let response = BidderResponse {
        state: BidderResponseState::Bid(response_ctx),
        ..Default::default()
    };

    let callout = BidderCallout {
        endpoint: Arc::new(Default::default()),
        req: Default::default(),
        ..Default::default()
    };
    let _ = callout.response.set(response);

    BidderContext {
        bidder: Arc::new(Bidder {
            id: "direct".to_string(),
            ..Default::default()
        }),
        callouts: vec![callout],
    }
}
