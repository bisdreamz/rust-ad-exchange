use log::warn;
use rtb::bid_request::Imp;
use rtb::bid_response::{Bid, SeatBid};
use rtb::{BidRequest, BidResponse};
use std::time::{SystemTime, UNIX_EPOCH};

/// Fill majority of macros we can during pre-delivery of a bid for potential win
/// Includes auction price and mbr, expected all auctions are 1st price
#[allow(dead_code)]
pub fn fill_predelivery_macros(
    text: String,
    req: &BidRequest,
    res: &BidResponse,
    seat: &SeatBid,
    bid: &Bid,
    imp: &Imp,
) -> String {
    let mut text = text;

    let multiplier = match &imp.qty {
        Some(qty) => {
            if qty.multiplier == 0.0 {
                ""
            } else {
                &*qty.multiplier.to_string()
            }
        }
        None => "",
    };

    text = text.replace(
        rtb::openrtb::spec::auction_macros::AUCTION_ID,
        req.id.as_str(),
    );
    text = text.replace(
        rtb::openrtb::spec::auction_macros::AUCTION_AD_ID,
        bid.id.as_str(),
    );
    text = text.replace(
        rtb::openrtb::spec::auction_macros::AUCTION_BID_ID,
        bid.id.as_str(),
    );
    text = text.replace(
        rtb::openrtb::spec::auction_macros::AUCTION_CURRENCY,
        res.cur.as_str(),
    );
    text = text.replace(
        rtb::openrtb::spec::auction_macros::AUCTION_IMP_ID,
        bid.impid.as_str(),
    );
    text = text.replace(
        rtb::openrtb::spec::auction_macros::AUCTION_MULTIPLIER,
        multiplier,
    );
    text = text.replace(rtb::openrtb::spec::auction_macros::AUCTION_MBR, "1".into()); // 1st price
    text = text.replace(
        rtb::openrtb::spec::auction_macros::AUCTION_PRICE,
        bid.price.to_string().as_str(),
    );
    text = text.replace(
        rtb::openrtb::spec::auction_macros::AUCTION_SEAT_ID,
        seat.seat.as_str(),
    );

    text
}

/// This fills any macros that cannot be completed until we have post impression knowledge
/// At time of writing, only the ['AUCTION_IMP_TS'] macro exists here
#[allow(dead_code)]
pub fn fill_delivery_macros(text: String) -> String {
    let mut text = text;

    let epoch = match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(n) => n.as_millis(),
        Err(e) => {
            warn!("Failed to calculate impression timestamp: {}", e);
            return text;
        }
    };

    text = text.replace(
        rtb::openrtb::spec::auction_macros::AUCTION_IMP_TS,
        epoch.to_string().as_str(),
    );

    text
}

/// This fills any macros that only pertain to loss urls
/// At time of writing, only the ['AUCTION_LOSS'] and ['AUCTION_MIN_TO_WIN'] macro exists here
#[allow(dead_code)]
pub fn fill_loss_macros(text: String, min_price_to_win: f32, loss_code: u32) -> String {
    let mut text = text;

    text = text.replace(
        rtb::openrtb::spec::auction_macros::AUCTION_LOSS,
        loss_code.to_string().as_str(),
    );
    text = text.replace(
        rtb::openrtb::spec::auction_macros::AUCTION_MIN_TO_WIN,
        min_price_to_win.to_string().as_str(),
    );

    text
}
