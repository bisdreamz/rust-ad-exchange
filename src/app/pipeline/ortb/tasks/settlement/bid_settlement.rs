use crate::app::pipeline::ortb::AuctionContext;
use crate::app::pipeline::ortb::context::{BidderContext, BidderResponseState};
use crate::core::spec::nobidreasons;
use anyhow::{Error, bail};
use async_trait::async_trait;
use pipeline::AsyncTask;
use rtb::bid_response::{Bid, SeatBid, SeatBidBuilder};
use rtb::common::bidresponsestate::BidResponseState;
use rtb::{BidResponseBuilder, child_span_info};
use tracing::{Instrument, debug, warn};

pub fn sort_bids_by_price(bids: &mut [Bid]) {
    bids.sort_by(|a, b| b.price.total_cmp(&a.price));
}

pub fn sort_seats_by_highest_bid(seats: &mut [SeatBid]) {
    seats.sort_by(|a, b| {
        let a_price = a.bid.first().map(|bid| bid.price).unwrap_or(0.0);
        let b_price = b.bid.first().map(|bid| bid.price).unwrap_or(0.0);
        b_price.total_cmp(&a_price)
    });
}

pub struct BidSettlementTask;

impl BidSettlementTask {
    fn build_bidder_seat_bids(&self, bidder_context: &BidderContext) -> Vec<Bid> {
        let mut seat_bids = Vec::with_capacity(bidder_context.callouts.len());

        for callout in bidder_context.callouts.iter() {
            let response_opt = &callout.response.get();

            if response_opt.is_none() {
                // warn!("Had callout without any response state assigned!");
                continue;
            }

            let response = response_opt.unwrap();

            let bid_response = match &response.state {
                BidderResponseState::Bid(bid_res) => bid_res,
                _ => continue,
            };

            for seat_context in &bid_response.seatbids {
                for bid_context in &seat_context.bids {
                    match &bid_context.filter_reason {
                        Some((_, reason)) => debug!("Skipping bid because: {}", reason),
                        None => seat_bids.push(bid_context.bid.clone()),
                    }
                }
            }
        }

        seat_bids
    }

    fn build_seats(&self, bidders: &Vec<BidderContext>) -> Vec<SeatBid> {
        let mut seats = Vec::with_capacity(bidders.len());

        for bidder in bidders.iter() {
            let mut seat_bids = self.build_bidder_seat_bids(bidder);

            sort_bids_by_price(&mut seat_bids);

            let bidder_seat_result = SeatBidBuilder::default()
                .seat(bidder.bidder.name.clone())
                .bid(seat_bids)
                .build();

            let bidder_seat = match bidder_seat_result {
                Ok(seat) => seat,
                Err(_) => {
                    warn!(
                        "Failed to build seatbid response for {}: skipping bids!",
                        bidder.bidder.name
                    );

                    continue;
                }
            };

            if !bidder_seat.bid.is_empty() {
                seats.push(bidder_seat);
            }
        }

        seats
    }

    async fn run0(&self, context: &AuctionContext) -> Result<(), Error> {
        let bidders = context.bidders.lock().await;

        let mut seats = self.build_seats(&bidders);

        if seats.is_empty() {
            let final_nobid_state = BidResponseState::NoBidReason {
                reqid: context.original_auction_id.clone(),
                nbr: nobidreasons::NO_CAMPAIGNS_FOUND,
                desc: Some("No bids received"),
            };

            if let Err(_) = context.res.set(final_nobid_state) {
                bail!("Built final no bid response but one already assigned?!");
            }

            debug!("Assigned no bid response to context");

            return Ok(());
        }

        sort_seats_by_highest_bid(&mut seats);

        let final_bid_response_result = BidResponseBuilder::default()
            .id(context.original_auction_id.clone())
            .seatbid(seats)
            .build();

        if let Err(e) = final_bid_response_result {
            warn!(
                "Failed to build final BidResponse, skipping all bids! {}",
                e
            );

            let brs = BidResponseState::NoBidReason {
                reqid: context.original_auction_id.clone(),
                nbr: rtb::spec::openrtb::nobidreason::TECHNICAL_ERROR,
                desc: Some("Failed to build final response"),
            };

            context.res.set(brs).expect("Shouldnt have brs");

            bail!(
                "Failed to build final bid response, skipping all bids! {}",
                e
            );
        }

        let final_bid_response_state = BidResponseState::Bid(final_bid_response_result?);

        if let Err(_) = context.res.set(final_bid_response_state) {
            bail!("Built final bid response but one already assigned?!");
        }

        debug!("Assigned valid bid response to context");

        Ok(())
    }
}

#[async_trait]
impl AsyncTask<AuctionContext, Error> for BidSettlementTask {
    async fn run(&self, context: &AuctionContext) -> Result<(), Error> {
        let span = child_span_info!("bid_settlement_task");

        self.run0(context).instrument(span).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rtb::bid_response::BidBuilder;

    #[test]
    fn test_sort_bids_by_price_descending() {
        let mut bids = vec![
            BidBuilder::default().price(1.5).build().unwrap(),
            BidBuilder::default().price(3.0).build().unwrap(),
            BidBuilder::default().price(2.0).build().unwrap(),
        ];

        sort_bids_by_price(&mut bids);

        assert_eq!(bids[0].price, 3.0);
        assert_eq!(bids[1].price, 2.0);
        assert_eq!(bids[2].price, 1.5);
    }

    #[test]
    fn test_sort_bids_by_price_empty() {
        let mut bids: Vec<Bid> = vec![];
        sort_bids_by_price(&mut bids);
        assert!(bids.is_empty());
    }

    #[test]
    fn test_sort_seats_by_highest_bid_descending() {
        let mut seats = vec![
            SeatBidBuilder::default()
                .seat("seat1".to_string())
                .bid(vec![BidBuilder::default().price(1.0).build().unwrap()])
                .build()
                .unwrap(),
            SeatBidBuilder::default()
                .seat("seat2".to_string())
                .bid(vec![BidBuilder::default().price(5.0).build().unwrap()])
                .build()
                .unwrap(),
            SeatBidBuilder::default()
                .seat("seat3".to_string())
                .bid(vec![BidBuilder::default().price(3.0).build().unwrap()])
                .build()
                .unwrap(),
        ];

        sort_seats_by_highest_bid(&mut seats);

        assert_eq!(seats[0].seat, "seat2");
        assert_eq!(seats[0].bid[0].price, 5.0);
        assert_eq!(seats[1].seat, "seat3");
        assert_eq!(seats[1].bid[0].price, 3.0);
        assert_eq!(seats[2].seat, "seat1");
        assert_eq!(seats[2].bid[0].price, 1.0);
    }

    #[test]
    fn test_sort_seats_empty_bids() {
        let mut seats = vec![
            SeatBidBuilder::default()
                .seat("seat1".to_string())
                .bid(vec![BidBuilder::default().price(2.0).build().unwrap()])
                .build()
                .unwrap(),
            SeatBidBuilder::default()
                .seat("seat_empty".to_string())
                .bid(vec![])
                .build()
                .unwrap(),
        ];

        sort_seats_by_highest_bid(&mut seats);

        assert_eq!(seats[0].seat, "seat1");
        assert_eq!(seats[1].seat, "seat_empty");
    }

    #[test]
    fn test_complete_bid_settlement_sorting() {
        let mut seats = vec![
            SeatBidBuilder::default()
                .seat("bidder_a".to_string())
                .bid(vec![
                    BidBuilder::default().price(2.0).build().unwrap(),
                    BidBuilder::default().price(5.0).build().unwrap(),
                    BidBuilder::default().price(1.0).build().unwrap(),
                ])
                .build()
                .unwrap(),
            SeatBidBuilder::default()
                .seat("bidder_b".to_string())
                .bid(vec![
                    BidBuilder::default().price(3.0).build().unwrap(),
                    BidBuilder::default().price(7.0).build().unwrap(),
                ])
                .build()
                .unwrap(),
            SeatBidBuilder::default()
                .seat("bidder_c".to_string())
                .bid(vec![
                    BidBuilder::default().price(4.0).build().unwrap(),
                    BidBuilder::default().price(6.0).build().unwrap(),
                    BidBuilder::default().price(2.5).build().unwrap(),
                ])
                .build()
                .unwrap(),
        ];

        // Sort bids within each seat
        for seat in &mut seats {
            let mut bids = seat.bid.clone();
            sort_bids_by_price(&mut bids);
            seat.bid = bids;
        }

        // Sort seats by highest bid
        sort_seats_by_highest_bid(&mut seats);

        // Verify seats are ordered by highest bid: bidder_b(7.0) > bidder_c(6.0) > bidder_a(5.0)
        assert_eq!(seats[0].seat, "bidder_b");
        assert_eq!(seats[0].bid[0].price, 7.0);
        assert_eq!(seats[0].bid[1].price, 3.0);

        assert_eq!(seats[1].seat, "bidder_c");
        assert_eq!(seats[1].bid[0].price, 6.0);
        assert_eq!(seats[1].bid[1].price, 4.0);
        assert_eq!(seats[1].bid[2].price, 2.5);

        assert_eq!(seats[2].seat, "bidder_a");
        assert_eq!(seats[2].bid[0].price, 5.0);
        assert_eq!(seats[2].bid[1].price, 2.0);
        assert_eq!(seats[2].bid[2].price, 1.0);
    }
}
