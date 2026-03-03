use crate::app::pipeline::ortb::AuctionContext;
use crate::app::pipeline::ortb::context::{BidderContext, BidderResponseState};
use crate::core::models::placement::FillPolicy;
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
    /// Returns Vec<(Bid, takes_priority)> for a single bidder context.
    fn build_bidder_seat_bids(
        &self,
        bidder_context: &BidderContext,
        fill_policy: &FillPolicy,
    ) -> Vec<(Bid, bool)> {
        let mut seat_bids = Vec::with_capacity(bidder_context.callouts.len());

        for callout in bidder_context.callouts.iter() {
            let response_opt = &callout.response.get();

            if response_opt.is_none() {
                continue;
            }

            let response = response_opt.unwrap();

            let bid_response = match &response.state {
                BidderResponseState::Bid(bid_res) => bid_res,
                _ => continue,
            };

            for seat_context in &bid_response.seatbids {
                for bid_context in &seat_context.bids {
                    if let Some((_, reason)) = &bid_context.filter_reason {
                        debug!("Skipping bid because: {}", reason);
                        continue;
                    }

                    // Under DirectAndRtbDeals, filter out pure open-auction RTB bids —
                    // only bids with a deal or direct campaign pass
                    if matches!(fill_policy, FillPolicy::DirectAndRtbDeals)
                        && bid_context.deal.get().is_none()
                        && bid_context.direct.get().is_none()
                    {
                        continue;
                    }

                    let takes_priority = bid_context
                        .deal
                        .get()
                        .map(|d| d.takes_priority)
                        .unwrap_or(false);

                    seat_bids.push((bid_context.bid.clone(), takes_priority));
                }
            }
        }

        seat_bids
    }

    /// Returns Vec<(SeatBid, has_priority_bid)> for all bidders (RTB + merged direct).
    fn build_seats(
        &self,
        bidders: &Vec<BidderContext>,
        fill_policy: &FillPolicy,
    ) -> Vec<(SeatBid, bool)> {
        let mut seats = Vec::with_capacity(bidders.len());

        for bidder in bidders.iter() {
            let bid_pairs = self.build_bidder_seat_bids(bidder, fill_policy);

            let has_priority = bid_pairs.iter().any(|(_, p)| *p);
            let mut bids: Vec<Bid> = bid_pairs.into_iter().map(|(b, _)| b).collect();

            sort_bids_by_price(&mut bids);

            let bidder_seat_result = SeatBidBuilder::default()
                .seat(bidder.bidder.id.clone())
                .bid(bids)
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
                seats.push((bidder_seat, has_priority));
            }
        }

        seats
    }

    async fn run0(&self, context: &AuctionContext) -> Result<(), Error> {
        let span = tracing::Span::current();
        let bidders = context.bidders.lock().await;
        span.record("bidders", tracing::field::debug(&bidders));

        let fill_policy = context
            .placement
            .as_ref()
            .map(|p| &p.fill_policy)
            .unwrap_or(&FillPolicy::HighestPrice);

        let seat_pairs = self.build_seats(&bidders, fill_policy);

        // If any seat has a priority deal bid, keep only priority seats
        let any_priority = seat_pairs.iter().any(|(_, p)| *p);
        let mut seats: Vec<SeatBid> = if any_priority {
            seat_pairs
                .into_iter()
                .filter(|(_, p)| *p)
                .map(|(s, _)| s)
                .collect()
        } else {
            seat_pairs.into_iter().map(|(s, _)| s).collect()
        };

        if seats.is_empty() {
            let (nbr, desc) = context
                .rtb_nbr
                .get()
                .map(|(n, d)| (*n, *d))
                .unwrap_or((nobidreasons::NO_CAMPAIGNS_FOUND, "No bids received"));

            let final_nobid_state = BidResponseState::NoBidReason {
                reqid: context.original_auction_id.clone(),
                nbr,
                desc: Some(desc),
            };

            if let Err(_) = context.res.set(final_nobid_state) {
                bail!("Built final no bid response but one already assigned?!");
            }

            span.record("outcome", "nobid");

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

        span.record("outcome", "bid");
        span.record("response", tracing::field::debug(&final_bid_response_state));

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
        let span = child_span_info!(
            "bid_settlement_task",
            outcome = tracing::field::Empty,
            bidders = tracing::field::Empty,
            response = tracing::field::Empty,
        );

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
