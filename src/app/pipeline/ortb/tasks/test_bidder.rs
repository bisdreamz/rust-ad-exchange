use crate::app::pipeline::ortb::AuctionContext;
use crate::app::pipeline::ortb::context::{
    BidResponseContext, BidderCallout, BidderContext, BidderResponseBuilder, BidderResponseState,
};
use crate::core::models::bidder::{BidderBuilder, Endpoint};
use anyhow::{Error, bail};
use async_trait::async_trait;
use pipeline::AsyncTask;
use rtb::bid_request::{Banner, Imp};
use rtb::bid_response::bid::AdmOneof;
use rtb::bid_response::{Bid, BidBuilder, SeatBidBuilder};
use rtb::{BidRequest, BidResponseBuilder};
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tracing::{debug, warn};

fn generate_display_bid(imp: &Imp, banner: &Banner) -> Result<Bid, Error> {
    let mut w = banner.w;
    let mut h = banner.h;

    if w == 0 && h == 0 {
        if !banner.format.is_empty() {
            let first_format = banner.format.first().unwrap();

            w = first_format.w;
            h = first_format.h;
        } else {
            warn!("bad banner?: {}", serde_json::to_string_pretty(banner)?);
            bail!("Banner object without w/h or formats! What heck!")
        }
    }

    let adm = "<div>Test Ad Content</div>";

    Ok(BidBuilder::default()
        .id("test_bid")
        .crid("test_crid")
        .cid("test_campaign_id")
        .impid(imp.id.clone())
        .price(imp.bidfloor * 1.2)
        .adomain(vec!["neuronic.dev".into()])
        .adm_oneof(AdmOneof::Adm(adm.into()))
        .w(w)
        .h(h)
        .build()?)
}

fn generate_psa_bids(req: &BidRequest) -> Vec<Bid> {
    let mut psa_bids = Vec::new();

    for imp in &req.imp {
        let banner = match &imp.banner {
            Some(banner) => banner,
            None => continue,
        };

        let bids = match generate_display_bid(imp, banner) {
            Ok(bids) => bids,
            Err(e) => {
                warn!("Failed building bids for force bid on imp: {}", e);
                continue;
            }
        };

        psa_bids.push(bids);
    }

    psa_bids
}

fn build_psa_bidder_context(req: BidRequest, bids: Vec<Bid>) -> Result<BidderContext, Error> {
    let seat_bid_result = SeatBidBuilder::default()
        .seat("test_bidder")
        .bid(bids)
        .build();

    if seat_bid_result.is_err() {
        bail!("Failed to build seat bid object, skipping test bids!");
    }

    let seat_bid = seat_bid_result?;

    let bid_response_result = BidResponseBuilder::default()
        .id(req.id.clone())
        .seatbid(vec![seat_bid])
        .build();

    if bid_response_result.is_err() {
        bail!("Failed to build final force bid response, skipping test bids!");
    }

    let bid_response_context = BidResponseContext::from(bid_response_result?);

    let bidder_response = BidderResponseBuilder::default()
        .state(BidderResponseState::Bid(bid_response_context))
        .latency(Duration::from_millis(1))
        .build()?;

    let callout_response = OnceLock::new();
    callout_response.set(bidder_response).unwrap();

    let bidder_callout = BidderCallout {
        skip_reason: OnceLock::new(),
        endpoint: Arc::new(Endpoint {
            name: "test_endpoint".to_string(),
            ..Default::default()
        }),
        req,
        response: callout_response,
    };

    let test_bidder = BidderBuilder::default()
        .name("test_bidder".to_string())
        .build()?;

    Ok(BidderContext {
        bidder: Arc::new(test_bidder),
        callouts: vec![bidder_callout],
    })
}

pub struct TestBidderTask;

#[async_trait]
impl AsyncTask<AuctionContext, Error> for TestBidderTask {
    async fn run(&self, context: &AuctionContext) -> Result<(), Error> {
        let req;
        {
            req = context.req.read().clone();
        }

        if !req.test {
            return Ok(());
        }

        let force_bid = match req.ext {
            Some(ref ext) => ext.custom().get_bool("force_bid").unwrap_or(false),
            None => false,
        };

        if !force_bid {
            return Ok(());
        }

        debug!("Found force_bid flag in request");

        let bids = generate_psa_bids(&req);

        if bids.is_empty() {
            warn!("Failed to generate any force_bids - unsupported request?");
            return Ok(());
        }

        let bidder_context = build_psa_bidder_context(req.clone(), bids)?;
        let mut bidders = context.bidders.lock().await;
        bidders.push(bidder_context);

        debug!("Injected test bids into response!");

        Ok(())
    }
}
