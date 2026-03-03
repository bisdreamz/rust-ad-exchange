use crate::app::pipeline::ortb::AuctionContext;
use crate::app::pipeline::ortb::context::BidContext;
use anyhow::{Error, bail};
use async_trait::async_trait;
use pipeline::AsyncTask;
use rtb::BidRequest;
use rtb::bid_request::{Banner, Imp};
use rtb::bid_response::bid::AdmOneof;
use rtb::bid_response::{Bid, BidBuilder};
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

        let bid = match generate_display_bid(imp, banner) {
            Ok(bid) => bid,
            Err(e) => {
                warn!("Failed building bid for force bid on imp: {}", e);
                continue;
            }
        };

        psa_bids.push(bid);
    }

    psa_bids
}

pub struct TestBidderTask;

#[async_trait]
impl AsyncTask<AuctionContext, Error> for TestBidderTask {
    async fn run(&self, context: &AuctionContext) -> Result<(), Error> {
        let req;
        {
            let req_read = context.req.read();
            if !req_read.test {
                return Ok(());
            }

            req = req_read.clone();
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

        let bid_contexts: Vec<BidContext> = bids.into_iter().map(BidContext::from).collect();

        let mut staging = context.direct_bid_staging.lock().await;
        staging.extend(bid_contexts);

        debug!("Injected {} test bids into direct staging", staging.len());

        Ok(())
    }
}
