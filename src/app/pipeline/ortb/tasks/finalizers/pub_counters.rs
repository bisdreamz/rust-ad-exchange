use crate::app::pipeline::ortb::AuctionContext;
use crate::app::pipeline::ortb::context::{BidderContext, BidderResponseState, IdentityContext};
use crate::core::firestore::counters::demand::DemandCounterStore;
use crate::core::firestore::counters::publisher::{PublisherCounterStore, PublisherCounters};
use anyhow::{Error, anyhow};
use async_trait::async_trait;
use pipeline::AsyncTask;
use rtb::bid_request::{Device, DistributionchannelOneof};
use rtb::{BidRequest, child_span_info};
use smallvec::SmallVec;
use std::sync::Arc;
use tracing::Instrument;

fn get_ad_formats(request: &BidRequest) -> SmallVec<[&'static str; 4]> {
    let mut formats = SmallVec::new();

    for imp in &request.imp {
        if imp.banner.is_some() && !formats.contains(&"banner") {
            formats.push("banner");
        }

        if imp.video.is_some() && !formats.contains(&"video") {
            formats.push("video");
        }

        if imp.native.is_some() && !formats.contains(&"native") {
            formats.push("native");
        }

        if imp.audio.is_some() && !formats.contains(&"audio") {
            formats.push("audio");
        }
    }

    formats
}

fn identity_has_cookie(identity: Option<&IdentityContext>) -> bool {
    if identity.is_none() {
        return false;
    }

    // this is our primary local exchange cookie id
    if identity.as_ref().unwrap().local_uid.get().is_some() {
        return true;
    }

    false
}

fn identity_app_has_ifa(device: Option<&Device>) -> bool {
    if device.is_none() {
        return false;
    }

    let device = device.unwrap();

    !device.ifa.is_empty()
}

fn has_cookie_or_maid(request: &BidRequest, identity: Option<&IdentityContext>) -> bool {
    if request.distributionchannel_oneof.is_none() {
        return false;
    }

    let channel = request.distributionchannel_oneof.as_ref().unwrap();

    match channel {
        DistributionchannelOneof::Site(_site) => identity_has_cookie(identity),
        DistributionchannelOneof::App(_) => identity_app_has_ifa(request.device.as_ref()),
        DistributionchannelOneof::Dooh(_) => {
            // not clear on app/site medium so check both
            identity_has_cookie(identity) || identity_app_has_ifa(request.device.as_ref())
        }
    }
}

/// Counts the number of resulting outbound auctions
/// produced by the inbound request
/// Returns (auctions, bids) counts
fn get_auction_stats(bidder_contexts: &Vec<BidderContext>) -> (u32, u32) {
    let mut auctions = 0u32;
    let mut bids = 0u32;

    for bidder_context in bidder_contexts {
        for callout in &bidder_context.callouts {
            if callout.skip_reason.get().is_none() {
                auctions += 1;

                if let Some(response) = callout.response.get().as_ref() {
                    if matches!(response.state, BidderResponseState::Bid(_)) {
                        bids += 1;
                    }
                }
            }
        }
    }

    (auctions, bids)
}

async fn build_pub_counters(context: &AuctionContext) -> Result<PublisherCounters, Error> {
    let mut counters = PublisherCounters::default();

    {
        // TODO Avoid double req locking by migrating to std rwlock
        let request = context.req.read();
        counters.request(request.imp.len() as u64, request.tmax as u64);

        if has_cookie_or_maid(&request, context.identity.get()) {
            counters.had_cookie_or_maid();
        }

        if context.block_reason.get().is_some() {
            counters.block();

            return Ok(counters);
        }
    }

    let bidders = context.bidders.lock().await;
    if !bidders.is_empty() {
        let (auctions_count, bids_count) = get_auction_stats(&bidders);

        if auctions_count > 0 {
            counters.request_auctioned();
            counters.auction(auctions_count as u64);
            counters.bid(bids_count as u64);
        }
    }

    Ok(counters)
}

/// Responsible for merging the recorded counters activity
/// after an auction, and merging with the respective counter
/// stores. This always runs, since we may have a pub req
/// that errs or halts half way through (e.g. blocked) but
/// we still need to record that activity
pub struct PubCountersTask {
    pub_store: Arc<PublisherCounterStore>,
}

impl PubCountersTask {
    pub fn new(store: Arc<PublisherCounterStore>) -> Self {
        PubCountersTask { pub_store: store }
    }

    async fn run0(&self, context: &AuctionContext) -> Result<(), Error> {
        let counters = build_pub_counters(context).await?;

        let request = context.req.read();
        let publisher = context
            .publisher
            .get()
            .ok_or_else(|| anyhow!("No publisher found in context?!"))?;

        self.pub_store.merge(
            publisher.id.as_str(),
            publisher.name.as_str(),
            &get_ad_formats(&request)[..],
            &counters,
        );

        Ok(())
    }
}

#[async_trait]
impl AsyncTask<AuctionCodntext, Error> for PubCountersTask {
    async fn run(&self, context: &AuctionContext) -> Result<(), Error> {
        let span = child_span_info!("pub_counters_task");

        self.run0(context).instrument(span).await
    }
}
