use crate::app::pipeline::ortb::AuctionContext;
use crate::app::pipeline::ortb::context::{BidderContext, BidderResponseState, IdentityContext};
use crate::core::firestore::counters::publisher::{PublisherCounterStore, PublisherCounters};
use anyhow::Error;
use async_trait::async_trait;
use pipeline::AsyncTask;
use rtb::bid_request::{Device, DistributionchannelOneof, Imp};
use rtb::{BidRequest, child_span_info};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tracing::Instrument;

/// Determine the primary format for an imp (priority: video > banner > native > audio)
fn imp_format(imp: &Imp) -> Option<&'static str> {
    if imp.video.is_some() {
        Some("video")
    } else if imp.banner.is_some() {
        Some("banner")
    } else if imp.native.is_some() {
        Some("native")
    } else if imp.audio.is_some() {
        Some("audio")
    } else {
        None
    }
}

fn identity_has_cookie(identity: Option<&IdentityContext>) -> bool {
    identity.and_then(|i| i.local_uid.get()).is_some()
}

fn identity_app_has_ifa(device: Option<&Device>) -> bool {
    device.map_or(false, |d| !d.ifa.is_empty())
}

fn has_cookie_or_maid(request: &BidRequest, identity: Option<&IdentityContext>) -> bool {
    match request.distributionchannel_oneof.as_ref() {
        Some(DistributionchannelOneof::Site(_)) => identity_has_cookie(identity),
        Some(DistributionchannelOneof::App(_)) => identity_app_has_ifa(request.device.as_ref()),
        Some(DistributionchannelOneof::Dooh(_)) => {
            identity_has_cookie(identity) || identity_app_has_ifa(request.device.as_ref())
        }
        None => false,
    }
}

/// Build aggregate and per-format counters with proper bid attribution
fn build_counters_from_request(
    request: &BidRequest,
    identity: Option<&IdentityContext>,
    is_blocked: bool,
) -> (
    PublisherCounters,
    HashMap<&'static str, PublisherCounters>,
    HashMap<String, &'static str>,
) {
    let mut aggregate = PublisherCounters::default();
    let mut imp_formats: HashMap<String, &'static str> = HashMap::new();
    let mut format_imp_counts: HashMap<&'static str, u64> = HashMap::new();
    let tmax = request.tmax as u64;
    let has_identity = has_cookie_or_maid(request, identity);

    // first pass count imps per format and build impid->format mapping
    for imp in &request.imp {
        if let Some(format) = imp_format(imp) {
            imp_formats.insert(imp.id.clone(), format);
            *format_imp_counts.entry(format).or_default() += 1;
        }
    }

    aggregate.request(request.imp.len() as u64, tmax);

    if has_identity {
        aggregate.had_cookie_or_maid();
    }

    if is_blocked {
        aggregate.block();
    }

    // build per-format counters with correct imp stats
    let mut by_format: HashMap<&'static str, PublisherCounters> = HashMap::new();
    for (format, imp_count) in format_imp_counts {
        let mut fc = PublisherCounters::default();
        fc.request(imp_count, tmax);

        if has_identity {
            fc.had_cookie_or_maid();
        }

        if is_blocked {
            fc.block();
        }
        by_format.insert(format, fc);
    }

    (aggregate, by_format, imp_formats)
}

/// Add auction/bid stats to counters, attributing bids to their format
fn add_auction_stats(
    aggregate: &mut PublisherCounters,
    by_format: &mut HashMap<&'static str, PublisherCounters>,
    imp_formats: &HashMap<String, &'static str>,
    bidders: &[BidderContext],
) {
    let mut total_auctions = 0u64;
    let mut total_bids = 0u64;
    let mut total_bids_filtered = 0u64;
    let mut format_auctions: HashMap<&'static str, u64> = HashMap::new();

    for bidder in bidders {
        for callout in &bidder.callouts {
            if callout.skip_reason.get().is_some() {
                continue;
            }

            total_auctions += 1;

            // count auction per format (dedupe - one auction per format per callout)
            let mut callout_formats: HashSet<&'static str> = HashSet::new();
            for imp in &callout.req.imp {
                if let Some(&format) = imp_formats.get(imp.id.as_str()) {
                    callout_formats.insert(format);
                }
            }
            for format in callout_formats {
                *format_auctions.entry(format).or_default() += 1;
            }

            if let Some(response) = callout.response.get() {
                if let BidderResponseState::Bid(resp_ctx) = &response.state {
                    for seat in &resp_ctx.seatbids {
                        for bid_ctx in &seat.bids {
                            total_bids += 1;

                            let filtered = bid_ctx.filter_reason.is_some();
                            if filtered {
                                total_bids_filtered += 1;
                            }

                            // attribute bid to its applicable format
                            if let Some(&format) = imp_formats.get(bid_ctx.bid.impid.as_str()) {
                                if let Some(fc) = by_format.get_mut(format) {
                                    fc.bid(1);

                                    if filtered {
                                        fc.bids_filtered(1);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    if total_auctions > 0 {
        aggregate.request_auctioned();
        aggregate.auction(total_auctions);
        aggregate.bid(total_bids);
        aggregate.bids_filtered(total_bids_filtered);
        
        for (format, count) in format_auctions {
            if let Some(fc) = by_format.get_mut(format) {
                fc.request_auctioned();
                fc.auction(count);
            }
        }
    }
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
        let Some(publisher) = context.publisher.get() else {
            // unrecognized seller, somewhat normal -
            // dont halt any further finalizer tasks
            return Ok(());
        };

        let is_blocked = context.block_reason.get().is_some();

        let (mut aggregate, mut by_format, imp_formats) = {
            let request = context.req.read();
            build_counters_from_request(&request, context.identity.get(), is_blocked)
        };

        if !is_blocked {
            let bidders = context.bidders.lock().await;
            add_auction_stats(&mut aggregate, &mut by_format, &imp_formats, &bidders);
        }

        self.pub_store.merge(
            publisher.id.as_str(),
            publisher.name.as_str(),
            &aggregate,
            &by_format,
        );

        Ok(())
    }
}

#[async_trait]
impl AsyncTask<AuctionContext, Error> for PubCountersTask {
    async fn run(&self, context: &AuctionContext) -> Result<(), Error> {
        let span = child_span_info!("pub_counters_task");

        self.run0(context).instrument(span).await
    }
}
