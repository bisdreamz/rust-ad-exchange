use crate::app::pipeline::ortb::context::{BidderCallout, BidderContext};
use crate::app::pipeline::ortb::AuctionContext;
use anyhow::Error;
use async_trait::async_trait;
use pipeline::AsyncTask;
use std::sync::OnceLock;
use rtb::child_span_info;
use tracing::{debug, trace, warn, Instrument};

fn expand_requests(callouts: Vec<BidderCallout>) -> Vec<BidderCallout> {
    let mut expanded_callouts = Vec::with_capacity(callouts.len() * 2);

    for callout in callouts {
        if callout.req.imp.len() == 1 {
            expanded_callouts.push(callout);
            continue;
        }

        let mut req = callout.req;
        let imps = std::mem::take(&mut req.imp);

        let mut reqs: Vec<_> = (1..imps.len())
            .map(|_| req.clone())
            .collect();
        reqs.push(req);

        for (imp, mut expanded_request) in imps.into_iter().zip(reqs) {
            expanded_request.imp.push(imp);

            expanded_callouts.push(BidderCallout {
                endpoint: callout.endpoint.clone(),
                req: expanded_request,
                response: OnceLock::new(),
            });
        }
    }

    expanded_callouts
}

/// Expands a single bidrequest (callout) with multiple imp entries, into a
/// list of bidder callouts with a single imp per bid request
pub struct MultiImpBreakoutTask;

fn expand_bidder(bidder_context: &mut BidderContext) {
    if bidder_context.bidder.multi_imp {
        trace!("Skipping bidder {} which supports multi imp", bidder_context.bidder.name);
        return;
    }

    let span = child_span_info!(
        "multi_imp_breakout_task_expand_bidder",
        bidder = tracing::field::Empty,
        old_imp_count = tracing::field::Empty,
        new_imp_count = tracing::field::Empty,
        expanded_reqs = tracing::field::Empty,
    ).entered();

    let old_count = bidder_context.callouts.len();
    let callouts = std::mem::take(&mut bidder_context.callouts);
    let expanded_callouts = expand_requests(callouts);
    let new_count = expanded_callouts.len();

    if !span.is_disabled() {
        let json_reqs = serde_json::to_string(
            &expanded_callouts.iter().map(|bc| &bc.req).collect::<Vec<_>>());

        match json_reqs {
            Ok(json_reqs_str) => {
                span.record("bidder", bidder_context.bidder.name.clone());
                span.record("old_imp_count", &old_count);
                span.record("new_imp_count", &new_count);
                span.record("expanded_reqs", json_reqs_str);
            },
            Err(_) => {
                warn!("Failed to serialize expanded callouts while recording span!");
            }
        }
    }

    debug!("Expanded bidder {} from {} -> {}",
                bidder_context.bidder.name, old_count, new_count);

    bidder_context.callouts = expanded_callouts;
}

impl MultiImpBreakoutTask {
    async fn run0(&self, context: &AuctionContext) -> Result<(), Error> {
        let mut bidder_contexts = context.bidders.lock().await;

        for bidder_context in bidder_contexts.iter_mut() {
            expand_bidder(bidder_context);
        }

        Ok(())
    }
}

#[async_trait]
impl AsyncTask<AuctionContext, Error> for MultiImpBreakoutTask {
    async fn run(&self, context: &AuctionContext) -> Result<(), Error> {
        let span = child_span_info!("multi_imp_breakout_task");

        self.run0(context).instrument(span).await
    }
}