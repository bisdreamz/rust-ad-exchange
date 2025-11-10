use crate::app::pipeline::ortb::AuctionContext;
use crate::app::pipeline::ortb::context::{BidderCallout, BidderContext};
use anyhow::Error;
use async_trait::async_trait;
use pipeline::AsyncTask;
use rtb::child_span_info;
use tracing::{Instrument, debug, trace, warn};

fn expand_requests(callouts: Vec<BidderCallout>) -> Vec<BidderCallout> {
    let mut expanded_callouts = Vec::with_capacity(callouts.len() * 2);

    if callouts.len() == 1 && callouts.first().unwrap().req.imp.len() == 1 {
        // if only one req exists and its 1 imp anyway.. short circuit return our arg
        return callouts;
    }

    for callout in callouts {
        if callout.req.imp.len() == 1 {
            expanded_callouts.push(callout);
            continue;
        }

        let mut req = callout.req;
        let imps = std::mem::take(&mut req.imp);

        let mut reqs: Vec<_> = (1..imps.len()).map(|_| req.clone()).collect();
        reqs.push(req);

        for (imp, mut expanded_request) in imps.into_iter().zip(reqs) {
            expanded_request.imp.push(imp);

            expanded_callouts.push(BidderCallout {
                endpoint: callout.endpoint.clone(),
                req: expanded_request,
                ..Default::default()
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
        trace!(
            "Skipping bidder {} which supports multi imp",
            bidder_context.bidder.name
        );
        return;
    }

    let span = child_span_info!(
        "multi_imp_breakout_task_expand_bidder",
        bidder = tracing::field::Empty,
        old_imp_count = tracing::field::Empty,
        new_imp_count = tracing::field::Empty,
        expanded_reqs = tracing::field::Empty,
    )
    .entered();

    let old_count = bidder_context.callouts.len();
    let callouts = std::mem::take(&mut bidder_context.callouts);
    let expanded_callouts = expand_requests(callouts);
    let new_count = expanded_callouts.len();

    if !span.is_disabled() {
        let json_reqs = serde_json::to_string(
            &expanded_callouts
                .iter()
                .map(|bc| &bc.req)
                .collect::<Vec<_>>(),
        );

        match json_reqs {
            Ok(json_reqs_str) => {
                span.record("bidder", bidder_context.bidder.name.clone());
                span.record("old_imp_count", &old_count);
                span.record("new_imp_count", &new_count);
                span.record("expanded_reqs", json_reqs_str);
            }
            Err(_) => {
                warn!("Failed to serialize expanded callouts while recording span!");
            }
        }
    }

    debug!(
        "Expanded bidder {} from {} -> {}",
        bidder_context.bidder.name, old_count, new_count
    );

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::models::bidder::Endpoint;
    use rtb::BidRequestBuilder;
    use rtb::bid_request::ImpBuilder;
    use std::sync::{Arc, OnceLock};

    fn create_test_endpoint() -> Arc<Endpoint> {
        Arc::new(Endpoint::default())
    }

    #[test]
    fn test_single_imp_unchanged() {
        let endpoint = create_test_endpoint();
        let req = BidRequestBuilder::default()
            .id("test_req_1".to_string())
            .imp(vec![
                ImpBuilder::default()
                    .id("imp1".to_string())
                    .build()
                    .unwrap(),
            ])
            .build()
            .unwrap();

        let callout = BidderCallout {
            endpoint: endpoint.clone(),
            req,
            ..Default::default()
        };

        let result = expand_requests(vec![callout]);

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].req.imp.len(), 1);
        assert_eq!(result[0].req.imp[0].id, "imp1");
        assert_eq!(result[0].req.id, "test_req_1");
    }

    #[test]
    fn test_multi_imp_expansion() {
        let endpoint = create_test_endpoint();
        let req = BidRequestBuilder::default()
            .id("test_req_multi".to_string())
            .imp(vec![
                ImpBuilder::default()
                    .id("imp1".to_string())
                    .build()
                    .unwrap(),
                ImpBuilder::default()
                    .id("imp2".to_string())
                    .build()
                    .unwrap(),
                ImpBuilder::default()
                    .id("imp3".to_string())
                    .build()
                    .unwrap(),
            ])
            .build()
            .unwrap();

        let callout = BidderCallout {
            endpoint: endpoint.clone(),
            req,
            response: OnceLock::new(),
            ..Default::default()
        };

        let result = expand_requests(vec![callout]);

        assert_eq!(result.len(), 3, "Should expand to 3 separate callouts");

        for expanded in result.iter() {
            assert_eq!(
                expanded.req.imp.len(),
                1,
                "Each expanded request should have exactly 1 imp"
            );
            assert_eq!(expanded.req.id, "test_req_multi", "Request ID preserved");
        }

        let imp_ids: Vec<_> = result.iter().map(|bc| bc.req.imp[0].id.as_str()).collect();
        assert_eq!(
            imp_ids,
            vec!["imp1", "imp2", "imp3"],
            "Each imp should be unique and in order"
        );
    }

    #[test]
    fn test_mixed_single_and_multi_imp() {
        let endpoint = create_test_endpoint();

        let single_imp_req = BidRequestBuilder::default()
            .id("single".to_string())
            .imp(vec![
                ImpBuilder::default().id("s1".to_string()).build().unwrap(),
            ])
            .build()
            .unwrap();

        let multi_imp_req = BidRequestBuilder::default()
            .id("multi".to_string())
            .imp(vec![
                ImpBuilder::default().id("m1".to_string()).build().unwrap(),
                ImpBuilder::default().id("m2".to_string()).build().unwrap(),
            ])
            .build()
            .unwrap();

        let callouts = vec![
            BidderCallout {
                endpoint: endpoint.clone(),
                req: single_imp_req,
                ..Default::default()
            },
            BidderCallout {
                endpoint: endpoint.clone(),
                req: multi_imp_req,
                ..Default::default()
            },
        ];

        let result = expand_requests(callouts);

        assert_eq!(result.len(), 3, "1 single + 2 multi = 3 total");

        let imp_ids: Vec<_> = result.iter().map(|bc| bc.req.imp[0].id.as_str()).collect();
        assert_eq!(imp_ids, vec!["s1", "m1", "m2"]);
    }

    #[tokio::test]
    async fn test_original_context_request_unchanged() {
        use crate::app::pipeline::ortb::context::BidderContext;
        use crate::core::models::bidder::Bidder;

        let original_req = BidRequestBuilder::default()
            .id("original_req".to_string())
            .imp(vec![
                ImpBuilder::default()
                    .id("imp1".to_string())
                    .build()
                    .unwrap(),
                ImpBuilder::default()
                    .id("imp2".to_string())
                    .build()
                    .unwrap(),
                ImpBuilder::default()
                    .id("imp3".to_string())
                    .build()
                    .unwrap(),
            ])
            .build()
            .unwrap();

        let context = AuctionContext::new("/test".to_string(), "pub123".to_string(), original_req);

        let endpoint = create_test_endpoint();
        let callout_req = context.req.read().clone();

        let mut bidder_context = BidderContext {
            bidder: Arc::new(Bidder {
                name: "test_bidder".to_string(),
                gzip: false,
                multi_imp: false,
            }),
            callouts: vec![BidderCallout {
                endpoint: endpoint.clone(),
                req: callout_req,
                ..Default::default()
            }],
        };

        expand_bidder(&mut bidder_context);

        assert_eq!(
            context.req.read().imp.len(),
            3,
            "Original context request should still have 3 imps"
        );
        assert_eq!(context.req.read().id, "original_req");
        assert_eq!(
            bidder_context.callouts.len(),
            3,
            "Bidder should have 3 expanded callouts"
        );
    }
}
