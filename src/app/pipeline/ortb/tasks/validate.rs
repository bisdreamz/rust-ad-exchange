use crate::app::pipeline::ortb::AuctionContext;
use anyhow::anyhow;
use pipeline::BlockingTask;
use rtb::child_span_info;
use rtb::common::bidresponsestate::BidResponseState;
use tracing::debug;

pub struct ValidateRequestTask;

impl BlockingTask<AuctionContext, anyhow::Error> for ValidateRequestTask {
    fn run(&self, context: &AuctionContext) -> Result<(), anyhow::Error> {
        let span = child_span_info!(
            "request_validate_task",
            invalid_reason = tracing::field::Empty
        )
        .entered();

        debug!(
            "Validating request for seller {} source {}",
            context.pubid, context.source
        );

        let req = context.req.read();

        if req.id.is_empty() {
            let brs = BidResponseState::NoBidReason {
                reqid: "missing".into(),
                nbr: rtb::spec::openrtb::nobidreason::INVALID_REQUEST,
                desc: Some("Missing req id".into()),
            };

            context
                .res
                .set(brs)
                .expect("Should not have response state assigned already");

            span.record("invalid_reason", "missing_auction_id");

            return Err(anyhow!("Auction missing id value"));
        }

        let device_opt = req.device.as_ref();
        if device_opt.is_none() {
            let brs = BidResponseState::NoBidReason {
                reqid: context.original_auction_id.clone(),
                nbr: rtb::spec::openrtb::nobidreason::INVALID_REQUEST,
                desc: Some("Missing device object".into()),
            };

            context
                .res
                .set(brs)
                .expect("Should not have response state assigned already");

            span.record("invalid_reason", "missing_device_object");

            return Err(anyhow!("Auction missing device object"));
        }

        let device = device_opt.unwrap();

        if device.ua.is_empty() {
            let brs = BidResponseState::NoBidReason {
                reqid: context.original_auction_id.clone(),
                nbr: rtb::spec::openrtb::nobidreason::INVALID_REQUEST,
                desc: Some("Missing device user-agent".into()),
            };

            context
                .res
                .set(brs)
                .expect("Should not have response state assigned already");

            span.record("invalid_reason", "missing_user_agent");

            return Err(anyhow!("Auction device object missing ua value"));
        }

        if req.imp.is_empty() {
            let brs = BidResponseState::NoBidReason {
                reqid: context.original_auction_id.clone(),
                nbr: rtb::spec::openrtb::nobidreason::INVALID_REQUEST,
                desc: Some("Empty imps".into()),
            };

            context
                .res
                .set(brs)
                .expect("Should not have response state assigned already");

            span.record("invalid_reason", "missing_imps");

            return Err(anyhow!("Auction missing imps"));
        }

        if req.distributionchannel_oneof.is_none() {
            let brs = BidResponseState::NoBidReason {
                reqid: context.original_auction_id.clone(),
                nbr: rtb::spec::openrtb::nobidreason::INVALID_REQUEST,
                desc: Some("Missing app, site, or dooh object".into()),
            };

            context
                .res
                .set(brs)
                .expect("Should not have response state assigned already");

            span.record("invalid_reason", "missing_app_site");

            return Err(anyhow!("Auction missing app, site, or dooh object"));
        }

        debug!("Request passed basic validation");
        span.record("invalid_reason", "none");

        Ok(())
    }
}
