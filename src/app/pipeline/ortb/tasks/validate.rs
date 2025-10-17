use anyhow::anyhow;
use pipeline::BlockingTask;
use rtb::common::bidresponsestate::BidResponseState;
use crate::app::pipeline::ortb::AuctionContext;

pub struct ValidateRequestTask;

impl BlockingTask<AuctionContext, anyhow::Error> for ValidateRequestTask {
    fn run(&self, context: &AuctionContext) -> Result<(), anyhow::Error> {
        let req = context.req.read();

        if req.id.is_empty() {
            let brs = BidResponseState::NoBidReason {
                reqid: "missing".into(),
                nbr: rtb::spec::nobidreason::INVALID_REQUEST,
                desc: Some("Missing req id".into())
            };

            context.res.set(brs).expect("Should not have response state assigned already");

            return Err(anyhow!("Auction missing id value"));
        }

        let device_opt = req.device.as_ref();
        if device_opt.is_none() {
            let brs = BidResponseState::NoBidReason {
                reqid: req.id.clone(),
                nbr: rtb::spec::nobidreason::INVALID_REQUEST,
                desc: Some("Missing device object".into())
            };

            context.res.set(brs).expect("Should not have response state assigned already");

            return Err(anyhow!("Auction missing device object"));
        }

        let device = device_opt.unwrap();

        if device.ua.is_empty() {
            let brs = BidResponseState::NoBidReason {
                reqid: req.id.clone(),
                nbr: rtb::spec::nobidreason::INVALID_REQUEST,
                desc: Some("Missing device user-agent".into())
            };

            context.res.set(brs).expect("Should not have response state assigned already");

            return Err(anyhow!("Auction device object missing ua value"));
        }

        if req.imp.is_empty() {
            let brs = BidResponseState::NoBidReason {
                reqid: req.id.clone(),
                nbr: rtb::spec::nobidreason::INVALID_REQUEST,
                desc: Some("Empty imps".into())
            };

            context.res.set(brs).expect("Should not have response state assigned already");

            return Err(anyhow!("Auction missing imps"));
        }

        if req.distributionchannel_oneof.is_none() {
            let brs = BidResponseState::NoBidReason {
                reqid: req.id.clone(),
                nbr: rtb::spec::nobidreason::INVALID_REQUEST,
                desc: Some("Missing app, site, or dooh object".into())
            };

            context.res.set(brs).expect("Should not have response state assigned already");

            return Err(anyhow!("Auction missing app, site, or dooh object"));
        }

        println!("Request passed basic validation");

        Ok(())
    }
}