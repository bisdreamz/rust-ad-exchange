use crate::app::pipeline::ortb::AuctionContext;
use crate::core::filters::bot::IpRiskFilter;
use anyhow::{anyhow, Error};
use pipeline::BlockingTask;
use rtb::common::bidresponsestate::BidResponseState;
use std::net::IpAddr;

pub struct IpBlockTask {
    filter: IpRiskFilter
}

impl IpBlockTask {
    pub fn new(filter: IpRiskFilter) -> Self {
        Self { filter }
    }
}

impl BlockingTask<AuctionContext, anyhow::Error> for IpBlockTask {
    fn run(&self, context: &AuctionContext) -> Result<(), Error> {
        let req_borrow = context.req.read();
        let dev_ip = &req_borrow.device.as_ref().expect("Should have device").ip;

        let ip_parse_result: Result<IpAddr, _> = dev_ip.parse();
        if let Err(_) = ip_parse_result {
            let msg = "Invalid IP received".into();

            let brs = BidResponseState::NoBidReason {
                reqid: req_borrow.id.clone(),
                nbr: rtb::spec::nobidreason::INVALID_REQUEST,
                desc: Some(msg)
            };

            context.res.set(brs).expect("Someone already set a BidResponseState!");

            return Err(anyhow!(msg));
        }

        let ip: IpAddr = ip_parse_result?;

        if self.filter.should_block(ip) {
            let msg = "High risk IP".into();

            let brs = BidResponseState::NoBidReason {
                reqid: req_borrow.id.clone(),
                nbr: rtb::spec::nobidreason::DC_PROXY_IP,
                desc: Some(msg)
            };

            context.res.set(brs).expect("Someone already set a BidResponseState!");

            return Err(anyhow!(msg));
        }

        println!("Ip {} passed risk filter", ip);

        Ok(())
    }
}