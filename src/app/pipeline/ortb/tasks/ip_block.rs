use crate::app::pipeline::ortb::AuctionContext;
use crate::core::filters::bot::IpRiskFilter;
use anyhow::{Error, anyhow};
use pipeline::BlockingTask;
use rtb::child_span_info;
use rtb::common::bidresponsestate::BidResponseState;
use std::net::IpAddr;
use tracing::debug;

pub struct IpBlockTask {
    filter: IpRiskFilter,
}

impl IpBlockTask {
    pub fn new(filter: IpRiskFilter) -> Self {
        Self { filter }
    }
}

impl BlockingTask<AuctionContext, anyhow::Error> for IpBlockTask {
    fn run(&self, context: &AuctionContext) -> Result<(), Error> {
        let span =
            child_span_info!("ip_block_task", ip_block_reason = tracing::field::Empty).entered();

        let req_borrow = context.req.read();
        let dev_ip = &req_borrow.device.as_ref().expect("Should have device").ip;

        span.record("ip", dev_ip);

        let ip_parse_result: Result<IpAddr, _> = dev_ip.parse();
        if let Err(_) = ip_parse_result {
            let msg = "Invalid IP received".into();

            let brs = BidResponseState::NoBidReason {
                reqid: req_borrow.id.clone(),
                nbr: rtb::spec::openrtb::nobidreason::INVALID_REQUEST,
                desc: Some(msg),
            };

            context
                .res
                .set(brs)
                .expect("Someone already set a BidResponseState!");

            span.record("ip_block_reason", "invalid_ip");

            return Err(anyhow!(msg));
        }

        let ip: IpAddr = ip_parse_result?;

        if self.filter.should_block(ip) {
            let msg = "High risk IP".into();

            let brs = BidResponseState::NoBidReason {
                reqid: req_borrow.id.clone(),
                nbr: rtb::spec::openrtb::nobidreason::CLOUD_DATACENTER_PROXY_IP,
                desc: Some(msg),
            };

            context
                .res
                .set(brs)
                .expect("Someone already set a BidResponseState!");

            span.record("ip_block_reason", "high_risk");

            return Err(anyhow!(msg));
        }

        span.record("ip_block_reason", "none");

        debug!("Ip {} passed risk filter", ip);

        Ok(())
    }
}
