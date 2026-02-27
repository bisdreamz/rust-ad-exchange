use crate::app::pipeline::ortb::context::PublisherBlockReason;
use crate::app::pipeline::ortb::{AuctionContext, telemetry};
use crate::core::filters::bot::IpRiskFilter;
use anyhow::{Error, anyhow};
use pipeline::BlockingTask;
use rtb::child_span_info;
use rtb::common::bidresponsestate::BidResponseState;
use std::net::IpAddr;
use tracing::{Span, debug};

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
        let parent_span = Span::current();
        let span =
            child_span_info!("ip_block_task", ip_block_reason = tracing::field::Empty).entered();

        let req_borrow = context.req.read();
        let device = &req_borrow
            .device
            .as_ref()
            .ok_or(anyhow!("Missing device"))?;
        let dev_ip = if device.ip.is_empty() {
            &device.ipv6
        } else {
            &device.ip
        };

        span.record("ip", dev_ip);

        let ip_parse_result: Result<IpAddr, _> = dev_ip.parse();
        if let Err(_) = ip_parse_result {
            let msg = "Invalid IP received".into();

            let brs = BidResponseState::NoBidReason {
                reqid: context.original_auction_id.clone(),
                nbr: rtb::spec::openrtb::nobidreason::INVALID_REQUEST,
                desc: Some(msg),
            };

            context
                .res
                .set(brs)
                .map_err(|_| anyhow!("Someone already set a BidResponseState!"))?;

            context
                .block_reason
                .set(PublisherBlockReason::IpInvalid)
                .map_err(|_| anyhow!("Failed to attach block pub reason on ctx"))?;

            span.record("ip_block_reason", "invalid_ip");
            parent_span.record(telemetry::SPAN_REQ_BLOCK_REASON, "invalid_ip");

            return Err(anyhow!(msg));
        }

        let ip: IpAddr = ip_parse_result?;

        if self.filter.should_block(ip) {
            let msg = "High risk IP".into();

            let brs = BidResponseState::NoBidReason {
                reqid: context.original_auction_id.clone(),
                nbr: rtb::spec::openrtb::nobidreason::CLOUD_DATACENTER_PROXY_IP,
                desc: Some(msg),
            };

            context
                .res
                .set(brs)
                .map_err(|_| anyhow!("Someone already set a BidResponseState!"))?;

            context
                .block_reason
                .set(PublisherBlockReason::IpDatacenter)
                .map_err(|_| anyhow!("Failed to attach block pub reason on ctx"))?;

            span.record("ip_block_reason", "high_risk");
            parent_span.record(telemetry::SPAN_REQ_BLOCK_REASON, "high_risk_ip");

            return Err(anyhow!(msg));
        }

        span.record("ip_block_reason", "none");

        debug!("Ip {} passed risk filter", ip);

        Ok(())
    }
}
