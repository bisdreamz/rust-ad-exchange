use crate::app::pipeline::ortb::context::{AuctionContext, PublisherBlockReason};
use crate::app::pipeline::ortb::telemetry;
use crate::core::enrichment::device::{DeviceLookup, DeviceType};
use anyhow::{Error, anyhow};
use pipeline::BlockingTask;
use rtb::bid_request::Device;
use rtb::child_span_info;
use rtb::common::bidresponsestate::BidResponseState;
use tracing::Span;

pub struct DeviceLookupTask {
    lookup: DeviceLookup,
}

impl DeviceLookupTask {
    pub fn new(lookup: DeviceLookup) -> Self {
        Self { lookup }
    }

    fn already_complete(dev: &Device) -> bool {
        !dev.os.is_empty() && !dev.make.is_empty() && dev.devicetype > 0
    }
}

impl BlockingTask<AuctionContext, Error> for DeviceLookupTask {
    fn run(&self, context: &AuctionContext) -> Result<(), Error> {
        let parent_span = Span::current();
        let span = child_span_info!(
            "device_lookup_task",
            dev_lookup_result = tracing::field::Empty,
            dev_make = tracing::field::Empty,
            dev_model = tracing::field::Empty,
            dev_os = tracing::field::Empty,
            dev_type = tracing::field::Empty,
        )
        .entered();

        // read lock scoped here — must be released before write lock below or deadlocks
        let (ua, is_complete) = {
            let req = context.req.read();
            let dev = req
                .device
                .as_ref()
                .ok_or_else(|| anyhow!("Missing device on bid request"))?;
            (dev.ua.clone(), Self::already_complete(dev))
        };

        span.record("user_agent", &ua);

        let device = match self.lookup.lookup_ua(&ua) {
            None => {
                let brs = BidResponseState::NoBidReason {
                    reqid: context.original_auction_id.clone(),
                    nbr: rtb::spec::openrtb::nobidreason::UNSUPPORTED_DEVICE,
                    desc: "Unrecognized user-agent string".into(),
                };
                context
                    .res
                    .set(brs)
                    .map_err(|_| anyhow!("BidResponseState already set"))?;
                context
                    .block_reason
                    .set(PublisherBlockReason::DeviceUnknown)
                    .map_err(|_| anyhow!("block_reason already set"))?;
                span.record("dev_lookup_result", "no_ua_result");
                parent_span.record(telemetry::SPAN_REQ_BLOCK_REASON, "ua_unknown");
                return Err(anyhow!("Unrecognized device ua"));
            }
            Some(d) => d,
        };

        if device.devtype == DeviceType::Bot {
            let brs = BidResponseState::NoBidReason {
                reqid: context.original_auction_id.clone(),
                nbr: rtb::spec::openrtb::nobidreason::KNOWN_WEB_CRAWLER,
                desc: "Detected ua bot".into(),
            };
            context
                .res
                .set(brs)
                .map_err(|_| anyhow!("BidResponseState already set"))?;
            context
                .block_reason
                .set(PublisherBlockReason::DeviceBot)
                .map_err(|_| anyhow!("block_reason already set"))?;
            span.record("dev_lookup_result", "bot");
            parent_span.record(telemetry::SPAN_REQ_BLOCK_REASON, "ua_bot");
            return Err(anyhow!("Bot user-agent"));
        }

        span.record("dev_os", device.os.as_str());
        span.record("dev_type", tracing::field::debug(&device.devtype));

        context.device.set(device.clone()).ok();

        if is_complete {
            span.record("dev_lookup_result", "skipped_complete");
            return Ok(());
        }

        let rtb_dev_type = match device.devtype {
            DeviceType::Desktop => rtb::spec::adcom::devicetype::PERSONAL_COMPUTER,
            DeviceType::Phone => rtb::spec::adcom::devicetype::PHONE,
            DeviceType::SetTop => rtb::spec::adcom::devicetype::SET_TOP_BOX,
            DeviceType::Tablet => rtb::spec::adcom::devicetype::TABLET,
            DeviceType::Tv => rtb::spec::adcom::devicetype::CONNECTED_TV,
            DeviceType::Unknown => rtb::spec::adcom::devicetype::MOBILE_TABLET_GENERAL,
            DeviceType::Bot => {
                return Err(anyhow!("Bot reached RTB device type mapping unexpectedly"));
            }
        };

        let mut req_mut = context.req.write();
        let dev_mut = req_mut
            .device
            .as_mut()
            .ok_or_else(|| anyhow!("Missing device on bid request during write"))?;

        if !device.os_raw.is_empty() {
            dev_mut.os = device.os_raw.to_string();
        }
        if let Some(model) = device.model {
            dev_mut.model = model;
            span.record("dev_model", &dev_mut.model);
        }
        if let Some(brand) = device.brand {
            dev_mut.make = brand;
            span.record("dev_make", &dev_mut.make);
        }

        dev_mut.devicetype = rtb_dev_type as i32;
        span.record("dev_lookup_result", "enriched");

        Ok(())
    }
}
