use crate::app::pipeline::ortb::context::AuctionContext;
use crate::app::pipeline::ortb::telemetry;
use crate::core::enrichment::device::{DeviceLookup, DeviceType};
use anyhow::{Error, anyhow};
use pipeline::BlockingTask;
use rtb::bid_request::Device;
use rtb::child_span_info;
use rtb::common::bidresponsestate::BidResponseState;
use tracing::Span;
use tracing::log::debug;

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

        let req_borrow = context.req.read();
        let dev_borrow = req_borrow.device.as_ref().expect("Should have device!");
        let ua = &dev_borrow.ua;

        span.record("user_agent", ua);

        if Self::already_complete(&dev_borrow) {
            span.record("dev_lookup_result", "skipped_complete");
            return Ok(());
        }

        let device_opt = self.lookup.lookup_ua(&ua);

        if device_opt.is_none() {
            let brs = BidResponseState::NoBidReason {
                reqid: req_borrow.id.clone(),
                nbr: rtb::spec::openrtb::nobidreason::UNSUPPORTED_DEVICE,
                desc: "Unrecognized user-agent string".into(),
            };

            context
                .res
                .set(brs)
                .expect("Someone already set a BidResponseState!");

            span.record("dev_lookup_result", "no_ua_result");
            parent_span.record(telemetry::SPAN_REQ_BLOCK_REASON, "ua_unknown");

            return Err(anyhow!("Unrecognized device ua"));
        }

        let device = device_opt.unwrap();

        if device.devtype == DeviceType::Bot {
            let brs = BidResponseState::NoBidReason {
                reqid: req_borrow.id.clone(),
                nbr: rtb::spec::openrtb::nobidreason::KNOWN_WEB_CRAWLER,
                desc: "Detected ua bot".into(),
            };

            context
                .res
                .set(brs)
                .expect("Someone already set a BidResponseState!");

            span.record("dev_lookup_result", "bot");
            parent_span.record(telemetry::SPAN_REQ_BLOCK_REASON, "ua_bot");

            return Err(anyhow!("Bot user-agent"));
        }

        drop(req_borrow); // very important or deadlocks!

        let mut req_mut = context.req.write();
        let dev_mut = req_mut.device.as_mut().unwrap();

        let rtb_dev_type = match device.devtype {
            DeviceType::Desktop => rtb::spec::adcom::devicetype::PERSONAL_COMPUTER,
            DeviceType::Phone => rtb::spec::adcom::devicetype::PHONE,
            DeviceType::SetTop => rtb::spec::adcom::devicetype::SET_TOP_BOX,
            DeviceType::Tablet => rtb::spec::adcom::devicetype::TABLET,
            DeviceType::Tv => rtb::spec::adcom::devicetype::CONNECTED_TV,
            DeviceType::Unknown => rtb::spec::adcom::devicetype::MOBILE_TABLET_GENERAL,
            DeviceType::Bot => panic!("Bot hit in switch but shouldnt be possible!"),
        };

        debug!("Injecting device details: {:?}", device);

        if device.os.is_some() {
            dev_mut.os = device.os.unwrap();
            span.record("dev_os", &dev_mut.os);
        }

        if device.model.is_some() {
            dev_mut.model = device.model.unwrap();
            span.record("dev_model", &dev_mut.model);
        }

        if device.brand.is_some() {
            dev_mut.make = device.brand.unwrap();
            span.record("dev_make", &dev_mut.make);
        }

        dev_mut.devicetype = rtb_dev_type as i32;
        span.record("dev_type", tracing::field::debug(device.devtype));

        Ok(())
    }
}
