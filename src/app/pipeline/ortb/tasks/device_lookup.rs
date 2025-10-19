use crate::app::pipeline::ortb::context::AuctionContext;
use crate::child_span_info;
use crate::core::enrichment::device::{DeviceLookup, DeviceType};
use anyhow::{anyhow, Error};
use pipeline::BlockingTask;
use rtb::bid_request::Device;
use rtb::common::bidresponsestate::BidResponseState;
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
        let span = child_span_info!("device_lookup_task").entered();

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
                nbr: rtb::spec::nobidreason::INVALID_REQUEST,
                desc: "Unrecognized user-agent string".into(),
            };

            context
                .res
                .set(brs)
                .expect("Someone already set a BidResponseState!");

            span.record("dev_lookup_result", "no_ua_result");

            return Err(anyhow!("Unrecognized device ua"));
        }

        let device = device_opt.unwrap();

        if device.devtype == DeviceType::Bot {
            let brs = BidResponseState::NoBidReason {
                reqid: req_borrow.id.clone(),
                nbr: rtb::spec::nobidreason::NONHUMAN_TRAFFIC,
                desc: "Detected bot".into(),
            };

            context
                .res
                .set(brs)
                .expect("Someone already set a BidResponseState!");

            span.record("dev_lookup_result", "bot");

            return Err(anyhow!("Bot user-agent"));
        }

        drop(req_borrow); // very important or deadlocks!

        let mut req_mut = context.req.write();
        let dev_mut = req_mut.device.as_mut().unwrap();

        let rtb_dev_type = match device.devtype {
            DeviceType::Desktop => rtb::spec::devicetype::PERSONAL_COMPUTER,
            DeviceType::Phone => rtb::spec::devicetype::PHONE,
            DeviceType::SetTop => rtb::spec::devicetype::SET_TOP_BOX,
            DeviceType::Tablet => rtb::spec::devicetype::TABLET,
            DeviceType::Tv => rtb::spec::devicetype::CONNECTED_TV,
            DeviceType::Unknown => rtb::spec::devicetype::MOBILE_TABLET_GENERAL,
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
