use crate::app::core::enrichment::device::{DeviceLookup, DeviceType};
use crate::app::pipeline::ortb::context::AuctionContext;
use anyhow::{anyhow, Error};
use pipeline::BlockingTask;
use rtb::bid_request::Device;
use rtb::common::bidresponsestate::BidResponseState;

pub struct DeviceLookupTask {
    lookup: DeviceLookup
}

impl DeviceLookupTask {
    pub fn new(lookup: DeviceLookup) -> Self {
        Self { lookup }
    }

    fn already_complete(dev: &Device) -> bool {
        !dev.os.is_empty() &&
            !dev.make.is_empty() &&
            dev.devicetype > 0
    }
}

impl BlockingTask<AuctionContext, Error> for DeviceLookupTask {
    fn run(&self, context: &AuctionContext) -> Result<(), Error> {
        let req_borrow = context.req.read();
        let dev_borrow = req_borrow.device.as_ref()
            .expect("Should have device!");
        let ua = &dev_borrow.ua;

        if Self::already_complete(&dev_borrow) {
            return Ok(())
        }

        let device_opt = self.lookup.lookup_ua(&ua);

        if device_opt.is_none() {
            let brs = BidResponseState::NoBidReason {
                reqid: req_borrow.id.clone(),
                nbr: rtb::spec::nobidreason::INVALID_REQUEST,
                desc: "Unrecognized user-agent string".into()
            };

            context.res.set(brs).expect("Someone already set a BidResponseState!");

            return Err(anyhow!("Unrecognized device ua"));
        }

        let device = device_opt.unwrap();

        if device.devtype == DeviceType::Bot {
            let brs = BidResponseState::NoBidReason {
                reqid: req_borrow.id.clone(),
                nbr: rtb::spec::nobidreason::NONHUMAN_TRAFFIC,
                desc: "Detected bot".into()
            };

            context.res.set(brs).expect("Someone already set a BidResponseState!");

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
            DeviceType::Bot => panic!("Bot hit in switch but shouldnt be possible!")
        };

        println!("Injecting device details: {:?}", device);

        if device.os.is_some() {
            dev_mut.os = device.os.unwrap();
        }

        if device.model.is_some() {
            dev_mut.model = device.model.unwrap();
        }

        if device.brand.is_some() {
            dev_mut.make = device.brand.unwrap();
        }

        dev_mut.devicetype = rtb_dev_type as i32;

        Ok(())
    }

}