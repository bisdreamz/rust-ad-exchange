use rtb::bid_request::DistributionchannelOneof;
use serde::{Deserialize, Serialize};
use strum::{Display, EnumString};

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize, EnumString, Display,
)]
#[strum(ascii_case_insensitive)]
pub enum Channel {
    Site,
    App,
    Dooh,
    #[default]
    Unknown,
}

impl Channel {
    pub fn from_distribution(dc: Option<&DistributionchannelOneof>) -> Self {
        match dc {
            Some(DistributionchannelOneof::Site(_)) => Channel::Site,
            Some(DistributionchannelOneof::App(_)) => Channel::App,
            Some(DistributionchannelOneof::Dooh(_)) => Channel::Dooh,
            None => Channel::Unknown,
        }
    }
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize, EnumString, Display,
)]
#[strum(ascii_case_insensitive)]
pub enum StatsDeviceType {
    Mobile,
    Desktop,
    Connected, // CTV + Connected Device + Set-Top Box
    Dooh,
    Tablet,
    #[default]
    Unknown,
}

impl StatsDeviceType {
    pub fn from_openrtb(device_type: i32) -> Self {
        use rtb::spec::adcom::devicetype;
        match device_type as u32 {
            devicetype::PHONE => StatsDeviceType::Mobile,
            devicetype::MOBILE_TABLET_GENERAL => StatsDeviceType::Mobile,
            devicetype::TABLET => StatsDeviceType::Tablet,
            devicetype::PERSONAL_COMPUTER => StatsDeviceType::Desktop,
            devicetype::CONNECTED_TV | devicetype::CONNECTED_DEVICE | devicetype::SET_TOP_BOX => {
                StatsDeviceType::Connected
            }
            devicetype::DOOH => StatsDeviceType::Dooh,
            _ => StatsDeviceType::Unknown,
        }
    }
}
