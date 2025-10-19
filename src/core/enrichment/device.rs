use anyhow::{Error, anyhow};
use derive_builder::Builder;
use fast_uaparser::{Device, OperatingSystem, ParserError};
use moka::sync::Cache;
use std::num::NonZeroU32;

#[derive(Clone, Debug, Default, PartialEq)]
pub enum DeviceType {
    #[default]
    Unknown,
    Bot,
    Desktop,
    Phone,
    Tv,
    Tablet,
    SetTop,
}

#[derive(Clone, Debug, Builder, Default)]
pub struct DeviceInfo {
    pub brand: Option<String>,
    pub model: Option<String>,
    pub os: Option<String>,
    pub devtype: DeviceType,
}

pub struct DeviceLookup {
    cache: Cache<String, Option<DeviceInfo>>,
}

fn extract_type(device_family: &str, os_family: &str) -> DeviceType {
    match device_family {
        "iPad" => return DeviceType::Tablet,
        "iPhone" => return DeviceType::Phone,
        "AppleTV" => return DeviceType::Tv,
        "Spider" => return DeviceType::Bot,
        _ => {}
    }

    let device_lower = device_family.to_lowercase();

    if device_lower.contains("spider")
        || device_lower.contains("bot")
        || device_lower.contains("crawler")
    {
        return DeviceType::Bot;
    }

    if device_lower.contains("tv")
        || device_lower.contains("roku")
        || device_lower.contains("chromecast")
        || device_lower.contains("netcast")
        || device_lower.contains("bravia")
    {
        return DeviceType::Tv;
    }

    if device_lower.contains("set-top")
        || device_lower.contains("settop")
        || device_lower.contains("console")
        || device_lower.contains("playstation")
        || device_lower.contains("xbox")
        || device_lower.contains("wii")
    {
        return DeviceType::SetTop;
    }

    if device_lower.contains("tablet")
        || device_lower.contains("kindle")
        || device_lower.contains("fire") && !device_lower.contains("fire tv")
        || device_lower.contains("surface")
    {
        return DeviceType::Tablet;
    }

    if device_lower.contains("phone")
        || device_lower.contains("mobile")
        || device_lower.contains("smartphone")
        || device_lower.contains("galaxy")
        || device_lower.contains("pixel")
        || device_lower.contains("oneplus")
        || device_lower.contains("xiaomi")
    {
        return DeviceType::Phone;
    }

    match os_family {
        "iOS" => return DeviceType::Phone,
        "Android" | "Fire OS" => return DeviceType::Phone,
        _ => {}
    }

    if os_family.len() > 0 {
        let os_lower = os_family.to_lowercase();
        if os_lower.contains("windows")
            || os_lower.contains("mac os")
            || os_lower.contains("linux")
            || os_lower.contains("ubuntu")
            || os_lower.contains("chrome os")
        {
            return DeviceType::Desktop;
        }
    }

    DeviceType::Unknown
}

impl DeviceLookup {
    pub fn try_new(cache_sz: NonZeroU32) -> Result<Self, Error> {
        fast_uaparser::init()
            .map(|_b| DeviceLookup {
                cache: Cache::new(cache_sz.get() as u64),
            })
            .map_err(|e| anyhow!(e))
    }

    fn load(user_agent: &String) -> Option<DeviceInfo> {
        let device_res: Result<Device, ParserError> = user_agent.parse();
        let os_res: Result<OperatingSystem, ParserError> = user_agent.parse();

        if device_res.is_err() || os_res.is_err() {
            return None;
        }

        let device = device_res.unwrap();
        let os = os_res.unwrap();

        let devtype = extract_type(&device.family, &os.family);

        let dev_info = DeviceInfo {
            brand: device.brand,
            model: device.model,
            os: Some(os.family),
            devtype,
        };

        Some(dev_info)
    }

    pub fn lookup_ua(&self, user_agent: &String) -> Option<DeviceInfo> {
        self.cache
            .get_with(user_agent.clone(), || DeviceLookup::load(user_agent))
    }
}
