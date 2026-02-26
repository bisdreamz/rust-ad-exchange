use anyhow::{Error, anyhow};
use compact_str::CompactString;
use derive_builder::Builder;
use fast_uaparser::{Device, OperatingSystem};
use moka::sync::Cache;
use serde::{Deserialize, Serialize};
use std::num::NonZeroU32;

#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
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

#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Os {
    Ios,
    Android,
    AndroidTv,
    FireOs,
    Windows,
    MacOs,
    Linux,
    ChromeOs,
    Roku,
    SamsungTvTizen,
    LgTvWebOs,
    VizioSmartCast,
    HisenseTvVidaa,
    PlayStation,
    Xbox,
    GenericSmartTv,
    #[default]
    Unknown,
}

impl Os {
    /// Human-readable name for spans and logging
    pub fn as_str(&self) -> &'static str {
        match self {
            Os::Ios => "iOS",
            Os::Android => "Android",
            Os::AndroidTv => "Android TV",
            Os::FireOs => "Fire OS",
            Os::Windows => "Windows",
            Os::MacOs => "macOS",
            Os::Linux => "Linux",
            Os::ChromeOs => "Chrome OS",
            Os::Roku => "Roku OS",
            Os::SamsungTvTizen => "Tizen",
            Os::LgTvWebOs => "webOS",
            Os::VizioSmartCast => "SmartCast",
            Os::HisenseTvVidaa => "Vidaa",
            Os::PlayStation => "PlayStation",
            Os::Xbox => "Xbox",
            Os::GenericSmartTv => "SmartTV",
            Os::Unknown => "unknown",
        }
    }

    pub fn parse(s: &str) -> Os {
        let l = s.trim().to_ascii_lowercase();
        if l.is_empty() {
            return Os::Unknown;
        }

        // ~2.42b — covers ios, ipados, apple ios, apple tv, appletv, atv os, tvos
        if l.starts_with("ios")
            || l.starts_with("ipados")
            || l.starts_with("apple")
            || l.starts_with("tvos")
            || l.starts_with("tv os")
            || l.starts_with("tv_os")
            || l.starts_with("atv os")
        {
            return Os::Ios;
        }
        // ~2.29b — disambiguate Android TV inside to avoid penalising the common case
        if l.starts_with("android") {
            if l.starts_with("android tv") || l.starts_with("androidtv") {
                return Os::AndroidTv;
            }
            return Os::Android;
        }
        // ~670m
        if l.starts_with("roku") {
            return Os::Roku;
        }
        // ~370m — Samsung smart TVs run Tizen
        if l.starts_with("tizen") || l.starts_with("samsung") {
            return Os::SamsungTvTizen;
        }
        // ~250m
        if l.starts_with("linux") {
            return Os::Linux;
        }
        // ~57m — Fire OS before Windows/Mac since it has higher volume
        if l.starts_with("fire os")
            || l.starts_with("fire tv")
            || l.starts_with("firetv")
            || l.starts_with("fireos")
            || l.starts_with("fire_os")
            || l == "fire"
        {
            return Os::FireOs;
        }
        // ~48m
        if l.starts_with("smartcast") || l.starts_with("vizio") {
            return Os::VizioSmartCast;
        }
        // ~18m
        if l.starts_with("windows") {
            return Os::Windows;
        }
        // ~17m — covers: mac os, mac os x, macos, os x
        if l.starts_with("mac") || l.starts_with("os x") {
            return Os::MacOs;
        }
        // covers LG NetCast (older LG platform) and LG proprietary
        if l.starts_with("webos")
            || l.starts_with("web0s")
            || l.starts_with("lgnetcast")
            || l.starts_with("lg ")
        {
            return Os::LgTvWebOs;
        }
        if l.starts_with("chrome os")
            || l.starts_with("chromium os")
            || l.starts_with("chromecast")
            || l == "chrome"
            || l == "chromeos"
        {
            return Os::ChromeOs;
        }
        // Google TV — uncommon, maps to AndroidTv, checked late
        if l.starts_with("google tv") || l.starts_with("googletv") {
            return Os::AndroidTv;
        }
        if l.starts_with("vidaa") {
            return Os::HisenseTvVidaa;
        }
        if l.starts_with("playstation") || l.starts_with("ps4") || l.starts_with("ps5") {
            return Os::PlayStation;
        }
        if l.starts_with("xbox") {
            return Os::Xbox;
        }
        // Sony Bravia, Vewd (Opera TV), Vega (Vestel/Philips), Xfinity/Comcast X1
        if l.starts_with("smarttv")
            || l.starts_with("smart tv")
            || l.starts_with("smart_tv")
            || l.starts_with("bravia")
            || l.starts_with("vewd")
            || l.starts_with("vega")
            || l.starts_with("xfinity")
        {
            return Os::GenericSmartTv;
        }

        Os::Unknown
    }
}

#[derive(Clone, Debug, Builder, Default)]
pub struct DeviceInfo {
    pub brand: Option<String>,
    pub model: Option<String>,
    #[builder(default)]
    pub os: Os,
    /// Raw OS family string from UA parser, retained for RTB injection
    #[builder(default)]
    pub os_raw: CompactString,
    #[builder(default)]
    pub devtype: DeviceType,
}

pub struct DeviceLookup {
    cache: Cache<String, Option<DeviceInfo>>,
}

fn extract_type(device_family: &str, os: &Os) -> DeviceType {
    match device_family {
        "iPad" => return DeviceType::Tablet,
        "iPhone" => return DeviceType::Phone,
        "AppleTV" => return DeviceType::Tv,
        "Spider" => return DeviceType::Bot,
        _ => {}
    }

    let dl = device_family.to_ascii_lowercase();

    if dl.starts_with("spider") || dl.contains("bot") || dl.contains("crawler") {
        return DeviceType::Bot;
    }

    if dl.contains("tv")
        || dl.starts_with("roku")
        || dl.starts_with("chromecast")
        || dl.contains("netcast")
        || dl.contains("bravia")
    {
        return DeviceType::Tv;
    }

    if dl.contains("set-top")
        || dl.contains("settop")
        || dl.contains("console")
        || dl.starts_with("playstation")
        || dl.starts_with("xbox")
        || dl.starts_with("wii")
    {
        return DeviceType::SetTop;
    }

    if dl.contains("tablet")
        || dl.starts_with("kindle")
        || (dl.starts_with("fire") && !dl.starts_with("fire tv"))
        || dl.starts_with("surface")
    {
        return DeviceType::Tablet;
    }

    if dl.contains("phone")
        || dl.contains("mobile")
        || dl.contains("smartphone")
        || dl.starts_with("galaxy")
        || dl.starts_with("pixel")
        || dl.starts_with("oneplus")
        || dl.starts_with("xiaomi")
    {
        return DeviceType::Phone;
    }

    match os {
        Os::Ios => DeviceType::Phone,
        Os::Android | Os::FireOs => DeviceType::Phone,
        Os::AndroidTv
        | Os::Roku
        | Os::SamsungTvTizen
        | Os::LgTvWebOs
        | Os::VizioSmartCast
        | Os::HisenseTvVidaa
        | Os::GenericSmartTv => DeviceType::Tv,
        Os::PlayStation | Os::Xbox => DeviceType::SetTop,
        Os::Windows | Os::MacOs | Os::Linux | Os::ChromeOs => DeviceType::Desktop,
        _ => DeviceType::Unknown,
    }
}

impl DeviceLookup {
    pub fn try_new(cache_sz: NonZeroU32) -> Result<Self, Error> {
        fast_uaparser::init()
            .map(|_b| DeviceLookup {
                cache: Cache::new(cache_sz.get() as u64),
            })
            .map_err(|e| anyhow!(e))
    }

    fn load(user_agent: &str) -> Option<DeviceInfo> {
        let device: Device = user_agent.parse().ok()?;
        let os_info: OperatingSystem = user_agent.parse().ok()?;

        let os_raw = CompactString::from(&os_info.family);
        let os = Os::parse(&os_info.family);
        let devtype = extract_type(&device.family, &os);

        Some(DeviceInfo {
            brand: device.brand,
            model: device.model,
            os,
            os_raw,
            devtype,
        })
    }

    pub fn lookup_ua(&self, user_agent: &String) -> Option<DeviceInfo> {
        self.cache
            .get_with(user_agent.clone(), || DeviceLookup::load(user_agent))
    }
}
