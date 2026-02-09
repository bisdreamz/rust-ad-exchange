use crate::core::models::shaping::TrafficShaping;
use crate::core::models::sync::SyncConfig;
use derive_builder::Builder;
use serde::{Deserialize, Serialize};
use strum::{Display, EnumString};

#[derive(Debug, Clone, Serialize, Deserialize, Builder)]
pub struct TargetingFormats {
    pub banner: bool,
    pub video: bool,
    pub native: bool,
    pub audio: bool,
}

impl Default for TargetingFormats {
    fn default() -> Self {
        Self {
            banner: true,
            video: true,
            native: true,
            audio: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Builder)]
pub struct TargetingChannelTypes {
    /// App requests are enabled
    pub app: bool,
    /// Site requests are enabled
    pub site: bool,
    /// Dooh requests are enabled. This is the *actual*
    /// 2.x channel in ortb 2.x, which replaces the site/app obj
    pub dooh: bool,
}

impl Default for TargetingChannelTypes {
    fn default() -> Self {
        Self {
            app: true,
            site: true,
            dooh: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Builder)]
pub struct TargetingDeviceTypes {
    pub mobile: bool,
    pub desktop: bool,
    pub ctv: bool,
    pub dooh: bool,
}

impl Default for TargetingDeviceTypes {
    fn default() -> Self {
        Self {
            mobile: true,
            desktop: true,
            ctv: true,
            dooh: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Builder, Default)]
#[serde(default)]
pub struct Targeting {
    /// Specific geos or all if empty
    pub geos: Vec<String>,
    /// Specific publishers only or all if empty
    pub pubs: Vec<String>,
    pub formats: TargetingFormats,
    /// Enable channels
    pub channels: TargetingChannelTypes,
    /// Enable device types
    pub devices: TargetingDeviceTypes,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, EnumString, Display)]
#[serde(rename_all = "lowercase")]
pub enum HttpProto {
    /// Force http1.1 only
    Http1,
    /// Force h2c prior knowledge
    H2c,
    /// Allow, but not force, http2 upgrades via alpn
    #[default]
    Http2,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, EnumString, Display)]
#[serde(rename_all = "lowercase")]
pub enum Encoding {
    #[default]
    Json,
    Protobuf,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, Builder)]
#[serde(default)]
pub struct Endpoint {
    pub enabled: bool,
    pub name: String,
    pub url: String,
    pub qps: usize,
    pub shaping: TrafficShaping,
    #[serde(default)]
    pub protocol: HttpProto,
    #[serde(default)]
    pub encoding: Encoding,
    pub targeting: Targeting,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, Builder)]
#[serde(default)]
pub struct Bidder {
    pub id: String,
    pub name: String,
    #[builder(default = "true")]
    pub gzip: bool,
    #[builder(default = "true")]
    pub multi_imp: bool,
    pub usersync: Option<SyncConfig>,
}
