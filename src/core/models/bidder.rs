use crate::core::models::shaping::TrafficShaping;
use derive_builder::Builder;
use serde::{Deserialize, Serialize};
use strum::{Display, EnumString};

#[derive(Debug, Clone, Serialize, Deserialize, Builder, Default)]
pub struct Targeting {
    pub geos: Vec<String>,
    pub banner: bool,
    pub video: bool,
    pub native: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, EnumString, Display)]
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
pub enum Encoding {
    #[default]
    Json,
    Protobuf,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, Builder)]
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
    pub name: String,
    #[builder(default = "true")]
    pub gzip: bool,
    #[builder(default = "true")]
    pub multi_imp: bool,
}
