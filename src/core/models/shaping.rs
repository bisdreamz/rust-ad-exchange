use serde::{Deserialize, Serialize};
use strum::{AsRefStr, Display, EnumString};

#[derive(Debug, Clone, Deserialize, Serialize, AsRefStr, EnumString, Display)]
#[serde(rename_all = "snake_case")]
pub enum Metric {
    Rpm,
    FillRate,
    BidRate,
}

#[derive(Debug, Clone, Deserialize, Serialize, EnumString, AsRefStr, Display)]
#[serde(rename_all = "snake_case")]
pub enum ShapingFeature {
    PubId,
    Geo,
    Domain,
    ZoneId,
    DeviceOs,
    DeviceConType,
    DeviceType,
    AdSizeFormat,
    UserMatched,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default, AsRefStr, Display)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum TrafficShaping {
    #[default]
    None,
    Tree {
        control_percent: u32,
        metric: Metric,
        features: Vec<ShapingFeature>,
    },
}
