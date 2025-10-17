use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Metric {
    Rpm,
    FillRate,
    BidRate
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Feature {
    PubId,
    Geo,
    Domain,
    ZoneId,
    DeviceOs,
    AdFormat
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum TrafficShaping {
    #[default]
    None,
    Tree {
        control_percent: usize,
        metric: Metric,
        features: Vec<Feature>
    }
}