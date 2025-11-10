use serde::{Deserialize, Serialize};
use strum::{AsRefStr, Display, EnumString};

#[derive(Debug, Clone, Deserialize, Serialize, AsRefStr, EnumString, Display)]
#[serde(rename_all = "snake_case")]
pub enum Metric {
    /// Actual (gross) revenue generated per million outbound auctions
    Rpm,
    /// Bid value per mission auctions - Effective potential value of bid CPMs
    Bvpm,
    /// Percentage of billed impressions versus auctions sent
    FillRate,
    /// Percentage of bids received versus auctions sent
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
        /// Minimum allowed passing threshold, which for
        /// endpoints without a qps limit (0) is the fixed threshold
        #[serde(default)]
        min_target_metric: f32,
    },
}
