use derive_builder::Builder;
use serde::{Deserialize, Serialize};
use crate::app::core::models::shaping::TrafficShaping;

#[derive(Debug, Clone, Serialize, Deserialize, Default, Builder)]
pub struct Targeting {
    geos: Vec<String>,
    banner: bool,
    video: bool,
    native: bool
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, Builder)]
pub struct Endpoint {
    name: String,
    url: String,
    qps: usize,
    shaping: TrafficShaping
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, Builder)]
pub struct Bidder {
    name: String,
    gzip: bool,
    multi_imp: bool,
    endpoints: Vec<Endpoint>
}