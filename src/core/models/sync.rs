use derive_builder::Builder;
use serde::{Deserialize, Serialize};
use strum::{AsRefStr, Display, EnumString};

/// Kind of deployment for a user sync URL, e.g. img or iframe
#[derive(Debug, Clone, Serialize, Deserialize, AsRefStr, Display, EnumString)]
#[serde(rename_all = "lowercase")]
pub enum SyncKind {
    /// User sync deployed as a single img pixel
    Image,
    /// User sync deployed as an iframe, can contain multiple downstream syncs
    Iframe,
}

/// Demand user sync config
#[derive(Debug, Clone, Serialize, Deserialize, Builder)]
#[serde(rename_all = "lowercase")]
pub struct SyncConfig {
    pub url: String,
    pub kind: SyncKind,
}
