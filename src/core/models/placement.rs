use crate::core::models::common::Status;
use crate::core::models::creative::CreativeFormat;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum FillPolicy {
    /// Only deal-backed direct campaigns. No open campaigns, no RTB.
    DealsOnly,
    /// All direct campaigns (open + deal-backed). No RTB.
    DirectOnly,
    /// All direct campaigns + RTB deals. No open RTB.
    DirectAndRtbDeals,
    /// Direct fills first, full RTB backfills remaining imps.
    RtbBackfill,
    /// All sources compete on price.
    HighestPrice,
}

/// Where and how the ad tag renders the creative on the page.
/// Serialized as an adjacently-tagged JSON object: { "type": "...", "config": { ... } }
/// so the JS tag can branch on `type` and forward `config` to the container constructor.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(tag = "type", content = "config", rename_all = "snake_case")]
pub enum ContainerType {
    /// Renders inline — appended after the calling <script> tag if on-page,
    /// or into document.body if running inside an ad-served iframe.
    #[default]
    InPlace,
    /// Renders into a specific DOM element identified by a CSS selector.
    /// Appends inside the target if it is a <div>; appends after it otherwise.
    InTarget(InTargetConfig),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InTargetConfig {
    pub selector: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Placement {
    pub id: String,
    pub pub_id: String,
    pub property_id: String,
    pub status: Status,
    pub name: String,
    pub fill_policy: FillPolicy,
    pub ad_units: Vec<CreativeFormat>,
    /// Defaults to InPlace if not configured.
    #[serde(default)]
    pub container: ContainerType,
    /// Publisher opt-in for expandable ads. Expandable creatives are only served
    /// when both this flag and render_caps.env_expandable are true.
    #[serde(default)]
    pub expandable: bool,
}
