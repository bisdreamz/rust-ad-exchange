use crate::core::models::placement::ContainerType;
use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct AdTagResponse {
    pub sync_frame_url: Option<String>,
    pub bid: Option<TagBid>,
}

#[derive(Debug, Serialize)]
pub struct TagBid {
    pub container: ContainerType,
    pub content: TagBidContent,
    pub expandable: bool,
}

/// Ad content delivered to the tag for rendering.
/// Direct custom/fluid ads use Banner with expandable: true on the enclosing TagBid.
#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TagBidContent {
    Banner { content: String, w: u32, h: u32 },
    Video { vast: String, w: u32, h: u32 },
    Audio { vast: String },
}
