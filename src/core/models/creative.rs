//! Definitions for creatives, used only
//! for direct, on-platform ran campaigns

use crate::core::models::common::Status;
use serde::{Deserialize, Serialize};

/// The raw underlying creative data
/// kind, separate from its conceptual
/// creative format
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum CreativeKind {
    Html,
    VastXml,
}

/// The conceptual creative
/// format, e.g. banner, video.
/// This is separated from
/// the raw underlying content
/// representation ['CreativeKind']
/// so custom ad formats can be
/// defined here, but logic later
/// can share the same html/vast
/// or underlying content logic
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum CreativeFormat {
    Banner { w: u32, h: u32 },
    Video,
    // Native,
}

impl CreativeFormat {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Banner { .. } => "banner",
            Self::Video => "video",
        }
    }

    pub fn kind(&self) -> CreativeKind {
        match self {
            Self::Video => CreativeKind::VastXml,
            _ => CreativeKind::Html,
        }
    }
}

impl Into<CreativeKind> for CreativeFormat {
    fn into(self) -> CreativeKind {
        self.kind()
    }
}

/// Creative used for direct campaigns
/// managed via self platform
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Creative {
    pub status: Status,
    pub id: String,
    pub name: String,
    pub campaign_id: String,
    pub format: CreativeFormat,
    /// The raw creative content,
    /// kind of which indicated by ['CreativeFormat']
    pub content: String,
}
