//! Definitions for creatives, used only
//! for direct, on-platform ran campaigns

use crate::core::models::common::Status;
use rtb::utils::adm::AdFormat;
use serde::{Deserialize, Serialize};

/// The raw underlying creative data
/// kind, separate from its conceptual
/// creative format
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum CreativeKind {
    Html,
    VastXml,
}

/// The canonical ad format type for the platform.
/// Separated from the raw underlying content
/// representation ['CreativeKind'] so custom ad
/// formats can be defined here, while logic later
/// can share the same html/vast content handling.
///
/// RTB bids produce [`AdFormat`] which maps into
/// this type via [`from_rtb`]. Direct campaign bids
/// carry this type directly via their creative.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum CreativeFormat {
    Banner { w: u32, h: u32 },
    Video,
    Native,
    Audio,
}

impl CreativeFormat {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Banner { .. } => "Banner",
            Self::Video => "Video",
            Self::Native => "Native",
            Self::Audio => "Audio",
        }
    }

    pub fn kind(&self) -> CreativeKind {
        match self {
            Self::Video | Self::Audio => CreativeKind::VastXml,
            _ => CreativeKind::Html,
        }
    }

    /// Construct from an RTB [`AdFormat`] with the bid's
    /// reported dimensions (relevant for banner).
    pub fn from_rtb(af: AdFormat, w: u32, h: u32) -> Self {
        match af {
            AdFormat::Banner => Self::Banner { w, h },
            AdFormat::Video => Self::Video,
            AdFormat::Native => Self::Native,
            AdFormat::Audio => Self::Audio,
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
