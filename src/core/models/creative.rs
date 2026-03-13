//! Definitions for creatives, used only
//! for direct, on-platform ran campaigns

use crate::core::models::common::Status;
use rtb::utils::adm::AdFormat;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BannerSize {
    pub w: u32,
    pub h: u32,
}

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
#[serde(tag = "type")]
pub enum CreativeFormat {
    Banner {
        preferred_size: BannerSize,
        #[serde(default)]
        alternate_sizes: Vec<BannerSize>,
    },
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

    pub fn banner_supported_sizes(&self) -> Vec<BannerSize> {
        match self {
            Self::Banner {
                preferred_size,
                alternate_sizes,
            } => {
                let mut sizes = Vec::with_capacity(alternate_sizes.len() + 1);
                sizes.push(*preferred_size);
                sizes.extend(alternate_sizes.iter().copied());
                sizes
            }
            _ => Vec::new(),
        }
    }

    /// Construct from an RTB [`AdFormat`] with the bid's
    /// reported dimensions (relevant for banner).
    pub fn from_rtb(af: AdFormat, w: u32, h: u32) -> Self {
        match af {
            AdFormat::Banner => {
                let size = BannerSize { w, h };
                Self::Banner {
                    preferred_size: size,
                    alternate_sizes: vec![],
                }
            }
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
/// managed via self platform. Decoupled from campaigns —
/// owned by buyer_id with optional brand association.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Creative {
    pub status: Status,
    pub id: String,
    pub name: String,
    /// Optional brand association (advertiser)
    #[serde(default)]
    pub advertiser_id: Option<String>,
    /// Owning company
    pub buyer_id: String,
    pub format: CreativeFormat,
    /// The raw creative content,
    /// kind of which indicated by ['CreativeFormat']
    pub content: String,
}
