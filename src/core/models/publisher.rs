use derive_builder::Builder;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Default, Builder)]
pub struct Publisher {
    pub id: String,
    pub enabled: bool,
    pub name: String,
    pub margin: u32,
    /// User sync return URL which should include
    /// the rx ID macro where our uid should go
    pub sync_url: Option<String>,
}
