use derive_builder::Builder;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub enum PublisherType {
    #[default]
    Publisher,
    Intermediary,
    Both,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, Builder)]
pub struct Publisher {
    pub id: String,
    /// Publisher domain, used in sellers.json
    pub domain: String,
    pub seller_type: PublisherType,
    pub enabled: bool,
    pub name: String,
    pub margin: u32,
    /// User sync return URL which should include
    /// the rx ID macro where our uid should go
    pub sync_url: Option<String>,
}
