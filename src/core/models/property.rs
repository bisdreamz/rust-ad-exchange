use crate::core::models::common::Status;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum PropertyKind {
    Site,
    App { bundle: String, store_url: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Property {
    pub id: String,
    pub pub_id: String,
    pub status: Status,
    pub name: String,
    pub kind: PropertyKind,
    pub domain: String,
    pub cats: Vec<String>,
    pub badv: Vec<String>,
    pub bcat: Vec<String>,
}
