use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use strum::{AsRefStr, Display, EnumString};
use crate::core::models::publisher::Publisher;

/// Represents the response action for how we should
/// respond to an incoming sync url request
#[derive(Debug, AsRefStr, EnumString, Display, Clone, PartialEq)]
pub enum SyncResponse {
    /// Respond with the attached HTML content, e,g,
    /// likely an iframe of partner syncs
    Content(String),
    /// An error occurred of some kind such as unknown
    /// pub id or other. Constant error string attached
    Error(String),
    /// If we have no matching partners to initiate sync with,
    /// just return a 204
    NoContent,
    // Redirect(String) if ever needed.. plain redirect etc
}

/// Context for pipeline which processes the initial call to us which
/// begins the syncing process from a supplier or ad markup drop
#[derive(Debug, Default)]
pub struct SyncOutContext {
    /// Publisher ID this sync init belongs to
    pub pubid: String,
    /// Publisher object extracted if pubid was valid
    pub publisher: OnceLock<Arc<Publisher>>,
    /// Cookies extracted from the http request
    pub cookies: HashMap<String, String>,
    /// The local user ID a.k.a our exchange ID for this user,
    /// as extracted from the cookies
    pub local_id: OnceLock<String>,
    /// The final ['SyncResponse'] which we should
    /// send as the http response
    pub response: OnceLock<SyncResponse>,
}

impl SyncOutContext {
    pub fn new(pubid: String, cookies: HashMap<String, String>) -> SyncOutContext {
        Self {
            pubid,
            cookies,
            ..Default::default()
        }
    }
}