use crate::core::models::bidder::Bidder;
use crate::core::usersync::model::SyncInEvent;
use rtb::common::DataUrl;
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

/// Context for pipeline when ingests incoming syncs to us from
/// partners sending us their buyeruid values, in which we host
/// the match table. Often, this is demand partners returning a
/// call to us with their buyeruid value after we have initiated
/// sync call to them
#[derive(Debug, Default)]
pub struct SyncInContext {
    /// The raw event url received before parsing or validation
    pub event_url: String,
    /// Cookies extracted from the http request
    pub cookies: HashMap<String, String>,
    /// The raw bidder id extracted from the http req url but not yet validated
    /// dynamic values may be extracted
    pub data_url: OnceLock<DataUrl>,
    /// The validated and recognized bidder object. If present, the bidder
    /// id value in the incoming sync url was a valid partner id
    pub bidder: OnceLock<Arc<Bidder>>,
    /// The local user ID (rxid) extracted from cookies or newly assigned
    pub local_uid: OnceLock<String>,
    /// The validated and parsed ['SyncInEvent'] from the
    /// inbound data url
    pub event: OnceLock<SyncInEvent>,
}

impl SyncInContext {
    pub fn new(event_url: String, cookies: HashMap<String, String>) -> Self {
        Self {
            event_url,
            cookies,
            ..Default::default()
        }
    }
}
