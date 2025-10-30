use crate::core::events::DataUrl;
use crate::core::events::billing::BillingEvent;
use std::sync::OnceLock;

pub struct BillingEventContext {
    /// The raw event url received
    pub event_url: String,
    /// The rich ['DataUrl'] extracted from the url, can carry extra task specific
    /// context e.g. traffic shaping params
    pub data_url: OnceLock<DataUrl>,
    /// The common event details
    pub details: OnceLock<BillingEvent>,
}

impl BillingEventContext {
    pub fn new(event_url: String) -> BillingEventContext {
        BillingEventContext {
            event_url,
            data_url: OnceLock::new(),
            details: OnceLock::new(),
        }
    }
}
