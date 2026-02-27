use crate::core::demand::notifications::CachedBidNotice;
use crate::core::events::billing::BillingEvent;
use rtb::common::DataUrl;
use std::sync::OnceLock;

#[derive(Debug, Default)]
pub struct BillingEventContext {
    /// The raw event url received
    pub event_url: String,
    /// The rich ['DataUrl'] extracted from the url, can carry extra task specific
    /// context e.g. traffic shaping params
    pub data_url: OnceLock<DataUrl>,
    /// The common event details
    pub details: OnceLock<BillingEvent>,
    /// The cached bid notice extracted from the event cache.
    /// Presence indicates a valid (non-expired, non-duplicate) event.
    /// Contains demand notice URLs, optional direct campaign details,
    /// and optional deal context (for both direct and RTB bids).
    pub bid_notice: OnceLock<CachedBidNotice>,
    /// If this event is considered expired, by way of
    /// either duplicate fire or TTL expiry
    pub expired: OnceLock<bool>,
}

impl BillingEventContext {
    pub fn new(event_url: String) -> BillingEventContext {
        BillingEventContext {
            event_url,
            ..Default::default()
        }
    }
}
