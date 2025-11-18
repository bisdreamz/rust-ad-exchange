use crate::core::demand::notifications::NoticeUrls;
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
    /// The extracted ['NoticeUrls'] from the billing event cache. This
    /// should always be extracted, even if the urls are None, because
    /// we also use the presence of this notice container as a de-dupe
    /// mechanism for incoming events. Then if URLs are present optionally,
    /// we will fire them for demand partners
    pub demand_urls: OnceLock<NoticeUrls>,
}

impl BillingEventContext {
    pub fn new(event_url: String) -> BillingEventContext {
        BillingEventContext {
            event_url,
            ..Default::default()
        }
    }
}
