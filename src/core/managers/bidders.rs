use std::sync::Arc;
use crate::core::config_manager::ConfigManager;
use crate::core::models::bidder::Bidder;

pub struct BidderManager {
    bidders: Arc<Vec<Bidder>>,
}

impl BidderManager {
    pub fn new(config_manager: &ConfigManager) -> Self {
        BidderManager {
            bidders: Arc::new(config_manager.get().bidders.clone()),
        }
    }

    pub fn bidders(&self) -> Arc<Vec<Bidder>> {
        self.bidders.clone()
    }
}