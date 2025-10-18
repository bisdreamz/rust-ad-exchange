use crate::core::config_manager::ConfigManager;
use crate::core::models::bidder::{Bidder, Endpoint};
use std::sync::Arc;

pub struct BidderManager {
    bidders: Vec<(Arc<Bidder>, Vec<Arc<Endpoint>>)>,
}

impl BidderManager {
    pub fn new(config_manager: &ConfigManager) -> Self {
        BidderManager {
            bidders: config_manager.get().bidders.iter().map(|b| {
                (Arc::new(b.bidder.clone()), b.endpoints.iter().map(|e| Arc::new(e.clone())).collect())
            }).collect(),
        }
    }

    pub fn bidders(&self) -> &Vec<(Arc<Bidder>, Vec<Arc<Endpoint>>)> {
        &self.bidders
    }
}