use crate::core::config_manager::ConfigManager;
use crate::core::models::bidder::{Bidder, Endpoint};
use std::collections::HashMap;
use std::sync::Arc;

pub struct BidderManager {
    bidders: Vec<(Arc<Bidder>, Vec<Arc<Endpoint>>)>,
    bidder_index: HashMap<String, Arc<Bidder>>,
}

impl BidderManager {
    pub fn new(config_manager: &ConfigManager) -> Self {
        let bidder_endpoints: Vec<(Arc<Bidder>, Vec<Arc<Endpoint>>)> = config_manager
            .get()
            .bidders
            .iter()
            .map(|b| {
                (
                    Arc::new(b.bidder.clone()),
                    b.endpoints.iter().map(|e| Arc::new(e.clone())).collect(),
                )
            })
            .collect();

        let bidder_index: HashMap<String, Arc<Bidder>> = bidder_endpoints
            .iter()
            .map(|(bidder, _)| (bidder.id.clone(), Arc::clone(bidder)))
            .collect();

        BidderManager {
            bidders: bidder_endpoints,
            bidder_index,
        }
    }

    pub fn bidders_endpoints(&self) -> &Vec<(Arc<Bidder>, Vec<Arc<Endpoint>>)> {
        &self.bidders
    }

    pub fn bidders(&self) -> Vec<Arc<Bidder>> {
        self.bidders
            .iter()
            .map(|(bidder, _)| Arc::clone(bidder))
            .collect()
    }

    pub fn bidder(&self, bidder_id: &String) -> Option<Arc<Bidder>> {
        self.bidder_index.get(bidder_id).cloned()
    }
}
