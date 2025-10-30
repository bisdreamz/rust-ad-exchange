use crate::core::config_manager::ConfigManager;
use crate::core::models::publisher::Publisher;
use arc_swap::ArcSwap;
use std::collections::HashMap;
use std::sync::Arc;

pub struct PublisherManager {
    pubs: ArcSwap<HashMap<String, Arc<Publisher>>>,
}

impl PublisherManager {
    pub fn new(config_manager: &ConfigManager) -> Self {
        let mut map = HashMap::new();
        config_manager
            .get()
            .publishers
            .iter()
            .for_each(|publisher| {
                map.insert(publisher.id.clone(), Arc::new(publisher.clone()));
            });

        PublisherManager {
            pubs: ArcSwap::from_pointee(map),
        }
    }

    pub fn get(&self, id: &String) -> Option<Arc<Publisher>> {
        match self.pubs.load().get(id) {
            Some(publisher) => Some(publisher.clone()),
            None => None,
        }
    }
}
