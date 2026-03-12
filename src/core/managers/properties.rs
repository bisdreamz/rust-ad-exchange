use crate::core::models::property::Property;
use crate::core::providers::{Provider, ProviderEvent};
use anyhow::Error;
use arc_swap::ArcSwap;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info};

pub struct PropertyManager {
    properties: ArcSwap<HashMap<String, Arc<Property>>>,
}

impl PropertyManager {
    pub async fn start(provider: Arc<dyn Provider<Property>>) -> Result<Arc<Self>, Error> {
        let manager = Arc::new(Self {
            properties: ArcSwap::from_pointee(HashMap::new()),
        });

        let mgr = manager.clone();
        let initial = provider
            .start(Box::new(move |event| mgr.handle_event(event)))
            .await?;

        manager.load(initial);
        Ok(manager)
    }

    fn load(&self, properties: Vec<Property>) {
        let total = properties.len();
        let map: HashMap<String, Arc<Property>> = properties
            .into_iter()
            .map(|p| (p.id.clone(), Arc::new(p)))
            .collect();

        info!("Loaded {} properties", total);
        self.properties.store(Arc::new(map));
    }

    fn handle_event(&self, event: ProviderEvent<Property>) {
        match event {
            ProviderEvent::Added(p) => {
                debug!("Property added: {} ({})", p.name, p.id);
                let mut map = (*self.properties.load_full()).clone();
                map.insert(p.id.clone(), Arc::new(p));
                self.properties.store(Arc::new(map));
            }
            ProviderEvent::Modified(p) => {
                debug!("Property modified: {} ({})", p.name, p.id);
                let mut map = (*self.properties.load_full()).clone();
                map.insert(p.id.clone(), Arc::new(p));
                self.properties.store(Arc::new(map));
            }
            ProviderEvent::Removed(id) => {
                debug!("Property removed: {}", id);
                let mut map = (*self.properties.load_full()).clone();
                map.remove(&id);
                self.properties.store(Arc::new(map));
            }
        }
    }

    pub fn get(&self, id: &str) -> Option<Arc<Property>> {
        self.properties.load().get(id).cloned()
    }
}
