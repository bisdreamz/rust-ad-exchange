use crate::core::models::publisher::Publisher;
use crate::core::providers::{Provider, ProviderEvent};
use anyhow::Error;
use arc_swap::ArcSwap;
use std::collections::HashMap;
use std::sync::Arc;

pub struct PublisherManager {
    pubs: ArcSwap<HashMap<String, Arc<Publisher>>>,
}

impl PublisherManager {
    pub async fn start(provider: Arc<dyn Provider<Publisher>>) -> Result<Arc<Self>, Error> {
        let manager = Arc::new(Self {
            pubs: ArcSwap::from_pointee(HashMap::new()),
        });

        let mgr = manager.clone();
        let initial = provider
            .start(Box::new(move |event| mgr.handle_event(event)))
            .await?;

        manager.load(initial);
        Ok(manager)
    }

    fn load(&self, publishers: Vec<Publisher>) {
        let map: HashMap<String, Arc<Publisher>> = publishers
            .into_iter()
            .map(|p| (p.id.clone(), Arc::new(p)))
            .collect();

        self.pubs.store(Arc::new(map));
    }

    fn handle_event(&self, event: ProviderEvent<Publisher>) {
        match event {
            ProviderEvent::Added(p) | ProviderEvent::Modified(p) => {
                let mut map = (*self.pubs.load_full()).clone();
                map.insert(p.id.clone(), Arc::new(p));
                self.pubs.store(Arc::new(map));
            }
            ProviderEvent::Removed(id) => {
                let mut map = (*self.pubs.load_full()).clone();
                map.remove(&id);
                self.pubs.store(Arc::new(map));
            }
        }
    }

    pub fn get(&self, id: &str) -> Option<Arc<Publisher>> {
        self.pubs.load().get(id).cloned()
    }
}
