use crate::core::models::buyer::Buyer;
use crate::core::providers::{Provider, ProviderEvent};
use anyhow::Error;
use arc_swap::ArcSwap;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::debug;

pub struct BuyerManager {
    buyers: ArcSwap<HashMap<String, Arc<Buyer>>>,
}

impl BuyerManager {
    pub async fn start(provider: Arc<dyn Provider<Buyer>>) -> Result<Arc<Self>, Error> {
        let manager = Arc::new(Self {
            buyers: ArcSwap::from_pointee(HashMap::new()),
        });

        let mgr = manager.clone();
        let initial = provider
            .start(Box::new(move |event| mgr.handle_event(event)))
            .await?;

        manager.load(initial);
        Ok(manager)
    }

    fn load(&self, buyers: Vec<Buyer>) {
        let map: HashMap<String, Arc<Buyer>> = buyers
            .into_iter()
            .map(|b| (b.id.clone(), Arc::new(b)))
            .collect();

        self.buyers.store(Arc::new(map));
    }

    fn handle_event(&self, event: ProviderEvent<Buyer>) {
        match event {
            ProviderEvent::Added(b) | ProviderEvent::Modified(b) => {
                debug!("Buyer updated: {}", b.id);
                let mut map = (*self.buyers.load_full()).clone();
                map.insert(b.id.clone(), Arc::new(b));
                self.buyers.store(Arc::new(map));
            }
            ProviderEvent::Removed(id) => {
                debug!("Buyer removed: {}", id);
                let mut map = (*self.buyers.load_full()).clone();
                map.remove(&id);
                self.buyers.store(Arc::new(map));
            }
        }
    }

    pub fn get(&self, id: &str) -> Option<Arc<Buyer>> {
        self.buyers.load().get(id).cloned()
    }
}
