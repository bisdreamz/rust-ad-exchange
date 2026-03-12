use crate::core::models::placement::Placement;
use crate::core::providers::{Provider, ProviderEvent};
use anyhow::Error;
use arc_swap::ArcSwap;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info};

pub struct PlacementManager {
    placements: ArcSwap<HashMap<String, Arc<Placement>>>,
}

impl PlacementManager {
    pub async fn start(provider: Arc<dyn Provider<Placement>>) -> Result<Arc<Self>, Error> {
        let manager = Arc::new(Self {
            placements: ArcSwap::from_pointee(HashMap::new()),
        });

        let mgr = manager.clone();
        let initial = provider
            .start(Box::new(move |event| mgr.handle_event(event)))
            .await?;

        manager.load(initial);
        Ok(manager)
    }

    fn load(&self, placements: Vec<Placement>) {
        let total = placements.len();
        let map: HashMap<String, Arc<Placement>> = placements
            .into_iter()
            .map(|p| (p.id.clone(), Arc::new(p)))
            .collect();

        info!("Loaded {} placements", total);
        self.placements.store(Arc::new(map));
    }

    fn handle_event(&self, event: ProviderEvent<Placement>) {
        match event {
            ProviderEvent::Added(p) => {
                debug!("Placement added: {} ({})", p.name, p.id);
                let mut map = (*self.placements.load_full()).clone();
                map.insert(p.id.clone(), Arc::new(p));
                self.placements.store(Arc::new(map));
            }
            ProviderEvent::Modified(p) => {
                debug!("Placement modified: {} ({})", p.name, p.id);
                let mut map = (*self.placements.load_full()).clone();
                map.insert(p.id.clone(), Arc::new(p));
                self.placements.store(Arc::new(map));
            }
            ProviderEvent::Removed(id) => {
                debug!("Placement removed: {}", id);
                let mut map = (*self.placements.load_full()).clone();
                map.remove(&id);
                self.placements.store(Arc::new(map));
            }
        }
    }

    pub fn get(&self, id: &str) -> Option<Arc<Placement>> {
        self.placements.load().get(id).cloned()
    }
}
