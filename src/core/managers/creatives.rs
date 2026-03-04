use crate::core::models::common::Status;
use crate::core::models::creative::Creative;
use crate::core::providers::{Provider, ProviderEvent};
use anyhow::Error;
use arc_swap::ArcSwap;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info};

struct CreativeCache {
    by_id: HashMap<String, Arc<Creative>>,
}

impl CreativeCache {
    fn build(creatives: impl IntoIterator<Item = Arc<Creative>>) -> Self {
        let mut by_id = HashMap::new();

        for creative in creatives {
            by_id.insert(creative.id.clone(), creative);
        }

        CreativeCache { by_id }
    }
}

pub struct CreativeManager {
    cache: ArcSwap<CreativeCache>,
}

impl CreativeManager {
    pub async fn start(provider: Arc<dyn Provider<Creative>>) -> Result<Arc<Self>, Error> {
        let manager = Arc::new(Self {
            cache: ArcSwap::from_pointee(CreativeCache {
                by_id: HashMap::new(),
            }),
        });

        let mgr = manager.clone();
        let initial = provider
            .start(Box::new(move |event| mgr.handle_event(event)))
            .await?;

        manager.load(initial);
        Ok(manager)
    }

    fn load(&self, creatives: Vec<Creative>) {
        let total = creatives.len();
        let cache = CreativeCache::build(creatives.into_iter().map(Arc::new));
        let active = cache
            .by_id
            .values()
            .filter(|c| c.status == Status::Active)
            .count();
        info!("Loaded {} active creatives (total: {})", active, total);
        self.cache.store(Arc::new(cache));
    }

    fn rebuild(&self, f: impl FnOnce(&mut HashMap<String, Arc<Creative>>)) {
        let prev = self.cache.load_full();
        let mut by_id = prev.by_id.clone();
        f(&mut by_id);
        let cache = CreativeCache::build(by_id.into_values());
        self.cache.store(Arc::new(cache));
    }

    fn handle_event(&self, event: ProviderEvent<Creative>) {
        match event {
            ProviderEvent::Added(c) | ProviderEvent::Modified(c) => {
                debug!("Creative updated: {}", c.id);
                self.rebuild(|m| {
                    m.insert(c.id.clone(), Arc::new(c));
                });
            }
            ProviderEvent::Removed(id) => {
                debug!("Creative removed: {}", id);
                self.rebuild(|m| {
                    m.remove(&id);
                });
            }
        }
    }

    /// Look up a creative by its ID. Used by the bid pipeline
    /// to resolve campaign.creatives[] entries.
    pub fn by_id(&self, id: &str) -> Option<Arc<Creative>> {
        self.cache.load().by_id.get(id).cloned()
    }
}
