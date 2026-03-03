use crate::core::models::advertiser::Advertiser;
use crate::core::providers::{Provider, ProviderEvent};
use anyhow::Error;
use arc_swap::ArcSwap;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info};

struct AdvertiserCache {
    by_id: HashMap<String, Arc<Advertiser>>,
}

impl AdvertiserCache {
    fn build(advertisers: impl IntoIterator<Item = Arc<Advertiser>>) -> Self {
        let mut by_id = HashMap::new();

        for adv in advertisers {
            by_id.insert(adv.id.clone(), adv);
        }

        AdvertiserCache { by_id }
    }
}

pub struct AdvertiserManager {
    cache: ArcSwap<AdvertiserCache>,
}

impl AdvertiserManager {
    pub async fn start(provider: Arc<dyn Provider<Advertiser>>) -> Result<Arc<Self>, Error> {
        let manager = Arc::new(Self {
            cache: ArcSwap::from_pointee(AdvertiserCache {
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

    fn load(&self, advertisers: Vec<Advertiser>) {
        let total = advertisers.len();
        let cache = AdvertiserCache::build(advertisers.into_iter().map(Arc::new));
        info!("Loaded {} advertisers", total);
        self.cache.store(Arc::new(cache));
    }

    fn rebuild(&self, f: impl FnOnce(&mut HashMap<String, Arc<Advertiser>>)) {
        let prev = self.cache.load_full();
        let mut by_id = prev.by_id.clone();
        f(&mut by_id);
        let cache = AdvertiserCache::build(by_id.into_values());
        self.cache.store(Arc::new(cache));
    }

    fn handle_event(&self, event: ProviderEvent<Advertiser>) {
        match event {
            ProviderEvent::Added(a) => {
                debug!(
                    "Advertiser added: {} ({}) buyer={}",
                    a.brand, a.id, a.buyer_id
                );
                self.rebuild(|m| {
                    m.insert(a.id.clone(), Arc::new(a));
                });
            }
            ProviderEvent::Modified(a) => {
                debug!(
                    "Advertiser modified: {} ({}) buyer={}",
                    a.brand, a.id, a.buyer_id
                );
                self.rebuild(|m| {
                    m.insert(a.id.clone(), Arc::new(a));
                });
            }
            ProviderEvent::Removed(id) => {
                debug!("Advertiser removed: {}", id);
                self.rebuild(|m| {
                    m.remove(&id);
                });
            }
        }
    }

    pub fn get(&self, id: &str) -> Option<Arc<Advertiser>> {
        self.cache.load().by_id.get(id).cloned()
    }
}
