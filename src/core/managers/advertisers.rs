use crate::core::models::advertiser::Advertiser;
use crate::core::providers::{Provider, ProviderEvent};
use anyhow::Error;
use arc_swap::ArcSwap;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::debug;

struct AdvertiserCache {
    by_id: HashMap<String, Arc<Advertiser>>,
    by_buyer: HashMap<String, Vec<Arc<Advertiser>>>,
}

impl AdvertiserCache {
    fn build(advertisers: impl IntoIterator<Item = Arc<Advertiser>>) -> Self {
        let mut by_id = HashMap::new();
        let mut by_buyer: HashMap<String, Vec<Arc<Advertiser>>> = HashMap::new();

        for adv in advertisers {
            by_id.insert(adv.id.clone(), Arc::clone(&adv));
            by_buyer
                .entry(adv.buyer_id.clone())
                .or_default()
                .push(adv);
        }

        AdvertiserCache { by_id, by_buyer }
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
                by_buyer: HashMap::new(),
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
        let cache = AdvertiserCache::build(advertisers.into_iter().map(Arc::new));
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
            ProviderEvent::Added(a) | ProviderEvent::Modified(a) => {
                debug!("Advertiser updated: {}", a.id);
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

    pub fn by_buyer(&self, buyer_id: &str) -> Vec<Arc<Advertiser>> {
        self.cache
            .load()
            .by_buyer
            .get(buyer_id)
            .cloned()
            .unwrap_or_default()
    }
}
