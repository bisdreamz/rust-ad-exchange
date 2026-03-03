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
    /// Creatives indexed by campaign_id. After a campaign wins
    /// targeting + budget selection, its creatives are fetched
    /// here for format matching and rotation.
    by_campaign: HashMap<String, Vec<Arc<Creative>>>,
}

impl CreativeCache {
    fn build(creatives: impl IntoIterator<Item = Arc<Creative>>) -> Self {
        let mut by_id = HashMap::new();
        let mut by_campaign: HashMap<String, Vec<Arc<Creative>>> = HashMap::new();

        for creative in creatives {
            by_id.insert(creative.id.clone(), Arc::clone(&creative));

            if creative.status != Status::Active {
                continue;
            }

            by_campaign
                .entry(creative.campaign_id.clone())
                .or_default()
                .push(creative);
        }

        CreativeCache { by_id, by_campaign }
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
                by_campaign: HashMap::new(),
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
        info!(
            "Loaded {} active creatives across {} campaigns (total: {})",
            cache.by_campaign.values().map(|v| v.len()).sum::<usize>(),
            cache.by_campaign.len(),
            total
        );
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

    /// Creatives belonging to a campaign. Called after campaign
    /// wins targeting + budget selection, for format matching
    /// and creative rotation.
    pub fn by_campaign(&self, campaign_id: &str) -> Vec<Arc<Creative>> {
        self.cache
            .load()
            .by_campaign
            .get(campaign_id)
            .cloned()
            .unwrap_or_default()
    }
}
