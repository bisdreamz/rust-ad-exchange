use crate::core::models::campaign::Campaign;
use crate::core::models::common::Status;
use crate::core::providers::{Provider, ProviderEvent};
use anyhow::Error;
use arc_swap::ArcSwap;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::debug;

struct CampaignCache {
    by_id: HashMap<String, Arc<Campaign>>,
    /// Campaigns indexed by company_id. Direct deals carry
    /// company_ids — this index resolves them to campaigns
    /// belonging to that company without a full scan.
    by_company: HashMap<String, Vec<Arc<Campaign>>>,
    /// All campaigns for open matching (DirectMatchingTask iterates
    /// this for every policy except DealsOnly)
    all: Arc<Vec<Arc<Campaign>>>,
}

impl CampaignCache {
    fn build(campaigns: impl IntoIterator<Item = Arc<Campaign>>) -> Self {
        let mut by_id = HashMap::new();
        let mut by_company: HashMap<String, Vec<Arc<Campaign>>> = HashMap::new();
        let mut all = Vec::new();

        for campaign in campaigns {
            by_id.insert(campaign.id.clone(), Arc::clone(&campaign));

            if campaign.status != Status::Active {
                continue;
            }

            by_company
                .entry(campaign.company_id.clone())
                .or_default()
                .push(Arc::clone(&campaign));
            all.push(campaign);
        }

        CampaignCache {
            by_id,
            by_company,
            all: Arc::new(all),
        }
    }
}

pub struct CampaignManager {
    cache: ArcSwap<CampaignCache>,
}

impl CampaignManager {
    pub async fn start(provider: Arc<dyn Provider<Campaign>>) -> Result<Arc<Self>, Error> {
        let manager = Arc::new(Self {
            cache: ArcSwap::from_pointee(CampaignCache {
                by_id: HashMap::new(),
                by_company: HashMap::new(),
                all: Arc::new(Vec::new()),
            }),
        });

        let mgr = manager.clone();
        let initial = provider
            .start(Box::new(move |event| mgr.handle_event(event)))
            .await?;

        manager.load(initial);
        Ok(manager)
    }

    fn load(&self, campaigns: Vec<Campaign>) {
        let cache = CampaignCache::build(campaigns.into_iter().map(Arc::new));
        self.cache.store(Arc::new(cache));
    }

    fn rebuild(&self, f: impl FnOnce(&mut HashMap<String, Arc<Campaign>>)) {
        let prev = self.cache.load_full();
        let mut by_id = prev.by_id.clone();
        f(&mut by_id);
        let cache = CampaignCache::build(by_id.into_values());
        self.cache.store(Arc::new(cache));
    }

    fn handle_event(&self, event: ProviderEvent<Campaign>) {
        match event {
            ProviderEvent::Added(c) | ProviderEvent::Modified(c) => {
                debug!("Campaign updated: {}", c.id);
                self.rebuild(|m| {
                    m.insert(c.id.clone(), Arc::new(c));
                });
            }
            ProviderEvent::Removed(id) => {
                debug!("Campaign removed: {}", id);
                self.rebuild(|m| {
                    m.remove(&id);
                });
            }
        }
    }

    pub fn get(&self, id: &str) -> Option<Arc<Campaign>> {
        self.cache.load().by_id.get(id).cloned()
    }

    /// Deal-mediated lookup: campaigns belonging to a specific company.
    /// Used when resolving DemandPolicy::Direct { company_ids } to campaigns.
    pub fn by_company(&self, company_id: &str) -> Vec<Arc<Campaign>> {
        self.cache
            .load()
            .by_company
            .get(company_id)
            .cloned()
            .unwrap_or_default()
    }

    /// Full campaign list for open matching. Used by DirectMatchingTask
    /// for all fill policies except DealsOnly. Arc clone is O(1).
    pub fn all(&self) -> Arc<Vec<Arc<Campaign>>> {
        Arc::clone(&self.cache.load().all)
    }
}
