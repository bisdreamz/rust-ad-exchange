use crate::core::models::campaign::Campaign;
use crate::core::models::common::Status;
use crate::core::providers::{Provider, ProviderEvent};
use anyhow::Error;
use arc_swap::ArcSwap;
use chrono::Utc;
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tracing::{debug, info, trace};

struct CampaignCache {
    by_id: HashMap<String, Arc<Campaign>>,
    /// Campaigns indexed by buyer_id. Direct deals carry
    /// buyer_ids — this index resolves them to campaigns
    /// belonging to that buyer without a full scan.
    by_buyer: HashMap<String, Vec<Arc<Campaign>>>,
    /// All campaigns for open matching (DirectMatchingTask iterates
    /// this for every policy except DealsOnly)
    all: Arc<Vec<Arc<Campaign>>>,
}

impl CampaignCache {
    fn build(campaigns: impl IntoIterator<Item = Arc<Campaign>>) -> Self {
        let mut by_id = HashMap::new();
        let mut by_buyer: HashMap<String, Vec<Arc<Campaign>>> = HashMap::new();
        let mut all = Vec::new();

        let now = Utc::now();

        for campaign in campaigns {
            by_id.insert(campaign.id.clone(), Arc::clone(&campaign));

            if campaign.status != Status::Active {
                trace!(campaign = %campaign.id, status = ?campaign.status, "Campaign skipped: inactive");
                continue;
            }

            if now < campaign.start_date {
                trace!(campaign = %campaign.id, start = %campaign.start_date, "Campaign skipped: not yet started");
                continue;
            }

            if now > campaign.end_date {
                trace!(campaign = %campaign.id, end = %campaign.end_date, "Campaign skipped: expired");
                continue;
            }

            by_buyer
                .entry(campaign.buyer_id.clone())
                .or_default()
                .push(Arc::clone(&campaign));
            all.push(campaign);
        }

        CampaignCache {
            by_id,
            by_buyer,
            all: Arc::new(all),
        }
    }
}

pub struct CampaignManager {
    cache: ArcSwap<CampaignCache>,
    /// Callbacks fired with IDs that were dropped from active indexes
    /// after a rebuild (expired, deactivated, or removed).
    on_expired_cbs: RwLock<Vec<Box<dyn Fn(&str) + Send + Sync>>>,
}

impl CampaignManager {
    pub async fn start(provider: Arc<dyn Provider<Campaign>>) -> Result<Arc<Self>, Error> {
        let manager = Arc::new(Self {
            cache: ArcSwap::from_pointee(CampaignCache {
                by_id: HashMap::new(),
                by_buyer: HashMap::new(),
                all: Arc::new(Vec::new()),
            }),
            on_expired_cbs: RwLock::new(Vec::new()),
        });

        let mgr = manager.clone();
        let initial = provider
            .start(Box::new(move |event| mgr.handle_event(event)))
            .await?;

        manager.load(initial);

        // Periodic sweep to expunge expired campaigns and activate future ones
        let mgr = manager.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(60));
            loop {
                interval.tick().await;
                mgr.refresh_indexes();
            }
        });

        Ok(manager)
    }

    /// Register a callback fired per-ID when a campaign leaves the active set
    /// (expired, deactivated, deleted). Used by pacers to evict stale state.
    pub fn on_expired(&self, cb: Box<dyn Fn(&str) + Send + Sync>) {
        self.on_expired_cbs.write().push(cb);
    }

    fn load(&self, campaigns: Vec<Campaign>) {
        let total = campaigns.len();
        let cache = CampaignCache::build(campaigns.into_iter().map(Arc::new));
        info!(
            "Loaded {} active campaigns across {} buyers (total: {})",
            cache.all.len(),
            cache.by_buyer.len(),
            total
        );
        self.cache.store(Arc::new(cache));
    }

    fn rebuild(&self, f: impl FnOnce(&mut HashMap<String, Arc<Campaign>>)) {
        let prev = self.cache.load_full();
        let prev_active: HashSet<String> = prev.all.iter().map(|c| c.id.clone()).collect();

        let mut by_id = prev.by_id.clone();
        f(&mut by_id);
        let cache = CampaignCache::build(by_id.into_values());
        let new_active: HashSet<String> = cache.all.iter().map(|c| c.id.clone()).collect();

        self.cache.store(Arc::new(cache));

        let cbs = self.on_expired_cbs.read();
        if !cbs.is_empty() {
            for id in prev_active.difference(&new_active) {
                debug!(campaign = %id, "Campaign left active set");
                for cb in cbs.iter() {
                    cb(id);
                }
            }
        }
    }

    fn handle_event(&self, event: ProviderEvent<Campaign>) {
        match event {
            ProviderEvent::Added(c) => {
                debug!(
                    "Campaign added: {} ({}) buyer={} budget={:.2}",
                    c.name, c.id, c.buyer_id, c.budget
                );
                self.rebuild(|m| {
                    m.insert(c.id.clone(), Arc::new(c));
                });
            }
            ProviderEvent::Modified(c) => {
                debug!(
                    "Campaign modified: {} ({}) status={:?}",
                    c.name, c.id, c.status
                );
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

    /// Re-derives indexes from the existing campaign set, dropping
    /// campaigns that have expired or aren't yet active based on
    /// current time. Called periodically by a background sweep.
    pub fn refresh_indexes(&self) {
        self.rebuild(|_| {});
    }

    /// Deal-mediated lookup: campaigns belonging to a specific buyer.
    /// Used when resolving DemandPolicy::Direct { buyer_ids } to campaigns.
    pub fn by_buyer(&self, buyer_id: &str) -> Vec<Arc<Campaign>> {
        self.cache
            .load()
            .by_buyer
            .get(buyer_id)
            .cloned()
            .unwrap_or_default()
    }

    /// Full campaign list for open matching. Used by DirectMatchingTask
    /// for all fill policies except DealsOnly. Arc clone is O(1).
    pub fn all(&self) -> Arc<Vec<Arc<Campaign>>> {
        Arc::clone(&self.cache.load().all)
    }
}
