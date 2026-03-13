use super::delivery::write_delivery_states;
use crate::core::models::campaign::Campaign;
use crate::core::models::common::{DeliveryState, Status};
use crate::core::providers::{Provider, ProviderEvent};
use anyhow::Error;
use arc_swap::ArcSwap;
use chrono::Utc;
use firestore::FirestoreDb;
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tracing::{debug, info};

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
    fn build(
        campaigns: impl IntoIterator<Item = Arc<Campaign>>,
        budget_check: Option<&dyn Fn(&Campaign) -> DeliveryState>,
    ) -> (Self, Vec<(String, DeliveryState)>) {
        let mut by_id = HashMap::new();
        let mut by_buyer: HashMap<String, Vec<Arc<Campaign>>> = HashMap::new();
        let mut all = Vec::new();
        let mut state_changes = Vec::new();

        let now = Utc::now();

        for campaign in campaigns {
            if campaign.status != Status::Active {
                by_id.insert(campaign.id.clone(), campaign);
                continue;
            }

            if now < campaign.start_date {
                by_id.insert(campaign.id.clone(), campaign);
                continue;
            }

            if now > campaign.end_date {
                let new_state = DeliveryState::FlightEnded;
                let campaign = if campaign.delivery_state != new_state {
                    state_changes.push((campaign.id.clone(), new_state.clone()));
                    Arc::new(Campaign {
                        delivery_state: new_state,
                        ..(*campaign).clone()
                    })
                } else {
                    campaign
                };
                by_id.insert(campaign.id.clone(), campaign);
                continue;
            }

            // Budget check — skip exhausted campaigns from the hot set
            if let Some(check) = &budget_check {
                let new_state = check(&campaign);
                let campaign = if campaign.delivery_state != new_state {
                    state_changes.push((campaign.id.clone(), new_state.clone()));
                    Arc::new(Campaign {
                        delivery_state: new_state.clone(),
                        ..(*campaign).clone()
                    })
                } else {
                    campaign
                };
                match new_state {
                    DeliveryState::DailyBudgetExhausted | DeliveryState::TotalBudgetExhausted => {
                        by_id.insert(campaign.id.clone(), campaign);
                        continue;
                    }
                    _ => {
                        by_buyer
                            .entry(campaign.buyer_id.clone())
                            .or_default()
                            .push(Arc::clone(&campaign));
                        by_id.insert(campaign.id.clone(), Arc::clone(&campaign));
                        all.push(campaign);
                    }
                }
                continue;
            }

            by_buyer
                .entry(campaign.buyer_id.clone())
                .or_default()
                .push(Arc::clone(&campaign));
            by_id.insert(campaign.id.clone(), Arc::clone(&campaign));
            all.push(campaign);
        }

        (
            CampaignCache {
                by_id,
                by_buyer,
                all: Arc::new(all),
            },
            state_changes,
        )
    }
}

pub struct CampaignManager {
    cache: ArcSwap<CampaignCache>,
    /// Callbacks fired with IDs that were dropped from active indexes
    /// after a rebuild (expired, deactivated, or removed).
    on_expired_cbs: RwLock<Vec<Box<dyn Fn(&str) + Send + Sync>>>,
    firestore_db: Option<Arc<FirestoreDb>>,
}

impl CampaignManager {
    pub async fn start(
        provider: Arc<dyn Provider<Campaign>>,
        db: Option<Arc<FirestoreDb>>,
    ) -> Result<Arc<Self>, Error> {
        let manager = Arc::new(Self {
            cache: ArcSwap::from_pointee(CampaignCache {
                by_id: HashMap::new(),
                by_buyer: HashMap::new(),
                all: Arc::new(Vec::new()),
            }),
            on_expired_cbs: RwLock::new(Vec::new()),
            firestore_db: db,
        });

        let mgr = manager.clone();
        let initial = provider
            .start(Box::new(move |event| mgr.handle_event(event)))
            .await?;

        manager.load(initial);

        Ok(manager)
    }

    /// Register a callback fired per-ID when a campaign leaves the active set
    /// (expired, deactivated, deleted). Used by pacers to evict stale state.
    pub fn on_expired(&self, cb: Box<dyn Fn(&str) + Send + Sync>) {
        self.on_expired_cbs.write().push(cb);
    }

    fn load(&self, campaigns: Vec<Campaign>) {
        let total = campaigns.len();
        let (cache, _) = CampaignCache::build(campaigns.into_iter().map(Arc::new), None);
        info!(
            "Loaded {} active campaigns across {} buyers (total: {})",
            cache.all.len(),
            cache.by_buyer.len(),
            total
        );
        self.cache.store(Arc::new(cache));
    }

    fn rebuild(
        &self,
        f: impl FnOnce(&mut HashMap<String, Arc<Campaign>>),
        budget_check: Option<&dyn Fn(&Campaign) -> DeliveryState>,
    ) {
        let prev = self.cache.load_full();
        let prev_active: HashSet<String> = prev.all.iter().map(|c| c.id.clone()).collect();

        let mut by_id = prev.by_id.clone();
        f(&mut by_id);
        let (cache, state_changes) = CampaignCache::build(by_id.into_values(), budget_check);
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

        if !state_changes.is_empty() {
            if let Some(db) = &self.firestore_db {
                let db = db.clone();
                tokio::spawn(write_delivery_states(db, "campaigns", state_changes, true));
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
                self.rebuild(
                    |m| {
                        m.insert(c.id.clone(), Arc::new(c));
                    },
                    None,
                );
            }
            ProviderEvent::Modified(c) => {
                debug!(
                    "Campaign modified: {} ({}) status={:?}",
                    c.name, c.id, c.status
                );
                self.rebuild(
                    |m| {
                        m.insert(c.id.clone(), Arc::new(c));
                    },
                    None,
                );
            }
            ProviderEvent::Removed(id) => {
                debug!("Campaign removed: {}", id);
                self.rebuild(
                    |m| {
                        m.remove(&id);
                    },
                    None,
                );
            }
        }
    }

    /// Periodic refresh with budget checking. Drops expired/future
    /// campaigns and filters budget-exhausted ones from the hot set.
    /// Writes delivery state changes to Firestore.
    pub fn refresh_with_budget(&self, budget_check: &dyn Fn(&Campaign) -> DeliveryState) {
        self.rebuild(|_| {}, Some(budget_check));
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
