use crate::core::models::common::Status;
use crate::core::models::deal::{Deal, DemandPolicy};
use crate::core::providers::{Provider, ProviderEvent};
use anyhow::Error;
use arc_swap::ArcSwap;
use chrono::Utc;
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tracing::{debug, info, trace};

/// Per-bidder RTB deals pre-split by private/open at load time
/// so match-time never filters on the private flag
#[derive(Debug, Clone, Default)]
pub struct BidderDeals {
    pub private: Vec<Arc<Deal>>,
    pub open: Vec<Arc<Deal>>,
}

struct DealCache {
    by_id: HashMap<String, Arc<Deal>>,
    direct: Arc<Vec<Arc<Deal>>>,
    /// RTB deals indexed by dsp_id, pre-split into private vs open.
    /// A deal appears under every dsp_id in its wdsps list.
    rtb_by_bidder: Arc<HashMap<String, BidderDeals>>,
}

impl DealCache {
    /// Collect all deal IDs that passed status/date filtering.
    fn active_ids(&self) -> HashSet<String> {
        let mut ids: HashSet<String> = self.direct.iter().map(|d| d.id.clone()).collect();
        for bidder_deals in self.rtb_by_bidder.values() {
            for d in &bidder_deals.private {
                ids.insert(d.id.clone());
            }
            for d in &bidder_deals.open {
                ids.insert(d.id.clone());
            }
        }
        ids
    }
}

impl DealCache {
    fn build(deals: impl IntoIterator<Item = Arc<Deal>>) -> Self {
        let mut by_id = HashMap::new();
        let mut direct = Vec::new();
        let mut rtb_by_bidder: HashMap<String, BidderDeals> = HashMap::new();
        let now = Utc::now();

        for deal in deals {
            by_id.insert(deal.id.clone(), Arc::clone(&deal));

            if deal.status != Status::Active {
                trace!(deal = %deal.id, status = ?deal.status, "Deal skipped: inactive");
                continue;
            }

            if deal.start_date.is_some_and(|s| now < s) {
                trace!(deal = %deal.id, start = %deal.start_date.unwrap(), "Deal skipped: not yet started");
                continue;
            }

            if deal.end_date.is_some_and(|e| now > e) {
                trace!(deal = %deal.id, end = %deal.end_date.unwrap(), "Deal skipped: expired");
                continue;
            }

            match &deal.policy {
                DemandPolicy::Direct { .. } => direct.push(Arc::clone(&deal)),
                DemandPolicy::Rtb { wdsps, private } => {
                    for (dsp_id, _wseats) in wdsps {
                        let entry = rtb_by_bidder.entry(dsp_id.clone()).or_default();
                        if *private {
                            entry.private.push(Arc::clone(&deal));
                        } else {
                            entry.open.push(Arc::clone(&deal));
                        }
                    }
                }
            }
        }

        DealCache {
            by_id,
            direct: Arc::new(direct),
            rtb_by_bidder: Arc::new(rtb_by_bidder),
        }
    }
}

pub struct DealManager {
    cache: ArcSwap<DealCache>,
    on_expired_cbs: RwLock<Vec<Box<dyn Fn(&str) + Send + Sync>>>,
}

impl DealManager {
    pub async fn start(provider: Arc<dyn Provider<Deal>>) -> Result<Arc<Self>, Error> {
        let manager = Arc::new(Self {
            cache: ArcSwap::from_pointee(DealCache {
                by_id: HashMap::new(),
                direct: Arc::new(Vec::new()),
                rtb_by_bidder: Arc::new(HashMap::new()),
            }),
            on_expired_cbs: RwLock::new(Vec::new()),
        });

        let mgr = manager.clone();
        let initial = provider
            .start(Box::new(move |event| mgr.handle_event(event)))
            .await?;

        manager.load(initial);

        // Periodic sweep to expunge expired deals and activate future deals
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

    fn load(&self, deals: Vec<Deal>) {
        let total = deals.len();
        let cache = DealCache::build(deals.into_iter().map(Arc::new));
        let rtb_count: usize = cache
            .rtb_by_bidder
            .values()
            .map(|b| b.private.len() + b.open.len())
            .sum();
        info!(
            "Loaded {} direct deals, {} RTB deals across {} bidders (total: {})",
            cache.direct.len(),
            rtb_count,
            cache.rtb_by_bidder.len(),
            total
        );
        self.cache.store(Arc::new(cache));
    }

    /// Register a callback fired per-ID when a deal leaves the active set
    /// (expired, deactivated, deleted). Used by pacers to evict stale state.
    pub fn on_expired(&self, cb: Box<dyn Fn(&str) + Send + Sync>) {
        self.on_expired_cbs.write().push(cb);
    }

    /// Clone-on-write rebuild: copies the id map, applies the mutation,
    /// then re-derives all index slices from scratch.
    fn rebuild(&self, f: impl FnOnce(&mut HashMap<String, Arc<Deal>>)) {
        let prev = self.cache.load_full();
        let prev_active = prev.active_ids();

        let mut by_id = prev.by_id.clone();
        f(&mut by_id);
        let cache = DealCache::build(by_id.into_values());
        let new_active = cache.active_ids();

        self.cache.store(Arc::new(cache));

        let cbs = self.on_expired_cbs.read();
        if !cbs.is_empty() {
            for id in prev_active.difference(&new_active) {
                debug!(deal = %id, "Deal left active set");
                for cb in cbs.iter() {
                    cb(id);
                }
            }
        }
    }

    fn handle_event(&self, event: ProviderEvent<Deal>) {
        match event {
            ProviderEvent::Added(d) => {
                debug!("Deal added: {} ({}) policy={:?}", d.name, d.id, d.policy);
                self.rebuild(|m| {
                    m.insert(d.id.clone(), Arc::new(d));
                });
            }
            ProviderEvent::Modified(d) => {
                debug!("Deal modified: {} ({}) status={:?}", d.name, d.id, d.status);
                self.rebuild(|m| {
                    m.insert(d.id.clone(), Arc::new(d));
                });
            }
            ProviderEvent::Removed(id) => {
                debug!("Deal removed: {}", id);
                self.rebuild(|m| {
                    m.remove(&id);
                });
            }
        }
    }

    /// Re-derives indexes from the existing deal set, dropping deals
    /// that have expired or aren't yet active based on current time.
    /// Called periodically by a background sweep.
    pub fn refresh_indexes(&self) {
        self.rebuild(|_| {});
    }

    pub fn get(&self, id: &str) -> Option<Arc<Deal>> {
        self.cache.load().by_id.get(id).cloned()
    }

    pub fn direct_deals(&self) -> Arc<Vec<Arc<Deal>>> {
        Arc::clone(&self.cache.load().direct)
    }

    /// RTB deals for a specific bidder (dsp_id), pre-split by private/open.
    /// Check private first — if any match the imp, skip open deals for
    /// that bidder+imp combination.
    pub fn rtb_deals_for_bidder(&self, dsp_id: &str) -> Option<BidderDeals> {
        self.cache.load().rtb_by_bidder.get(dsp_id).cloned()
    }
}
