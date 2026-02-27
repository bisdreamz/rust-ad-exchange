use crate::core::models::common::Status;
use crate::core::models::deal::{Deal, DemandPolicy};
use crate::core::providers::{Provider, ProviderEvent};
use anyhow::Error;
use arc_swap::ArcSwap;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::debug;

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
    fn build(deals: impl IntoIterator<Item = Arc<Deal>>) -> Self {
        let mut by_id = HashMap::new();
        let mut direct = Vec::new();
        let mut rtb_by_bidder: HashMap<String, BidderDeals> = HashMap::new();

        for deal in deals {
            by_id.insert(deal.id.clone(), Arc::clone(&deal));

            if deal.status != Status::Active {
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
}

impl DealManager {
    pub async fn start(provider: Arc<dyn Provider<Deal>>) -> Result<Arc<Self>, Error> {
        let manager = Arc::new(Self {
            cache: ArcSwap::from_pointee(DealCache {
                by_id: HashMap::new(),
                direct: Arc::new(Vec::new()),
                rtb_by_bidder: Arc::new(HashMap::new()),
            }),
        });

        let mgr = manager.clone();
        let initial = provider
            .start(Box::new(move |event| mgr.handle_event(event)))
            .await?;

        manager.load(initial);
        Ok(manager)
    }

    fn load(&self, deals: Vec<Deal>) {
        let cache = DealCache::build(deals.into_iter().map(Arc::new));
        self.cache.store(Arc::new(cache));
    }

    /// Clone-on-write rebuild: copies the id map, applies the mutation,
    /// then re-derives all index slices from scratch
    fn rebuild(&self, f: impl FnOnce(&mut HashMap<String, Arc<Deal>>)) {
        let prev = self.cache.load_full();
        let mut by_id = prev.by_id.clone();
        f(&mut by_id);
        let cache = DealCache::build(by_id.into_values());
        self.cache.store(Arc::new(cache));
    }

    fn handle_event(&self, event: ProviderEvent<Deal>) {
        match event {
            ProviderEvent::Added(d) | ProviderEvent::Modified(d) => {
                debug!("Deal updated: {}", d.id);
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
