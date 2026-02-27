use crate::app::pipeline::ortb::direct::pacing::DealImpressionTracker;
use crate::core::providers::{Provider, ProviderEvent};
use ahash::AHashMap;
use anyhow::Error;
use parking_lot::RwLock;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, error, warn};

/// Reads deal impression counts from Firestore, fed by `DealCounterStore`
/// writing to the `pacing_deals` collection.
///
/// Write path: billing event → `RecordCampaignBillingCountersTask` →
/// `DealCounterStore::merge()` → Firestore flush → listener here updates `persisted`.
///
/// Deals with no pacing doc yet return 0.
pub struct FirestoreDealTracker {
    /// deal_id → total impressions from Firestore
    persisted: RwLock<AHashMap<String, u64>>,
}

/// Shape of the counter docs written by DealCounterStore.
#[derive(Clone, Deserialize)]
pub struct DealPacingDoc {
    pub fields: HashMap<String, String>,
    pub stats: Option<DealPacingStats>,
}

#[derive(Clone, Deserialize)]
pub struct DealPacingStats {
    #[serde(default)]
    pub impressions: u64,
}

impl FirestoreDealTracker {
    pub async fn start(provider: Arc<dyn Provider<DealPacingDoc>>) -> Result<Arc<Self>, Error> {
        let tracker = Arc::new(Self {
            persisted: RwLock::new(AHashMap::new()),
        });

        let t = tracker.clone();
        let initial = provider
            .start(Box::new(move |event| t.handle_event(event)))
            .await?;

        tracker.load(initial);
        Ok(tracker)
    }

    fn load(&self, docs: Vec<DealPacingDoc>) {
        let mut imps = AHashMap::new();

        for doc in &docs {
            let Some(deal_id) = doc.fields.get("deal_id") else {
                continue;
            };

            let count = doc.stats.as_ref().map(|s| s.impressions).unwrap_or(0);

            *imps.entry(deal_id.clone()).or_default() += count;
        }

        debug!("loaded impression counts for {} deals", imps.len());
        *self.persisted.write() = imps;
    }

    fn handle_event(&self, event: ProviderEvent<DealPacingDoc>) {
        match event {
            ProviderEvent::Added(doc) | ProviderEvent::Modified(doc) => {
                let Some(deal_id) = doc.fields.get("deal_id") else {
                    error!("deal pacing doc missing deal_id");
                    return;
                };

                let count = doc.stats.as_ref().map(|s| s.impressions).unwrap_or(0);

                self.persisted.write().insert(deal_id.clone(), count);
            }
            ProviderEvent::Removed(doc_id) => {
                let mut imps = self.persisted.write();
                let before = imps.len();
                imps.retain(|deal_id, _| !doc_id.contains(deal_id));
                let removed = before - imps.len();

                if removed == 0 {
                    warn!(
                        "deal pacing doc removed but no matching deal found: {}",
                        doc_id
                    );
                } else {
                    debug!(
                        "deal pacing doc removed: {}, cleaned {} entries",
                        doc_id, removed
                    );
                }
            }
        }
    }
}

impl DealImpressionTracker for FirestoreDealTracker {
    fn total_impressions(&self, deal_id: &str) -> u64 {
        self.persisted.read().get(deal_id).copied().unwrap_or(0)
    }

    /// No-op — persistence handled by `DealCounterStore` via billing pipeline.
    fn record_impression(&self, _deal_id: &str) {}
}
