use crate::app::pipeline::ortb::direct::pacing::SpendTracker;
use crate::core::providers::{Provider, ProviderEvent};
use ahash::AHashMap;
use anyhow::Error;
use parking_lot::RwLock;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, error, warn};

const MICROS_PER_DOLLAR: f64 = 1_000_000.0;

/// Reads campaign spend from the unbucketed `direct_spend_by_campaign`
/// collection written by CampaignCounterStore. Uses the standard
/// FirestoreProvider listener pattern — initial load then real-time
/// document change events.
///
/// Entirely decoupled from CampaignCounterStore — they share the
/// collection path, nothing else. The counter store writes; this
/// tracker reads.
pub struct FirestoreSpendTracker {
    spend: RwLock<AHashMap<String, u64>>,
}

/// Shape of the counter docs written by CampaignCounterStore.
/// Only the fields we need for spend tracking.
#[derive(Clone, Deserialize)]
pub struct SpendDoc {
    pub fields: HashMap<String, String>,
    pub stats: Option<SpendStats>,
}

#[derive(Clone, Deserialize)]
pub struct SpendStats {
    #[serde(default)]
    pub revenue_cpm_sum: f64,
}

impl FirestoreSpendTracker {
    /// Load initial spend docs and start listening for changes.
    pub async fn start(provider: Arc<dyn Provider<SpendDoc>>) -> Result<Arc<Self>, Error> {
        let tracker = Arc::new(Self {
            spend: RwLock::new(AHashMap::new()),
        });

        let t = tracker.clone();
        let initial = provider
            .start(Box::new(move |event| t.handle_event(event)))
            .await?;

        tracker.load(initial);
        Ok(tracker)
    }

    fn load(&self, docs: Vec<SpendDoc>) {
        let mut spend = AHashMap::new();

        for doc in &docs {
            let Some(campaign_id) = doc.fields.get("campaign_id") else {
                continue;
            };

            let micros = Self::extract_micros(doc);
            *spend.entry(campaign_id.clone()).or_default() += micros;
        }

        debug!("loaded spend for {} campaigns", spend.len());

        *self.spend.write() = spend;
    }

    fn handle_event(&self, event: ProviderEvent<SpendDoc>) {
        match event {
            ProviderEvent::Added(doc) | ProviderEvent::Modified(doc) => {
                let Some(campaign_id) = doc.fields.get("campaign_id") else {
                    error!(
                        "spend doc missing campaign_id — untracked spend, potential budget overspend"
                    );
                    return;
                };

                let micros = Self::extract_micros(&doc);
                self.spend.write().insert(campaign_id.clone(), micros);
            }
            ProviderEvent::Removed(doc_id) => {
                let mut spend = self.spend.write();
                let before = spend.len();
                spend.retain(|campaign_id, _| !doc_id.contains(campaign_id));
                let removed = before - spend.len();

                if removed == 0 {
                    warn!(
                        "spend doc removed but no matching campaign found: {}",
                        doc_id
                    );
                } else {
                    debug!("spend doc removed: {}, cleaned {} entries", doc_id, removed);
                }
            }
        }
    }

    fn extract_micros(doc: &SpendDoc) -> u64 {
        let cpm_sum = doc.stats.as_ref().map(|s| s.revenue_cpm_sum).unwrap_or(0.0);

        // revenue_cpm_sum is the sum of per-impression CPM rates.
        // Divide by 1000 to convert CPM-space → actual dollars.
        (cpm_sum / 1000.0 * MICROS_PER_DOLLAR) as u64
    }
}

impl SpendTracker for FirestoreSpendTracker {
    fn total_spend(&self, campaign_id: &str) -> Option<f64> {
        self.spend
            .read()
            .get(campaign_id)
            .map(|micros| *micros as f64 / MICROS_PER_DOLLAR)
    }

    /// No-op — spend arrives indirectly:
    /// `CampaignCounterStore` flushes `CampaignCounters` to the
    /// `pacing_campaigns` collection (unbucketed, one doc per
    /// campaign keyed by [buyer_id, campaign_id]).
    /// This tracker's Firestore listener on that same collection
    /// picks up the updated `revenue_cpm_sum` field via `SpendDoc`.
    fn record_spend(&self, _campaign_id: &str, _amount: f64) {}

    /// Campaigns are registered implicitly via the Firestore listener.
    /// Docs appearing in the `pacing_campaigns` collection are picked
    /// up by `handle_event` and inserted into the spend map.
    /// Explicit registration seeds the map for campaigns with no
    /// spend doc yet (new campaigns that haven't had a counter flush).
    fn register(&self, campaign_id: &str, initial_spend: f64) {
        let micros = (initial_spend * MICROS_PER_DOLLAR) as u64;
        self.spend
            .write()
            .entry(campaign_id.to_owned())
            .or_insert(micros);
    }
}
