use crate::app::pipeline::ortb::direct::pacing::SpendTracker;
use crate::app::pipeline::ortb::direct::pacing::daily_map::{DailyMap, is_bucket_today};
use crate::core::providers::{Provider, ProviderEvent};
use ahash::AHashMap;
use anyhow::Error;
use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, error, warn};

/// Precision multiplier for atomic integer storage.
/// CPM values are stored as `cpm * MICROS` for u64 atomics.
const MICROS: f64 = 1_000_000.0;

/// Reads campaign spend from the unbucketed `direct_spend_by_campaign`
/// collection written by CampaignCounterStore. Uses the standard
/// FirestoreProvider listener pattern — initial load then real-time
/// document change events.
///
/// Values are stored and returned as **CPM sums** — the sum of
/// per-impression CPM rates. Actual dollars = cpm_sum / 1000.
/// Internally uses CPM-micros (u64) for atomic precision.
///
/// Entirely decoupled from CampaignCounterStore — they share the
/// collection path, nothing else. The counter store writes; this
/// tracker reads.
pub struct FirestoreSpendTracker {
    /// Lifetime (total) spend per campaign — from unbucketed collection
    spend: RwLock<AHashMap<String, u64>>,
    /// Today's spend per campaign — from daily-bucketed collection
    daily: DailyMap,
}

/// Shape of the counter docs written by CampaignCounterStore.
/// Only the fields we need for spend tracking.
#[derive(Clone, Deserialize)]
pub struct SpendDoc {
    pub fields: HashMap<String, String>,
    pub stats: Option<SpendStats>,
    #[serde(
        default,
        deserialize_with = "firestore::serialize_as_optional_timestamp::deserialize"
    )]
    pub bucket: Option<DateTime<Utc>>,
}

#[derive(Clone, Deserialize)]
pub struct SpendStats {
    #[serde(default)]
    pub revenue_cpm_sum: f64,
}

impl FirestoreSpendTracker {
    /// Load initial spend docs and start listening for changes.
    /// `provider` reads the unbucketed lifetime collection.
    /// `daily_provider` reads the daily-bucketed collection.
    pub async fn start(
        provider: Arc<dyn Provider<SpendDoc>>,
        daily_provider: Arc<dyn Provider<SpendDoc>>,
    ) -> Result<Arc<Self>, Error> {
        let tracker = Arc::new(Self {
            spend: RwLock::new(AHashMap::new()),
            daily: DailyMap::new(),
        });

        let t = tracker.clone();
        let initial = provider
            .start(Box::new(move |event| t.handle_event(event)))
            .await?;

        tracker.load(initial);

        let t = tracker.clone();
        let initial_daily = daily_provider
            .start(Box::new(move |event| t.handle_daily_event(event)))
            .await?;

        tracker.load_daily(initial_daily);

        Ok(tracker)
    }

    fn load(&self, docs: Vec<SpendDoc>) {
        let mut spend = AHashMap::new();

        for doc in &docs {
            let Some(campaign_id) = doc.fields.get("campaign_id") else {
                continue;
            };

            let micros = Self::extract_cpm_micros(doc);
            *spend.entry(campaign_id.clone()).or_default() += micros;
        }

        debug!("loaded spend for {} campaigns", spend.len());

        *self.spend.write() = spend;
    }

    /// Load daily docs, skipping any whose bucket date is not today.
    fn load_daily(&self, docs: Vec<SpendDoc>) {
        let entries = docs.iter().filter_map(|doc| {
            if let Some(bucket) = &doc.bucket {
                if !is_bucket_today(bucket) {
                    return None;
                }
            }
            let campaign_id = doc.fields.get("campaign_id")?;
            Some((campaign_id.clone(), Self::extract_cpm_micros(doc)))
        });
        self.daily.load(entries, "spend");
    }

    fn handle_event(&self, event: ProviderEvent<SpendDoc>) {
        Self::apply_lifetime_event(&self.spend, event);
    }

    fn handle_daily_event(&self, event: ProviderEvent<SpendDoc>) {
        match &event {
            ProviderEvent::Added(doc) | ProviderEvent::Modified(doc) => {
                if let Some(bucket) = &doc.bucket {
                    if !is_bucket_today(bucket) {
                        return;
                    }
                }
                let Some(campaign_id) = doc.fields.get("campaign_id") else {
                    error!(
                        "spend doc missing campaign_id — untracked spend, potential budget overspend"
                    );
                    return;
                };
                self.daily
                    .insert(campaign_id.clone(), Self::extract_cpm_micros(doc));
            }
            ProviderEvent::Removed(doc_id) => {
                self.daily.remove_matching(doc_id);
            }
        }
    }

    fn apply_lifetime_event(
        target: &RwLock<AHashMap<String, u64>>,
        event: ProviderEvent<SpendDoc>,
    ) {
        match event {
            ProviderEvent::Added(doc) | ProviderEvent::Modified(doc) => {
                let Some(campaign_id) = doc.fields.get("campaign_id") else {
                    error!(
                        "spend doc missing campaign_id — untracked spend, potential budget overspend"
                    );
                    return;
                };

                let micros = Self::extract_cpm_micros(&doc);
                target.write().insert(campaign_id.clone(), micros);
            }
            ProviderEvent::Removed(doc_id) => {
                let mut spend = target.write();
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

    fn extract_cpm_micros(doc: &SpendDoc) -> u64 {
        let cpm_sum = doc.stats.as_ref().map(|s| s.revenue_cpm_sum).unwrap_or(0.0);
        (cpm_sum * MICROS) as u64
    }
}

impl SpendTracker for FirestoreSpendTracker {
    fn total_spend(&self, campaign_id: &str) -> f64 {
        self.spend
            .read()
            .get(campaign_id)
            .map(|micros| *micros as f64 / MICROS)
            .unwrap_or(0.0)
    }

    fn daily_spend(&self, campaign_id: &str) -> f64 {
        self.daily.get(campaign_id) as f64 / MICROS
    }

    /// No-op — spend arrives indirectly via Firestore listeners.
    fn record_spend(&self, _campaign_id: &str, _amount: f64) {}
}
