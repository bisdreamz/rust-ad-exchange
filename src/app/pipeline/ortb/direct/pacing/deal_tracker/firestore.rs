use crate::app::pipeline::ortb::direct::pacing::DealImpressionTracker;
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

/// Reads deal impression counts from Firestore, fed by `DealCounterStore`
/// writing to the `pacing_deals` collection.
///
/// Write path: billing event → `RecordCampaignBillingCountersTask` →
/// `DealCounterStore::merge()` → Firestore flush → listener here updates `persisted`.
///
/// Deals with no pacing doc yet return 0.
pub struct FirestoreDealTracker {
    /// deal_id → total impressions from Firestore (lifetime)
    persisted: RwLock<AHashMap<String, u64>>,
    /// Today's impressions — auto-resets at midnight UTC
    daily: DailyMap,
}

/// Shape of the counter docs written by DealCounterStore.
#[derive(Clone, Deserialize)]
pub struct DealPacingDoc {
    pub fields: HashMap<String, String>,
    pub stats: Option<DealPacingStats>,
    #[serde(
        default,
        deserialize_with = "firestore::serialize_as_optional_timestamp::deserialize"
    )]
    pub bucket: Option<DateTime<Utc>>,
}

#[derive(Clone, Deserialize)]
pub struct DealPacingStats {
    #[serde(default)]
    pub impressions: u64,
}

impl FirestoreDealTracker {
    pub async fn start(
        provider: Arc<dyn Provider<DealPacingDoc>>,
        daily_provider: Arc<dyn Provider<DealPacingDoc>>,
    ) -> Result<Arc<Self>, Error> {
        let tracker = Arc::new(Self {
            persisted: RwLock::new(AHashMap::new()),
            daily: DailyMap::new(),
        });

        let t = tracker.clone();
        let initial = provider
            .start(Box::new(move |event| t.handle_event(event)))
            .await?;

        Self::load(&tracker.persisted, initial);

        let t = tracker.clone();
        let initial_daily = daily_provider
            .start(Box::new(move |event| t.handle_daily_event(event)))
            .await?;

        tracker.load_daily(initial_daily);

        Ok(tracker)
    }

    fn load(target: &RwLock<AHashMap<String, u64>>, docs: Vec<DealPacingDoc>) {
        let mut imps = AHashMap::new();

        for doc in &docs {
            let Some(deal_id) = doc.fields.get("deal_id") else {
                continue;
            };

            let count = doc.stats.as_ref().map(|s| s.impressions).unwrap_or(0);

            *imps.entry(deal_id.clone()).or_default() += count;
        }

        debug!("loaded impression counts for {} deals", imps.len());
        *target.write() = imps;
    }

    /// Load daily docs, skipping any whose bucket date is not today.
    fn load_daily(&self, docs: Vec<DealPacingDoc>) {
        let entries = docs.iter().filter_map(|doc| {
            if let Some(bucket) = &doc.bucket {
                if !is_bucket_today(bucket) {
                    return None;
                }
            }
            let deal_id = doc.fields.get("deal_id")?;
            let count = doc.stats.as_ref().map(|s| s.impressions).unwrap_or(0);
            Some((deal_id.clone(), count))
        });
        self.daily.load(entries, "impressions");
    }

    fn handle_event(&self, event: ProviderEvent<DealPacingDoc>) {
        Self::apply_lifetime_event(&self.persisted, event);
    }

    fn handle_daily_event(&self, event: ProviderEvent<DealPacingDoc>) {
        match &event {
            ProviderEvent::Added(doc) | ProviderEvent::Modified(doc) => {
                if let Some(bucket) = &doc.bucket {
                    if !is_bucket_today(bucket) {
                        return;
                    }
                }
                let Some(deal_id) = doc.fields.get("deal_id") else {
                    error!("deal pacing doc missing deal_id");
                    return;
                };
                let count = doc.stats.as_ref().map(|s| s.impressions).unwrap_or(0);
                self.daily.insert(deal_id.clone(), count);
            }
            ProviderEvent::Removed(doc_id) => {
                self.daily.remove_matching(doc_id);
            }
        }
    }

    fn apply_lifetime_event(
        target: &RwLock<AHashMap<String, u64>>,
        event: ProviderEvent<DealPacingDoc>,
    ) {
        match event {
            ProviderEvent::Added(doc) | ProviderEvent::Modified(doc) => {
                let Some(deal_id) = doc.fields.get("deal_id") else {
                    error!("deal pacing doc missing deal_id");
                    return;
                };

                let count = doc.stats.as_ref().map(|s| s.impressions).unwrap_or(0);

                target.write().insert(deal_id.clone(), count);
            }
            ProviderEvent::Removed(doc_id) => {
                let mut imps = target.write();
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

    fn daily_impressions(&self, deal_id: &str) -> u64 {
        self.daily.get(deal_id)
    }

    /// No-op — persistence handled by `DealCounterStore` via billing pipeline.
    fn record_impression(&self, _deal_id: &str) {}
}
