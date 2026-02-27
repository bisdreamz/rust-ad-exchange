use crate::core::firestore::counters::store::CounterStore;
use crate::core::firestore::counters::{CounterBuffer, CounterValue};
use firestore::FirestoreDb;
use std::sync::Arc;
use std::time::Duration;

#[derive(Default, Debug, Clone)]
pub struct DealCounters {
    impressions: u64,
}

impl DealCounters {
    pub fn impression(&mut self) {
        self.impressions += 1;
    }
}

impl CounterBuffer for DealCounters {
    fn merge(&mut self, other: &Self) {
        self.impressions += other.impressions;
    }

    fn counter_pairs(&self) -> Vec<(&'static str, CounterValue)> {
        vec![("impressions", CounterValue::Int(self.impressions))]
    }
}

/// Collection name for the unbucketed deal impression store.
/// Shared between DealCounterStore (writes) and
/// FirestoreDealTracker (reads).
pub const DEAL_PACING_COLLECTION: &str = "pacing_deals";

pub struct DealCounterStore {
    by_deal: Arc<CounterStore<DealCounters>>,
}

impl DealCounterStore {
    pub fn new(db: Arc<FirestoreDb>, update_interval: Duration) -> Self {
        Self {
            by_deal: CounterStore::new(
                db,
                DEAL_PACING_COLLECTION.to_string(),
                vec!["deal_id"],
                None, // unbucketed — one doc per deal, cumulative
                update_interval,
            ),
        }
    }

    pub fn merge(&self, deal_id: &str, buffer: &DealCounters) {
        self.by_deal.merge(&[deal_id], buffer);
    }
}
