use crate::core::firestore::counters::store::CounterStore;
use crate::core::firestore::counters::{CounterBuffer, CounterValue};
use firestore::FirestoreDb;
use std::sync::Arc;
use std::time::Duration;

#[derive(Default, Debug, Clone)]
pub struct DealCounters {
    /// Bid responses from DSPs (or direct bids staged) that claimed this deal
    bids: u64,
    /// Won impressions (recorded via billing event)
    impressions: u64,
    /// Gross CPM sum charged to buyers (/ 1000 = dollars). Admin-visible only.
    revenue_cpm_sum: f64,
    /// Net CPM sum paid to publishers (/ 1000 = dollars). Publisher + admin visible.
    cost_cpm_sum: f64,
}

impl DealCounters {
    pub fn bid(&mut self) {
        self.bids += 1;
    }

    pub fn impression(&mut self) {
        self.impressions += 1;
    }

    pub fn record_spend(&mut self, cpm_gross: f64, cpm_cost: f64) {
        self.revenue_cpm_sum += cpm_gross;
        self.cost_cpm_sum += cpm_cost;
    }
}

impl CounterBuffer for DealCounters {
    fn merge(&mut self, other: &Self) {
        self.bids += other.bids;
        self.impressions += other.impressions;
        self.revenue_cpm_sum += other.revenue_cpm_sum;
        self.cost_cpm_sum += other.cost_cpm_sum;
    }

    fn counter_pairs(&self) -> Vec<(&'static str, CounterValue)> {
        vec![
            ("bids", CounterValue::Int(self.bids)),
            ("impressions", CounterValue::Int(self.impressions)),
            ("revenue_cpm_sum", CounterValue::Float(self.revenue_cpm_sum)),
            ("cost_cpm_sum", CounterValue::Float(self.cost_cpm_sum)),
        ]
    }
}

/// Collection name for the unbucketed (lifetime) deal impression store.
pub const DEAL_PACING_COLLECTION: &str = "pacing_deals";

/// Collection name for the daily-bucketed deal impression store.
pub const DEAL_PACING_DAILY_COLLECTION: &str = "pacing_deals_daily";

pub struct DealCounterStore {
    /// Unbucketed — one doc per deal, cumulative across entire flight.
    /// Used for Total delivery goals.
    by_deal: Arc<CounterStore<DealCounters>>,
    /// Daily-bucketed — one doc per deal per day.
    /// Used for Daily delivery goals.
    by_deal_daily: Arc<CounterStore<DealCounters>>,
}

impl DealCounterStore {
    pub fn new(db: Arc<FirestoreDb>, update_interval: Duration) -> Self {
        Self {
            by_deal: CounterStore::new(
                db.clone(),
                DEAL_PACING_COLLECTION.to_string(),
                vec!["deal_id"],
                None, // unbucketed — one doc per deal, cumulative
                update_interval,
            ),
            by_deal_daily: CounterStore::new(
                db,
                DEAL_PACING_DAILY_COLLECTION.to_string(),
                vec!["deal_id"],
                Some(Duration::from_hours(24)),
                update_interval,
            ),
        }
    }

    pub fn merge(&self, deal_id: &str, buffer: &DealCounters) {
        self.by_deal.merge(&[deal_id], buffer);
        self.by_deal_daily.merge(&[deal_id], buffer);
    }

    pub async fn shutdown(&self) {
        self.by_deal.shutdown().await;
        self.by_deal_daily.shutdown().await;
    }
}
