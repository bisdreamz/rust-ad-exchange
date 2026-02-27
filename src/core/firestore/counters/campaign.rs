use crate::core::firestore::counters::store::CounterStore;
use crate::core::firestore::counters::{CounterBuffer, CounterValue};
use firestore::FirestoreDb;
use std::sync::Arc;
use std::time::Duration;

const MICROS: f64 = 1_000_000.0;

#[derive(Default, Debug, Clone)]
pub struct CampaignCounters {
    auctions: u64,
    bids: u64,
    bids_filtered: u64,
    impressions: u64,
    revenue_micros: u64,
    cost_micros: u64,
}

impl CampaignCounters {
    pub fn auction(&mut self) {
        self.auctions += 1;
    }

    pub fn bid(&mut self) {
        self.bids += 1;
    }

    pub fn bids_filtered(&mut self, count: u64) {
        self.bids_filtered += count;
    }

    pub fn impression(&mut self, revenue_cpm: f64, cost_cpm: f64) {
        self.impressions += 1;
        self.revenue_micros += (revenue_cpm * MICROS) as u64;
        self.cost_micros += (cost_cpm * MICROS) as u64;
    }
}

impl CounterBuffer for CampaignCounters {
    fn merge(&mut self, other: &Self) {
        self.auctions += other.auctions;
        self.bids += other.bids;
        self.bids_filtered += other.bids_filtered;
        self.impressions += other.impressions;
        self.revenue_micros += other.revenue_micros;
        self.cost_micros += other.cost_micros;
    }

    fn counter_pairs(&self) -> Vec<(&'static str, CounterValue)> {
        vec![
            ("auctions", CounterValue::Int(self.auctions)),
            ("bids", CounterValue::Int(self.bids)),
            ("bids_filtered", CounterValue::Int(self.bids_filtered)),
            ("impressions", CounterValue::Int(self.impressions)),
            (
                "revenue_cpm_sum",
                CounterValue::Float(self.revenue_micros as f64 / MICROS),
            ),
            (
                "cost_cpm_sum",
                CounterValue::Float(self.cost_micros as f64 / MICROS),
            ),
        ]
    }
}

pub struct CampaignCounterStore {
    by_buyer: Arc<CounterStore<CampaignCounters>>,
    by_campaign: Arc<CounterStore<CampaignCounters>>,
    by_detail: Arc<CounterStore<CampaignCounters>>,
    /// Unbucketed spend per campaign — one doc per campaign with
    /// cumulative server-side increments. Read by FirestoreSpendTracker
    /// for pacing decisions.
    spend: Arc<CounterStore<CampaignCounters>>,
}

/// Collection name for the unbucketed spend store.
/// Shared between CampaignCounterStore (writes) and
/// FirestoreSpendTracker (reads).
pub const SPEND_COLLECTION: &str = "pacing_campaigns";

impl CampaignCounterStore {
    pub fn new(
        db: Arc<FirestoreDb>,
        collection: &str,
        bucket: Duration,
        update_interval: Duration,
    ) -> Self {
        Self {
            by_buyer: CounterStore::new(
                db.clone(),
                format!("{collection}_by_buyer"),
                vec!["buyer_id", "buyer_name"],
                Some(bucket),
                update_interval,
            ),
            by_campaign: CounterStore::new(
                db.clone(),
                format!("{collection}_by_campaign"),
                vec!["buyer_id", "buyer_name", "campaign_id", "campaign_name"],
                Some(bucket),
                update_interval,
            ),
            by_detail: CounterStore::new_with_collection_fn(
                db.clone(),
                format!("{collection}_detail"),
                vec![
                    "pub_id",
                    "campaign_id",
                    "campaign_name",
                    "creative_id",
                    "creative_format",
                    "deal_id",
                    "dev_type",
                    "dev_os",
                    "country",
                    "advertiser",
                    "source",
                ],
                Some(bucket),
                update_interval,
                Box::new(|values| format!("campaigns/{}/stats", values[1])),
            ),
            spend: CounterStore::new(
                db,
                SPEND_COLLECTION.to_string(),
                vec!["buyer_id", "campaign_id"],
                None, // unbucketed — one doc per campaign, cumulative
                update_interval,
            ),
        }
    }

    pub fn merge(
        &self,
        buyer_id: &str,
        buyer_name: &str,
        campaign_id: &str,
        campaign_name: &str,
        pub_id: &str,
        creative_id: &str,
        creative_format: &str,
        deal_id: &str,
        dev_type: &str,
        dev_os: &str,
        country: &str,
        advertiser: &str,
        source: &str,
        buffer: &CampaignCounters,
    ) {
        self.by_buyer.merge(&[buyer_id, buyer_name], buffer);

        self.by_campaign.merge(
            &[buyer_id, buyer_name, campaign_id, campaign_name],
            buffer,
        );

        self.by_detail.merge(
            &[
                pub_id,
                campaign_id,
                campaign_name,
                creative_id,
                creative_format,
                deal_id,
                dev_type,
                dev_os,
                country,
                advertiser,
                source,
            ],
            buffer,
        );

        self.spend.merge(&[buyer_id, campaign_id], buffer);
    }

    pub async fn shutdown(&self) {
        self.by_buyer.shutdown().await;
        self.by_campaign.shutdown().await;
        self.by_detail.shutdown().await;
        self.spend.shutdown().await;
    }
}
