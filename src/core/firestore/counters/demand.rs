use crate::core::firestore::counters::store::CounterStore;
use crate::core::firestore::counters::{CounterBuffer, CounterValue};
use crate::core::spec::{Channel, StatsDeviceType};
use firestore::FirestoreDb;
use std::sync::Arc;
use std::time::Duration;

const MICROS: f64 = 1_000_000.0;

#[derive(Default, Debug, Clone)]
pub struct DemandCounters {
    pub requests_matched: u64,
    pub requests_qps_limited: u64,
    pub requests_shaping_blocked: u64,
    pub auctions: u64,
    pub bids: u64,
    pub bids_filtered: u64,
    pub timeouts: u64,
    pub errors: u64,
    pub impressions: u64,
    pub revenue_micros: u64,
    pub cost_micros: u64,
}

impl DemandCounters {
    pub fn request_matched(&mut self) {
        self.requests_matched += 1;
    }

    pub fn request_qps_limited(&mut self) {
        self.requests_qps_limited += 1;
    }

    pub fn request_shaping_blocked(&mut self) {
        self.requests_shaping_blocked += 1;
    }

    pub fn auction(&mut self) {
        self.auctions += 1;
    }

    pub fn bid(&mut self) {
        self.bids += 1;
    }

    pub fn bid_filtered(&mut self) {
        self.bids_filtered += 1;
    }

    pub fn timeout(&mut self) {
        self.timeouts += 1;
    }

    pub fn error(&mut self) {
        self.errors += 1;
    }

    pub fn impression(&mut self, revenue_cpm: f64, cost_cpm: f64) {
        self.impressions += 1;
        self.revenue_micros += (revenue_cpm * MICROS) as u64;
        self.cost_micros += (cost_cpm * MICROS) as u64;
    }
}

impl CounterBuffer for DemandCounters {
    fn merge(&mut self, other: &Self) {
        self.requests_matched += other.requests_matched;
        self.requests_qps_limited += other.requests_qps_limited;
        self.requests_shaping_blocked += other.requests_shaping_blocked;
        self.auctions += other.auctions;
        self.bids += other.bids;
        self.bids_filtered += other.bids_filtered;
        self.timeouts += other.timeouts;
        self.errors += other.errors;
        self.impressions += other.impressions;
        self.revenue_micros += other.revenue_micros;
        self.cost_micros += other.cost_micros;
    }

    fn counter_pairs(&self) -> Vec<(&'static str, CounterValue)> {
        vec![
            ("requests_matched", CounterValue::Int(self.requests_matched)),
            (
                "requests_qps_limited",
                CounterValue::Int(self.requests_qps_limited),
            ),
            (
                "requests_shaping_blocked",
                CounterValue::Int(self.requests_shaping_blocked),
            ),
            ("auctions", CounterValue::Int(self.auctions)),
            ("bids", CounterValue::Int(self.bids)),
            ("bids_filtered", CounterValue::Int(self.bids_filtered)),
            ("timeouts", CounterValue::Int(self.timeouts)),
            ("errors", CounterValue::Int(self.errors)),
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

pub struct DemandCounterStore {
    by_bidder: Arc<CounterStore<DemandCounters>>,
    by_endpoint: Arc<CounterStore<DemandCounters>>,
}

impl DemandCounterStore {
    pub fn new(
        db: Arc<FirestoreDb>,
        collection: &str,
        bucket: Duration,
        update_interval: Duration,
    ) -> Self {
        Self {
            by_bidder: CounterStore::new(
                db.clone(),
                format!("{collection}_by_bidder"),
                vec!["bidder_id", "bidder_name", "channel", "device_type"],
                Some(bucket),
                update_interval,
            ),
            by_endpoint: CounterStore::new(
                db.clone(),
                format!("{collection}_by_endpoint"),
                vec![
                    "bidder_id",
                    "bidder_name",
                    "endpoint",
                    "channel",
                    "device_type",
                ],
                Some(bucket),
                update_interval,
            ),
        }
    }

    pub fn merge_bidder(
        &self,
        bidder_id: &str,
        bidder_name: &str,
        channel: Channel,
        device_type: StatsDeviceType,
        buffer: &DemandCounters,
    ) {
        let ch = channel.to_string();
        let dt = device_type.to_string();
        self.by_bidder
            .merge(&[bidder_id, bidder_name, &ch, &dt], buffer);
    }

    pub fn merge_endpoint(
        &self,
        bidder_id: &str,
        bidder_name: &str,
        endpoint: &str,
        channel: Channel,
        device_type: StatsDeviceType,
        buffer: &DemandCounters,
    ) {
        let ch = channel.to_string();
        let dt = device_type.to_string();
        self.by_endpoint
            .merge(&[bidder_id, bidder_name, endpoint, &ch, &dt], buffer);
    }

    pub fn merge_impression(
        &self,
        bidder_id: &str,
        bidder_name: &str,
        endpoint: &str,
        channel: Channel,
        device_type: StatsDeviceType,
        buffer: &DemandCounters,
    ) {
        let ch = channel.to_string();
        let dt = device_type.to_string();
        self.by_bidder
            .merge(&[bidder_id, bidder_name, &ch, &dt], buffer);
        self.by_endpoint
            .merge(&[bidder_id, bidder_name, endpoint, &ch, &dt], buffer);
    }

    pub async fn shutdown(&self) {
        self.by_bidder.shutdown().await;
        self.by_endpoint.shutdown().await;
    }
}
