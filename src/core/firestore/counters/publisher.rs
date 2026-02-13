use crate::core::firestore::counters::store::CounterStore;
use crate::core::firestore::counters::{CounterBuffer, CounterValue};
use crate::core::spec::{Channel, StatsDeviceType};
use firestore::FirestoreDb;
use rtb::utils::adm::AdFormat;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

const MICROS: f64 = 1_000_000.0;

#[derive(Default, Debug, Clone)]
pub struct PublisherCounters {
    requests: u64,
    requests_blocked: u64,
    requests_auctioned: u64,
    had_cookie_or_maid: u64,
    request_imps: u64,
    req_tmax_sum: u64,
    auctions: u64,
    bids: u64,
    bids_filtered: u64,
    impressions: u64,
    revenue_micros: u64,
    cost_micros: u64,
}

impl PublisherCounters {
    /// Increment request counters, imp len counter, and tmax
    /// counters. This should always be called for a known pub
    pub fn request(&mut self, imps_len: u64, tmax: u64) {
        self.requests += 1;
        self.request_imps += imps_len;
        self.req_tmax_sum += tmax;
    }

    /// Mark a request blocked.
    /// This does not increment the request counter.
    pub fn block(&mut self) {
        self.requests_blocked += 1;
    }

    /// Mark request sent to at least one bidder.
    /// Should ONLY be called once
    /// per inbound request.
    pub fn request_auctioned(&mut self) {
        self.requests_auctioned += 1;
    }

    /// Mark this request as having a recognized
    /// buyeruid value (cookie for web) or
    /// a validated device id present
    pub fn had_cookie_or_maid(&mut self) {
        self.had_cookie_or_maid += 1;
    }

    /// Increment the auction counter, which
    /// should be called every time a request
    /// is sent to a demand partner
    pub fn auction(&mut self, count: u64) {
        self.auctions += count;
    }

    /// Mark a bid as received. This should be called
    /// every time a bid is received from a demand partner.
    pub fn bid(&mut self, count: u64) {
        self.bids += count;
    }

    /// Mark count of bids that were
    /// filtered e.g. blocked adomain, belowfloor
    pub fn bids_filtered(&mut self, count: u64) {
        self.bids_filtered += count;
    }

    /// Increment impression counters and record revenue
    pub fn impression(&mut self, revenue_cpm: f64, cost_cpm: f64) {
        self.impressions += 1;
        self.revenue_micros += (revenue_cpm * MICROS) as u64;
        self.cost_micros += (cost_cpm * MICROS) as u64;
    }
}

impl CounterBuffer for PublisherCounters {
    fn merge(&mut self, other: &Self) {
        self.requests += other.requests;
        self.requests_blocked += other.requests_blocked;
        self.requests_auctioned += other.requests_auctioned;
        self.had_cookie_or_maid += other.had_cookie_or_maid;
        self.request_imps += other.request_imps;
        self.req_tmax_sum += other.req_tmax_sum;
        self.auctions += other.auctions;
        self.bids += other.bids;
        self.bids_filtered += other.bids_filtered;
        self.impressions += other.impressions;
        self.revenue_micros += other.revenue_micros;
        self.cost_micros += other.cost_micros;
    }

    fn counter_pairs(&self) -> Vec<(&'static str, CounterValue)> {
        vec![
            ("requests", CounterValue::Int(self.requests)),
            ("requests_blocked", CounterValue::Int(self.requests_blocked)),
            (
                "requests_auctioned",
                CounterValue::Int(self.requests_auctioned),
            ),
            (
                "had_cookie_or_maid",
                CounterValue::Int(self.had_cookie_or_maid),
            ),
            ("request_imps", CounterValue::Int(self.request_imps)),
            ("req_tmax_sum", CounterValue::Int(self.req_tmax_sum)),
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

pub struct PublisherCounterStore {
    by_pub: Arc<CounterStore<PublisherCounters>>,
    by_format: Arc<CounterStore<PublisherCounters>>,
}

impl PublisherCounterStore {
    pub fn new(
        db: Arc<FirestoreDb>,
        collection: &str,
        bucket: Duration,
        update_interval: Duration,
    ) -> Self {
        Self {
            by_pub: CounterStore::new(
                db.clone(),
                format!("{}_by_pub", collection),
                vec!["pub_id", "pub_name", "channel", "device_type"],
                Some(bucket),
                update_interval,
            ),
            by_format: CounterStore::new(
                db.clone(),
                format!("{}_by_format", collection),
                vec!["pub_id", "pub_name", "ad_format", "channel", "device_type"],
                Some(bucket),
                update_interval,
            ),
        }
    }

    pub fn merge(
        &self,
        pub_id: &str,
        pub_name: &str,
        channel: Channel,
        device_type: StatsDeviceType,
        aggregate: &PublisherCounters,
        by_format: &HashMap<&str, PublisherCounters>,
    ) {
        let ch = channel.to_string();
        let dt = device_type.to_string();

        self.by_pub.merge(&[pub_id, pub_name, &ch, &dt], aggregate);

        for (ad_format, counters) in by_format {
            self.by_format
                .merge(&[pub_id, pub_name, ad_format, &ch, &dt], counters);
        }
    }

    pub fn merge_impression(
        &self,
        pub_id: &str,
        pub_name: &str,
        format: AdFormat,
        channel: Channel,
        device_type: StatsDeviceType,
        counters: &PublisherCounters,
    ) {
        let ch = channel.to_string();
        let dt = device_type.to_string();

        self.by_pub.merge(&[pub_id, pub_name, &ch, &dt], counters);
        self.by_format
            .merge(&[pub_id, pub_name, format.as_str(), &ch, &dt], counters);
    }

    /// Close and flush counters
    pub async fn shutdown(&self) {
        self.by_pub.shutdown().await;
        self.by_format.shutdown().await;
    }
}
