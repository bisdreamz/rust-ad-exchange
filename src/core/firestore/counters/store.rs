use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::core::firestore::counters::{CounterBuffer, CounterValue};
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use firestore::{FirestoreBatch, FirestoreDb, FirestoreSimpleBatchWriter};
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;
use tracing::{debug, error};

fn bucket_key(bucket_width: Duration) -> (DateTime<Utc>, String) {
    let now = Utc::now();
    let secs = now.timestamp();
    let width = bucket_width.as_secs() as i64;
    let truncated = secs - (secs % width);

    let dt =
        DateTime::from_timestamp(truncated, 0).expect("Failed to create datetime from timestamp");

    let suffix = dt.format("%Y_%m_%d_%H_%M").to_string();
    (dt, suffix)
}

/// Document shape written to Firestore
/// `fields` and `bucket` are set via the object + field mask.
/// `stats` is included for structural clarity but excluded
/// from the mask and only set/updated by increments
#[derive(Serialize, Deserialize)]
struct CounterDoc {
    fields: HashMap<String, String>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "firestore::serialize_as_optional_timestamp::serialize",
        deserialize_with = "firestore::serialize_as_optional_timestamp::deserialize"
    )]
    bucket: Option<DateTime<Utc>>,
    stats: HashMap<String, usize>,
}

pub struct CounterEntry<C: CounterBuffer> {
    pub values: Vec<String>,
    pub counters: C,
}

/// Generic counter store. Field names are fixed at construction,
/// callers pass values in the same order to `get()`.
/// Flush writes each entry as a single Firestore update+transform.
pub struct CounterStore<C: CounterBuffer> {
    db: Arc<FirestoreDb>,
    collection: String,
    field_names: Vec<&'static str>,
    entries: DashMap<String, CounterEntry<C>>,
    time_bucket: Option<Duration>,
    update_interval: Duration,
    shutdown: Notify,
}

impl<C: CounterBuffer> CounterStore<C> {
    /// Creates a new counter store that flushes to `collection`.
    /// `field_names` sets the order of field values passed to `get()`,
    /// e.g. `vec!["pub_id", "site_id"]` means `get(&["abc", "xyz"])`
    /// writes `{ fields: { pub_id: "abc", site_id: "xyz" } }`.
    /// `time_bucket` groups counters into time windows when set.
    pub fn new(
        db: Arc<FirestoreDb>,
        collection: String,
        field_names: Vec<&'static str>,
        time_bucket: Option<Duration>,
        update_interval: Duration,
    ) -> Arc<Self> {
        let store = Arc::new(CounterStore {
            db,
            collection,
            field_names,
            entries: DashMap::new(),
            time_bucket,
            update_interval,
            shutdown: Notify::new(),
        });

        let self_clone = store.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = tokio::time::sleep(self_clone.update_interval) => {
                        self_clone.flush().await;

                        debug!("Flushing store counters for collection {}",
                    self_clone.collection);
                    },
                    _ = self_clone.shutdown.notified() => break,
                }
            }
        });

        store
    }

    /// Merge a completed counter buffer into the store.
    /// Values must match the order of field_names from new().
    pub fn merge(&self, values: &[&str], buffer: &C) {
        assert_eq!(
            values.len(),
            self.field_names.len(),
            "Field -> value count mismatch in CounterStore"
        );

        let key = values.join("_");

        self.entries
            .entry(key)
            .or_insert_with(|| CounterEntry {
                values: values.iter().map(|v| v.to_string()).collect(),
                counters: C::default(),
            })
            .counters
            .merge(buffer);
    }

    fn merge_back(&self, key: String, values: Vec<String>, buffer: C) {
        self.entries
            .entry(key)
            .or_insert_with(|| CounterEntry {
                values,
                counters: C::default(),
            })
            .counters
            .merge(&buffer);
    }

    async fn commit_batch(
        &self,
        batch: FirestoreBatch<'_, FirestoreSimpleBatchWriter>,
        pending_docs: &mut HashMap<String, (String, Vec<String>, C)>,
    ) {
        if batch.writes.is_empty() {
            return;
        }

        match batch.write().await {
            Ok(resp) => {
                let mut has_error = false;

                if resp.statuses.is_empty() || resp.statuses.len() != pending_docs.len() {
                    has_error = true;
                    error!("batch write mismatched status count");
                }

                for (idx, status) in resp.statuses.iter().enumerate() {
                    if status.code != 0 {
                        has_error = true;
                        error!(
                            "batch write failed at index {}: code={} message={}",
                            idx, status.code, status.message
                        );
                    }
                }
                // On success we drop pending_docs; on partial errors Firestore does not
                // identify which write failed, so merge everything back to be safe.
                if has_error {
                    for (_doc_id, (key, values, buffer)) in pending_docs.drain() {
                        self.merge_back(key, values, buffer);
                    }
                } else {
                    pending_docs.clear();
                }
            }
            Err(e) => {
                error!("batch write failed: {e}");
                for (_doc_id, (key, values, buffer)) in pending_docs.drain() {
                    self.merge_back(key, values, buffer);
                }
            }
        }
    }

    /// Flush all entries to Firestore. Each becomes a single
    /// update+transform: object sets fields/bucket via mask,
    /// transforms increment stats.* server-side.
    /// On failure the buffer is merged back, no counts lost.
    pub async fn flush(&self) {
        // prune any map entries that had zero activity
        self.entries.retain(|_, entry| {
            !entry
                .counters
                .counter_pairs()
                .iter()
                .all(|(_, v)| v.is_zero())
        });

        if self.entries.is_empty() {
            debug!(
                "No counters to flush for collection {}, no activity found",
                self.collection
            );
            return;
        }

        let bucket = self.time_bucket.map(bucket_key);

        let taken: Vec<(String, Vec<String>, C)> = self
            .entries
            .iter_mut()
            .map(|mut e| {
                let key = e.key().clone();
                let values = e.value().values.clone();
                let counters = e.value_mut().counters.clone_and_reset();
                (key, values, counters)
            })
            .collect();

        let writer = match self.db.create_simple_batch_writer().await {
            Ok(writer) => writer,
            Err(e) => {
                error!("failed to create batch writer: {e}");
                for (key, values, buffer) in taken {
                    self.merge_back(key, values, buffer);
                }
                return;
            }
        };

        let mut batch = writer.new_batch();
        let mut pending_docs: HashMap<String, (String, Vec<String>, C)> = HashMap::new();

        for (key, values, buffer) in taken {
            let pairs = buffer.counter_pairs();

            if pairs.iter().all(|(_, v)| v.is_zero()) {
                continue;
            }

            let doc_id = match &bucket {
                Some((_, suffix)) => format!("{key}_{suffix}"),
                None => key.clone(),
            };

            let fields: HashMap<String, String> = self
                .field_names
                .iter()
                .zip(values.iter())
                .map(|(name, val)| (name.to_string(), val.clone()))
                .collect();

            let doc = CounterDoc {
                fields,
                bucket: bucket.as_ref().map(|(dt, _)| *dt),
                stats: HashMap::new(),
            };

            // mask covers only fields/bucket — stats.* left to transforms
            let mut mask = vec!["fields"];
            if bucket.is_some() {
                mask.push("bucket");
            }

            pending_docs.insert(doc_id.clone(), (key.clone(), values.clone(), buffer));

            let result = self
                .db
                .fluent()
                .update()
                .fields(mask)
                .in_col(&self.collection)
                .document_id(&doc_id)
                .object(&doc)
                .transforms(|t| {
                    t.fields(pairs.iter().map(|(name, val)| match val {
                        CounterValue::Int(n) => {
                            t.field(format!("stats.{name}")).increment(*n as i64)
                        }
                        CounterValue::Float(f) => t.field(format!("stats.{name}")).increment(*f),
                    }))
                })
                .add_to_batch(&mut batch);

            match result {
                Ok(_) => {
                    // Firestore hard limit is 500 writes per batch; we stay well under it.
                    // NOTE: This batching is also a workaround for firestore crate 0.47.1
                    // dropping transforms when using .execute(); add_to_batch preserves
                    // the stats increments.
                    if batch.writes.len() >= 100 {
                        let batch_to_write = batch;
                        self.commit_batch(batch_to_write, &mut pending_docs).await;
                        batch = writer.new_batch();
                    }
                }
                Err(e) => {
                    error!("failed to queue counter write for doc_id={doc_id}: {e}");
                    if let Some((key, values, buffer)) = pending_docs.remove(&doc_id) {
                        self.merge_back(key, values, buffer);
                    }
                }
            }
        }

        // Flush any remaining writes.
        self.commit_batch(batch, &mut pending_docs).await;
    }

    /// Shutdown update timer and flush
    /// remaining counters to firestore
    pub async fn shutdown(&self) {
        self.shutdown.notify_one();
        self.flush().await;
    }
}
