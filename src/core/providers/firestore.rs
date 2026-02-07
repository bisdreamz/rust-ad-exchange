use crate::core::providers::{Provider, ProviderEvent};
use anyhow::Error;
use async_trait::async_trait;
use firestore::{
    FirestoreDb, FirestoreListenEvent, FirestoreListener, FirestoreListenerTarget,
    FirestoreMemListenStateStorage,
};
use parking_lot::Mutex;
use serde::de::DeserializeOwned;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, warn};

/// Firestore TargetChangeType::Reset
/// Sent when the server can't resume from the client's token (e.g. after ~30 min disconnect),
/// indicating a full re-snapshot will follow. We dont expect
/// this to happen in prod but better safe than sorry
const TARGET_CHANGE_TYPE_RESET: i32 = 4;

pub struct FirestoreProvider<T> {
    db: Arc<FirestoreDb>,
    collection: String,
    _marker: std::marker::PhantomData<T>,
}

impl<T> FirestoreProvider<T>
where
    T: DeserializeOwned + Clone + Send + Sync + 'static,
{
    pub fn new(db: Arc<FirestoreDb>, collection: impl Into<String>) -> Self {
        Self {
            db,
            collection: collection.into(),
            _marker: std::marker::PhantomData,
        }
    }

    async fn load(&self) -> Result<(Vec<T>, HashSet<String>), Error> {
        let docs = self
            .db
            .fluent()
            .select()
            .from(self.collection.as_str())
            .query()
            .await
            .map_err(|e| {
                error!(
                    "Firestore query failed for collection {}: {} ({:?})",
                    self.collection, e, e
                );
                e
            })?;

        let mut results = Vec::with_capacity(docs.len());
        let mut seen = HashSet::new();

        for doc in docs {
            seen.insert(doc.name.clone());
            match FirestoreDb::deserialize_doc_to::<T>(&doc) {
                Ok(obj) => results.push(obj),
                Err(err) => {
                    warn!(
                        "Failed to deserialize document {} during load: {}",
                        doc.name, err
                    );
                }
            }
        }

        debug!(
            "Loaded {} documents from Firestore collection {}",
            results.len(),
            self.collection
        );
        Ok((results, seen))
    }

    fn spawn_listener(
        &self,
        seen_docs: HashSet<String>,
        on_event: Arc<dyn Fn(ProviderEvent<T>) + Send + Sync>,
    ) {
        let db = self.db.clone();
        let collection = self.collection.clone();
        let seen_docs = Arc::new(Mutex::new(seen_docs));

        tokio::spawn(async move {
            let mut backoff = Duration::from_secs(1);
            let max_backoff = Duration::from_secs(60);

            loop {
                match Self::create_listener(
                    db.clone(),
                    &collection,
                    seen_docs.clone(),
                    on_event.clone(),
                )
                .await
                {
                    Ok(_listener) => {
                        debug!("Firestore listener for {} is running", collection);
                        std::future::pending::<()>().await;
                    }
                    Err(err) => {
                        error!(
                            "Failed to start Firestore listener for {}: {}, retrying in {:?}",
                            collection, err, backoff
                        );
                        tokio::time::sleep(backoff).await;
                        backoff = (backoff * 2).min(max_backoff);
                    }
                }
            }
        });
    }

    async fn reconcile(
        db: &FirestoreDb,
        collection: &str,
        seen_docs: &Mutex<HashSet<String>>,
        on_event: &(dyn Fn(ProviderEvent<T>) + Send + Sync),
    ) -> Result<(), Error> {
        let docs = db.fluent().select().from(collection).query().await?;

        let current_docs: HashSet<String> = docs.iter().map(|d| d.name.clone()).collect();

        // detect removals of docs we knew about that no longer exist
        let removed: Vec<String> = {
            let seen = seen_docs.lock();
            seen.difference(&current_docs).cloned().collect()
        };

        for doc_path in &removed {
            let doc_id = doc_path.rsplit('/').next().unwrap_or(doc_path).to_string();
            on_event(ProviderEvent::Removed(doc_id));
        }

        // update seen_docs, remove stale entries, add any we missed.
        {
            let mut seen = seen_docs.lock();
            for doc_path in &removed {
                seen.remove(doc_path);
            }

            for doc_name in &current_docs {
                seen.insert(doc_name.clone());
            }
        }

        if !removed.is_empty() {
            debug!(
                "Reconciled collection {}: removed {} stale documents",
                collection,
                removed.len()
            );
        }

        Ok(())
    }

    async fn create_listener(
        db: Arc<FirestoreDb>,
        collection: &str,
        seen_docs: Arc<Mutex<HashSet<String>>>,
        on_event: Arc<dyn Fn(ProviderEvent<T>) + Send + Sync>,
    ) -> Result<FirestoreListener<FirestoreDb, FirestoreMemListenStateStorage>, Error> {
        let mut listener = db
            .create_listener(FirestoreMemListenStateStorage::new())
            .await?;

        db.fluent()
            .select()
            .from(collection)
            .listen()
            .add_target(FirestoreListenerTarget::new(1), &mut listener)?;

        debug!("Starting Firestore listener for collection: {}", collection);

        let collection = collection.to_string();
        listener
            .start(move |event| {
                let on_event = on_event.clone();
                let seen_docs = seen_docs.clone();
                let db = db.clone();
                let collection = collection.clone();

                async move {
                    match event {
                        FirestoreListenEvent::DocumentChange(change) => {
                            if let Some(doc) = change.document {
                                match FirestoreDb::deserialize_doc_to::<T>(&doc) {
                                    Ok(obj) => {
                                        let is_new = seen_docs.lock().insert(doc.name.clone());

                                        let ev = if is_new {
                                            ProviderEvent::Added(obj)
                                        } else {
                                            ProviderEvent::Modified(obj)
                                        };
                                        on_event(ev);
                                    }
                                    Err(err) => {
                                        warn!("Failed to deserialize Firestore document: {}", err)
                                    }
                                }
                            }
                        }
                        FirestoreListenEvent::DocumentDelete(del) => {
                            seen_docs.lock().remove(&del.document);
                            let doc_id = del
                                .document
                                .rsplit('/')
                                .next()
                                .unwrap_or(&del.document)
                                .to_string();
                            on_event(ProviderEvent::Removed(doc_id));
                        }
                        FirestoreListenEvent::DocumentRemove(rem) => {
                            seen_docs.lock().remove(&rem.document);
                            let doc_id = rem
                                .document
                                .rsplit('/')
                                .next()
                                .unwrap_or(&rem.document)
                                .to_string();
                            on_event(ProviderEvent::Removed(doc_id));
                        }
                        FirestoreListenEvent::TargetChange(tc)
                            if tc.target_change_type == TARGET_CHANGE_TYPE_RESET =>
                        {
                            warn!("Firestore listener reset for {}, reconciling", collection);
                            if let Err(err) =
                                Self::reconcile(&db, &collection, &seen_docs, on_event.as_ref())
                                    .await
                            {
                                warn!("Reconciliation failed for {}: {}", collection, err);
                            }
                        }
                        _ => {}
                    }
                    Ok(())
                }
            })
            .await?;

        Ok(listener)
    }
}

#[async_trait]
impl<T> Provider<T> for FirestoreProvider<T>
where
    T: DeserializeOwned + Clone + Send + Sync + 'static,
{
    async fn start(
        &self,
        on_event: Box<dyn Fn(ProviderEvent<T>) + Send + Sync>,
    ) -> Result<Vec<T>, Error> {
        let (data, seen_docs) = self.load().await?;
        self.spawn_listener(seen_docs, Arc::from(on_event));
        Ok(data)
    }
}
