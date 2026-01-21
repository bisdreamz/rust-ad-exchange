use crate::core::providers::{Provider, ProviderEvent};
use anyhow::Error;
use async_trait::async_trait;
use firestore::{
    FirestoreDb, FirestoreListenEvent, FirestoreListenerTarget, FirestoreMemListenStateStorage,
};
use parking_lot::RwLock;
use serde::de::DeserializeOwned;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, warn};

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
            .await?;

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
        let seen_docs = Arc::new(RwLock::new(seen_docs));

        tokio::spawn(async move {
            let mut backoff = Duration::from_secs(1);
            let max_backoff = Duration::from_secs(60);
            let mut is_reconnect = false;

            loop {
                match Self::run_listener(
                    &db,
                    &collection,
                    seen_docs.clone(),
                    on_event.clone(),
                    is_reconnect,
                )
                .await
                {
                    Ok(()) => {
                        warn!(
                            "Firestore listener for {} exited unexpectedly, reconnecting",
                            collection
                        );
                    }
                    Err(err) => {
                        error!(
                            "Firestore listener for {} failed: {}, reconnecting in {:?}",
                            collection, err, backoff
                        );
                        tokio::time::sleep(backoff).await;
                        backoff = (backoff * 2).min(max_backoff);
                        is_reconnect = true;
                        continue;
                    }
                }

                backoff = Duration::from_secs(1);
                is_reconnect = true;
            }
        });
    }

    async fn reconcile(
        db: &FirestoreDb,
        collection: &str,
        seen_docs: &RwLock<HashSet<String>>,
        on_event: &(dyn Fn(ProviderEvent<T>) + Send + Sync),
    ) -> Result<(), Error> {
        let docs = db.fluent().select().from(collection).query().await?;

        let mut current_docs = HashSet::new();
        for doc in &docs {
            current_docs.insert(doc.name.clone());
        }

        let removed: Vec<String> = {
            let seen = seen_docs.read();
            seen.difference(&current_docs).cloned().collect()
        };

        for doc_path in removed {
            let doc_id = doc_path.rsplit('/').next().unwrap_or(&doc_path).to_string();
            on_event(ProviderEvent::Removed(doc_id));
        }

        for doc in docs {
            match FirestoreDb::deserialize_doc_to::<T>(&doc) {
                Ok(obj) => on_event(ProviderEvent::Modified(obj)),
                Err(err) => {
                    warn!(
                        "Failed to deserialize document {} during reconcile: {}",
                        doc.name, err
                    );
                }
            }
        }

        *seen_docs.write() = current_docs;

        debug!(
            "Reconciled {} documents for collection {}",
            seen_docs.read().len(),
            collection
        );
        Ok(())
    }

    async fn run_listener(
        db: &FirestoreDb,
        collection: &str,
        seen_docs: Arc<RwLock<HashSet<String>>>,
        on_event: Arc<dyn Fn(ProviderEvent<T>) + Send + Sync>,
        is_reconnect: bool,
    ) -> Result<(), Error> {
        if is_reconnect {
            Self::reconcile(db, collection, &seen_docs, on_event.as_ref()).await?;
        }

        let mut listener = db
            .create_listener(FirestoreMemListenStateStorage::new())
            .await?;

        db.fluent()
            .select()
            .from(collection)
            .listen()
            .add_target(FirestoreListenerTarget::new(1), &mut listener)?;

        debug!("Starting Firestore listener for collection: {}", collection);

        listener
            .start(move |event| {
                let seen_docs = seen_docs.clone();
                let on_event = on_event.clone();

                async move {
                    match event {
                        FirestoreListenEvent::DocumentChange(change) => {
                            if let Some(doc) = change.document {
                                match FirestoreDb::deserialize_doc_to::<T>(&doc) {
                                    Ok(obj) => {
                                        let is_new = seen_docs.write().insert(doc.name.clone());
                                        let ev = if is_new {
                                            ProviderEvent::Added(obj)
                                        } else {
                                            ProviderEvent::Modified(obj)
                                        };
                                        on_event(ev);
                                    }
                                    Err(err) => {
                                        warn!("Failed to deserialize Firestore document: {}", err);
                                    }
                                }
                            }
                        }
                        FirestoreListenEvent::DocumentDelete(del) => {
                            seen_docs.write().remove(&del.document);
                            let doc_id = del
                                .document
                                .rsplit('/')
                                .next()
                                .unwrap_or(&del.document)
                                .to_string();
                            on_event(ProviderEvent::Removed(doc_id));
                        }
                        FirestoreListenEvent::DocumentRemove(rem) => {
                            seen_docs.write().remove(&rem.document);
                            let doc_id = rem
                                .document
                                .rsplit('/')
                                .next()
                                .unwrap_or(&rem.document)
                                .to_string();
                            on_event(ProviderEvent::Removed(doc_id));
                        }
                        _ => {}
                    }
                    Ok(())
                }
            })
            .await?;

        Ok(())
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
