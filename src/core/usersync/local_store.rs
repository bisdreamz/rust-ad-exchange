use crate::core::usersync::model::SyncEntry;
use crate::core::usersync::store::SyncStore;
use moka::sync::{Cache, CacheBuilder};
use std::time::Duration;

/// Local user sync store for development purposes
pub struct LocalStore {
    cache: Cache<String, SyncEntry>,
}

impl LocalStore {
    pub fn new(sync_ttl: Duration) -> Self {
        Self {
            cache: CacheBuilder::default().time_to_live(sync_ttl).build(),
        }
    }
}

impl SyncStore for LocalStore {
    async fn store(&self, local_id: &String, remote_id: String) -> Option<SyncEntry> {
        let old_val = self.cache.get(local_id);

        self.cache.insert(local_id.clone(), SyncEntry::new(remote_id));

        old_val
    }

    async fn load(&self, local_id: &String) -> Option<SyncEntry> {
        self.cache.get(local_id)
    }
}