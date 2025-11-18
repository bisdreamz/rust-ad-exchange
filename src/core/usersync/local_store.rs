use crate::core::usersync::model::SyncEntry;
use crate::core::usersync::store::SyncStore;
use async_trait::async_trait;
use moka::sync::{Cache, CacheBuilder};
use std::collections::HashMap;
use std::time::Duration;

/// Local user sync store for development purposes
pub struct LocalStore {
    /// Cache of local uid -> map<partner_id, sync_entry>
    cache: Cache<String, HashMap<String, SyncEntry>>,
}

impl LocalStore {
    pub fn new(sync_ttl: Duration) -> Self {
        Self {
            cache: CacheBuilder::default().time_to_live(sync_ttl).build(),
        }
    }
}

#[async_trait]
impl SyncStore for LocalStore {
    async fn append(
        &self,
        local_id: &String,
        partner_id: &String,
        remote_id: String,
    ) -> Option<SyncEntry> {
        let mut map_entry = self.cache.get(local_id).unwrap_or_default();

        let old_value = map_entry.get(partner_id).cloned();

        map_entry.insert(local_id.clone(), SyncEntry::new(remote_id));

        self.cache.insert(local_id.clone(), map_entry);

        old_value
    }

    async fn load(&self, local_id: &String) -> Option<HashMap<String, SyncEntry>> {
        self.cache.get(local_id)
    }
}
