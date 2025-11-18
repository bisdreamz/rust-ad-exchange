use crate::core::usersync::model::SyncEntry;
use async_trait::async_trait;
use std::collections::HashMap;

/// A user syncing backend store, e.g. database which
/// holds user sync results
#[async_trait]
pub trait SyncStore: Send + Sync {
    /// Store a buyeruid value for a provided partner,
    /// which appends (or updates) it in our list of
    /// partner syncs indexed by local uid
    async fn append(
        &self,
        local_id: &String,
        partner_id: &String,
        remote_id: String,
    ) -> Option<SyncEntry>;

    /// Loads sync entries indexed by partner id under local id
    /// E.g. buyer_id 123 -> sync entry
    async fn load(&self, local_id: &String) -> Option<HashMap<String, SyncEntry>>;
}
