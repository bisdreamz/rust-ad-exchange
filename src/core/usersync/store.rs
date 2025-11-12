use crate::core::usersync::model::SyncEntry;

/// A user syncing backend store, e.g. database which
/// holds user sync results
pub trait SyncStore {
    async fn store(&self, local_id: &String, remote_id: String) -> Option<SyncEntry>;

    async fn load(&self, local_id: &String) -> Option<SyncEntry>;
}