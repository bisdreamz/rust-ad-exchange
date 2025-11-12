use rtb::common::utils;
use serde::{Deserialize, Serialize};

/// Represents a detailed entry of a stored user sync result with a
/// third party
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SyncEntry {
    /// Timestamp of last update to record
    pub ts: u64,
    /// Remote id - the id value of the remote partner this entry belongs to,
    /// e.g. a DSP buyeruid value
    pub rid: String
}

impl SyncEntry {
    pub fn new(remote_id: String) -> SyncEntry {
        Self {
            ts: utils::epoch_timestamp(),
            rid: remote_id
        }
    }
}