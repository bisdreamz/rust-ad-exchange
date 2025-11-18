use anyhow::Error;
use derive_builder::Builder;
use rtb::common::{DataUrl, utils};
use serde::{Deserialize, Serialize};

pub const FIELD_URL_SYNC_IN_REMOTE_UID: &str = "buid";
pub const FIELD_URL_SYNC_IN_PARTNER_ID: &str = "pid";

/// Represents a detailed entry of a stored user sync result with a
/// third party
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SyncEntry {
    /// Timestamp of last update to record
    pub ts: u64,
    /// Remote id - the id value of the remote partner this entry belongs to,
    /// e.g. a DSP buyeruid value
    pub rid: String,
}

impl SyncEntry {
    pub fn new(remote_id: String) -> SyncEntry {
        Self {
            ts: utils::epoch_timestamp(),
            rid: remote_id,
        }
    }
}

/// Describes a structured inbound sync event,
/// in which a partner is forwarding us their
/// buyeruid value while we host the match
/// table. Often for demand, but could be
/// used for suppliers also
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Builder)]
pub struct SyncInEvent {
    /// Local exchange uid
    pub local_uid: String,
    /// Remote partner buyeruid value being sent to us,
    /// which we need to store mapping for
    pub remote_uid: String,
    /// Partner id of the remote partner
    pub partner_id: String,
}

impl SyncInEvent {
    /// Extracts a well structured ['SyncInEvent'] from a ['DataUrl']
    pub fn from(data_url: &DataUrl, local_uid: String) -> Result<Self, Error> {
        Ok(SyncInEventBuilder::default()
            .local_uid(local_uid)
            .remote_uid(data_url.get_required_string(FIELD_URL_SYNC_IN_REMOTE_UID)?)
            .partner_id(data_url.get_required_string(FIELD_URL_SYNC_IN_PARTNER_ID)?)
            .build()?)
    }
}
