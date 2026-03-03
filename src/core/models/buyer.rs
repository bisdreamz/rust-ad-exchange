use crate::core::models::common::Status;
use serde::{Deserialize, Serialize};

/// A buyer profile — the company profile for running direct campaigns.
/// Parallel to Publisher (supply) and Bidder (DSP demand).
/// The `id` is the buyer_id.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Buyer {
    pub id: String,
    pub buyer_name: String,
    pub status: Status,
}
