use super::{BidReservationRing, SpendPacer, SpendTracker};
use crate::core::models::campaign::Campaign;
use dashmap::DashMap;
use std::sync::Arc;

/// Spend ASAP — just guard the total budget.
///
/// Effective spend = persisted (SpendTracker) + buffered (ring).
/// Fast campaigns must be on a paced deal; the deal ceiling
/// prevents runaway, the budget check here is the hard cap.
pub struct FastPacer {
    tracker: Arc<dyn SpendTracker>,
    rings: DashMap<String, BidReservationRing>,
}

impl FastPacer {
    pub fn new(tracker: Arc<dyn SpendTracker>) -> Self {
        Self {
            tracker,
            rings: DashMap::new(),
        }
    }

    fn effective_spend(&self, campaign_id: &str) -> Option<f64> {
        let persisted = self.tracker.total_spend(campaign_id)?;
        let buffered = self
            .rings
            .get(campaign_id)
            .map(|r| r.total())
            .unwrap_or(0.0);

        Some(persisted + buffered)
    }
}

impl SpendPacer for FastPacer {
    fn passes(&self, campaign: &Campaign, price: f64) -> bool {
        let spent = match self.effective_spend(&campaign.id) {
            Some(s) => s,
            None => return false, // unregistered campaign — fail closed
        };

        if spent + price > campaign.budget {
            return false;
        }

        // Approved — buffer the bid value
        self.rings
            .entry(campaign.id.clone())
            .or_insert_with(BidReservationRing::default)
            .reserve(price);

        true
    }
}
