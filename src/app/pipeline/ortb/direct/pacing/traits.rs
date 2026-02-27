use crate::core::models::campaign::Campaign;
use crate::core::models::deal::Deal;

/// Spend pacing check for direct campaigns.
/// Implementations maintain internally synced spend state
/// (atomics, ArcSwap, etc.) — the check itself is always
/// synchronous on the hot path.
pub trait SpendPacer: Send + Sync {
    fn passes(&self, campaign: &Campaign, price: f64) -> bool;
}

/// Campaign spend tracking for pacing decisions.
///
/// Implementations must be safe for concurrent access from
/// multiple pipeline tasks. Reads should be fast (sub-microsecond)
/// since they sit on the auction hot path.
///
/// Spend values are in dollars. Freshness depends on the
/// implementation — in-memory atomics are near-realtime,
/// external stores may lag by the sync interval.
pub trait SpendTracker: Send + Sync {
    /// Total spend in dollars for this campaign across the entire flight.
    /// Returns None if the campaign is not registered — callers must
    /// treat None as deny (fail closed).
    fn total_spend(&self, campaign_id: &str) -> Option<f64>;

    /// Record incremental spend from a billed impression.
    /// Called from the billing events pipeline.
    fn record_spend(&self, campaign_id: &str, amount: f64);

    /// Register a campaign for tracking with initial spend.
    /// Must be called at startup for every active campaign.
    fn register(&self, campaign_id: &str, initial_spend: f64);
}

/// Deal delivery pacing check.
/// Gates whether a deal participates in matching based on
/// its impression delivery schedule. Synchronous on the hot path.
pub trait DealPacer: Send + Sync {
    /// Should this deal match right now given its delivery schedule?
    /// Returns true if the deal has budget remaining and passes
    /// its rate limit window.
    fn passes(&self, deal: &Deal) -> bool;

    /// Record a delivered impression against this deal.
    /// Called from the billing events pipeline.
    fn record_impression(&self, deal_id: &str);
}

/// Deal impression tracking for pacing decisions.
/// Analogous to SpendTracker but counts impressions, not dollars.
pub trait DealImpressionTracker: Send + Sync {
    /// Total impressions delivered for this deal across its flight.
    /// Returns None if the deal is not registered — callers must
    /// treat None as deny (fail closed).
    fn total_impressions(&self, deal_id: &str) -> Option<u64>;

    /// Record an impression from a billing event.
    fn record_impression(&self, deal_id: &str);

    /// Register a deal for tracking with initial impression count.
    /// Must be called at startup for every active deal.
    fn register(&self, deal_id: &str, initial_impressions: u64);
}
