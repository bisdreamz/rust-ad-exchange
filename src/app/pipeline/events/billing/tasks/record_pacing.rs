use crate::app::pipeline::events::billing::context::BillingEventContext;
use crate::app::pipeline::ortb::direct::pacing::{DealPacer, SpendTracker};
use anyhow::{Error, anyhow};
use pipeline::BlockingTask;
use std::sync::Arc;
use tracing::{debug, trace};

/// Updates spend tracker and deal pacer from billing events.
/// Always runs — pacing must stay accurate regardless of
/// whether Firestore counter stores are present.
pub struct RecordPacingTask {
    spend_tracker: Arc<dyn SpendTracker>,
    deal_pacer: Arc<dyn DealPacer>,
}

impl RecordPacingTask {
    pub fn new(spend_tracker: Arc<dyn SpendTracker>, deal_pacer: Arc<dyn DealPacer>) -> Self {
        Self {
            spend_tracker,
            deal_pacer,
        }
    }
}

impl BlockingTask<BillingEventContext, Error> for RecordPacingTask {
    fn run(&self, context: &BillingEventContext) -> Result<(), Error> {
        let details = context
            .details
            .get()
            .ok_or_else(|| anyhow!("No billing event details on context!"))?;

        let notice = context
            .bid_notice
            .get()
            .ok_or_else(|| anyhow!("No bid notice on billing context!"))?;

        if let Some(deal) = &notice.deal {
            self.deal_pacer.record_impression(&deal.id);
            trace!(deal = %deal.id, "Deal impression recorded");
        }

        if let Some(direct) = &notice.direct {
            let campaign = &direct.campaign;
            // Record the raw CPM rate — tracker stores CPM sums.
            // Actual dollars = cpm / 1000; conversion happens in the pacer.
            self.spend_tracker
                .record_spend(&campaign.id, details.cpm_gross);

            let total_cpm = self.spend_tracker.total_spend(&campaign.id);
            let daily_cpm = self.spend_tracker.daily_spend(&campaign.id);
            debug!(
                campaign = %campaign.id,
                cpm = details.cpm_gross,
                spend_dollars = details.cpm_gross / 1000.0,
                budget = campaign.budget,
                budget_type = ?campaign.budget_type,
                total_dollars = format_args!("{:.4}", total_cpm / 1000.0),
                daily_dollars = format_args!("{:.4}", daily_cpm / 1000.0),
                "Campaign spend recorded"
            );
        }

        Ok(())
    }
}
