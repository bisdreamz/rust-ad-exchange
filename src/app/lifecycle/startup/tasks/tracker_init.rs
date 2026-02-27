use crate::app::context::StartupContext;
use crate::app::pipeline::ortb::direct::pacing::{
    CampaignSpendPacer, DealImpressionTracker, EvenDealPacer, FirestoreDealTracker,
    FirestoreSpendTracker, InMemoryDealTracker, InMemorySpendTracker, system_epoch_clock,
};
use crate::core::firestore::counters::campaign::SPEND_COLLECTION;
use crate::core::firestore::counters::deal::DEAL_PACING_COLLECTION;
use crate::core::providers::FirestoreProvider;
use anyhow::{Error, anyhow};
use async_trait::async_trait;
use pipeline::AsyncTask;
use std::sync::Arc;
use tracing::{info, instrument};

/// Default pacing window for deal delivery rate limiting (seconds).
const DEAL_PACING_WINDOW_SECS: u64 = 60;

/// Default pacing window for campaign spend rate limiting (seconds).
const CAMPAIGN_PACING_WINDOW_SECS: u64 = 3600;

/// Creates spend and deal trackers + the deal pacer based on
/// whether Firestore is configured. Must run after CounterStoresTask,
/// ClusterDiscoveryTask, and DirectManagersLoadTask.
pub struct TrackerInitTask;

#[async_trait]
impl AsyncTask<StartupContext, Error> for TrackerInitTask {
    #[instrument(skip_all, name = "tracker_init_task")]
    async fn run(&self, context: &StartupContext) -> Result<(), Error> {
        let firestore_opt = context
            .firestore
            .get()
            .ok_or_else(|| anyhow!("Firestore state not set yet on context!"))?;

        let cluster = context
            .cluster_manager
            .get()
            .ok_or_else(|| anyhow!("Cluster manager not set yet on context!"))?
            .clone();

        let clock = system_epoch_clock();

        // --- Spend tracker ---
        match firestore_opt {
            Some(db) => {
                let provider = Arc::new(FirestoreProvider::new(db.clone(), SPEND_COLLECTION));
                let tracker = FirestoreSpendTracker::start(provider).await?;
                context
                    .spend_tracker
                    .set(tracker)
                    .map_err(|_| anyhow!("Failed to set spend tracker on context"))?;

                info!("Started Firestore spend tracker");
            }
            None => {
                let tracker = Arc::new(InMemorySpendTracker::new());
                context
                    .spend_tracker
                    .set(tracker)
                    .map_err(|_| anyhow!("Failed to set spend tracker on context"))?;

                info!("Using in-memory spend tracker");
            }
        }

        // --- Deal impression tracker ---
        let deal_tracker: Arc<dyn DealImpressionTracker> = match firestore_opt {
            Some(db) => {
                let provider = Arc::new(FirestoreProvider::new(db.clone(), DEAL_PACING_COLLECTION));
                let tracker = FirestoreDealTracker::start(provider).await?;
                info!("Started Firestore deal impression tracker");
                tracker
            }
            None => {
                info!("Using in-memory deal impression tracker");
                Arc::new(InMemoryDealTracker::new())
            }
        };

        context
            .deal_tracker
            .set(deal_tracker.clone())
            .map_err(|_| anyhow!("Failed to set deal tracker on context"))?;

        // --- Deal pacer ---
        let cluster_for_pacer = cluster.clone();
        let deal_pacer = Arc::new(EvenDealPacer::new(
            deal_tracker,
            Box::new(move || cluster_for_pacer.cluster_size()),
            DEAL_PACING_WINDOW_SECS,
            clock.clone(),
        ));
        context
            .deal_pacer
            .set(deal_pacer)
            .map_err(|_| anyhow!("Failed to set deal pacer on context"))?;

        info!("Deal pacer initialized");

        // --- Campaign spend pacer ---
        let spend_tracker_for_pacer = context
            .spend_tracker
            .get()
            .ok_or_else(|| anyhow!("Spend tracker not set (just created it?)"))?
            .clone();

        let spend_pacer = Arc::new(CampaignSpendPacer::new(
            spend_tracker_for_pacer,
            cluster.clone(),
            CAMPAIGN_PACING_WINDOW_SECS,
            clock,
        ));
        context
            .spend_pacer
            .set(spend_pacer)
            .map_err(|_| anyhow!("Failed to set spend pacer on context"))?;

        info!("Campaign spend pacer initialized");

        Ok(())
    }
}
