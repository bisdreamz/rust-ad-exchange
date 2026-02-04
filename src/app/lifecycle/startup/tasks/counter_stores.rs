use crate::app::context::StartupContext;
use crate::core::firestore::counters::demand::DemandCounterStore;
use crate::core::firestore::counters::publisher::PublisherCounterStore;
use anyhow::{Error, anyhow};
use pipeline::BlockingTask;
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, instrument};

/// Responsible for creating and attaching the firestore
/// activity counters for demand and publishers stats
/// to the startup context so they may be cleanly
/// flushed and shutdown
pub struct CounterStoresTask;

impl BlockingTask<StartupContext, Error> for CounterStoresTask {
    #[instrument(skip_all, name = "counter_stores_task")]
    fn run(&self, context: &StartupContext) -> Result<(), Error> {
        // whether firestore is enabled or not, the firestore option
        // should be explicitly set
        let firestore_opt = context
            .firestore
            .get()
            .ok_or_else(|| anyhow!("Firestore state not set yet on context!"))?;

        match firestore_opt {
            None => {
                info!("Firestore is not enabled, skipping counter stores creation");

                context.counters_pub_store.set(None).map_err(|_| {
                    anyhow!("Failed to set publisher counter store on startup context")
                })?;
                context.counters_demand_store.set(None).map_err(|_| {
                    anyhow!("Failed to set demand counter store on startup context")
                })?;
            }
            Some(firestore) => {
                let pub_store = PublisherCounterStore::new(
                    firestore.clone(),
                    "stats_by_pub",
                    Duration::from_hours(1),
                    Duration::from_mins(1),
                );
                context
                    .counters_pub_store
                    .set(Some(Arc::new(pub_store)))
                    .map_err(|_| {
                        anyhow!("Failed to set publisher counter store on startup context")
                    })?;

                let demand_store = DemandCounterStore::new(
                    firestore.clone(),
                    "stats_by_demand",
                    Duration::from_hours(1),
                    Duration::from_mins(1),
                );
                context
                    .counters_demand_store
                    .set(Some(Arc::new(demand_store)))
                    .map_err(|_| {
                        anyhow!("Failed to set demand counter store on startup context")
                    })?;

                info!("Created publisher and demand counter stores");
            }
        }

        Ok(())
    }
}
