use crate::app::context::StartupContext;
use crate::core::managers::{
    AdvertiserManager, BuyerManager, CampaignManager, CreativeManager, DealManager,
};
use crate::core::providers::FirestoreProvider;
use anyhow::{Error, anyhow};
use async_trait::async_trait;
use pipeline::AsyncTask;
use std::sync::Arc;
use tracing::{info, instrument};

/// Loads all direct-campaign entity managers (Buyer, Advertiser,
/// Campaign, Creative, Deal) from Firestore. If Firestore is not
/// configured, managers start with empty state — the
/// DirectCampaignMatchingTask still runs but finds no campaigns.
pub struct DirectManagersLoadTask;

#[async_trait]
impl AsyncTask<StartupContext, Error> for DirectManagersLoadTask {
    #[instrument(skip_all, name = "direct_managers_load_task")]
    async fn run(&self, context: &StartupContext) -> Result<(), Error> {
        let firestore_opt = context
            .firestore
            .get()
            .ok_or_else(|| anyhow!("Firestore state not set"))?;

        let db = match firestore_opt {
            Some(db) => db.clone(),
            None => {
                info!("No Firestore configured, direct campaign managers will have empty state");
                return Ok(());
            }
        };

        let (buyers, advertisers, campaigns, creatives, deals) = tokio::try_join!(
            BuyerManager::start(Arc::new(FirestoreProvider::new(db.clone(), "buyers"))),
            AdvertiserManager::start(Arc::new(FirestoreProvider::new(db.clone(), "advertisers"))),
            CampaignManager::start(Arc::new(FirestoreProvider::new(db.clone(), "campaigns"))),
            CreativeManager::start(Arc::new(FirestoreProvider::new(db.clone(), "creatives"))),
            DealManager::start(Arc::new(FirestoreProvider::new(db.clone(), "deals"))),
        )?;

        context
            .buyer_manager
            .set(buyers)
            .map_err(|_| anyhow!("buyer_manager already set on context"))?;
        context
            .advertiser_manager
            .set(advertisers)
            .map_err(|_| anyhow!("advertiser_manager already set on context"))?;
        context
            .campaign_manager
            .set(campaigns)
            .map_err(|_| anyhow!("campaign_manager already set on context"))?;
        context
            .creative_manager
            .set(creatives)
            .map_err(|_| anyhow!("creative_manager already set on context"))?;
        context
            .deal_manager
            .set(deals)
            .map_err(|_| anyhow!("deal_manager already set on context"))?;

        info!("All direct campaign managers loaded");
        Ok(())
    }
}
