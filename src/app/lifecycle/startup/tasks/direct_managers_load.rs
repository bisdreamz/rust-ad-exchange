use crate::app::context::StartupContext;
use crate::core::config_manager::ConfigManager;
use crate::core::managers::{
    AdvertiserManager, BuyerManager, CampaignManager, CreativeManager, DealManager,
};
use crate::core::models::advertiser::Advertiser;
use crate::core::models::buyer::Buyer;
use crate::core::models::campaign::Campaign;
use crate::core::models::creative::Creative;
use crate::core::models::deal::Deal;
use crate::core::providers::{
    ConfigAdvertiserProvider, ConfigBuyerProvider, ConfigCampaignProvider, ConfigCreativeProvider,
    ConfigDealProvider, FirestoreProvider, Provider,
};
use anyhow::{Error, anyhow};
use async_trait::async_trait;
use pipeline::AsyncTask;
use std::sync::Arc;
use tracing::{info, instrument};

/// Loads all direct-campaign entity managers (Buyer, Advertiser,
/// Campaign, Creative, Deal) from Firestore or config fallback.
/// Follows the same pattern as BidderManagerLoadTask / PubsManagerLoadTask.
pub struct DirectManagersLoadTask {
    config_manager: Arc<ConfigManager>,
}

impl DirectManagersLoadTask {
    pub fn new(config_manager: Arc<ConfigManager>) -> Self {
        Self { config_manager }
    }
}

#[async_trait]
impl AsyncTask<StartupContext, Error> for DirectManagersLoadTask {
    #[instrument(skip_all, name = "direct_managers_load_task")]
    async fn run(&self, context: &StartupContext) -> Result<(), Error> {
        let firestore_opt = context
            .firestore
            .get()
            .ok_or_else(|| anyhow!("Firestore state not set"))?;

        let (camp_prov, crea_prov, deal_prov, buy_prov, adv_prov): (
            Arc<dyn Provider<Campaign>>,
            Arc<dyn Provider<Creative>>,
            Arc<dyn Provider<Deal>>,
            Arc<dyn Provider<Buyer>>,
            Arc<dyn Provider<Advertiser>>,
        ) = match firestore_opt {
            Some(db) => {
                info!("Loading direct managers from Firestore");
                (
                    Arc::new(FirestoreProvider::new(db.clone(), "campaigns")),
                    Arc::new(FirestoreProvider::new(db.clone(), "creatives")),
                    Arc::new(FirestoreProvider::new(db.clone(), "deals")),
                    Arc::new(FirestoreProvider::new(db.clone(), "buyers")),
                    Arc::new(FirestoreProvider::new(db.clone(), "advertisers")),
                )
            }
            None => {
                info!("Loading direct managers from config");
                let cm = &self.config_manager;
                (
                    Arc::new(ConfigCampaignProvider::new(cm.clone())),
                    Arc::new(ConfigCreativeProvider::new(cm.clone())),
                    Arc::new(ConfigDealProvider::new(cm.clone())),
                    Arc::new(ConfigBuyerProvider::new(cm.clone())),
                    Arc::new(ConfigAdvertiserProvider::new(cm.clone())),
                )
            }
        };

        let (buyers, advertisers, campaigns, creatives, deals) = tokio::try_join!(
            BuyerManager::start(buy_prov),
            AdvertiserManager::start(adv_prov),
            CampaignManager::start(camp_prov, firestore_opt.clone()),
            CreativeManager::start(crea_prov),
            DealManager::start(deal_prov, firestore_opt.clone()),
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
