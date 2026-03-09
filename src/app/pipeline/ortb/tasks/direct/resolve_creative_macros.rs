use crate::app::pipeline::creatives::macros::resolve_creative_content;
use crate::app::pipeline::ortb::AuctionContext;
use crate::core::managers::AdvertiserManager;
use anyhow::Error;
use async_trait::async_trait;
use pipeline::AsyncTask;
use rtb::bid_response::bid::AdmOneof;
use std::sync::Arc;
use tracing::{debug, warn};

/// Resolves `${CDN_DOMAIN}` and `${CLICK_URL}` macros in staged direct
/// campaign bids. Runs after `DirectCampaignMatchingTask` and before
/// merge, so bids enter the shared pipeline with fully resolved adm.
pub struct ResolveDirectCreativeMacrosTask {
    cdn_domain: String,
    advertiser_manager: Arc<AdvertiserManager>,
}

impl ResolveDirectCreativeMacrosTask {
    pub fn new(cdn_domain: String, advertiser_manager: Arc<AdvertiserManager>) -> Self {
        Self {
            cdn_domain,
            advertiser_manager,
        }
    }
}

#[async_trait]
impl AsyncTask<AuctionContext, Error> for ResolveDirectCreativeMacrosTask {
    async fn run(&self, ctx: &AuctionContext) -> Result<(), Error> {
        let mut staging = ctx.direct_bid_staging.lock().await;
        if staging.is_empty() {
            return Ok(());
        }

        let mut drop_indices = Vec::new();

        for (i, bid_ctx) in staging.iter_mut().enumerate() {
            let direct = match bid_ctx.direct.get() {
                Some(d) => d,
                None => continue,
            };

            let campaign = &direct.campaign;

            // Look up advertiser — required for click URL fallback
            let advertiser = match self.advertiser_manager.get(&campaign.advertiser_id) {
                Some(a) => a,
                None => {
                    warn!(
                        campaign_id = %campaign.id,
                        advertiser_id = %campaign.advertiser_id,
                        "Advertiser not found for direct campaign bid, dropping bid"
                    );
                    drop_indices.push(i);
                    continue;
                }
            };

            // Campaign click_url takes priority, fall back to advertiser domain
            let click_url = match &campaign.click_url {
                Some(url) => url.clone(),
                None => format!("https://{}", advertiser.domain),
            };

            // Resolve macros in adm
            if let Some(AdmOneof::Adm(ref adm)) = bid_ctx.bid.adm_oneof {
                let resolved = resolve_creative_content(adm, &self.cdn_domain, &click_url);
                bid_ctx.bid.adm_oneof = Some(AdmOneof::Adm(resolved));
            }
        }

        // Remove dropped bids in reverse order to preserve indices
        for i in drop_indices.into_iter().rev() {
            let dropped = staging.swap_remove(i);
            debug!(
                bid_id = %dropped.bid.id,
                "Dropped direct bid due to missing advertiser"
            );
        }

        Ok(())
    }
}
