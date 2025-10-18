use std::path::PathBuf;
use config::Config;
use derive_builder::Builder;
use serde::{Deserialize, Serialize};
use crate::core::models::bidder::{Bidder, Endpoint};

#[derive(Debug, Clone, Serialize, Deserialize, Default, Builder)]
pub struct BidderConfig {
    pub bidder: Bidder,
    pub endpoints: Vec<Endpoint>
}

#[derive(Debug, Clone, Serialize, Deserialize, Builder)]
pub struct CacheConfig {
    pub cache_device_sz: usize,
    pub cache_ip_sz: usize,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            cache_device_sz: 250_000,
            cache_ip_sz: 100_000
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, Builder)]
pub struct RexConfig {
    #[serde(default)]
    pub caches: CacheConfig,
    pub bidders: Vec<BidderConfig>
}

impl RexConfig {
    pub fn load(path: &PathBuf) -> Result<RexConfig, anyhow::Error> {
        let cfg = Config::builder()
            .add_source(
                config::File::from(path.to_path_buf())
            )
            .build()?;

        Ok(cfg.try_deserialize()?)
    }
}