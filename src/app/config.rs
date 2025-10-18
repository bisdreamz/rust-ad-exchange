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

#[derive(Debug, Clone, Serialize, Deserialize, Default, Builder)]
pub struct RexConfig {
    // ssl, initial bidders, db details..
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