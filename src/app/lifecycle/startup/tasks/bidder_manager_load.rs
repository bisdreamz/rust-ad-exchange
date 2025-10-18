use crate::app::context::StartupContext;
use crate::core::config_manager::ConfigManager;
use crate::core::managers::bidders::BidderManager;
use anyhow::{format_err, Error};
use pipeline::BlockingTask;
use std::sync::Arc;

/// Responsible for loading the bidder configs
/// Right now, simply a task to run after config manager
/// task runs to parse+validate the config, but later
/// could load real configs from a db
pub struct BidderManagerLoadTask {
    config_manager: Arc<ConfigManager>,
}

impl BidderManagerLoadTask {
    pub fn new(config_manager: Arc<ConfigManager>) -> Self {
        BidderManagerLoadTask { config_manager }
    }
}

impl BlockingTask<StartupContext, Error> for BidderManagerLoadTask {
    fn run(&self, context: &StartupContext) -> Result<(), Error> {
        let bidder_manager = BidderManager::new(self.config_manager.as_ref());

        context.bidder_manager.set(Arc::new(bidder_manager))
            .map_err(|_| format_err!("Can't init bidder manager"))?;

        Ok(())
    }
}