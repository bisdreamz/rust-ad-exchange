use crate::app::context::StartupContext;
use crate::core::config_manager::ConfigManager;
use crate::core::managers::PublisherManager;
use anyhow::{Error, format_err};
use pipeline::BlockingTask;
use std::sync::Arc;
use tracing::instrument;

/// Responsible for loading the publisher configs
/// Right now, simply a task to run after config manager
/// task runs to parse+validate the config, but later
/// could load real configs from a db
pub struct PubsManagerLoadTask {
    config_manager: Arc<ConfigManager>,
}

impl PubsManagerLoadTask {
    pub fn new(config_manager: Arc<ConfigManager>) -> Self {
        PubsManagerLoadTask { config_manager }
    }
}

impl BlockingTask<StartupContext, Error> for PubsManagerLoadTask {
    #[instrument(skip_all, name = "pubs_manager_load_task")]
    fn run(&self, context: &StartupContext) -> Result<(), Error> {
        let pub_manager = PublisherManager::new(self.config_manager.as_ref());

        context
            .pub_manager
            .set(Arc::new(pub_manager))
            .map_err(|_| format_err!("Can't init publisher manager"))?;

        Ok(())
    }
}
