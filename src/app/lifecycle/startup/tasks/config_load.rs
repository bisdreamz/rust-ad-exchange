use crate::app::lifecycle::context::StartupContext;
use crate::core::config_manager::ConfigManager;
use anyhow::{bail, format_err, Error};
use pipeline::BlockingTask;
use std::sync::Arc;

pub struct ConfigLoadTask {
    manager: Arc<ConfigManager>
}

impl ConfigLoadTask {
    pub fn new(manager: Arc<ConfigManager>) -> Self {
        Self { manager }
    }
}

impl BlockingTask<StartupContext, anyhow::Error> for ConfigLoadTask {
    fn run(&self, context: &StartupContext) -> Result<(), Error> {
        self.manager.start()?;

        println!("Config loaded");
        println!("{:?}", self.manager.get());

        context.config.set(self.manager.get().clone())
            .map_err(|e| format_err!("Error assigning config to context: {:?}", e))?;

        Ok(())
    }
}