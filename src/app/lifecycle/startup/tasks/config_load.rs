use crate::app::lifecycle::context::StartupContext;
use crate::core::config_manager::ConfigManager;
use anyhow::Error;
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
    fn run(&self, _ctx: &StartupContext) -> Result<(), Error> {
        self.manager.start()?;

        println!("Config loaded");
        println!("{:?}", self.manager.get());

        Ok(())
    }
}