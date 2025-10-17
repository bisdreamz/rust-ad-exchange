use crate::app::lifecycle::context::StartupContext;
use crate::app::core::config_manager::ConfigManager;
use anyhow::Error;
use pipeline::BlockingTask;

pub (crate) struct ConfigLoadTask {
    manager: ConfigManager,
}

impl ConfigLoadTask {
    pub fn new(manager: ConfigManager) -> Self {
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