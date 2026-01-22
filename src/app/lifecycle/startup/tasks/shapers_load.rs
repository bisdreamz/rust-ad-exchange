use crate::app::context::StartupContext;
use crate::core::managers::ShaperManager;
use anyhow::{Error, anyhow};
use pipeline::BlockingTask;
use rtb::child_span_info;
use std::sync::Arc;
use tracing::debug;

pub struct ShapersManagerLoadTask;

impl BlockingTask<StartupContext, Error> for ShapersManagerLoadTask {
    fn run(&self, context: &StartupContext) -> Result<(), Error> {
        let _span = child_span_info!("shaper_manager_load_task").entered();

        let bidder_manager = context
            .bidder_manager
            .get()
            .ok_or_else(|| anyhow!("Bidder manager not initialized, cant setup shaping"))?;

        let shaper_manager = ShaperManager::new(bidder_manager)
            .map_err(|e| anyhow!("Failed loading shaping manager: {:?}", e))?;

        context
            .shaping_manager
            .set(Arc::new(shaper_manager))
            .map_err(|_| anyhow!("Can't set shaping on context, exist already?"))?;

        debug!("Attached shaping manager to startup context");

        Ok(())
    }
}
