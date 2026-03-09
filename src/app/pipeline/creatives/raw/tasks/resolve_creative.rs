use crate::app::pipeline::creatives::raw::context::RawCreativeContext;
use crate::core::managers::CreativeManager;
use anyhow::Error;
use pipeline::BlockingTask;
use rtb::child_span_info;
use std::sync::Arc;
use tracing::debug;

/// Resolves the creative ID from the request path against the
/// CreativeManager cache. Attaches the full creative to the context
/// if found, otherwise leaves it empty (handler returns 204).
pub struct ResolveCreativeTask {
    creative_manager: Arc<CreativeManager>,
}

impl ResolveCreativeTask {
    pub fn new(creative_manager: Arc<CreativeManager>) -> Self {
        Self { creative_manager }
    }
}

impl BlockingTask<RawCreativeContext, Error> for ResolveCreativeTask {
    fn run(&self, context: &RawCreativeContext) -> Result<(), Error> {
        let span = child_span_info!(
            "resolve_creative_task",
            creative_id = context.creative_id.as_str(),
            result = tracing::field::Empty,
            format = tracing::field::Empty,
            creative = tracing::field::Empty,
        );

        match self.creative_manager.by_id(&context.creative_id) {
            Some(creative) => {
                span.record("result", "found");
                span.record("format", creative.format.as_str());
                span.record("creative", tracing::field::debug(&creative));

                debug!(
                    creative_id = context.creative_id.as_str(),
                    format = creative.format.as_str(),
                    "Resolved creative for raw serving"
                );

                context
                    .creative
                    .set(creative)
                    .map_err(|_| anyhow::anyhow!("Creative already set on context"))?;
            }
            None => {
                span.record("result", "not_found");
                debug!(
                    creative_id = context.creative_id.as_str(),
                    "Creative not found"
                );
            }
        }

        Ok(())
    }
}
