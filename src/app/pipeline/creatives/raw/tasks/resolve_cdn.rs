use crate::app::pipeline::creatives::macros::resolve_creative_content;
use crate::app::pipeline::creatives::raw::context::RawCreativeContext;
use anyhow::Error;
use pipeline::BlockingTask;
use rtb::child_span_info;

pub struct ResolveCdnTask {
    cdn_base: String,
}

impl ResolveCdnTask {
    pub fn new(cdn_base: String) -> Self {
        Self { cdn_base }
    }
}

impl BlockingTask<RawCreativeContext, Error> for ResolveCdnTask {
    fn run(&self, context: &RawCreativeContext) -> Result<(), Error> {
        let span = child_span_info!(
            "resolve_cdn_task",
            creative_id = context.creative_id.as_str(),
            result = tracing::field::Empty,
        );

        let Some(creative) = context.creative.get() else {
            span.record("result", "no_creative");
            return Ok(());
        };

        let resolved = resolve_creative_content(&creative.content, &self.cdn_base, "#");

        span.record("result", "resolved");

        context
            .resolved_content
            .set(resolved)
            .map_err(|_| anyhow::anyhow!("resolved_content already set"))?;

        Ok(())
    }
}
