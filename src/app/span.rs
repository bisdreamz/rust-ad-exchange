use anyhow::Error;
use async_trait::async_trait;
use pipeline::{AsyncTask, Pipeline};
use tracing::{Instrument, Span};

/// A task which wraps a pipeline to enable a convenience way
/// to wrap it in a span, which allows easy use both standard
/// spans as well as ['sample_or_attach_root_span'] for
/// span prefiltering
pub struct WrappedPipelineTask<T: Send + Sync> {
    pipeline: Pipeline<T, Error>,
    span_provider: Box<dyn Fn() -> Span + Send + Sync>,
}

impl<T: Send + Sync> WrappedPipelineTask<T> {
    /// Create a wrapped pipeline that will execute
    /// under the resulting span from the span provider
    pub fn new<F>(pipeline: Pipeline<T, Error>, span_provider: F) -> Self
    where F: Fn() -> Span + Sync + Send + 'static {
        WrappedPipelineTask {
            pipeline,
            span_provider: Box::new(span_provider),
        }
    }
}

#[async_trait]
impl <T: Send + Sync> AsyncTask<T, Error> for WrappedPipelineTask<T> {
    async fn run(&self, context: &T) -> Result<(), Error> {
        let span = (self.span_provider)();

        self.pipeline.run(context).instrument(span).await
    }
}