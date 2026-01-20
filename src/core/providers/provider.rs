use async_trait::async_trait;

#[derive(Debug, Clone)]
pub enum ProviderEvent<T> {
    Added(T),
    Modified(T),
    Removed(String),
}

#[async_trait]
pub trait Provider<T>: Send + Sync {
    async fn start(
        &self,
        on_event: Box<dyn Fn(ProviderEvent<T>) + Send + Sync>,
    ) -> Result<Vec<T>, anyhow::Error>;
}
