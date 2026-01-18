use async_trait::async_trait;

/// Generic trait for discovery of peer servers
/// in a cluster, e.g. by bare metal or kubernetes
#[async_trait]
pub trait ClusterDiscovery: Send + Sync {

    /// Returns the size (total count) of all healthy
    /// targets within the cluster, including self.
    /// Not async as cluster
    /// management and updates are expected to
    /// be handled in the background by impl
    fn cluster_size(&self) -> usize;

    /// Register a callback to be invoked when
    /// the cluster size changes. Impls should
    /// support registration of multiple callbacks
    ///
    /// Registered callback receives a param
    /// of the update cluster size
    fn on_change(&self, cb: Box<dyn Fn(usize) + Send + Sync>);

}