use crate::core::cluster::ClusterDiscovery;

/// A static peer discovery implementation
/// which always returns the same peer
/// count, e.g. for testing of bare metal
/// deployments w/o k8s api
pub struct FixedClusterDiscovery {
    peer_count: usize,
}

impl FixedClusterDiscovery {
    pub fn new(peer_count: usize) -> Self {
        assert!(peer_count > 0, "Cluster size must be > 0");

        Self { peer_count }
    }
}

impl ClusterDiscovery for FixedClusterDiscovery {
    fn cluster_size(&self) -> usize {
        self.peer_count
    }

    fn on_change(&self, _cb: Box<dyn Fn(usize) + Send + Sync>) {
        // Static config with no changes ever,
        // so nothin' to do
    }
}
