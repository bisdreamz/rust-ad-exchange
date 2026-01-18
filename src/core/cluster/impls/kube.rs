use crate::core::cluster::ClusterDiscovery;
use anyhow::Result;
use kube_discovery::{KubeDiscovery, PeerEvent};
use std::sync::{Arc, RwLock};
use tracing::{error, info, trace};

/// Discovers peers via k8s api
/// to monitor cluster sizing
pub struct KubeClusterDiscovery {
    kd: KubeDiscovery,
    callbacks: Arc<RwLock<Vec<Box<dyn Fn(usize) + Send + Sync>>>>,
}

impl KubeClusterDiscovery {
    /// Connect to kube api and start cluster monitoring
    pub async fn start() -> Result<KubeClusterDiscovery> {
        let callbacks: Arc<RwLock<Vec<Box<dyn Fn(usize) + Send + Sync>>>> =
            Arc::new(RwLock::new(Vec::new()));

        let callbacks_clone = callbacks.clone();

        let kd = KubeDiscovery::start_auto(
            |e| error!("Error in k8s peer discovery service: {}", e),
            Some(move |peer_event: PeerEvent| {
                let new_size = match &peer_event {
                    PeerEvent::Added { peer, total } => {
                        info!(
                            "New peer discovered: {}, cluster size: {}",
                            peer.hostname, total
                        );
                        *total
                    }
                    PeerEvent::Removed { peer, total } => {
                        info!("Peer removed: {}, cluster size: {}", peer.hostname, total);
                        *total
                    }
                };

                if let Ok(cbs) = callbacks_clone.read() {
                    for cb in cbs.iter() {
                        trace!("Invoking call back for cluster change handler");

                        cb(new_size);
                    }
                }
            }),
        )
        .await?;

        Ok(Self { kd, callbacks })
    }
}

impl ClusterDiscovery for KubeClusterDiscovery {
    fn cluster_size(&self) -> usize {
        self.kd.peer_count(true)
    }

    fn on_change(&self, cb: Box<dyn Fn(usize) + Send + Sync>) {
        if let Ok(mut cbs) = self.callbacks.write() {
            cbs.push(cb);
        }
    }
}
