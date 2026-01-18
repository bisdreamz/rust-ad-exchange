use crate::app::config::ClusterConfig;
use crate::app::context::StartupContext;
use crate::core::cluster::ClusterDiscovery;
use crate::core::cluster::impls::{FixedClusterDiscovery, KubeClusterDiscovery};
use anyhow::{Error, anyhow};
use async_trait::async_trait;
use pipeline::AsyncTask;
use std::sync::Arc;
use tracing::{info, instrument};

/// Adds the cluster discovery agent
/// to the startup context and ensures
/// a successful connection if applicable
pub struct ClusterDiscoveryTask;

#[async_trait]
impl AsyncTask<StartupContext, Error> for ClusterDiscoveryTask {
    #[instrument(skip_all, name = "cluster_discovery_task")]
    async fn run(&self, context: &StartupContext) -> Result<(), Error> {
        let cluster_cfg = &context
            .config
            .get()
            .ok_or(anyhow!("Cluster config not set on startup context!"))?
            .cluster;

        let cluster_discovery: Arc<dyn ClusterDiscovery> = match cluster_cfg {
            ClusterConfig::Fixed(cluster_sz) => {
                info!("Cluster size fixed to {}", cluster_sz);

                Arc::new(FixedClusterDiscovery::new(cluster_sz.clone()))
            }
            ClusterConfig::K8s => {
                let k8s = KubeClusterDiscovery::start().await.map_err(|e| {
                    anyhow!("Failed to connect to kube cluster API: {}", e.to_string())
                })?;

                info!(
                    "Connected to k8s for cluster discovery, current size {}",
                    k8s.cluster_size()
                );

                Arc::new(k8s)
            }
        };

        context
            .cluster_manager
            .set(cluster_discovery)
            .map_err(|_| anyhow!("Failed to set cluster manager on startup context"))?;

        Ok(())
    }
}
