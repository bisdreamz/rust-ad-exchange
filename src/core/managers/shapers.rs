use crate::core::managers::{DemandChange, DemandManager};
use crate::core::models::bidder::{Bidder, Endpoint};
use crate::core::models::shaping::{ShapingFeature, TrafficShaping};
use crate::core::shaping::tree::TreeShaper;
use anyhow::{Error, bail};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, info, warn};

fn validate_tree_params(control_percent: u32, features: &Vec<ShapingFeature>) -> Result<(), Error> {
    if control_percent == 0 {
        bail!("Endpoint cannot have control percent of 0");
    }

    if features.is_empty() {
        bail!("Endpoint cannot have empty features");
    }

    Ok(())
}

fn create_shaper_for_endpoint(bidder: &Bidder, endpoint: &Endpoint) -> Option<Arc<TreeShaper>> {
    match &endpoint.shaping {
        TrafficShaping::None => {
            info!(
                "Bidder {} endpoint {} disabled shaping",
                bidder.name, endpoint.name
            );
            None
        }
        TrafficShaping::Tree {
            control_percent,
            metric,
            features,
            min_target_metric,
        } => {
            if let Err(e) = validate_tree_params(*control_percent, features) {
                warn!(
                    "Invalid shaping params for endpoint {}: {}",
                    endpoint.name, e
                );
                return None;
            }

            info!(
                "Bidder {} endpoint {} shaping ctrl {}% metric {} features {:?}",
                bidder.name, endpoint.name, control_percent, metric, features
            );

            Some(Arc::new(TreeShaper::new(
                &features,
                &metric,
                10,
                &Duration::from_secs(10 * 60),
                control_percent.clone(),
                endpoint.qps as u32,
                min_target_metric.clone(),
            )))
        }
    }
}

pub struct ShaperManager {
    shapers: RwLock<HashMap<String, Option<Arc<TreeShaper>>>>, // endpoint name -> entry
}

impl ShaperManager {
    pub fn new(manager: &DemandManager) -> Result<Self, Error> {
        let mut shape_map = HashMap::new();

        for (bidder, endpoints) in manager.bidders_endpoints() {
            for endpoint in endpoints {
                if endpoint.enabled == false {
                    debug!(
                        "Skipping endpoint {} because it is not enabled",
                        endpoint.name
                    );
                    continue;
                }

                shape_map.insert(
                    endpoint.name.clone(),
                    create_shaper_for_endpoint(&bidder, &endpoint),
                );
            }
        }

        Ok(Self {
            shapers: RwLock::new(shape_map),
        })
    }

    pub fn register_demand_listener(mgr: Arc<Self>, demand: &DemandManager) {
        demand.on_change(Box::new(move |change| {
            mgr.handle_demand_change(change);
        }));
    }

    fn handle_demand_change(&self, change: &DemandChange) {
        let mut shapers = self.shapers.write();

        match change {
            DemandChange::Added {
                bidder, endpoints, ..
            } => {
                for ep in endpoints {
                    if !ep.enabled {
                        continue;
                    }
                    info!("Adding shaper for new endpoint {}", ep.name);
                    shapers.insert(ep.name.clone(), create_shaper_for_endpoint(bidder, ep));
                }
            }
            DemandChange::Modified {
                bidder,
                endpoints: new_eps,
                prev_endpoints,
            } => {
                // Remove endpoints no longer present
                for prev_ep in prev_endpoints {
                    if !new_eps.iter().any(|e| e.name == prev_ep.name) {
                        info!("Removing shaper for endpoint {}", prev_ep.name);
                        shapers.remove(&prev_ep.name);
                    }
                }
                // Update or create
                for ep in new_eps {
                    if !ep.enabled {
                        shapers.remove(&ep.name);
                        continue;
                    }
                    match &ep.shaping {
                        TrafficShaping::None => {
                            shapers.insert(ep.name.clone(), None);
                        }
                        TrafficShaping::Tree {
                            control_percent,
                            metric,
                            features,
                            min_target_metric,
                        } => {
                            if let Some(existing) = shapers.get(&ep.name).and_then(|o| o.as_ref()) {
                                if existing.features() == features.as_slice() {
                                    existing.update_config(
                                        metric.clone(),
                                        *control_percent,
                                        ep.qps as u32,
                                        *min_target_metric,
                                    );
                                    info!(
                                        "Dynamically updated shaper config for endpoint {}",
                                        ep.name
                                    );
                                    continue;
                                }
                            }
                            info!(
                                "Rebuilding shaper for endpoint {} (features changed or new)",
                                ep.name
                            );
                            shapers.insert(ep.name.clone(), create_shaper_for_endpoint(bidder, ep));
                        }
                    }
                }
            }
            DemandChange::Removed { bidder_id, prev_endpoints, .. } => {
                for ep in prev_endpoints {
                    info!("Bidder {} deleted, removing shaper for endpoint {}",
                        bidder_id, ep.name);
                    shapers.remove(&ep.name);
                }
            }
        }
    }

    pub fn shaper(&self, _bidder_id: &String, endpoint_id: &String) -> Option<Arc<TreeShaper>> {
        match self.shapers.read().get(endpoint_id) {
            Some(shaper_entry) => shaper_entry.clone(),
            None => None,
        }
    }
}
