use crate::core::managers::BidderManager;
use crate::core::models::shaping::{ShapingFeature, TrafficShaping};
use crate::core::shaping::tree::TreeShaper;
use anyhow::{Error, bail};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, info};

fn validate_tree_params(control_percent: u32, features: &Vec<ShapingFeature>) -> Result<(), Error> {
    if control_percent == 0 {
        bail!("Endpoint cannot have control percent of 0");
    }

    if features.is_empty() {
        bail!("Endpoint cannot have empty features");
    }

    Ok(())
}
pub struct ShaperManager {
    shapers: HashMap<String, Option<Arc<TreeShaper>>>, // endpoint id -> entry
}

impl ShaperManager {
    pub fn new(manager: &BidderManager) -> Result<Self, Error> {
        let mut shape_map = HashMap::new();

        for (bidder, endpoints) in manager.bidders() {
            for endpoint in endpoints {
                if endpoint.enabled == false {
                    debug!(
                        "Skipping endpoint {} because it is not enabled",
                        endpoint.name
                    );
                    continue;
                }

                let shaping_opt = match &endpoint.shaping {
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
                        info!(
                            "Bidder {} endpoint {} shaping ctrl {}% metric {} features {:?}",
                            bidder.name, endpoint.name, control_percent, metric, features
                        );

                        validate_tree_params(*control_percent, features)?;

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
                };

                shape_map.insert(endpoint.name.clone(), shaping_opt);
            }
        }

        Ok(Self { shapers: shape_map })
    }

    pub fn shaper(&self, _bidder_id: &String, endpoint_id: &String) -> Option<Arc<TreeShaper>> {
        match self.shapers.get(endpoint_id) {
            Some(shaper_entry) => shaper_entry.clone(),
            None => None,
        }
    }
}
