use crate::app::config::BidderConfig;
use crate::core::models::bidder::{Bidder, Endpoint};
use crate::core::providers::{Provider, ProviderEvent};
use anyhow::Error;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

struct BidderData {
    list: Vec<(Arc<Bidder>, Vec<Arc<Endpoint>>)>,
    index: HashMap<String, Arc<Bidder>>,
}

pub struct DemandManager {
    data: RwLock<BidderData>,
}

impl DemandManager {
    pub async fn start(provider: Arc<dyn Provider<BidderConfig>>) -> Result<Arc<Self>, Error> {
        let manager = Arc::new(Self {
            data: RwLock::new(BidderData {
                list: Vec::new(),
                index: HashMap::new(),
            }),
        });

        let mgr = manager.clone();
        let initial = provider
            .start(Box::new(move |event| mgr.handle_event(event)))
            .await?;

        manager.load(initial);
        Ok(manager)
    }

    fn load(&self, configs: Vec<BidderConfig>) {
        let list: Vec<(Arc<Bidder>, Vec<Arc<Endpoint>>)> = configs
            .into_iter()
            .map(|bc| {
                (
                    Arc::new(bc.bidder),
                    bc.endpoints.into_iter().map(Arc::new).collect(),
                )
            })
            .collect();

        let index: HashMap<String, Arc<Bidder>> = list
            .iter()
            .map(|(bidder, _)| (bidder.id.clone(), Arc::clone(bidder)))
            .collect();

        *self.data.write() = BidderData { list, index };
    }

    fn handle_event(&self, event: ProviderEvent<BidderConfig>) {
        let mut data = self.data.write();

        match event {
            ProviderEvent::Added(bc) | ProviderEvent::Modified(bc) => {
                let bidder = Arc::new(bc.bidder);
                let endpoints: Vec<Arc<Endpoint>> =
                    bc.endpoints.into_iter().map(Arc::new).collect();

                data.index.insert(bidder.id.clone(), bidder.clone());

                if let Some(pos) = data.list.iter().position(|(b, _)| b.id == bidder.id) {
                    data.list[pos] = (bidder, endpoints);
                } else {
                    data.list.push((bidder, endpoints));
                }
            }
            ProviderEvent::Removed(id) => {
                data.index.remove(&id);
                data.list.retain(|(b, _)| b.id != id);
            }
        }
    }

    pub fn bidders_endpoints(&self) -> Vec<(Arc<Bidder>, Vec<Arc<Endpoint>>)> {
        self.data.read().list.clone()
    }

    pub fn bidders(&self) -> Vec<Arc<Bidder>> {
        self.data.read().index.values().cloned().collect()
    }

    pub fn bidder(&self, bidder_id: &str) -> Option<Arc<Bidder>> {
        self.data.read().index.get(bidder_id).cloned()
    }
}
