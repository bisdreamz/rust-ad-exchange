use crate::app::config::BidderConfig;
use crate::core::models::bidder::{Bidder, Endpoint};
use crate::core::providers::{Provider, ProviderEvent};
use anyhow::Error;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::debug;

pub enum DemandChange {
    Added {
        bidder: Arc<Bidder>,
        endpoints: Vec<Arc<Endpoint>>,
    },
    Modified {
        bidder: Arc<Bidder>,
        endpoints: Vec<Arc<Endpoint>>,
        prev_endpoints: Vec<Arc<Endpoint>>,
    },
    Removed {
        bidder_id: String,
        prev_endpoints: Vec<Arc<Endpoint>>,
    },
}

struct BidderData {
    list: Vec<(Arc<Bidder>, Vec<Arc<Endpoint>>)>,
    index: HashMap<String, Arc<Bidder>>,
}

pub struct DemandManager {
    data: RwLock<BidderData>,
    callbacks: RwLock<Vec<Box<dyn Fn(&DemandChange) + Send + Sync>>>,
}

impl DemandManager {
    pub async fn start(provider: Arc<dyn Provider<BidderConfig>>) -> Result<Arc<Self>, Error> {
        let manager = Arc::new(Self {
            data: RwLock::new(BidderData {
                list: Vec::new(),
                index: HashMap::new(),
            }),
            callbacks: RwLock::new(Vec::new()),
        });

        let mgr = manager.clone();
        let initial = provider
            .start(Box::new(move |event| mgr.handle_event(event)))
            .await?;

        manager.load(initial);
        Ok(manager)
    }

    pub fn on_change(&self, cb: Box<dyn Fn(&DemandChange) + Send + Sync>) {
        self.callbacks.write().push(cb);
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
        let change = {
            let mut data = self.data.write();

            match event {
                ProviderEvent::Added(bc) => {
                    debug!("Bidder added: {}", bc.bidder.id);
                    let bidder = Arc::new(bc.bidder);
                    let endpoints: Vec<Arc<Endpoint>> =
                        bc.endpoints.into_iter().map(Arc::new).collect();

                    data.index.insert(bidder.id.clone(), bidder.clone());
                    data.list.push((bidder.clone(), endpoints.clone()));

                    DemandChange::Added { bidder, endpoints }
                }
                ProviderEvent::Modified(bc) => {
                    debug!("Bidder modified: {}", bc.bidder.id);
                    let bidder = Arc::new(bc.bidder);
                    let endpoints: Vec<Arc<Endpoint>> =
                        bc.endpoints.into_iter().map(Arc::new).collect();

                    let prev_endpoints = data
                        .list
                        .iter()
                        .find(|(b, _)| b.id == bidder.id)
                        .map(|(_, eps)| eps.clone())
                        .unwrap_or_default();

                    data.index.insert(bidder.id.clone(), bidder.clone());

                    if let Some(pos) = data.list.iter().position(|(b, _)| b.id == bidder.id) {
                        data.list[pos] = (bidder.clone(), endpoints.clone());
                    } else {
                        data.list.push((bidder.clone(), endpoints.clone()));
                    }

                    DemandChange::Modified {
                        bidder,
                        endpoints,
                        prev_endpoints,
                    }
                }
                ProviderEvent::Removed(id) => {
                    debug!("Bidder removed: {}", id);

                    let prev_endpoints = data
                        .list
                        .iter()
                        .find(|(b, _)| b.id == id)
                        .map(|(_, eps)| eps.clone())
                        .unwrap_or_default();

                    data.index.remove(&id);
                    data.list.retain(|(b, _)| b.id != id);

                    DemandChange::Removed {
                        bidder_id: id,
                        prev_endpoints,
                    }
                }
            }
        };

        for cb in self.callbacks.read().iter() {
            cb(&change);
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
