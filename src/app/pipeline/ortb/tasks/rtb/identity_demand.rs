use crate::app::pipeline::ortb::AuctionContext;
use crate::app::pipeline::ortb::context::{BidderContext, IdentityContext};
use crate::core::models::publisher::Publisher;
use crate::core::usersync::SyncStore;
use anyhow::{Error, anyhow};
use async_trait::async_trait;
use opentelemetry::metrics::Counter;
use opentelemetry::{KeyValue, global};
use pipeline::AsyncTask;
use rtb::child_span_info;
use std::sync::{Arc, LazyLock};
use tracing::{Instrument, Span, debug, trace, warn};

static COUNTER_BUYERUID_MATCHES: LazyLock<Counter<u64>> = LazyLock::new(|| {
    global::meter("rex:demand:syncing")
        .u64_counter("syncing.demand_buyeruid_matches")
        .with_description("Count of demand specific buyeruid matches")
        .with_unit("1")
        .build()
});

/// Responsible for injecting buyeruid values into bidder specific callouts
pub struct IdentityDemandTask {
    store: Arc<dyn SyncStore>,
}

impl IdentityDemandTask {
    pub fn new(store: Arc<dyn SyncStore>) -> Self {
        Self { store }
    }
}

impl IdentityDemandTask {
    /// Injects (web) synced buyeruid values for the given demand partners
    /// if we have a local exchange id and a map for this buyer
    async fn inject_buyer_uids(
        &self,
        identity: &IdentityContext,
        bidder_contexts: &mut Vec<BidderContext>,
        publisher: &Publisher,
    ) {
        let local_uid = match identity.local_uid.get() {
            Some(local_uid) => local_uid,
            None => return,
        };

        debug!("Injecting buyeruids for local_uid {}", local_uid);

        let buyer_uids_map = match self.store.load(local_uid).await {
            Some(map) => map,
            None => return,
        };

        let span = Span::current();

        if !span.is_disabled() {
            span.record("buyer_uids", format!("{:?}", buyer_uids_map));
        }

        if tracing::enabled!(tracing::Level::TRACE) {
            trace!("Buyeruids map: {:?}", buyer_uids_map);
        }

        debug!(
            "Found {} unique buyeruids for local_uid {}",
            buyer_uids_map.len(),
            local_uid
        );

        for bidder_context in bidder_contexts.iter_mut() {
            let buyeruid = match buyer_uids_map.get(&bidder_context.bidder.id) {
                Some(buyeruid) => buyeruid,
                None => continue,
            };

            COUNTER_BUYERUID_MATCHES.add(
                1,
                &[
                    KeyValue::new("pub_id", publisher.id.clone()),
                    KeyValue::new("pub_name", publisher.name.clone()),
                    KeyValue::new("bidder_id", bidder_context.bidder.id.clone()),
                    KeyValue::new("bidder_name", bidder_context.bidder.name.clone()),
                ],
            );

            debug!(
                "Injecting buyeruid of {:?} for bidder {} callouts",
                buyeruid, bidder_context.bidder.name
            );

            for callout in bidder_context.callouts.iter_mut() {
                match &mut callout.req.user {
                    Some(user) => {
                        if !user.buyeruid.is_empty() {
                            warn!(
                                "Buyeruid already set on callout req? Should not happen! Overwriting"
                            );
                        }

                        if user.id.is_empty() {
                            warn!(
                                "Bidder callout user id (xchg id) was empty?! Injecting local uid"
                            );
                            user.id = local_uid.clone();
                        }

                        user.buyeruid = buyeruid.rid.clone();
                    }
                    None => {
                        warn!("Cannot inject buyeruid! No one created User object on buyer req?!")
                    }
                }
            }
        }
    }

    async fn run0(&self, context: &AuctionContext) -> Result<(), Error> {
        let identity = match context.identity.get() {
            Some(identity) => identity,
            None => return Ok(()),
        };

        let mut bidders = context.bidders.lock().await;
        let publisher = context
            .publisher
            .get()
            .ok_or_else(|| anyhow!("No publisher found in context!"))?;

        self.inject_buyer_uids(identity, &mut bidders, publisher)
            .await;

        Ok(())
    }
}

#[async_trait]
impl AsyncTask<AuctionContext, Error> for IdentityDemandTask {
    async fn run(&self, context: &AuctionContext) -> Result<(), Error> {
        let span = child_span_info!("identity_demand_task", buyer_uids = tracing::field::Empty);

        self.run0(context).instrument(span).await
    }
}
