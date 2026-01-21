use crate::app::config::SchainConfig;
use crate::app::pipeline::ortb::tasks::utils;
use crate::app::pipeline::ortb::{AuctionContext};
use anyhow::{anyhow, Error};
use pipeline::BlockingTask;
use rtb::bid_request::{SourceBuilder, SupplyChainBuilder, SupplyChainNodeBuilder};
use rtb::child_span_info;
use tracing::{debug, trace};

/// Will append our schain hop to the supplychain obj
/// if a config is Some
pub struct SchainAppendTask {
    config: Option<SchainConfig>
}

impl SchainAppendTask {
    pub fn new(config: Option<SchainConfig>) -> Self {
        Self { config }
    }
}

impl BlockingTask<AuctionContext, Error> for SchainAppendTask {
    fn run(&self, context: &AuctionContext) -> Result<(), Error> {
        let config = match &self.config {
            None => {
                trace!("No schain config present, skipping schain append");

                return Ok(())
            },
            Some(config) => config
        };

        let span = child_span_info!(
            "schain_append_task",
            schain = tracing::field::Empty
        )
        .entered();

        let mut req = context.req.write();

        let mut source = match req.source.take() {
            Some(s) => s,
            None => {
                SourceBuilder::default()
                    .tid(req.id.clone())
                    .build()
                    .map_err(|_| anyhow!("Failed to create source object"))?
            }
        };

        // take schain to we can ensure it ends up in a single location
        let taken_schain_opt = utils::schain::take_schain(&mut source);

        let node = SupplyChainNodeBuilder::default()
            .hp(true)
            .rid(req.id.clone())
            .sid(context.pubid.clone())
            .asi(config.asi.clone())
            .name(config.name.clone())
            .build()
            .map_err(|_| anyhow!("Failed to create schain node"))?;

        let schain = match taken_schain_opt {
            Some(mut schain) => {
                debug!("Source object present, adding schain node");

                schain.nodes.push(node);

                schain
            },
            None => {
                // no schain received, expecting partners behave,
                // this is a direct publisher w/o hops. TODO
                // later integration methods to really ensure this!
                SupplyChainBuilder::default()
                    .nodes(vec![node])
                    .complete(true)
                    .ver("1.0")
                    .build()
                    .map_err(|_| anyhow!("Failed to create schain object"))?
            }
        };

        if !span.is_none() {
            span.record("schain", tracing::field::debug(&schain));
        }

        source.schain = Some(schain);
        req.source = Some(source);

        debug!("Appended schain written to source.schain");

        Ok(())
    }
}
