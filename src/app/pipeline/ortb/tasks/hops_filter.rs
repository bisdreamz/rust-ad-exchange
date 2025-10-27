use crate::app::pipeline::ortb::AuctionContext;
use anyhow::{Error, anyhow, bail};
use pipeline::BlockingTask;
use rtb::child_span_info;
use rtb::common::bidresponsestate::BidResponseState;
use tracing::log::debug;

/// Will hard block inbound requests which exceed the
/// configured allowed schain nodes length limit
pub struct SchainHopsGlobalFilter {
    hop_limit: u32,
}

impl SchainHopsGlobalFilter {
    pub fn new(hop_limit: u32) -> Self {
        Self { hop_limit }
    }
}

impl BlockingTask<AuctionContext, Error> for SchainHopsGlobalFilter {
    fn run(&self, context: &AuctionContext) -> Result<(), Error> {
        let span = child_span_info!(
            "schain_hops_filter_task",
            hops_filter_result = tracing::field::Empty,
            hops_filter_len = tracing::field::Empty,
        )
        .entered();

        if self.hop_limit == 0 {
            debug!("No schain limit configured, skipping filter");
            span.record("hops_filter_result", "skipped_no_limit");
            return Ok(());
        }

        let req = context.req.read();

        if req.source.is_none() {
            span.record("hops_filter_result", "passed_no_source");
            span.record("hops_filter_len", 0);

            debug!("No source object present, passing schain len filter");

            return Ok(());
        }

        let source = req.source.as_ref().unwrap();

        let mut schain_opt = source.schain.as_ref();

        // very commonly still placed in source.ext.schain
        if schain_opt.is_none() {
            let source_ext_opt = source.ext.as_ref();

            if let Some(source_ext) = source_ext_opt {
                #[allow(deprecated)]
                if let Some(source_ext_schain) = source_ext.schain.as_ref() {
                    schain_opt = Some(source_ext_schain);
                }
            }
        }

        if schain_opt.is_none() {
            span.record("hops_filter_result", "passed_no_source.schain");
            span.record("hops_filter_len", 0);

            debug!("Source object present, but no sourcr.schain. Passing schain len filter");

            return Ok(());
        }

        let schain = schain_opt.unwrap();

        if schain.nodes.len() > self.hop_limit as usize {
            span.record("hops_filter_result", "blocked_too_many");
            span.record("hops_filter_len", schain.nodes.len());

            let brs = BidResponseState::NoBidReason {
                reqid: req.id.clone(),
                nbr: rtb::spec::openrtb::nobidreason::BLOCKED_SUPPLYCHAIN_NODE,
                desc: "Too many schain hops".into(),
            };

            context
                .res
                .set(brs)
                .map_err(|_| anyhow!("Someone already assigned brs during schain filter"))?;

            bail!(
                "Hops filter limit exceeded: {} > {}",
                schain.nodes.len(),
                self.hop_limit
            );
        }

        debug!(
            "Passed schain length filter ({} <= {})",
            schain.nodes.len(),
            self.hop_limit
        );

        span.record("hops_filter_result", "passed");

        Ok(())
    }
}
