use crate::app::pipeline::ortb::AuctionContext;
use anyhow::Error;
use opentelemetry::metrics::Histogram;
use opentelemetry::{KeyValue, global};
use pipeline::BlockingTask;
use rtb::child_span_info;
use std::ops::Sub;
use std::sync::LazyLock;

static HIST_TMAX: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    global::meter("rex:supply.tmax")
        .u64_histogram("tmax.value")
        .with_description("Request tmax from publisher")
        .with_unit("s")
        .build()
});

/// Reduces the inbound tmax to enable
/// us processing time, 15-30ms depending on
/// inbound value
pub struct TmaxOffsetTask;

impl BlockingTask<AuctionContext, anyhow::Error> for TmaxOffsetTask {
    fn run(&self, context: &AuctionContext) -> Result<(), Error> {
        let span = child_span_info!("tmax_offset_task", tmax = tracing::field::Empty);

        let publisher = &context.publisher;

        let attrs = vec![
            KeyValue::new("pub_id", publisher.id.clone()),
            KeyValue::new("pub_name", publisher.name.clone()),
        ];

        let mut req = context.req.write();

        let req_tmax = req.tmax;
        HIST_TMAX.record(req_tmax as u64, &attrs);
        span.record("tmax", req_tmax);

        let offset = if req.tmax > 200 { 30 } else { 15 };

        let tmax = req.tmax.sub(offset).max(50);

        req.tmax = tmax;

        Ok(())
    }
}
