use crate::app::pipeline::events::billing::context::BillingEventContext;
use anyhow::{Error, anyhow};
use opentelemetry::metrics::{Counter, Histogram};
use opentelemetry::{KeyValue, global};
use pipeline::BlockingTask;
use rtb::child_span_info;
use std::ops::Div;
use std::sync::LazyLock;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::debug;
use crate::core::events::billing::EventSource;

static IMP_TOTAL: LazyLock<Counter<u64>> = LazyLock::new(|| {
    global::meter("rex:events:billing")
        .u64_counter("events.billing.imps")
        .with_description("All imp billing event handler calls")
        .with_unit("1")
        .build()
});

static REV_GROSS: LazyLock<Counter<f64>> = LazyLock::new(|| {
    global::meter("rex:events:billing")
        .f64_counter("events.billing.revenue.gross")
        .with_description("Sum of imp gross spend (charged to demand)")
        .with_unit("{USD}")
        .build()
});

static REV_COST: LazyLock<Counter<f64>> = LazyLock::new(|| {
    global::meter("rex:events:billing")
        .f64_counter("events.billing.revenue.cost")
        .with_description("Sum of imp pub cost (paid to pub)")
        .with_unit("{USD}")
        .build()
});

static IMP_DURATION: LazyLock<Histogram<f64>> = LazyLock::new(|| {
    global::meter("rex:events:billing")
        .f64_histogram("events.billing.delay")
        .with_description("Bid to billing event delay (s)")
        .with_unit("s")
        .build()
});

/// Records raw billing event call metrics for OTEL export
/// This is intended to record *all* events, even if the notice_urls
/// was expired! This is to ensure we have full visibility on
/// incoming event calls. This task should be placed before a
/// bail on missing URLs task, and the demand burl fire should
/// be after that
pub struct RecordBillingMetricsTask;

impl BlockingTask<BillingEventContext, Error> for RecordBillingMetricsTask {
    fn run(&self, context: &BillingEventContext) -> Result<(), Error> {
        let span = child_span_info!(
            "record_billing_event",
            pub_id = tracing::field::Empty,
            bidder_id = tracing::field::Empty,
            bidder_endpoint_id = tracing::field::Empty,
            ad_format = tracing::field::Empty,
            cpm_gross = tracing::field::Empty,
            cpm_cost = tracing::field::Empty,
            source = tracing::field::Empty,
            imp_delay_secs = tracing::field::Empty,
            expired = tracing::field::Empty,
        );

        let event = context
            .details
            .get()
            .ok_or_else(|| anyhow!("Billing event missing on context! Cant record imp"))?;

        let timestamp_millis = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64;

        let imp_delay_millis = timestamp_millis.saturating_sub(event.bid_timestamp);
        let imp_delay = Duration::from_millis(imp_delay_millis);

        debug!(
            "Recorded billing event pub {} bidder {} gross price {} cost {} format {} delay {}s",
            event.pub_id,
            event.bidder_id,
            event.cpm_gross,
            event.cpm_cost,
            event.bid_ad_format,
            imp_delay.as_secs()
        );

        let source = event.event_source.as_ref().unwrap_or(&EventSource::Unknown).to_string();

        span.record("pub_id", event.pub_id.clone());
        span.record("bidder_id", event.bidder_id.clone());
        span.record("bidder_endpoint_id", event.endpoint_id.clone());
        span.record("ad_format", event.bid_ad_format.to_string());
        span.record("cpm_gross", event.cpm_gross);
        span.record("cpm_cost", event.cpm_cost);
        span.record("imp_delay_secs", imp_delay.as_secs());
        span.record("expired", context.demand_urls.get().is_none());
        span.record("source", source.clone());

        let attrs = vec![
            KeyValue::new("pub_id", event.pub_id.clone()),
            KeyValue::new("bidder_id", event.bidder_id.clone()),
            KeyValue::new("bidder_endpoint_id", event.endpoint_id.clone()),
            KeyValue::new("ad_format", event.bid_ad_format.to_string()),
            KeyValue::new("source", source.clone()),
        ];

        IMP_TOTAL.add(1, &attrs);
        IMP_DURATION.record(imp_delay.as_secs_f64(), &attrs);
        REV_GROSS.add(event.cpm_gross.div(1000.0) as f64, &attrs);
        REV_COST.add(event.cpm_cost.div(1000.0) as f64, &attrs);

        Ok(())
    }
}
