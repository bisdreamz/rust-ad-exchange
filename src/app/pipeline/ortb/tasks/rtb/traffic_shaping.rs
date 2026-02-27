use crate::app::pipeline::ortb::AuctionContext;
use crate::app::pipeline::ortb::context::{BidderCallout, BidderContext, CalloutSkipReason};
use crate::core::managers::ShaperManager;
use crate::core::shaping::tree::ShapingDecision;
use anyhow::Error;
use async_trait::async_trait;
use opentelemetry::metrics::{Counter, Gauge, Histogram};
use opentelemetry::{KeyValue, global};
use pipeline::AsyncTask;
use rtb::child_span_info;
use std::sync::{Arc, LazyLock};
use tracing::{Instrument, debug, warn};

static COUNTER_SHAPING_OUTCOMES: LazyLock<Counter<u64>> = LazyLock::new(|| {
    global::meter("rex:demand:shaping")
        .u64_counter("shaping.calls")
        .with_description("Count of per endpoint shaping decisions")
        .with_unit("1")
        .build()
});

static GAUGE_SHAPING_METRICS: LazyLock<Gauge<f64>> = LazyLock::new(|| {
    global::meter("rex:demand:shaping")
        .f64_gauge("shaping.metrics_threshold")
        .with_description("The dynamic threshold to pass traffic shaping")
        .build()
});

static HISTOGRAM_METRIC_VALUES: LazyLock<Histogram<f64>> = LazyLock::new(|| {
    global::meter("rex:demand:shaping")
        .f64_histogram("shaping.metric_values")
        .with_description("Distribution of all predicted metric values")
        .build()
});

pub struct TrafficShapingTask {
    manager: Arc<ShaperManager>,
}

impl TrafficShapingTask {
    pub fn new(manager: Arc<ShaperManager>) -> Self {
        Self { manager }
    }

    fn evaluate_record_endpoint(&self, bidder_context: &BidderContext, callout: &BidderCallout) {
        let shaper = match self
            .manager
            .shaper(&bidder_context.bidder.name, &callout.endpoint.name)
        {
            Some(shaper) => shaper,
            None => {
                debug!(
                    "No shaping enabled for endpoint {}! Skipping!",
                    callout.endpoint.name
                );
                return;
            }
        };

        let span = child_span_info!(
            "shaping_evaluate_record_endpoint",
            bidder_name = tracing::field::Empty,
            endpoint_name = tracing::field::Empty,
            outcome = tracing::field::Empty,
            metric_value = tracing::field::Empty,
            metric_target = tracing::field::Empty,
            feature_string = tracing::field::Empty,
        );

        callout
            .shaping
            .set(shaper.clone())
            .unwrap_or_else(|_| warn!("Someone attached shaper to callout context already!"));

        match shaper.passes_shaping(&callout.req) {
            Ok(res) => {
                debug!(
                    "Bidder {} endpoint {} shaping result: {:?}",
                    bidder_context.bidder.name, callout.endpoint.name, res
                );

                if !span.is_disabled() {
                    span.record("bidder_name", bidder_context.bidder.name.clone());
                    span.record("endpoint_name", callout.endpoint.name.clone());
                    span.record("outcome", res.decision.to_string());
                    span.record("metric_value", res.metric_value);
                    span.record("metric_target", res.metric_target);
                    span.record("features", format!("{:?}", res.features));
                }

                let mut attrs = vec![
                    KeyValue::new("bidder", bidder_context.bidder.name.clone()),
                    KeyValue::new("endpoint", callout.endpoint.name.clone()),
                ];

                GAUGE_SHAPING_METRICS.record(res.metric_target as f64, &attrs);

                attrs.push(KeyValue::new("pred_depth", res.pred_depth.to_string()));
                attrs.push(KeyValue::new("outcome", res.decision.to_string()));

                COUNTER_SHAPING_OUTCOMES.add(1, &attrs);
                HISTOGRAM_METRIC_VALUES.record(res.metric_value as f64, &attrs);

                if matches!(res.decision, ShapingDecision::Blocked) {
                    callout
                        .skip_reason
                        .set(CalloutSkipReason::TrafficShaping)
                        .unwrap_or_else(|_| {
                            warn!("Failed to set skip reason, already exists on ctx")
                        });
                }
            }
            Err(err) => {
                COUNTER_SHAPING_OUTCOMES.add(
                    1,
                    &[
                        KeyValue::new("outcome", "prediction_error".to_string()),
                        KeyValue::new("bidder", bidder_context.bidder.name.clone()),
                        KeyValue::new("endpoint", callout.endpoint.name.clone()),
                    ],
                );

                warn!(
                    "Shaping failed for endpoint {}: {}",
                    callout.endpoint.name, err
                );
            }
        }
    }

    async fn run0(&self, context: &AuctionContext) -> Result<(), Error> {
        let bidders = context.bidders.lock().await;

        for bidder_context in bidders.iter() {
            for callout in &bidder_context.callouts {
                if callout.skip_reason.get().is_some() {
                    continue;
                }

                self.evaluate_record_endpoint(bidder_context, callout);
            }
        }

        Ok(())
    }
}

#[async_trait]
impl AsyncTask<AuctionContext, Error> for TrafficShapingTask {
    async fn run(&self, context: &AuctionContext) -> Result<(), Error> {
        let span = child_span_info!("traffic_shaping_task");

        self.run0(context).instrument(span).await
    }
}
