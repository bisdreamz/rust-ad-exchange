use std::collections::HashMap;
use std::convert::TryFrom;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::time::Duration;

use crate::app::config::{FileRotation, LogType, LoggingConfig, OtelProto};
use anyhow::{anyhow, Context, Result};
use http::Uri;
use opentelemetry::{global, trace::TracerProvider, KeyValue};
use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use opentelemetry_sdk::logs::SdkLoggerProvider;
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::{Sampler, SdkTracerProvider};
use opentelemetry_sdk::Resource;
use tonic::metadata::{MetadataKey, MetadataMap, MetadataValue};
use tonic::transport::ClientTlsConfig;
use tracing_appender::non_blocking::{NonBlocking, WorkerGuard};
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::fmt::writer::MakeWriter;
use tracing_subscriber::layer::{Layer, Layered, SubscriberExt};
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{fmt, EnvFilter, Registry};

type FilteredRegistry = Layered<EnvFilter, Registry>;
type DynLayer = Box<dyn Layer<FilteredRegistry> + Send + Sync + 'static>;

static APPENDER_GUARDS: OnceLock<Vec<WorkerGuard>> = OnceLock::new();

pub struct ObservabilityProviders {
    pub tracer: Option<SdkTracerProvider>,
    pub meter: Option<SdkMeterProvider>,
    pub logger: Option<SdkLoggerProvider>,
}

fn create_global_filter(level: &str) -> EnvFilter {
    let crate_name = env!("CARGO_PKG_NAME");

    EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("error"))
        .add_directive(
            format!("{}={}", crate_name, level)
                .parse()
                .expect("failed to build directive"),
        )
}

pub fn init(config: &LoggingConfig) -> Result<Option<ObservabilityProviders>> {
    config.validate()?;

    let filter = create_global_filter(&config.level);
    let mut layers: Vec<DynLayer> = Vec::new();
    let mut guards: Vec<WorkerGuard> = Vec::new();
    let mut tracer_provider: Option<SdkTracerProvider> = None;
    let mut meter_provider: Option<SdkMeterProvider> = None;
    let mut logger_provider: Option<SdkLoggerProvider> = None;

    for sink in &config.sinks {
        match &sink.dest {
            LogType::Stdout { color, json, spans } => {
                let (writer, guard) = tracing_appender::non_blocking(std::io::stdout());
                guards.push(guard);
                layers.push(build_fmt_layer(writer, *json, *color, *spans));
            }
            LogType::File {
                path,
                json,
                rotation,
                max_files,
                spans,
            } => {
                let (writer, guard) = create_file_writer(path, rotation, *max_files)?;
                guards.push(guard);
                layers.push(build_fmt_layer(writer, *json, false, *spans));
            }
            LogType::Otel {
                endpoint,
                proto,
                spans,
                logs,
                metrics,
                metrics_interval_secs,
                headers,
            } => {
                let components = configure_otel(
                    endpoint,
                    proto,
                    config.span_sample_rate,
                    *spans,
                    *logs,
                    *metrics,
                    *metrics_interval_secs,
                    headers,
                )?;

                if let Some(layer) = components.layer {
                    layers.push(layer);
                }

                if let Some(logs_layer) = components.logs_layer {
                    layers.push(logs_layer);
                }

                if let Some(tp) = components.tracer {
                    if tracer_provider.is_some() {
                        return Err(anyhow!("Multiple OTLP span exporters configured"));
                    }
                    tracer_provider = Some(tp);
                }

                if let Some(mp) = components.meter {
                    if meter_provider.is_some() {
                        return Err(anyhow!("Multiple OTLP metric exporters configured"));
                    }
                    meter_provider = Some(mp);
                }

                if let Some(lp) = components.logger {
                    if logger_provider.is_some() {
                        return Err(anyhow!("Multiple OTLP log exporters configured"));
                    }
                    logger_provider = Some(lp);
                }
            }
        }
    }

    Registry::default()
        .with(filter)
        .with(layers)
        .try_init()
        .context("failed to initialize tracing subscriber")?;

    if !guards.is_empty() {
        APPENDER_GUARDS
            .set(guards)
            .map_err(|_| anyhow!("observability already initialized"))?;
    }

    if let Some(provider) = tracer_provider.as_ref() {
        global::set_text_map_propagator(TraceContextPropagator::new());
        let _ = global::set_tracer_provider(provider.clone());
    }

    if let Some(provider) = meter_provider.as_ref() {
        let _ = global::set_meter_provider(provider.clone());
    }

    if tracer_provider.is_none() && meter_provider.is_none() && logger_provider.is_none() {
        return Ok(None);
    }

    Ok(Some(ObservabilityProviders {
        tracer: tracer_provider,
        meter: meter_provider,
        logger: logger_provider,
    }))
}

pub fn shutdown(handles: &ObservabilityProviders) -> Result<()> {
    if let Some(tracer) = handles.tracer.as_ref() {
        let _ = tracer.shutdown();
    }

    if let Some(meter) = handles.meter.as_ref() {
        let _ = meter.shutdown();
    }

    // TODO logs shutdown fails for direct otel exporters - perhaps because still being used?

    Ok(())
}

fn build_fmt_layer<W>(writer: W, json: bool, color: bool, include_span_events: bool) -> DynLayer
where
    W: for<'a> MakeWriter<'a> + Send + Sync + 'static,
    W: Clone,
{
    let span_events = if include_span_events {
        Some(FmtSpan::NEW | FmtSpan::CLOSE)
    } else {
        None
    };

    if json {
        let mut layer = fmt::layer().json().with_writer(writer);
        if let Some(events) = span_events {
            layer = layer.with_span_events(events);
        }
        Box::new(layer) as DynLayer
    } else {
        let mut layer = fmt::layer().compact().with_ansi(color).with_writer(writer);
        if let Some(events) = span_events {
            layer = layer.with_span_events(events);
        }
        Box::new(layer) as DynLayer
    }
}

fn create_file_writer(
    path: &Path,
    rotation: &FileRotation,
    max_files: usize,
) -> Result<(NonBlocking, WorkerGuard)> {
    let file_name = path
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| anyhow!("Invalid file name in path: {}", path.display()))?;

    let directory: PathBuf = path
        .parent()
        .map(|dir| dir.to_path_buf())
        .unwrap_or_else(|| PathBuf::from("."));

    if !directory.as_os_str().is_empty() {
        std::fs::create_dir_all(&directory)
            .with_context(|| format!("failed to create log directory {}", directory.display()))?;
    }

    let file_appender = match rotation {
        FileRotation::Daily => tracing_appender::rolling::daily(&directory, file_name),
        FileRotation::Hourly => tracing_appender::rolling::hourly(&directory, file_name),
        FileRotation::Never => tracing_appender::rolling::never(&directory, file_name),
    };

    let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);

    cleanup_old_files(&directory, file_name, max_files)?;

    Ok((non_blocking, guard))
}

fn cleanup_old_files(directory: &Path, prefix: &str, max_files: usize) -> Result<()> {
    if max_files == 0 {
        return Ok(());
    }

    let mut files: Vec<_> = std::fs::read_dir(directory)?
        .filter_map(|entry| entry.ok())
        .filter(|entry| {
            entry
                .file_name()
                .to_str()
                .map(|name| name.starts_with(prefix))
                .unwrap_or(false)
        })
        .filter_map(|entry| {
            entry
                .metadata()
                .ok()
                .and_then(|meta| meta.modified().ok().map(|time| (entry.path(), time)))
        })
        .collect();

    if files.len() <= max_files {
        return Ok(());
    }

    files.sort_by(|a, b| b.1.cmp(&a.1));

    for (path, _) in files.iter().skip(max_files) {
        let _ = std::fs::remove_file(path);
    }

    Ok(())
}

struct OtelComponents {
    layer: Option<DynLayer>,
    logs_layer: Option<DynLayer>,
    tracer: Option<SdkTracerProvider>,
    meter: Option<SdkMeterProvider>,
    logger: Option<SdkLoggerProvider>,
}

fn configure_otel(
    endpoint: &str,
    proto: &OtelProto,
    sample_rate: f32,
    spans: bool,
    logs: bool,
    metrics: bool,
    metrics_interval_secs: u32,
    headers: &HashMap<String, String>,
) -> Result<OtelComponents> {
    let service_name =
        std::env::var("OTEL_SERVICE_NAME").unwrap_or_else(|_| env!("CARGO_PKG_NAME").to_string());
    let resource = Resource::builder()
        .with_service_name(service_name)
        .with_attribute(KeyValue::new("service.version", env!("CARGO_PKG_VERSION")))
        .build();

    let effective_headers = collect_headers(headers);

    let mut layer = None;
    let mut logs_layer = None;
    let mut tracer = None;
    let mut logger = None;
    let mut meter = None;

    if spans {
        let exporter = match proto {
            OtelProto::Grpc => {
                let builder = opentelemetry_otlp::SpanExporter::builder().with_tonic();
                configure_grpc_builder(builder, endpoint, &effective_headers)?.build()?
            }
            OtelProto::Http => configure_http_builder(
                opentelemetry_otlp::SpanExporter::builder().with_http(),
                endpoint,
                &effective_headers,
            )?
            .build()?,
        };

        let sampler =
            Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(sample_rate as f64)));
        let tracer_provider = SdkTracerProvider::builder()
            .with_sampler(sampler)
            .with_resource(resource.clone())
            .with_batch_exporter(exporter)
            .build();

        let tracer_layer: DynLayer = Box::new(
            tracing_opentelemetry::layer()
                .with_tracer(tracer_provider.tracer(env!("CARGO_PKG_NAME"))),
        );

        layer = Some(tracer_layer);
        tracer = Some(tracer_provider);
    }

    if logs {
        let exporter = match proto {
            OtelProto::Grpc => {
                let builder = opentelemetry_otlp::LogExporter::builder().with_tonic();
                configure_grpc_builder(builder, endpoint, &effective_headers)?.build()?
            }
            OtelProto::Http => configure_http_builder(
                opentelemetry_otlp::LogExporter::builder().with_http(),
                endpoint,
                &effective_headers,
            )?
            .build()?,
        };

        let logger_provider = SdkLoggerProvider::builder()
            .with_resource(resource.clone())
            .with_batch_exporter(exporter)
            .build();

        // Filter OTEL SDK logs from looping back to OTEL (prevents circular dependencies)
        let appender_layer = OpenTelemetryTracingBridge::new(&logger_provider);
        let filtered_layer: DynLayer = Box::new(
            appender_layer.with_filter(
                tracing_subscriber::filter::filter_fn(|metadata| {
                    !metadata.target().starts_with("opentelemetry")
                })
            )
        );
        logs_layer = Some(filtered_layer);
        logger = Some(logger_provider);
    }

    if metrics {
        let exporter = match proto {
            OtelProto::Grpc => {
                let builder = opentelemetry_otlp::MetricExporter::builder().with_tonic();
                configure_grpc_builder(builder, endpoint, &effective_headers)?.build()?
            }
            OtelProto::Http => configure_http_builder(
                opentelemetry_otlp::MetricExporter::builder().with_http(),
                endpoint,
                &effective_headers,
            )?
            .build()?,
        };

        let interval_secs = metrics_interval_secs.max(1) as u64;
        let reader = PeriodicReader::builder(exporter)
            .with_interval(Duration::from_secs(interval_secs))
            .build();

        let provider = SdkMeterProvider::builder()
            .with_resource(resource)
            .with_reader(reader)
            .build();

        meter = Some(provider);
    }

    Ok(OtelComponents {
        layer,
        logs_layer,
        tracer,
        meter,
        logger,
    })
}

fn collect_headers(explicit: &HashMap<String, String>) -> HashMap<String, String> {
    let mut merged = explicit.clone();

    if let Ok(api_key) = std::env::var("DD_API_KEY") {
        if !api_key.is_empty() {
            merged.entry("dd-api-key".to_string()).or_insert(api_key);
        }
    }

    merged
}

fn configure_grpc_builder<T>(
    mut builder: T,
    endpoint: &str,
    headers: &HashMap<String, String>,
) -> Result<T>
where
    T: opentelemetry_otlp::WithExportConfig + opentelemetry_otlp::WithTonicConfig,
{
    if !endpoint.is_empty() {
        builder = builder.with_endpoint(endpoint);
    }

    if let Some(tls) = tls_config_for(endpoint)? {
        builder = builder.with_tls_config(tls);
    }

    if let Some(metadata) = build_metadata(headers)? {
        builder = builder.with_metadata(metadata);
    }

    Ok(builder)
}

#[cfg(feature = "otel-http")]
fn configure_http_builder<T>(
    mut builder: T,
    endpoint: &str,
    headers: &HashMap<String, String>,
) -> Result<T>
where
    T: opentelemetry_otlp::WithExportConfig + opentelemetry_otlp::WithHttpConfig,
{
    if !endpoint.is_empty() {
        builder = builder.with_endpoint(endpoint);
    }

    if !headers.is_empty() {
        builder = builder.with_headers(headers.clone());
    }

    Ok(builder)
}

#[cfg(not(feature = "otel-http"))]
fn configure_http_builder<T>(
    builder: T,
    endpoint: &str,
    headers: &HashMap<String, String>,
) -> Result<T> {
    if !endpoint.is_empty() || !headers.is_empty() {
        return Err(anyhow!(
            "OTLP HTTP exporter requested but the build does not enable the `otel-http` feature"
        ));
    }

    Ok(builder)
}

fn tls_config_for(endpoint: &str) -> Result<Option<ClientTlsConfig>> {
    let uri = match endpoint.parse::<Uri>() {
        Ok(uri) => uri,
        Err(_) => match format!("https://{}", endpoint).parse::<Uri>() {
            Ok(uri) => uri,
            Err(_) => return Ok(None),
        },
    };

    if uri.scheme_str() != Some("https") {
        return Ok(None);
    }

    let mut config = ClientTlsConfig::new().with_enabled_roots();
    if let Some(host) = uri.host() {
        config = config.domain_name(host);
    }

    Ok(Some(config))
}

fn build_metadata(headers: &HashMap<String, String>) -> Result<Option<MetadataMap>> {
    if headers.is_empty() {
        return Ok(None);
    }

    let mut metadata = MetadataMap::new();
    for (key, value) in headers {
        let key = MetadataKey::from_bytes(key.as_bytes()).context("invalid gRPC metadata key")?;
        let value =
            MetadataValue::try_from(value.as_str()).context("invalid gRPC metadata value")?;
        metadata.insert(key, value);
    }

    Ok(Some(metadata))
}
