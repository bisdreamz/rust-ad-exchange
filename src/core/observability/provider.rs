use crate::app::config::{FileRotation, LogType, LoggingConfig, OtelProto};
use anyhow::{anyhow, Context, Result};
use opentelemetry::{global, trace::TracerProvider, KeyValue};
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::{Sampler, SdkTracerProvider};
use opentelemetry_sdk::Resource;
use std::path::Path;
use std::time::SystemTime;
use tracing_subscriber::fmt::format::{FmtSpan, Writer};
use tracing_subscriber::fmt::time::FormatTime;
use tracing_subscriber::layer::{Layer, SubscriberExt};
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{fmt, EnvFilter};

type DynLayer = Box<dyn Layer<tracing_subscriber::Registry> + Send + Sync + 'static>;

struct CompactTime;

impl FormatTime for CompactTime {
    fn format_time(&self, w: &mut Writer<'_>) -> std::fmt::Result {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap();
        let secs = now.as_secs();

        const SECONDS_PER_DAY: u64 = 86400;
        const DAYS_BEFORE_UNIX_EPOCH: i64 = 719468;

        let days_since_epoch = (secs / SECONDS_PER_DAY) as i64 + DAYS_BEFORE_UNIX_EPOCH;
        let seconds_today = secs % SECONDS_PER_DAY;

        let (year, month, day) = days_to_ymd(days_since_epoch);
        let hours = seconds_today / 3600;
        let minutes = (seconds_today / 60) % 60;
        let seconds = seconds_today % 60;

        write!(w, "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
               year, month, day, hours, minutes, seconds)
    }
}

fn days_to_ymd(days: i64) -> (i64, u8, u8) {
    let z = days + 306;
    let h = 100 * z - 25;
    let a = h / 3652425;
    let b = a - a / 4;
    let y = (100 * b + h) / 36525;
    let c = b + z - 365 * y - y / 4;
    let m = (5 * c + 456) / 153;
    let d = c - (153 * m - 457) / 5;

    let year = y + (m > 12) as i64;
    let month = if m > 12 { m - 12 } else { m };

    (year, month as u8, d as u8)
}

pub fn init(config: &LoggingConfig) -> Result<Option<SdkTracerProvider>> {
    config.validate()?;

    let crate_name = env!("CARGO_PKG_NAME");
    let filter = EnvFilter::from_default_env()
        .add_directive("error".parse()?)
        .add_directive(format!("{}={}", crate_name, config.level).parse()?);

    let mut layers: Vec<DynLayer> = Vec::new();
    let mut otel_provider: Option<SdkTracerProvider> = None;

    for sink in &config.sinks {
        match &sink.dest {
            LogType::Stdout { color, json } => {
                let (non_blocking, _guard) = tracing_appender::non_blocking(std::io::stdout());
                std::mem::forget(_guard);

                if *json {
                    if sink.spans {
                        layers.push(fmt::layer().json().with_writer(non_blocking).with_span_events(FmtSpan::NEW | FmtSpan::CLOSE).boxed());
                    } else {
                        layers.push(fmt::layer().json().with_writer(non_blocking).boxed());
                    }
                } else {
                    if sink.spans {
                        layers.push(fmt::layer().compact().with_timer(CompactTime).with_ansi(*color).with_writer(non_blocking).with_span_events(FmtSpan::NEW | FmtSpan::CLOSE).boxed());
                    } else {
                        layers.push(fmt::layer().compact().with_timer(CompactTime).with_ansi(*color).with_writer(non_blocking).boxed());
                    }
                }
            }
            LogType::File { path, json, rotation, max_files } => {
                let writer = create_file_writer(path, rotation, *max_files)?;

                if *json {
                    if sink.spans {
                        layers.push(fmt::layer().json().with_writer(writer).with_span_events(FmtSpan::NEW | FmtSpan::CLOSE).boxed());
                    } else {
                        layers.push(fmt::layer().json().with_writer(writer).boxed());
                    }
                } else {
                    if sink.spans {
                        layers.push(fmt::layer().compact().with_timer(CompactTime).with_writer(writer).with_span_events(FmtSpan::NEW | FmtSpan::CLOSE).boxed());
                    } else {
                        layers.push(fmt::layer().compact().with_timer(CompactTime).with_writer(writer).boxed());
                    }
                }
            }
            LogType::Otel { endpoint, proto } => {
                if otel_provider.is_some() {
                    return Err(anyhow!(
                        "Multiple OTLP sinks configured. Only one is currently supported."
                    ));
                }

                let (otel_layer, provider) =
                    create_otel_layer(endpoint, proto, config.span_sample_rate)?;
                layers.push(otel_layer);
                otel_provider = Some(provider);
            }
        }
    }

    tracing_subscriber::registry()
        .with(layers)
        .with(filter)
        .try_init()
        .context("failed to initialize tracing subscriber")?;

    if let Some(ref provider) = otel_provider {
        global::set_text_map_propagator(TraceContextPropagator::new());
        let _ = global::set_tracer_provider(provider.clone());
    }

    Ok(otel_provider)
}

pub fn shutdown(provider: &SdkTracerProvider) -> Result<()> {
    provider.shutdown().context("failed to shutdown tracer provider")
}

fn create_file_writer(
    path: &Path,
    rotation: &FileRotation,
    max_files: usize,
) -> Result<tracing_appender::non_blocking::NonBlocking> {
    let file_name = path.file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| anyhow!("Invalid file name in path: {}", path.display()))?;

    let directory = path.parent()
        .ok_or_else(|| anyhow!("Invalid directory in path: {}", path.display()))?;

    if !directory.as_os_str().is_empty() {
        std::fs::create_dir_all(directory).with_context(|| {
            format!("failed to create log directory {}", directory.display())
        })?;
    }

    let file_appender = match rotation {
        FileRotation::Daily => tracing_appender::rolling::daily(directory, file_name),
        FileRotation::Hourly => tracing_appender::rolling::hourly(directory, file_name),
        FileRotation::Never => tracing_appender::rolling::never(directory, file_name),
    };

    let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);

    cleanup_old_files(directory, file_name, max_files)?;

    std::mem::forget(_guard);

    Ok(non_blocking)
}

fn cleanup_old_files(directory: &Path, prefix: &str, max_files: usize) -> Result<()> {
    if max_files == 0 {
        return Ok(());
    }

    let mut files: Vec<_> = std::fs::read_dir(directory)?
        .filter_map(|entry| entry.ok())
        .filter(|entry| {
            entry.file_name()
                .to_str()
                .map(|name| name.starts_with(prefix))
                .unwrap_or(false)
        })
        .filter_map(|entry| {
            entry.metadata().ok().and_then(|meta| {
                meta.modified().ok().map(|time| (entry.path(), time))
            })
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

fn create_otel_layer(
    endpoint: &str,
    proto: &OtelProto,
    sample_rate: f32,
) -> Result<(DynLayer, SdkTracerProvider)> {
    use opentelemetry_otlp::WithExportConfig;

    let exporter = match proto {
        OtelProto::Grpc => {
            let mut builder = opentelemetry_otlp::SpanExporter::builder().with_tonic();
            if !endpoint.is_empty() {
                builder = builder.with_endpoint(endpoint);
            }

            builder
                .build()
                .context("failed to build OTLP gRPC exporter")?
        }
        OtelProto::Http => build_http_exporter(endpoint)?,
    };

    let sampler = Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(sample_rate as f64)));

    let service_name =
        std::env::var("OTEL_SERVICE_NAME").unwrap_or_else(|_| env!("CARGO_PKG_NAME").to_string());
    let resource = Resource::builder()
        .with_service_name(service_name)
        .with_attribute(KeyValue::new("service.version", env!("CARGO_PKG_VERSION")))
        .build();

    let tracer_provider = SdkTracerProvider::builder()
        .with_sampler(sampler)
        .with_resource(resource)
        .with_batch_exporter(exporter)
        .build();

    let tracer = tracer_provider.tracer(env!("CARGO_PKG_NAME"));
    let layer = tracing_opentelemetry::layer().with_tracer(tracer).boxed();

    Ok((layer, tracer_provider))
}

#[cfg(feature = "otel-http")]
fn build_http_exporter(endpoint: &str) -> Result<opentelemetry_otlp::SpanExporter> {
    use opentelemetry_otlp::WithExportConfig;

    let mut builder = opentelemetry_otlp::SpanExporter::builder().with_http();
    if !endpoint.is_empty() {
        builder = builder.with_endpoint(endpoint);
    }

    builder
        .build()
        .context("failed to build OTLP HTTP exporter")
}

#[cfg(not(feature = "otel-http"))]
fn build_http_exporter(_: &str) -> Result<opentelemetry_otlp::SpanExporter> {
    Err(anyhow!(
        "OTLP HTTP exporter requested but the build does not enable the `otel-http` feature"
    ))
}
