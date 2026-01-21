use crate::core::models::bidder::{Bidder, Endpoint};
use crate::core::models::publisher::Publisher;
use config::Config;
use derive_builder::Builder;
use rtb::server::TlsConfig;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize, Default, Builder)]
pub struct BidderConfig {
    pub bidder: Bidder,
    pub endpoints: Vec<Endpoint>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Builder)]
pub struct CacheConfig {
    pub cache_device_sz: usize,
    pub cache_ip_sz: usize,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            cache_device_sz: 250_000,
            cache_ip_sz: 100_000,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, Builder)]
pub struct EventConfig {
    /// The public domain to be used when building event notificaion urls, e.g. burl
    pub domain: String,
    /// The preferred path to use in event urls for billing events, e.g. /billing
    pub billing_path: String,
    /// The length of time we cache demand event URLs, and how long we will
    /// wait to consider an impression valid
    #[serde(with = "humantime_serde")]
    pub ttl: Duration,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClusterConfig {
    /// Sets a fixed cluster size for manual testing
    /// or bare metal deployment
    Fixed(usize),
    /// Auto discovery of cluster peers & updates
    /// via k8s api
    K8s,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        ClusterConfig::Fixed(1)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FirestoreConfig {
    pub project_id: String,
    #[serde(default)]
    pub database_id: Option<String>,
    #[serde(default)]
    pub credentials_path: Option<PathBuf>,
    #[serde(default)]
    pub emulator_host: Option<String>,
}

/// Configuration for appending schain to bid requests
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchainConfig {
    /// schain ASI, e.g. domain e.g. myads.com
    pub asi: String,
    /// schain partner name e.g. MyAds
    pub name: String
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, Builder)]
pub struct RexConfig {
    pub ssl: Option<TlsConfig>,
    #[serde(default)]
    pub caches: CacheConfig,
    pub bidders: Vec<BidderConfig>,
    pub publishers: Vec<Publisher>,
    pub notifications: EventConfig,
    #[serde(default)]
    pub cluster: ClusterConfig,
    #[serde(default)]
    pub logging: LoggingConfig,
    pub schain: Option<SchainConfig>,
    pub schain_limit: u32,
    #[serde(default)]
    pub firestore: Option<FirestoreConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OtelProto {
    Http,
    Grpc,
}

impl Default for OtelProto {
    fn default() -> Self {
        OtelProto::Grpc
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FileRotation {
    Daily,
    Hourly,
    Never,
}

impl Default for FileRotation {
    fn default() -> Self {
        FileRotation::Daily
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogSink {
    #[serde(flatten)]
    pub dest: LogType,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum LogType {
    Stdout {
        #[serde(default = "default_logtype_color")]
        color: bool,
        #[serde(default)]
        json: bool,
        #[serde(default = "default_true")]
        spans: bool,
    },
    File {
        path: PathBuf,
        #[serde(default)]
        json: bool,
        #[serde(default)]
        rotation: FileRotation,
        #[serde(default)]
        max_files: usize,
        #[serde(default = "default_true")]
        spans: bool,
    },
    Otel {
        endpoint: String,
        #[serde(default)]
        proto: OtelProto,
        #[serde(default = "default_true")]
        spans: bool,
        #[serde(default)]
        logs: bool,
        #[serde(default = "default_true")]
        metrics: bool,
        #[serde(default = "default_metrics_interval")]
        metrics_interval_secs: u32,
        #[serde(default)]
        headers: std::collections::HashMap<String, String>,
    },
}

fn default_logtype_color() -> bool {
    true
}

fn default_true() -> bool {
    true
}

fn default_metrics_interval() -> u32 {
    5
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingConfig {
    #[serde(default)]
    pub level: String,
    #[serde(default)]
    pub span_sample_rate: f32,
    #[serde(default)]
    pub sinks: Vec<LogSink>,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: "info".to_string(),
            span_sample_rate: 0.01,
            sinks: vec![LogSink {
                dest: LogType::Stdout {
                    color: true,
                    json: false,
                    spans: true,
                },
            }],
        }
    }
}

impl LoggingConfig {
    /// Validates the logging configuration
    pub fn validate(&self) -> Result<(), anyhow::Error> {
        if self.sinks.is_empty() {
            anyhow::bail!("At least one logging sink must be configured");
        }

        // Validate level can be parsed
        self.level.parse::<tracing::Level>().map_err(|_| {
            anyhow::anyhow!(
                "Invalid log level: '{}'. Valid levels: trace, debug, info, warn, error",
                self.level
            )
        })?;

        // Validate sample rate
        if !(0.0..=1.0).contains(&self.span_sample_rate) {
            anyhow::bail!(
                "span_sample_rate must be between 0.0 and 1.0, got {}",
                self.span_sample_rate
            );
        }

        Ok(())
    }
}

impl RexConfig {
    pub fn load(path: &PathBuf) -> Result<RexConfig, anyhow::Error> {
        let cfg = Config::builder()
            .add_source(config::File::from(path.to_path_buf()))
            .build()?;

        Ok(cfg.try_deserialize()?)
    }
}
