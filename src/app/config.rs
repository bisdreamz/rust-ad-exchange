use crate::core::models::bidder::{Bidder, Endpoint};
use config::Config;
use derive_builder::Builder;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

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
pub struct RexConfig {
    #[serde(default)]
    pub caches: CacheConfig,
    pub bidders: Vec<BidderConfig>,
    #[serde(default)]
    pub logging: LoggingConfig,
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
    /// Whether spans should be exported to this sink
    pub spans: bool,
    // maybe have metrics here later
    /// The kind of observability sink
    pub dest: LogType
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum LogType {
    Stdout {
        #[serde(default = "default_logtype_color")]
        color: bool,
        #[serde(default)]
        json: bool,
    },
    File {
        path: PathBuf,
        #[serde(default)]
        json: bool,
        #[serde(default)]
        rotation: FileRotation,
        #[serde(default)]
        max_files: usize,
    },
    Otel {
        endpoint: String,
        #[serde(default)]
        proto: OtelProto,
    },
}

fn default_logtype_color() -> bool {
    true
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
            sinks: vec![
                LogSink {
                    spans: true,
                    dest: LogType::Stdout {
                        color: true,
                        json: false,
                    }
                }
            ],
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
        self.level.parse::<tracing::Level>()
            .map_err(|_| anyhow::anyhow!("Invalid log level: '{}'. Valid levels: trace, debug, info, warn, error", self.level))?;

        // Validate sample rate
        if !(0.0..=1.0).contains(&self.span_sample_rate) {
            anyhow::bail!("span_sample_rate must be between 0.0 and 1.0, got {}", self.span_sample_rate);
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
