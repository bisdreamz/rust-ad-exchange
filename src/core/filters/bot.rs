use anyhow::bail;
use arc_swap::ArcSwap;
use ip_network::IpNetwork;
use ip_network_table::IpNetworkTable;
use moka::sync::Cache;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;
use tracing::debug;

const DATA_URL: &str =
    "https://raw.githubusercontent.com/the-furry-hubofeverything/vps-ranges/refs/heads/main/ip.txt";

pub struct IpRiskFilter {
    table: ArcSwap<IpNetworkTable<()>>,
    cache: Cache<String, bool>,
    ranges: usize,
}

impl IpRiskFilter {
    pub async fn try_new(
        cache_max_size: usize,
        cache_ttl: Duration,
    ) -> Result<Self, anyhow::Error> {
        if cache_max_size == 0 && cache_ttl.is_zero() {
            bail!("Cache max size and ttl cannot both be zero");
        }

        let mut cache_builder = Cache::<String, bool>::builder();
        if cache_max_size > 0 {
            cache_builder = cache_builder.max_capacity(cache_max_size as u64);
        }

        if !cache_ttl.is_zero() {
            cache_builder = cache_builder.time_to_live(cache_ttl);
        }

        let mut filter = IpRiskFilter {
            table: ArcSwap::new(Arc::new(IpNetworkTable::new())),
            cache: cache_builder.build(),
            ranges: 0,
        };

        filter.load().await?;

        Ok(filter)
    }

    async fn load(&mut self) -> Result<usize, anyhow::Error> {
        let text = reqwest::get(DATA_URL)
            .await?
            .error_for_status()?
            .text()
            .await?;

        let mut table = IpNetworkTable::new();

        let mut success = 0;

        for line in text.lines() {
            let trimmed = line.trim();

            if let Ok(network) = trimmed.parse::<IpNetwork>() {
                table.insert(network, ());

                success += 1;
            }
        }

        if success > 10000 {
            self.table.store(Arc::new(table))
        } else {
            bail!(
                "Suspiciously low successful line count {}, avoiding ip bot update",
                success
            )
        }

        self.ranges = success;

        Ok(success)
    }

    pub fn should_block(&self, ip: IpAddr) -> bool {
        debug!("Checking {} for block", ip);
        self.cache.get_with(ip.to_string(), || {
            debug!("Loading {}", ip);
            self.table.load().longest_match(ip).is_some()
        })
    }

    pub fn ranges(&self) -> usize {
        self.ranges
    }
}
