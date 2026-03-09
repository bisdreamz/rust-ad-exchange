use crate::app::lifecycle::context::StartupContext;
use anyhow::Error;
use pipeline::BlockingTask;
use tracing::{info, instrument};

pub struct ValidateCdnDomainTask;

impl BlockingTask<StartupContext, Error> for ValidateCdnDomainTask {
    #[instrument(skip_all, name = "validate_cdn_domain_task")]
    fn run(&self, context: &StartupContext) -> Result<(), Error> {
        let config = context
            .config
            .get()
            .ok_or_else(|| anyhow::anyhow!("Config not loaded"))?;

        let Some(domain) = config.asset_cdn_domain.as_deref() else {
            info!("asset_cdn_domain not configured — creative ad serving disabled");
            return Ok(());
        };

        let parsed = url::Url::parse(&format!("https://{}", domain)).map_err(|e| {
            anyhow::anyhow!(
                "asset_cdn_domain \"{}\" is not a valid domain: {}",
                domain,
                e
            )
        })?;

        let host = parsed
            .host_str()
            .ok_or_else(|| anyhow::anyhow!("asset_cdn_domain \"{}\" has no valid host", domain))?;

        if parsed.path() != "/"
            || parsed.query().is_some()
            || parsed.fragment().is_some()
            || !parsed.username().is_empty()
        {
            anyhow::bail!(
                "asset_cdn_domain must be a bare domain with no path, query, or credentials: \"{}\"",
                domain
            );
        }

        context
            .cdn_base
            .set(format!("//{}", host))
            .map_err(|_| anyhow::anyhow!("cdn_base already set"))?;

        Ok(())
    }
}
