use crate::app::pipeline::ortb::AuctionContext;
use anyhow::{Error, anyhow};
use pipeline::BlockingTask;
use rtb::bid_request::DistributionchannelOneof;
use rtb::child_span_info;

/// Task to catch & filter obvious junk supply.
/// Primarily things by likely spoofed bundles
/// that shouldnt be avail on the open market,
/// e.g. netflix or hulu
pub struct JunkFilterTask;

impl BlockingTask<AuctionContext, Error> for JunkFilterTask {
    fn run(&self, context: &AuctionContext) -> Result<(), Error> {
        let _span = child_span_info!("junk_filter_task");

        let req = context.req.read();

        let channel = req
            .distributionchannel_oneof
            .as_ref()
            .ok_or_else(|| anyhow!("No distributionchannel"))?;

        match channel {
            DistributionchannelOneof::Site(_) => {}
            DistributionchannelOneof::App(app) => {
                let bundle = app.bundle.to_lowercase();

                if bundle.contains("netflix")
                    || bundle.contains("hulu")
                    || bundle.contains("disney")
                    || bundle.starts_with("com.amazon.avod")
                    || bundle.starts_with("com.amazon.firetv")
                    || bundle.starts_with("com.peacocktv")
                    || bundle.starts_with("com.hbo.")
                    || bundle.contains("zhiliaoapp")
                    || bundle.starts_with("com.instagram")
                    || bundle.starts_with("com.whatsapp")
                {
                    return Err(anyhow!("Junk bundle"));
                }
            }
            DistributionchannelOneof::Dooh(_) => {}
        }

        Ok(())
    }
}
