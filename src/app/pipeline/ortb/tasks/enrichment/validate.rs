use crate::app::pipeline::ortb::AuctionContext;
use crate::app::pipeline::ortb::context::PublisherBlockReason;
use crate::core::spec::nobidreasons;
use anyhow::anyhow;
use pipeline::BlockingTask;
use rtb::bid_request::DistributionchannelOneof;
use rtb::child_span_info;
use rtb::common::bidresponsestate::BidResponseState;
use tracing::debug;

pub struct ValidateRequestTask;

impl BlockingTask<AuctionContext, anyhow::Error> for ValidateRequestTask {
    fn run(&self, context: &AuctionContext) -> Result<(), anyhow::Error> {
        let span = child_span_info!(
            "request_validate_task",
            invalid_reason = tracing::field::Empty
        )
        .entered();

        debug!(
            "Validating request for seller {} source {}",
            context.pubid, context.source
        );

        let req = context.req.read();

        if req.id.is_empty() {
            let brs = BidResponseState::NoBidReason {
                reqid: "missing".into(),
                nbr: rtb::spec::openrtb::nobidreason::INVALID_REQUEST,
                desc: Some("Missing req id".into()),
            };

            context
                .res
                .set(brs)
                .expect("Should not have response state assigned already");

            context
                .block_reason
                .set(PublisherBlockReason::MissingAuctionId)
                .map_err(|_| anyhow!("Failed to attach block pub reason on ctx"))?;

            span.record("invalid_reason", "missing_auction_id");

            return Err(anyhow!("Auction missing id value"));
        }

        let device_opt = req.device.as_ref();
        if device_opt.is_none() {
            let brs = BidResponseState::NoBidReason {
                reqid: context.original_auction_id.clone(),
                nbr: rtb::spec::openrtb::nobidreason::INVALID_REQUEST,
                desc: Some("Missing device object".into()),
            };

            context
                .res
                .set(brs)
                .expect("Should not have response state assigned already");

            context
                .block_reason
                .set(PublisherBlockReason::MissingDevice)
                .map_err(|_| anyhow!("Failed to attach block pub reason on ctx"))?;

            span.record("invalid_reason", "missing_device_object");

            return Err(anyhow!("Auction missing device object"));
        }

        let device = device_opt.unwrap();

        if device.ua.is_empty() || (device.ip.is_empty() && device.ipv6.is_empty()) {
            let brs = BidResponseState::NoBidReason {
                reqid: context.original_auction_id.clone(),
                nbr: nobidreasons::MISSING_DEVICE_DETAILS,
                desc: Some("Missing device user-agent or IP".into()),
            };

            context
                .res
                .set(brs)
                .expect("Should not have response state assigned already");

            context
                .block_reason
                .set(PublisherBlockReason::MissingDeviceDetails)
                .map_err(|_| anyhow!("Failed to attach block pub reason on ctx"))?;

            span.record("invalid_reason", "missing_dev_ua_or_ip");

            return Err(anyhow!("Auction device object missing ua or ip"));
        }

        if req.imp.is_empty() {
            let brs = BidResponseState::NoBidReason {
                reqid: context.original_auction_id.clone(),
                nbr: rtb::spec::openrtb::nobidreason::INVALID_REQUEST,
                desc: Some("Empty imps".into()),
            };

            context
                .res
                .set(brs)
                .expect("Should not have response state assigned already");

            span.record("invalid_reason", "missing_imps");

            return Err(anyhow!("Auction missing imps"));
        }

        if req.distributionchannel_oneof.is_none() {
            let brs = BidResponseState::NoBidReason {
                reqid: context.original_auction_id.clone(),
                nbr: rtb::spec::openrtb::nobidreason::INVALID_REQUEST,
                desc: Some("Missing app, site, or dooh object".into()),
            };

            context
                .res
                .set(brs)
                .expect("Should not have response state assigned already");

            context
                .block_reason
                .set(PublisherBlockReason::MissingAppSite)
                .map_err(|_| anyhow!("Failed to attach block pub reason on ctx"))?;

            span.record("invalid_reason", "missing_app_site");

            return Err(anyhow!("Auction missing app, site, or dooh object"));
        }

        match req.distributionchannel_oneof.as_ref().unwrap() {
            DistributionchannelOneof::Site(site) => {
                if site.domain.is_empty() && site.page.is_empty() {
                    context
                        .res
                        .set(BidResponseState::NoBidReason {
                            reqid: context.original_auction_id.clone(),
                            nbr: nobidreasons::MISSING_DOMAIN_OR_BUNDLE,
                            desc: Some("Missing site domain".into()),
                        })
                        .map_err(|_| anyhow!("Failed to attach block pub reason on ctx"))?;

                    context
                        .block_reason
                        .set(PublisherBlockReason::MissingAppSiteDomain)
                        .map_err(|_| anyhow!("Failed to attach block pub reason on ctx"))?;

                    return Err(anyhow!("Auction missing site domain"));
                }
            }
            DistributionchannelOneof::App(app) => {
                // todo block on missing store url?
                if app.bundle.is_empty() {
                    context
                        .res
                        .set(BidResponseState::NoBidReason {
                            reqid: context.original_auction_id.clone(),
                            nbr: nobidreasons::MISSING_DOMAIN_OR_BUNDLE,
                            desc: Some("Missing app bundle".into()),
                        })
                        .map_err(|_| anyhow!("Failed to attach block pub reason on ctx"))?;

                    context
                        .block_reason
                        .set(PublisherBlockReason::MissingAppSiteDomain)
                        .map_err(|_| anyhow!("Failed to attach block pub reason on ctx"))?;

                    return Err(anyhow!("Auction missing app bundle"));
                }
            }
            DistributionchannelOneof::Dooh(_) => {}
        };

        if req.tmax > 0 && req.tmax < 50 {
            context
                .res
                .set(BidResponseState::NoBidReason {
                    reqid: context.original_auction_id.clone(),
                    nbr: rtb::spec::openrtb::nobidreason::INSUFFICIENT_AUCTION_TIME,
                    desc: Some("Tmax too low".into()),
                })
                .map_err(|_| anyhow!("Failed to attach block pub reason on ctx"))?;

            context
                .block_reason
                .set(PublisherBlockReason::TmaxTooLow)
                .map_err(|_| anyhow!("Failed to attach block pub reason on ctx"))?;

            return Err(anyhow!("Auction tmax too low (< 50ms)"));
        }

        debug!("Request passed basic validation");
        span.record("invalid_reason", "none");

        Ok(())
    }
}
