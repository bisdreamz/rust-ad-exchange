use crate::app::pipeline::adtag::context::AdtagContext;
use crate::app::pipeline::adtag::response::{AdTagResponse, TagBid, TagBidContent};
use crate::core::models::placement::Placement;
use crate::core::usersync::utils::build_sync_out_url;
use anyhow::{Error, anyhow, bail};
use async_trait::async_trait;
use pipeline::AsyncTask;
use rtb::BidResponse;
use rtb::bid_response::Bid;
use rtb::bid_response::bid::AdmOneof;
use rtb::child_span_info;
use rtb::common::bidresponsestate::BidResponseState;
use rtb::spec::adcom::creative_attributes;
use rtb::utils::adm::{AdFormat, detect_ad_format};
use std::sync::Arc;
use tracing::{Instrument, debug, warn};

pub struct BuildResponseTask {
    cookie_domain: Option<String>,
}

impl BuildResponseTask {
    pub fn new(cookie_domain: Option<String>) -> Self {
        Self { cookie_domain }
    }
}

impl BuildResponseTask {
    async fn run0(&self, ctx: &AdtagContext) -> Result<(), Error> {
        let auction_ctx = ctx
            .auction_ctx
            .get()
            .ok_or_else(|| anyhow!("auction_ctx not set"))?;

        let placement = ctx
            .placement
            .get()
            .ok_or_else(|| anyhow!("placement not set"))?;

        let bid = match auction_ctx.res.get() {
            Some(BidResponseState::Bid(response)) => resolve_tag_bid(response, placement)?,
            _ => None,
        };

        // Suppress cookie sync when GDPR applies without a TC string — we have
        // no signal that the user consented to sync. GPP with applicable sections
        // similarly indicates a regulated context; defer sync until proper consent
        // parsing is in place.
        let consent = &ctx.request.consent;
        let gdpr_blocks_sync = consent
            .tcf
            .as_ref()
            .map(|t| t.gdpr_applies == Some(true) && t.tc_string.is_empty())
            .unwrap_or(false);
        let gpp_blocks_sync = consent
            .gpp
            .as_ref()
            .map(|g| !g.gpp_applicable_sections.is_empty())
            .unwrap_or(false);

        let sync_frame_url = if gdpr_blocks_sync || gpp_blocks_sync {
            debug!(
                gdpr_blocks_sync,
                gpp_blocks_sync, "suppressing sync frame — consent signal blocks sync"
            );
            None
        } else {
            match (&self.cookie_domain, ctx.publisher.get()) {
                (Some(domain), Some(publisher)) => {
                    let url = build_sync_out_url(domain, &publisher.id);
                    debug!(sync_frame_url = %url, "attaching sync frame url");
                    Some(url)
                }
                (None, _) => {
                    debug!("cookie_domain not configured — skipping sync frame");
                    None
                }
                (_, None) => {
                    warn!("publisher not set on context — skipping sync frame");
                    None
                }
            }
        };

        let response = AdTagResponse {
            sync_frame_url,
            bid,
        };

        ctx.response
            .set(response)
            .map_err(|_| anyhow!("response already set"))
    }
}

fn resolve_tag_bid(
    response: &BidResponse,
    placement: &Arc<Placement>,
) -> Result<Option<TagBid>, Error> {
    let winner_seat = match response.seatbid.first() {
        Some(s) => s,
        None => {
            warn!("Bid response has no seatbids");
            return Ok(None);
        }
    };

    let winner_bid = match winner_seat.bid.first() {
        Some(b) => b,
        None => {
            warn!("Winning seatbid has no bids");
            return Ok(None);
        }
    };

    let content = translate_bid(winner_bid)?;
    let expandable = is_expandable(winner_bid);

    Ok(Some(TagBid {
        container: placement.container.clone(),
        content,
        expandable,
    }))
}

/// Returns true if the bid declares any expandable creative attribute.
fn is_expandable(bid: &Bid) -> bool {
    bid.attr.iter().any(|&a| {
        let a = a as u32;
        a == creative_attributes::EXPANDABLE_AUTOMATIC
            || a == creative_attributes::EXPANDABLE_USER_CLICK
            || a == creative_attributes::EXPANDABLE_USER_ROLLOVER
    })
}

fn translate_bid(bid: &Bid) -> Result<TagBidContent, Error> {
    let adm = match &bid.adm_oneof {
        Some(AdmOneof::Adm(s)) => s.clone(),
        _ => bail!("Bid has no text adm"),
    };

    let format = detect_ad_format(bid)
        .ok_or_else(|| anyhow!("Could not classify ad format from bid adm"))?;

    match format {
        AdFormat::Banner => Ok(TagBidContent::Banner {
            content: adm,
            w: bid.w.max(0) as u32,
            h: bid.h.max(0) as u32,
        }),
        AdFormat::Video => Ok(TagBidContent::Video {
            vast: adm,
            w: bid.w.max(0) as u32,
            h: bid.h.max(0) as u32,
        }),
        AdFormat::Audio => Ok(TagBidContent::Audio { vast: adm }),
        AdFormat::Native => bail!("Native bids are not supported in adtag v1"),
    }
}

#[async_trait]
impl AsyncTask<AdtagContext, Error> for BuildResponseTask {
    async fn run(&self, ctx: &AdtagContext) -> Result<(), Error> {
        let span = child_span_info!("build_response_task");

        self.run0(ctx).instrument(span).await
    }
}
