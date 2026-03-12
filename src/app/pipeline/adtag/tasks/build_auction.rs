use crate::app::pipeline::adtag::context::AdtagContext;
use crate::app::pipeline::ortb::AuctionContext;
use crate::app::pipeline::ortb::HttpRequestContext;
use crate::core::models::creative::CreativeFormat;
use crate::core::models::property::PropertyKind;
use anyhow::{Error, anyhow, bail};
use pipeline::BlockingTask;
use rtb::BidRequestBuilder;
use rtb::bid_request::{
    App, Audio, Banner, DeviceBuilder, DistributionchannelOneof, Imp, Regs, SiteBuilder,
    UserBuilder, Video,
};
use rtb::child_span_info;
use rtb::extensions::ExtWithCustom;
use rtb::spec::adcom::expandable_directions;
use std::net::IpAddr;
use uuid::Uuid;

pub struct BuildAuctionTask;

impl BlockingTask<AdtagContext, Error> for BuildAuctionTask {
    fn run(&self, ctx: &AdtagContext) -> Result<(), Error> {
        let placement = ctx
            .placement
            .get()
            .ok_or_else(|| anyhow!("placement not resolved"))?;
        let property = ctx
            .property
            .get()
            .ok_or_else(|| anyhow!("property not resolved"))?;
        let publisher = ctx
            .publisher
            .get()
            .ok_or_else(|| anyhow!("publisher not resolved"))?;
        let known_page = ctx.known_page.get();

        let serve_expandable = placement.expandable && ctx.request.render_caps.env_expandable;

        let resolved_page = known_page.and_then(|kp| kp.as_ref());
        let page_url = resolved_page.map(|kp| kp.page_url.as_str()).unwrap_or("");
        let page_domain = resolved_page
            .map(|kp| kp.extracted_domain.as_str())
            .unwrap_or("");

        let _span = child_span_info!(
            "build_auction_task",
            serve_expandable,
            page_url,
            page_domain,
        )
        .entered();

        let ip = match ctx.http.ip {
            Some(IpAddr::V4(ip)) => ip.to_string(),
            _ => String::new(),
        };

        let ipv6 = match ctx.http.ip {
            Some(IpAddr::V6(ip)) => ip.to_string(),
            _ => String::new(),
        };

        let device = DeviceBuilder::default()
            .ua(ctx.http.user_agent.clone().unwrap_or_default())
            .ip(ip)
            .ipv6(ipv6)
            .build()?;

        if placement.ad_units.is_empty() {
            bail!("Placement {} has no ad units configured", placement.id);
        }

        let imps: Vec<Imp> = placement
            .ad_units
            .iter()
            .enumerate()
            .filter_map(|(i, unit)| {
                let mut imp = Imp {
                    id: (i + 1).to_string(),
                    tagid: placement.id.clone(),
                    ..Default::default()
                };

                match unit {
                    CreativeFormat::Banner { w, h } => {
                        imp.banner = Some(Banner {
                            w: *w as i32,
                            h: *h as i32,
                            topframe: ctx.request.render_caps.top_frame,
                            // vertical expansion only for v1
                            expdir: if serve_expandable {
                                vec![
                                    expandable_directions::UP as i32,
                                    expandable_directions::DOWN as i32,
                                ]
                            } else {
                                vec![]
                            },
                            ..Default::default()
                        });
                    }
                    CreativeFormat::Video => {
                        imp.video = Some(Video::default());
                    }
                    CreativeFormat::Audio => {
                        imp.audio = Some(Audio::default());
                    }
                    CreativeFormat::Native => return None,
                }

                Some(imp)
            })
            .collect();

        if imps.is_empty() {
            bail!(
                "Placement {} has no supported ad units for adtag v1",
                placement.id
            );
        }

        let page_url = known_page.and_then(|kp| kp.as_ref().map(|p| p.page_url.clone()));
        let domain = known_page
            .and_then(|kp| kp.as_ref().map(|p| p.extracted_domain.clone()))
            .unwrap_or_else(|| property.domain.clone());

        let distribution_channel = match &property.kind {
            PropertyKind::Site => {
                let site = SiteBuilder::default()
                    .id(property.id.clone())
                    .domain(domain)
                    .page(page_url.unwrap_or_default())
                    .cat(property.cats.clone())
                    .build()?;
                DistributionchannelOneof::Site(site)
            }
            PropertyKind::App { bundle, store_url } => {
                let app = App {
                    id: property.id.clone(),
                    domain: domain,
                    bundle: bundle.clone(),
                    storeurl: store_url.clone(),
                    cat: property.cats.clone(),
                    ..Default::default()
                };
                DistributionchannelOneof::App(app)
            }
        };

        let auction_id = Uuid::new_v4().to_string();

        let consent = &ctx.request.consent;

        let regs = (consent.tcf.is_some() || consent.gpp.is_some()).then(|| Regs {
            gdpr: consent
                .tcf
                .as_ref()
                .and_then(|t| t.gdpr_applies)
                .unwrap_or(false),
            gpp: consent
                .gpp
                .as_ref()
                .map(|g| g.gpp_string.clone())
                .unwrap_or_default(),
            gpp_sid: consent
                .gpp
                .as_ref()
                .map(|g| {
                    g.gpp_applicable_sections
                        .iter()
                        .filter_map(|s| s.parse::<i32>().ok())
                        .collect()
                })
                .unwrap_or_default(),
            ..Default::default()
        });

        let user = consent
            .tcf
            .as_ref()
            .map(|tcf| {
                UserBuilder::default()
                    .consent(tcf.tc_string.clone())
                    .build()
            })
            .transpose()?;

        let mut req_builder = BidRequestBuilder::default();
        req_builder
            .id(auction_id.clone())
            .imp(imps)
            .device(device)
            .distributionchannel_oneof(distribution_channel)
            .tmax(400i32);

        if let Some(regs) = regs {
            req_builder.regs(regs);
        }

        if let Some(user) = user {
            req_builder.user(user);
        }

        if ctx.request.force_bid {
            let mut ext = ExtWithCustom::<rtb::bid_request::Ext>::default();

            ext.custom_mut().insert_bool("force_bid".to_string(), true);

            req_builder.test(true).ext(ext);
        }

        let bid_request = req_builder.build()?;

        let http = HttpRequestContext {
            ip: ctx.http.ip,
            user_agent: ctx.http.user_agent.clone(),
            sec_ch_ua: ctx.http.sec_ch_ua.clone(),
            sec_ch_ua_mobile: ctx.http.sec_ch_ua_mobile,
            sec_ch_ua_platform: ctx.http.sec_ch_ua_platform.clone(),
            referer: ctx.http.referer.clone(),
            cookies: ctx.http.cookies.clone(),
        };

        let auction_ctx = AuctionContext::new(
            auction_id,
            "adtag".to_string(),
            publisher.clone(),
            Some(placement.clone()),
            bid_request,
            http,
        );

        ctx.auction_ctx
            .set(auction_ctx)
            .map_err(|_| anyhow!("auction_ctx already set"))
    }
}
