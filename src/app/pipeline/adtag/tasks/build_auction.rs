use crate::app::pipeline::adtag::context::AdtagContext;
use crate::app::pipeline::ortb::AuctionContext;
use crate::app::pipeline::ortb::HttpRequestContext;
use crate::core::models::creative::{BannerSize, CreativeFormat};
use crate::core::models::property::PropertyKind;
use crate::core::spec::nobidreasons;
use anyhow::{Error, anyhow, bail};
use pipeline::BlockingTask;
use rtb::BidRequestBuilder;
use rtb::bid_request::{
    App, Audio, Banner, DeviceBuilder, DistributionchannelOneof, Format as BannerFormat, Imp, Regs,
    SiteBuilder, UserBuilder, Video,
};
use rtb::child_span_info;
use rtb::common::bidresponsestate::BidResponseState;
use rtb::extensions::ExtWithCustom;
use rtb::spec::adcom::expandable_directions;
use std::net::IpAddr;
use uuid::Uuid;

pub struct BuildAuctionTask;

const NO_VIEWPORT_IMPRESSION_DESC: &str = "No impressions match available viewport";

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

        let http = clone_http_context(&ctx.http);
        let auction_id = Uuid::new_v4().to_string();

        let device = DeviceBuilder::default()
            .ua(ctx.http.user_agent.clone().unwrap_or_default())
            .ip(ip)
            .ipv6(ipv6)
            .build()?;

        if placement.ad_units.is_empty() {
            bail!("Placement {} has no ad units configured", placement.id);
        }

        let mut imps = Vec::new();
        let mut viewport_filtered_banner_unit = false;

        for (i, unit) in placement.ad_units.iter().enumerate() {
            let mut imp = Imp {
                id: (i + 1).to_string(),
                tagid: placement.id.clone(),
                ..Default::default()
            };

            match unit {
                CreativeFormat::Banner {
                    preferred_size,
                    alternate_sizes,
                } => {
                    if preferred_size.w == 0 || preferred_size.h == 0 {
                        bail!(
                            "Placement {} banner ad unit has invalid preferred size {}x{}",
                            placement.id,
                            preferred_size.w,
                            preferred_size.h
                        );
                    }

                    let eligible_sizes: Vec<BannerSize> = std::iter::once(*preferred_size)
                        .chain(alternate_sizes.iter().copied())
                        .filter(|size| size.w > 0 && size.h > 0)
                        .filter(|size| fits_render_caps(*size, &ctx.request.render_caps))
                        .collect();

                    if eligible_sizes.is_empty() {
                        viewport_filtered_banner_unit = true;
                        continue;
                    }

                    let primary_size = eligible_sizes[0];
                    let format: Vec<BannerFormat> = eligible_sizes
                        .iter()
                        .copied()
                        .map(|size| BannerFormat {
                            w: size.w as i32,
                            h: size.h as i32,
                            ..Default::default()
                        })
                        .collect();

                    imp.banner = Some(Banner {
                        w: primary_size.w as i32,
                        h: primary_size.h as i32,
                        format,
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
                CreativeFormat::Native => continue,
            }

            imps.push(imp);
        }

        if imps.is_empty() {
            if viewport_filtered_banner_unit {
                let auction_ctx = AuctionContext::new(
                    auction_id,
                    "adtag".to_string(),
                    publisher.clone(),
                    Some(placement.clone()),
                    rtb::BidRequest::default(),
                    http,
                );

                auction_ctx
                    .res
                    .set(BidResponseState::NoBidReason {
                        reqid: auction_ctx.original_auction_id.clone(),
                        nbr: nobidreasons::NO_IMPRESSIONS_MATCH_AVAILABLE_VIEWPORT,
                        desc: Some(NO_VIEWPORT_IMPRESSION_DESC),
                    })
                    .map_err(|_| anyhow!("auction response already set"))?;

                return ctx
                    .auction_ctx
                    .set(auction_ctx)
                    .map_err(|_| anyhow!("auction_ctx already set"));
            }

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

fn fits_render_caps(
    size: BannerSize,
    caps: &crate::app::pipeline::adtag::request::RenderCaps,
) -> bool {
    if let Some(max_w) = caps.max_w {
        if size.w > max_w {
            return false;
        }
    }

    if let Some(max_h) = caps.max_h {
        if size.h > max_h {
            return false;
        }
    }

    true
}

fn clone_http_context(http: &HttpRequestContext) -> HttpRequestContext {
    HttpRequestContext {
        ip: http.ip,
        user_agent: http.user_agent.clone(),
        sec_ch_ua: http.sec_ch_ua.clone(),
        sec_ch_ua_mobile: http.sec_ch_ua_mobile,
        sec_ch_ua_platform: http.sec_ch_ua_platform.clone(),
        referer: http.referer.clone(),
        cookies: http.cookies.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::pipeline::adtag::request::{AdTagRequest, Consent, RenderCaps};
    use crate::core::models::common::Status;
    use crate::core::models::placement::{ContainerType, FillPolicy, Placement};
    use crate::core::models::property::{Property, PropertyKind};
    use crate::core::models::publisher::{Publisher, PublisherType};
    use std::sync::Arc;

    fn banner_placement() -> Placement {
        Placement {
            id: "plc_1".into(),
            pub_id: "pub_1".into(),
            property_id: "prop_1".into(),
            status: Status::Active,
            name: "Placement".into(),
            fill_policy: FillPolicy::HighestPrice,
            ad_units: vec![CreativeFormat::Banner {
                preferred_size: BannerSize { w: 728, h: 90 },
                alternate_sizes: vec![BannerSize { w: 320, h: 50 }],
            }],
            container: ContainerType::InPlace,
            expandable: false,
        }
    }

    fn site_property() -> Property {
        Property {
            id: "prop_1".into(),
            pub_id: "pub_1".into(),
            status: Status::Active,
            name: "Property".into(),
            kind: PropertyKind::Site,
            domain: "example.com".into(),
            cats: vec![],
            badv: vec![],
            bcat: vec![],
        }
    }

    fn publisher() -> Publisher {
        Publisher {
            id: "pub_1".into(),
            domain: "example.com".into(),
            seller_type: PublisherType::Publisher,
            enabled: true,
            name: "Publisher".into(),
            margin: 0,
            sync_url: None,
        }
    }

    fn ctx_with_caps(max_w: Option<u32>, max_h: Option<u32>) -> AdtagContext {
        let request = AdTagRequest {
            placement_id: "plc_1".into(),
            consent: Consent::default(),
            page_url: None,
            render_caps: RenderCaps {
                env_expandable: false,
                top_frame: true,
                max_w,
                max_h,
            },
            force_bid: false,
        };

        let ctx = AdtagContext::new(request, HttpRequestContext::default());

        ctx.placement
            .set(Arc::new(banner_placement()))
            .expect("placement should set");
        ctx.property
            .set(Arc::new(site_property()))
            .expect("property should set");
        ctx.publisher
            .set(Arc::new(publisher()))
            .expect("publisher should set");

        ctx
    }

    #[test]
    fn keeps_preferred_size_when_viewport_supports_it() {
        let ctx = ctx_with_caps(Some(1024), Some(768));

        BuildAuctionTask.run(&ctx).expect("task should succeed");

        let auction_ctx = ctx.auction_ctx.get().expect("auction ctx should exist");
        let req = auction_ctx.req.read();
        let banner = req.imp[0].banner.as_ref().expect("banner imp expected");

        assert_eq!(banner.w, 728);
        assert_eq!(banner.h, 90);
        assert_eq!(banner.format.len(), 2);
        assert_eq!(banner.format[0].w, 728);
        assert_eq!(banner.format[0].h, 90);
        assert_eq!(banner.format[1].w, 320);
        assert_eq!(banner.format[1].h, 50);
    }

    #[test]
    fn falls_back_to_smaller_banner_size_when_preferred_does_not_fit() {
        let ctx = ctx_with_caps(Some(375), Some(667));

        BuildAuctionTask.run(&ctx).expect("task should succeed");

        let auction_ctx = ctx.auction_ctx.get().expect("auction ctx should exist");
        let req = auction_ctx.req.read();
        let banner = req.imp[0].banner.as_ref().expect("banner imp expected");

        assert_eq!(banner.w, 320);
        assert_eq!(banner.h, 50);
        assert_eq!(banner.format.len(), 1);
        assert_eq!(banner.format[0].w, 320);
        assert_eq!(banner.format[0].h, 50);
    }

    #[test]
    fn returns_prefilled_no_bid_when_no_banner_sizes_fit_viewport() {
        let ctx = ctx_with_caps(Some(200), Some(40));

        BuildAuctionTask.run(&ctx).expect("task should succeed");

        let auction_ctx = ctx.auction_ctx.get().expect("auction ctx should exist");
        match auction_ctx.res.get().expect("auction result should exist") {
            BidResponseState::NoBidReason { nbr, desc, .. } => {
                assert_eq!(*nbr, nobidreasons::NO_IMPRESSIONS_MATCH_AVAILABLE_VIEWPORT);
                assert_eq!(*desc, Some(NO_VIEWPORT_IMPRESSION_DESC));
            }
            other => panic!("expected no-bid reason, got {other:?}"),
        }
    }
}
