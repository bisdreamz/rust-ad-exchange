use crate::core::models::shaping::ShapingFeature;
use logictree::Feature;
use rtb::BidRequest;
use rtb::bid_request::{Banner, DistributionchannelOneof};
use rtb::bid_response::Bid;
use rtb::utils::adm::AdFormat;
use smallvec::SmallVec;
use tracing::{debug, trace};

pub const MISSING_STR: &str = "*";
pub const MISSING_U32: u32 = 0;

fn add_if_not_exist(value: &String, dest: &mut SmallVec<[String; 8]>) {
    if !dest.contains(&value) {
        dest.push(value.clone());
    }
}

fn extract_feature_pubid(req: &BidRequest) -> Feature {
    let publisher_channel = match &req.distributionchannel_oneof {
        Some(publisher) => publisher,
        None => return Feature::string(ShapingFeature::PubId.as_ref(), MISSING_STR),
    };

    let channel_id = match publisher_channel {
        DistributionchannelOneof::Site(site) => &site.id,
        DistributionchannelOneof::App(app) => &app.id,
        DistributionchannelOneof::Dooh(dooh) => &dooh.id,
    };

    if channel_id.is_empty() {
        return Feature::string(ShapingFeature::PubId.as_ref(), MISSING_STR);
    }

    Feature::string(ShapingFeature::PubId.as_ref(), channel_id)
}

fn extract_feature_geo(req: &BidRequest) -> Feature {
    let device = match &req.device {
        Some(device) => device,
        None => return Feature::string(ShapingFeature::Geo.as_ref(), MISSING_STR),
    };

    let geo = match &device.geo {
        Some(geo) => geo,
        None => return Feature::string(ShapingFeature::Geo.as_ref(), MISSING_STR),
    };

    if geo.country.is_empty() {
        return Feature::string(ShapingFeature::Geo.as_ref(), MISSING_STR);
    }

    Feature::string(ShapingFeature::Geo.as_ref(), &geo.country)
}

fn extract_feature_domain(req: &BidRequest) -> Feature {
    let publisher_channel = match &req.distributionchannel_oneof {
        Some(publisher) => publisher,
        None => return Feature::string(ShapingFeature::Domain.as_ref(), MISSING_STR),
    };

    let domain = match publisher_channel {
        DistributionchannelOneof::Site(site) => &site.domain,
        DistributionchannelOneof::App(app) => &app.bundle,
        DistributionchannelOneof::Dooh(dooh) => &dooh.domain,
    };

    if domain.is_empty() {
        return Feature::string(ShapingFeature::Domain.as_ref(), MISSING_STR);
    }

    Feature::string(ShapingFeature::Domain.as_ref(), domain)
}

fn extract_feature_device_os(req: &BidRequest) -> Feature {
    let device = match &req.device {
        Some(device) => device,
        None => return Feature::string(ShapingFeature::DeviceOs.as_ref(), MISSING_STR),
    };

    if device.os.is_empty() {
        return Feature::string(ShapingFeature::DeviceOs.as_ref(), MISSING_STR);
    }

    Feature::string(ShapingFeature::DeviceOs.as_ref(), &device.os)
}

fn extract_feature_device_type(req: &BidRequest) -> Feature {
    let device = match &req.device {
        Some(device) => device,
        None => return Feature::u32(ShapingFeature::DeviceType.as_ref(), MISSING_U32),
    };

    Feature::u32(
        ShapingFeature::DeviceType.as_ref(),
        device.devicetype as u32,
    )
}

fn extract_feature_device_con_type(req: &BidRequest) -> Feature {
    let device = match &req.device {
        Some(device) => device,
        None => return Feature::u32(ShapingFeature::DeviceConType.as_ref(), MISSING_U32),
    };

    Feature::u32(
        ShapingFeature::DeviceConType.as_ref(),
        device.connectiontype as u32,
    )
}

fn extract_feature_tagid(req: &BidRequest) -> Feature {
    let mut values = SmallVec::<[String; 8]>::new();

    for imp in &req.imp {
        if imp.tagid.is_empty() {
            continue;
        }

        add_if_not_exist(&imp.tagid, &mut values);
    }

    if values.is_empty() {
        return Feature::string(ShapingFeature::ZoneId.as_ref(), MISSING_STR);
    }

    Feature::multi_string(ShapingFeature::ZoneId.as_ref(), values)
}

fn format_ad_size_format(format: AdFormat, size: Option<(u32, u32)>) -> String {
    match format {
        AdFormat::Banner => {
            let (w, h) = size.unwrap_or((0, 0));
            format!("b_{}x{}", w, h)
        }
        AdFormat::Video => {
            let (w, h) = size.unwrap_or((0, 0));
            format!("v_{}x{}", w, h)
        }
        AdFormat::Native => {
            "n".to_string() // no size
        }
        AdFormat::Audio => {
            "a".to_string() // no size
        }
    }
}

/// Generate an entry for each available ad size to ensure they
/// are all properly recorded
fn add_size_formats_banner(banner: &Banner, dest: &mut SmallVec<[String; 8]>) {
    let pw = banner.w;
    let ph = banner.h;

    if pw > 0 && ph > 0 {
        let str = format_ad_size_format(AdFormat::Banner, Some((pw as u32, ph as u32)));
        add_if_not_exist(&str, dest);
    }

    if !banner.format.is_empty() {
        for f in &banner.format {
            if (f.w == 0 || f.h == 0) || (f.w == pw && f.h == ph) {
                continue;
            }

            let str = format_ad_size_format(AdFormat::Banner, Some((f.w as u32, f.h as u32)));
            add_if_not_exist(&str, dest);
        }
    }
}

fn extract_buyer_user_matched(req: &BidRequest) -> Feature {
    let channel = match &req.distributionchannel_oneof {
        Some(channel) => channel,
        None => return Feature::boolean(ShapingFeature::UserMatched.as_ref(), false),
    };

    let matched = match &channel {
        DistributionchannelOneof::Site(_) => {
            req.user.is_some() && !req.user.as_ref().unwrap().buyeruid.is_empty()
        }
        DistributionchannelOneof::App(_) => {
            let ifa = &req.device.as_ref().unwrap().ifa;
            !ifa.is_empty() && !ifa.starts_with("0000")
        }
        _ => true, // just treat as matched for other channels for now
    };

    Feature::boolean(ShapingFeature::UserMatched.as_ref(), matched)
}

/// Records ad format size feature specific to the bid, so we dont
/// cross contaminate *available* request features such as
/// multiple sizes on a request, with the actual size details
/// being bid on by the demand partner. This is important to ensure
/// we dont misattribute positive activity like bidding on other
/// ad segments that had overlap on available sizes but no
/// real bidding actiivty
fn extract_feature_ad_format_sizes_bid(req: &BidRequest, bid: &Bid) -> Feature {
    let mut values = SmallVec::<[String; 8]>::new();

    if let Some(format) = rtb::utils::detect_ad_format(&bid)
        && bid.w > 0
        && bid.h > 0
    {
        let str = format_ad_size_format(format, Some((bid.w as u32, bid.h as u32)));
        trace!(
            "Have bid details, recording specific shaping details: {}",
            str
        );

        add_if_not_exist(&str, &mut values);

        return Feature::multi_string(ShapingFeature::AdSizeFormat.as_ref(), values);
    }

    debug!("Bid with 0 w/h (audio?) or failed to extract bid format, defaulting to req features");

    extract_feature_ad_format_sizes(req)
}

/// Records the features of ad format_size for the given request.
/// This produces combinations of every available ad format and
/// size (from w/h and format) to ensure we properly record the
/// available features on a request.
fn extract_feature_ad_format_sizes(req: &BidRequest) -> Feature {
    let mut values = SmallVec::<[String; 8]>::new();

    for imp in &req.imp {
        if imp.banner.is_some() {
            let banner = imp.banner.as_ref().unwrap();
            add_size_formats_banner(&banner, &mut values);
        }

        if imp.video.is_some() {
            let video = imp.video.as_ref().unwrap();
            let str =
                format_ad_size_format(AdFormat::Video, Some((video.w as u32, video.h as u32)));
            add_if_not_exist(&str, &mut values);
        }

        if imp.audio.is_some() {
            let str = format_ad_size_format(AdFormat::Audio, None);
            add_if_not_exist(&str, &mut values);
        }

        if imp.native.is_some() {
            let str = format_ad_size_format(AdFormat::Native, None);
            add_if_not_exist(&str, &mut values);
        }
    }

    Feature::multi_string(ShapingFeature::AdSizeFormat.as_ref(), values)
}

pub fn extract_shaping_feature(
    feature: &ShapingFeature,
    req: &BidRequest,
    bid_opt: Option<&Bid>,
) -> Feature {
    match feature {
        ShapingFeature::PubId => extract_feature_pubid(req),
        ShapingFeature::Geo => extract_feature_geo(req),
        ShapingFeature::Domain => extract_feature_domain(req),
        ShapingFeature::DeviceOs => extract_feature_device_os(req),
        ShapingFeature::DeviceType => extract_feature_device_type(req),
        ShapingFeature::DeviceConType => extract_feature_device_con_type(req),
        ShapingFeature::AdSizeFormat => match bid_opt {
            Some(bid) => extract_feature_ad_format_sizes_bid(req, bid),
            None => extract_feature_ad_format_sizes(req),
        },
        ShapingFeature::ZoneId => extract_feature_tagid(req),
        ShapingFeature::UserMatched => extract_buyer_user_matched(req),
    }
}
