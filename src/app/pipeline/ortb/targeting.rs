use crate::app::pipeline::ortb::AuctionContext;
use crate::core::models::targeting::{AllowedPropertyTypes, CommonTargeting};
use compact_str::CompactString;
use rtb::bid_request::{DistributionchannelOneof, Imp};

/// Evaluates whether a [`CommonTargeting`] passes for a specific imp.
pub fn matches_targeting(targeting: &CommonTargeting, ctx: &AuctionContext, imp: &Imp) -> bool {
    let req = ctx.req.read();
    let channel = req.distributionchannel_oneof.as_ref();

    let geo_country = req
        .device
        .as_ref()
        .and_then(|d| d.geo.as_ref())
        .filter(|g| !g.country.is_empty())
        .map(|g| CompactString::from(g.country.as_str()));

    if !targeting.geos_filter.check(geo_country.as_ref()) {
        return false;
    }

    // ctx.device populated by DeviceLookupTask; None if lookup didn't run or failed
    let device = ctx.device.get();

    if !targeting.dev_os_filter.check(device.map(|d| &d.os)) {
        return false;
    }

    if !targeting.dev_type_filter.check(device.map(|d| &d.devtype)) {
        return false;
    }

    if !targeting
        .pub_id_filter
        .check(Some(&CompactString::from(ctx.pubid.as_str())))
    {
        return false;
    }

    // site.domain for sites, app.bundle for apps, dooh.domain for dooh
    let bundle_domain = channel.and_then(|ch| match ch {
        DistributionchannelOneof::App(app) if !app.bundle.is_empty() => {
            Some(CompactString::from(app.bundle.as_str()))
        }
        DistributionchannelOneof::Site(site) if !site.domain.is_empty() => {
            Some(CompactString::from(site.domain.as_str()))
        }
        DistributionchannelOneof::Dooh(dooh) if !dooh.domain.is_empty() => {
            Some(CompactString::from(dooh.domain.as_str()))
        }
        _ => None,
    });

    if !targeting.bundle_domain_filter.check(bundle_domain.as_ref()) {
        return false;
    }

    // imp.tagid is the placement identifier in OpenRTB
    let placement_id = (!imp.tagid.is_empty()).then(|| CompactString::from(imp.tagid.as_str()));

    if !targeting.placement_id_filter.check(placement_id.as_ref()) {
        return false;
    }

    match targeting.allowed_property_types {
        AllowedPropertyTypes::Any => {}
        AllowedPropertyTypes::App => {
            if !matches!(channel, Some(DistributionchannelOneof::App(_))) {
                return false;
            }
        }
        AllowedPropertyTypes::Site => {
            if !matches!(channel, Some(DistributionchannelOneof::Site(_))) {
                return false;
            }
        }
        AllowedPropertyTypes::Dooh => {
            if !matches!(channel, Some(DistributionchannelOneof::Dooh(_))) {
                return false;
            }
        }
    }

    true
}
