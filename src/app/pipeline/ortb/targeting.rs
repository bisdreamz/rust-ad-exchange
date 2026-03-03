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
        .check(Some(&CompactString::from(ctx.publisher.id.as_str())))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::enrichment::device::{DeviceInfoBuilder, DeviceType, Os};
    use crate::core::models::targeting::{AllowedPropertyTypes, CommonTargeting, ListFilter};
    use ahash::AHashSet;
    use compact_str::CompactString;
    use rtb::BidRequestBuilder;
    use rtb::bid_request::{DeviceBuilder, GeoBuilder, ImpBuilder, SiteBuilder};

    fn default_ctx() -> AuctionContext {
        let req = BidRequestBuilder::default()
            .id("test".to_string())
            .imp(vec![
                ImpBuilder::default()
                    .id("imp1".to_string())
                    .build()
                    .unwrap(),
            ])
            .build()
            .unwrap();
        let mut ctx = AuctionContext::test_default("pub1");
        *ctx.req.get_mut() = req;
        ctx
    }

    fn default_imp() -> Imp {
        ImpBuilder::default()
            .id("imp1".to_string())
            .build()
            .unwrap()
    }

    fn ctx_with_geo(country: &str) -> AuctionContext {
        let geo = GeoBuilder::default()
            .country(country.to_string())
            .build()
            .unwrap();
        let device = DeviceBuilder::default().geo(geo).build().unwrap();
        let req = BidRequestBuilder::default()
            .id("test".to_string())
            .imp(vec![default_imp()])
            .device(device)
            .build()
            .unwrap();
        let mut ctx = AuctionContext::test_default("pub1");
        *ctx.req.get_mut() = req;
        ctx
    }

    fn ctx_with_site(domain: &str) -> AuctionContext {
        let site = SiteBuilder::default()
            .domain(domain.to_string())
            .build()
            .unwrap();
        let req = BidRequestBuilder::default()
            .id("test".to_string())
            .imp(vec![default_imp()])
            .distributionchannel_oneof(DistributionchannelOneof::Site(site))
            .build()
            .unwrap();
        let mut ctx = AuctionContext::test_default("pub1");
        *ctx.req.get_mut() = req;
        ctx
    }

    fn allow_set<T: Eq + std::hash::Hash>(vals: Vec<T>) -> ListFilter<T> {
        ListFilter::Allow(vals.into_iter().collect())
    }

    fn deny_set<T: Eq + std::hash::Hash>(vals: Vec<T>) -> ListFilter<T> {
        ListFilter::Deny(vals.into_iter().collect())
    }

    #[test]
    fn default_targeting_passes() {
        let targeting = CommonTargeting::default();
        let ctx = default_ctx();
        let imp = default_imp();
        assert!(matches_targeting(&targeting, &ctx, &imp));
    }

    #[test]
    fn geo_allow_match() {
        let targeting = CommonTargeting {
            geos_filter: allow_set(vec![CompactString::from("US")]),
            ..Default::default()
        };
        let ctx = ctx_with_geo("US");
        let imp = default_imp();
        assert!(matches_targeting(&targeting, &ctx, &imp));
    }

    #[test]
    fn geo_allow_reject() {
        let targeting = CommonTargeting {
            geos_filter: allow_set(vec![CompactString::from("US")]),
            ..Default::default()
        };
        let ctx = ctx_with_geo("GB");
        let imp = default_imp();
        assert!(!matches_targeting(&targeting, &ctx, &imp));
    }

    #[test]
    fn geo_deny_blocks() {
        let targeting = CommonTargeting {
            geos_filter: deny_set(vec![CompactString::from("CN")]),
            ..Default::default()
        };
        let ctx = ctx_with_geo("CN");
        let imp = default_imp();
        assert!(!matches_targeting(&targeting, &ctx, &imp));
    }

    #[test]
    fn geo_deny_passes_other() {
        let targeting = CommonTargeting {
            geos_filter: deny_set(vec![CompactString::from("CN")]),
            ..Default::default()
        };
        let ctx = ctx_with_geo("US");
        let imp = default_imp();
        assert!(matches_targeting(&targeting, &ctx, &imp));
    }

    #[test]
    fn pub_id_allow_match() {
        let targeting = CommonTargeting {
            pub_id_filter: allow_set(vec![CompactString::from("pub1")]),
            ..Default::default()
        };
        let ctx = default_ctx();
        let imp = default_imp();
        assert!(matches_targeting(&targeting, &ctx, &imp));
    }

    #[test]
    fn pub_id_allow_reject() {
        let targeting = CommonTargeting {
            pub_id_filter: allow_set(vec![CompactString::from("pub_other")]),
            ..Default::default()
        };
        let ctx = default_ctx();
        let imp = default_imp();
        assert!(!matches_targeting(&targeting, &ctx, &imp));
    }

    #[test]
    fn os_allow_match() {
        let targeting = CommonTargeting {
            dev_os_filter: allow_set(vec![Os::Ios]),
            ..Default::default()
        };
        let ctx = default_ctx();
        let info = DeviceInfoBuilder::default()
            .brand(None)
            .model(None)
            .os(Os::Ios)
            .build()
            .unwrap();
        let _ = ctx.device.set(info);
        let imp = default_imp();
        assert!(matches_targeting(&targeting, &ctx, &imp));
    }

    #[test]
    fn os_allow_reject() {
        let targeting = CommonTargeting {
            dev_os_filter: allow_set(vec![Os::Ios]),
            ..Default::default()
        };
        let ctx = default_ctx();
        let info = DeviceInfoBuilder::default()
            .brand(None)
            .model(None)
            .os(Os::Android)
            .build()
            .unwrap();
        let _ = ctx.device.set(info);
        let imp = default_imp();
        assert!(!matches_targeting(&targeting, &ctx, &imp));
    }

    #[test]
    fn devtype_allow_match() {
        let targeting = CommonTargeting {
            dev_type_filter: allow_set(vec![DeviceType::Phone]),
            ..Default::default()
        };
        let ctx = default_ctx();
        let info = DeviceInfoBuilder::default()
            .brand(None)
            .model(None)
            .devtype(DeviceType::Phone)
            .build()
            .unwrap();
        let _ = ctx.device.set(info);
        let imp = default_imp();
        assert!(matches_targeting(&targeting, &ctx, &imp));
    }

    #[test]
    fn devtype_deny_rejects() {
        let targeting = CommonTargeting {
            dev_type_filter: deny_set(vec![DeviceType::Bot]),
            ..Default::default()
        };
        let ctx = default_ctx();
        let info = DeviceInfoBuilder::default()
            .brand(None)
            .model(None)
            .devtype(DeviceType::Bot)
            .build()
            .unwrap();
        let _ = ctx.device.set(info);
        let imp = default_imp();
        assert!(!matches_targeting(&targeting, &ctx, &imp));
    }

    #[test]
    fn domain_allow_match() {
        let targeting = CommonTargeting {
            bundle_domain_filter: allow_set(vec![CompactString::from("example.com")]),
            ..Default::default()
        };
        let ctx = ctx_with_site("example.com");
        let imp = default_imp();
        assert!(matches_targeting(&targeting, &ctx, &imp));
    }

    #[test]
    fn domain_allow_reject() {
        let targeting = CommonTargeting {
            bundle_domain_filter: allow_set(vec![CompactString::from("example.com")]),
            ..Default::default()
        };
        let ctx = ctx_with_site("other.com");
        let imp = default_imp();
        assert!(!matches_targeting(&targeting, &ctx, &imp));
    }

    #[test]
    fn placement_allow_match() {
        let targeting = CommonTargeting {
            placement_id_filter: allow_set(vec![CompactString::from("plc1")]),
            ..Default::default()
        };
        let ctx = default_ctx();
        let imp = ImpBuilder::default()
            .id("imp1".to_string())
            .tagid("plc1".to_string())
            .build()
            .unwrap();
        assert!(matches_targeting(&targeting, &ctx, &imp));
    }

    #[test]
    fn placement_allow_reject() {
        let targeting = CommonTargeting {
            placement_id_filter: allow_set(vec![CompactString::from("plc1")]),
            ..Default::default()
        };
        let ctx = default_ctx();
        let imp = ImpBuilder::default()
            .id("imp1".to_string())
            .tagid("plc_other".to_string())
            .build()
            .unwrap();
        assert!(!matches_targeting(&targeting, &ctx, &imp));
    }

    #[test]
    fn property_type_site_match() {
        let targeting = CommonTargeting {
            allowed_property_types: AllowedPropertyTypes::Site,
            ..Default::default()
        };
        let ctx = ctx_with_site("example.com");
        let imp = default_imp();
        assert!(matches_targeting(&targeting, &ctx, &imp));
    }

    #[test]
    fn property_type_site_rejects_none() {
        let targeting = CommonTargeting {
            allowed_property_types: AllowedPropertyTypes::Site,
            ..Default::default()
        };
        // default_ctx has no distributionchannel_oneof
        let ctx = default_ctx();
        let imp = default_imp();
        assert!(!matches_targeting(&targeting, &ctx, &imp));
    }

    #[test]
    fn empty_allow_denies_all() {
        let targeting = CommonTargeting {
            geos_filter: ListFilter::Allow(AHashSet::new()),
            ..Default::default()
        };
        let ctx = ctx_with_geo("US");
        let imp = default_imp();
        assert!(!matches_targeting(&targeting, &ctx, &imp));
    }

    #[test]
    fn empty_deny_allows_all() {
        let targeting = CommonTargeting {
            geos_filter: ListFilter::Deny(AHashSet::new()),
            ..Default::default()
        };
        let ctx = ctx_with_geo("US");
        let imp = default_imp();
        assert!(matches_targeting(&targeting, &ctx, &imp));
    }
}
