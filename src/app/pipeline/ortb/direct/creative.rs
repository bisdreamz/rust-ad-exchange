use crate::core::managers::CreativeManager;
use crate::core::models::campaign::CampaignCreative;
use crate::core::models::common::Status;
use crate::core::models::creative::{BannerSize, Creative, CreativeFormat};
use rtb::bid_request::Imp;
use std::sync::Arc;
use tracing::trace;

pub struct SelectedCreative {
    pub creative: Arc<Creative>,
    pub banner_size: Option<BannerSize>,
}

/// Selects a creative from the campaign's attached creatives that matches
/// the imp's format requirements. Resolves creative IDs via the manager,
/// filters for enabled + active + format match, then picks one at random
/// for rotation.
pub fn select_creative(
    campaign_creatives: &[CampaignCreative],
    creative_manager: &CreativeManager,
    imp: &Imp,
) -> Option<SelectedCreative> {
    let eligible: Vec<_> = campaign_creatives
        .iter()
        .filter(|cc| cc.enabled)
        .filter_map(|cc| creative_manager.by_id(&cc.creative_id))
        .filter(|c| c.status == Status::Active)
        .filter_map(|creative| {
            let banner_size = match_format(&creative.format, imp);
            if banner_size.is_none() && !matches_non_banner(&creative.format, imp) {
                trace!(
                    creative = %creative.id,
                    format = %creative.format.as_str(),
                    "Creative format mismatch"
                );
                return None;
            }

            Some(SelectedCreative {
                creative,
                banner_size,
            })
        })
        .collect();

    if eligible.is_empty() {
        return None;
    }

    let idx = fastrand::usize(..eligible.len());
    let selected = &eligible[idx];
    trace!(
        creative = %selected.creative.id,
        format = %selected.creative.format.as_str(),
        eligible = eligible.len(),
        "Creative selected"
    );
    Some(SelectedCreative {
        creative: Arc::clone(&selected.creative),
        banner_size: selected.banner_size,
    })
}

fn matches_non_banner(format: &CreativeFormat, imp: &Imp) -> bool {
    match format {
        CreativeFormat::Banner { .. } => false,
        CreativeFormat::Video => imp.video.is_some(),
        CreativeFormat::Native => imp.native.is_some(),
        CreativeFormat::Audio => imp.audio.is_some(),
    }
}

fn match_format(format: &CreativeFormat, imp: &Imp) -> Option<BannerSize> {
    match format {
        CreativeFormat::Banner { .. } => match_banner_size(format, imp),
        _ => None,
    }
}

fn match_banner_size(format: &CreativeFormat, imp: &Imp) -> Option<BannerSize> {
    let supported = imp_banner_sizes(imp)?;
    let creative_sizes = format.banner_supported_sizes();

    supported
        .into_iter()
        .find(|size| creative_sizes.contains(size))
}

fn imp_banner_sizes(imp: &Imp) -> Option<Vec<BannerSize>> {
    let banner = imp.banner.as_ref()?;
    let mut sizes = Vec::new();

    if banner.w > 0 && banner.h > 0 {
        sizes.push(BannerSize {
            w: banner.w as u32,
            h: banner.h as u32,
        });
    }

    for format in &banner.format {
        if format.w <= 0 || format.h <= 0 {
            continue;
        }

        let size = BannerSize {
            w: format.w as u32,
            h: format.h as u32,
        };

        if !sizes.contains(&size) {
            sizes.push(size);
        }
    }

    Some(sizes)
}

#[cfg(test)]
mod tests {
    use super::{imp_banner_sizes, match_banner_size};
    use crate::core::models::creative::{BannerSize, CreativeFormat};
    use rtb::bid_request::{Banner, Format, Imp};

    fn banner_imp(primary: BannerSize, alternates: &[BannerSize]) -> Imp {
        Imp {
            id: "1".to_string(),
            banner: Some(Banner {
                w: primary.w as i32,
                h: primary.h as i32,
                format: std::iter::once(primary)
                    .chain(alternates.iter().copied())
                    .map(|size| Format {
                        w: size.w as i32,
                        h: size.h as i32,
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[test]
    fn dedupes_primary_size_from_banner_format_list() {
        let primary = BannerSize { w: 300, h: 250 };
        let mobile = BannerSize { w: 320, h: 50 };
        let imp = banner_imp(primary, &[mobile]);

        let sizes = imp_banner_sizes(&imp).expect("banner sizes");

        assert_eq!(sizes, vec![primary, mobile]);
    }

    #[test]
    fn prefers_request_primary_size_when_creative_supports_it() {
        let primary = BannerSize { w: 300, h: 250 };
        let mobile = BannerSize { w: 320, h: 50 };
        let imp = banner_imp(primary, &[mobile]);
        let creative = CreativeFormat::Banner {
            preferred_size: mobile,
            alternate_sizes: vec![primary],
        };

        let chosen = match_banner_size(&creative, &imp).expect("matched banner size");

        assert_eq!(chosen, primary);
    }
}
