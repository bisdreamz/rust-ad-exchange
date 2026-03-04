use crate::core::managers::CreativeManager;
use crate::core::models::campaign::CampaignCreative;
use crate::core::models::common::Status;
use crate::core::models::creative::{Creative, CreativeFormat};
use rtb::bid_request::Imp;
use std::sync::Arc;
use tracing::trace;

/// Selects a creative from the campaign's attached creatives that matches
/// the imp's format requirements. Resolves creative IDs via the manager,
/// filters for enabled + active + format match, then picks one at random
/// for rotation.
pub fn select_creative(
    campaign_creatives: &[CampaignCreative],
    creative_manager: &CreativeManager,
    imp: &Imp,
) -> Option<Arc<Creative>> {
    let eligible: Vec<_> = campaign_creatives
        .iter()
        .filter(|cc| cc.enabled)
        .filter_map(|cc| creative_manager.by_id(&cc.creative_id))
        .filter(|c| c.status == Status::Active)
        .filter(|c| {
            let pass = matches_format(&c.format, imp);
            if !pass {
                trace!(creative = %c.id, format = %c.format.as_str(), "Creative format mismatch");
            }
            pass
        })
        .collect();

    if eligible.is_empty() {
        return None;
    }

    let idx = fastrand::usize(..eligible.len());
    let selected = Arc::clone(&eligible[idx]);
    trace!(
        creative = %selected.id,
        format = %selected.format.as_str(),
        eligible = eligible.len(),
        "Creative selected"
    );
    Some(selected)
}

fn matches_format(format: &CreativeFormat, imp: &Imp) -> bool {
    match format {
        CreativeFormat::Banner { w, h } => imp
            .banner
            .as_ref()
            .map_or(false, |b| b.w == *w as i32 && b.h == *h as i32),
        CreativeFormat::Video => imp.video.is_some(),
        CreativeFormat::Native => imp.native.is_some(),
        CreativeFormat::Audio => imp.audio.is_some(),
    }
}
