use crate::core::models::creative::{Creative, CreativeFormat};
use rtb::bid_request::Imp;
use std::sync::Arc;

/// Selects a creative from the campaign's creatives that matches
/// the imp's format requirements. If multiple match, picks one
/// at random for rotation.
pub fn select_creative(creatives: &[Arc<Creative>], imp: &Imp) -> Option<Arc<Creative>> {
    let eligible: Vec<_> = creatives
        .iter()
        .filter(|c| matches_format(&c.format, imp))
        .collect();

    if eligible.is_empty() {
        return None;
    }

    let idx = fastrand::usize(..eligible.len());
    Some(Arc::clone(eligible[idx]))
}

fn matches_format(format: &CreativeFormat, imp: &Imp) -> bool {
    match format {
        CreativeFormat::Banner { w, h } => imp
            .banner
            .as_ref()
            .map_or(false, |b| b.w == *w as i32 && b.h == *h as i32),
        CreativeFormat::Video => imp.video.is_some(),
    }
}
