use crate::core::models::bidder::Bidder;
use crate::core::models::sync::{SyncConfig, SyncKind};
use crate::core::usersync::constants;
use std::sync::Arc;
use tracing::{debug, warn};
use uuid::Uuid;

/// Check if buyer id value contains our
/// expected ['REX_USER_PREFIX'] prefixed value
pub fn validate_local_id(local_id: &str) -> bool {
    local_id
        .trim()
        .starts_with(constants::CONST_REX_USER_ID_PREFIX)
}

/// Generate a local buyer id value with our
/// ['REX_USER_PREFIX'] platform prefix
pub fn generate_local_id() -> String {
    format!("{}{}", constants::CONST_REX_USER_ID_PREFIX, Uuid::new_v4())
}

/// Build the partner pixel of type image of iframe, and place our local
/// exchange id in the specified macro value if present in the partner url
pub fn build_kind_pixel(sync: &SyncConfig, local_uid: &String, local_uid_macro: &str) -> String {
    let safe_url = sync
        .url
        .replace('&', "&amp;")
        .replace('"', "&quot;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace(local_uid_macro, local_uid);

    match sync.kind {
        SyncKind::Image => {
            format!(
                "<img height=\"1\" width=\"1\" style='display: none;' src=\"{}\" />",
                safe_url
            )
        }
        SyncKind::Iframe => {
            format!(
                "<iframe height=\"1\" width=\"1\" style='display: none;' src=\"{}\"></iframe>",
                safe_url
            )
        }
    }
}

/// Builds the iframe html content for the sync pixels
/// of the provided bidders and optionally a return to
/// the upstream supplier to complete our supply sync
///
/// # Arguments
/// * 'local_uid' - The local exchange ID which gets placed in the
/// ['usersync::constants::CONST_REX_LOCAL_ID_MACRO'] macro location if present in
/// any partner sync URLs
pub fn generate_sync_iframe_html(
    local_uid: &String,
    bidders: Vec<Arc<Bidder>>,
    pub_sync: Option<SyncConfig>,
) -> String {
    let mut pixels = Vec::with_capacity(bidders.len() + 1);
    let target_local_uid_macro = constants::CONST_REX_LOCAL_ID_MACRO;

    if let Some(sync) = pub_sync {
        if !sync.url.trim().is_empty() {
            pixels.push(build_kind_pixel(&sync, local_uid, target_local_uid_macro));
            debug!("Appended publisher sync pixel to iframe content");
        }
    }

    for bidder in bidders.iter() {
        let bidder_sync = match &bidder.usersync {
            Some(sync) => sync,
            None => continue,
        };

        if bidder_sync.url.trim().is_empty() {
            warn!(
                "Syncing configured for bidder {} but pixel url empty!",
                bidder.name
            );
            continue;
        }

        pixels.push(build_kind_pixel(
            bidder_sync,
            local_uid,
            target_local_uid_macro,
        ));
        debug!("Appended sync pixel to iframe content for {}", bidder.name);
    }

    pixels.join("\n")
}
