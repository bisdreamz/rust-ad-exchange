use crate::core::usersync;
use std::collections::HashMap;
use tracing::warn;

/// Convenience method for extracting a local sync uid value from
/// cookies map or assigning a new value. Uses the
/// ['usersync::constants::CONST_REX_COOKIE_ID'] cookie param
pub fn extract_or_assign_local_uid(cookies: &HashMap<String, String>) -> (String, bool) {
    match cookies.get(usersync::constants::CONST_REX_COOKIE_ID_PARAM) {
        Some(uid) => {
            if usersync::utils::validate_local_id(uid) {
                (uid.clone(), true)
            } else {
                let new_uid = usersync::utils::generate_local_id();

                warn!(
                    "Found invalid uid cookie value {}, how?! Re-assigning to {}",
                    uid, new_uid
                );

                (new_uid, false)
            }
        }
        None => {
            let new_uid = usersync::utils::generate_local_id();

            (new_uid, false)
        }
    }
}
