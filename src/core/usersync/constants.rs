/// A prefix added to every local user IDs so we can easily
/// validate them and catch when partners accidentally
/// send us external IDS in our buyeruid field
pub const CONST_REX_USER_ID_PREFIX: &str = "rx-";

/// The cookie id param in which the exchange user
/// id is stored
pub const CONST_REX_COOKIE_ID_PARAM: &str = "rxid";

/// The macro placeholder to be used in partner sync urls which
/// represents the spot which we replace with the local exchange
/// user ID
pub const CONST_REX_LOCAL_ID_MACRO: &str = "{RXID}";
