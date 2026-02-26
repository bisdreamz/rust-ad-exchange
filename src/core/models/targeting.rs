use crate::core::enrichment::device::{DeviceType, Os};
use ahash::AHashSet;
use compact_str::CompactString;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::hash::Hash;

/// Standard representation
/// of the common include/exclude
/// list
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "values",
    bound = "T: Eq + Hash + Serialize + DeserializeOwned"
)]
pub enum ListFilter<T: Eq + Hash> {
    /// Allow any and all values
    Any,
    /// Allow only the chosen non-empty values
    Allow(AHashSet<T>),
    /// Block any of the chosen non-empty values
    Deny(AHashSet<T>),
}

impl<T: Eq + Hash> Default for ListFilter<T> {
    fn default() -> Self {
        Self::Any
    }
}

impl<T: Eq + Hash> ListFilter<T> {
    /// - `Any`        → always passes
    /// - `Allow(set)` → val must be Some and in the set; empty set denies all
    /// - `Deny(set)`  → val must be None or absent from the set; empty set allows all
    pub fn check(&self, val: Option<&T>) -> bool {
        match self {
            Self::Any => true,
            Self::Allow(set) => val.map_or(false, |v| set.contains(v)),
            Self::Deny(set) => val.map_or(true, |v| !set.contains(v)),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub enum AllowedPropertyTypes {
    App,
    Site,
    Dooh,
    #[default]
    Any,
}

/// Common targeting parameters across
/// models which may use them, e.g.
/// campaigns, deals, or other
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CommonTargeting {
    pub geos_filter: ListFilter<CompactString>,
    pub dev_os_filter: ListFilter<Os>,
    pub dev_type_filter: ListFilter<DeviceType>,
    pub pub_id_filter: ListFilter<CompactString>,
    pub bundle_domain_filter: ListFilter<CompactString>,
    pub placement_id_filter: ListFilter<CompactString>,
    pub allowed_property_types: AllowedPropertyTypes,
}
