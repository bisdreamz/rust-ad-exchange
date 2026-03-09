use crate::core::models::creative::{Creative, CreativeKind};
use std::sync::{Arc, OnceLock};

pub struct RawCreativeContext {
    /// The creative ID from the request path
    pub creative_id: String,
    /// Resolved creative, set by ResolveCreativeTask
    pub creative: OnceLock<Arc<Creative>>,
    /// Fully resolved creative content with macros substituted, set by ResolveCdnTask
    pub resolved_content: OnceLock<String>,
}

impl RawCreativeContext {
    pub fn new(creative_id: String) -> Self {
        Self {
            creative_id,
            creative: OnceLock::new(),
            resolved_content: OnceLock::new(),
        }
    }

    /// Content-Type header value based on the resolved creative's format.
    /// None when no creative was resolved.
    pub fn content_type(&self) -> Option<&'static str> {
        self.creative.get().map(|c| match c.format.kind() {
            CreativeKind::Html => "text/html; charset=utf-8",
            CreativeKind::VastXml => "application/xml; charset=utf-8",
        })
    }
}
