/// Resolves platform creative macros in compiled creative content.
///
/// - `${CDN_DOMAIN}` → the configured CDN base URL (protocol-relative, e.g. "//ads.example.com")
/// - `${CLICK_URL}`  → the campaign click URL, or "#" for preview contexts
pub fn resolve_creative_content(content: &str, cdn_base: &str, click_url: &str) -> String {
    content
        .replace("${CDN_DOMAIN}", cdn_base)
        .replace("${CLICK_URL}", click_url)
}
