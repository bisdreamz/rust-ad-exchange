use serde::Deserialize;

/// Inbound request body from the Neuronic ad tag.
/// HTTP context (UA, IP, cookies) is extracted at the handler level via extract_http_context.
/// Consent mirrors the JS getConsent() output shape — extend both in tandem.
#[derive(Debug, Deserialize)]
pub struct AdTagRequest {
    pub placement_id: String,

    #[serde(default)]
    pub consent: Consent,

    /// Only present when the JS tag confirmed the URL as known-good:
    /// same-origin window.top access, or a GAM %%PATTERN:url%% macro expansion.
    /// Absent = unknown origin; the pipeline falls back to the property domain only.
    pub page_url: Option<String>,

    #[serde(default)]
    pub render_caps: RenderCaps,

    /// Dev/testing only. When true, forces the TestBidderTask to inject a synthetic
    /// winning bid regardless of real demand. Requires BidRequest.test = true to activate.
    /// Must be gated to non-production environments server-side.
    /// Set by the JS tag via the `?neuronic_force_bid=1` URL parameter.
    #[serde(default)]
    pub force_bid: bool,
}

/// Rendering environment capabilities assessed by the JS tag.
/// JS owns this determination — it is responsible for rendering the markup.
#[derive(Debug, Deserialize, Default)]
pub struct RenderCaps {
    /// Environment supports expandable ads: tag is in top frame, a friendly
    /// iframe it can break out of, or a SafeFrame. JS evaluates this.
    /// Actual serving of expandable ads also requires placement.expandable.
    #[serde(default)]
    pub env_expandable: bool,
    /// Tag is executing in the top-level window (window.self === window.top).
    /// Maps to OpenRTB banner.topframe.
    #[serde(default)]
    pub top_frame: bool,
}

/// Top-level consent envelope. Absent field = signal unavailable or timed out on the client.
#[derive(Debug, Deserialize, Default)]
pub struct Consent {
    /// GDPR TCF consent — forwarded as-is to demand partners for their own parsing
    pub tcf: Option<TcfConsent>,
    /// IAB GPP multi-jurisdictional privacy signal
    pub gpp: Option<GppConsent>,
}

#[derive(Debug, Deserialize)]
pub struct TcfConsent {
    pub tc_string: String,
    pub gdpr_applies: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub struct GppConsent {
    pub gpp_string: String,
    #[serde(default)]
    pub gpp_applicable_sections: Vec<String>,
}
