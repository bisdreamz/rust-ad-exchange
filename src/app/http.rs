use crate::app::pipeline::ortb::HttpRequestContext;
use crate::core::usersync;
use actix_web::HttpRequest;
use actix_web::cookie::time::Duration as CookieDuration;
use actix_web::cookie::{Cookie, SameSite};
use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::OnceLock;

pub static COOKIE_DOMAIN: OnceLock<Option<String>> = OnceLock::new();

pub fn extract_client_ip(req: &HttpRequest) -> Option<IpAddr> {
    let headers = req.headers();

    if let Some(ip) = headers
        .get("CF-Connecting-IP")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse().ok())
    {
        return Some(ip);
    }

    if let Some(ip) = headers
        .get("X-Forwarded-For")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.split(',').next())
        .and_then(|s| s.trim().parse().ok())
    {
        return Some(ip);
    }

    req.peer_addr().map(|a| a.ip())
}

pub fn extract_http_context(req: &HttpRequest) -> HttpRequestContext {
    let headers = req.headers();

    HttpRequestContext {
        ip: extract_client_ip(req),
        user_agent: headers
            .get("user-agent")
            .and_then(|v| v.to_str().ok())
            .map(str::to_string),
        sec_ch_ua: headers
            .get("sec-ch-ua")
            .and_then(|v| v.to_str().ok())
            .map(str::to_string),
        sec_ch_ua_mobile: headers
            .get("sec-ch-ua-mobile")
            .and_then(|v| v.to_str().ok())
            .map(|s| s == "?1"),
        sec_ch_ua_platform: headers
            .get("sec-ch-ua-platform")
            .and_then(|v| v.to_str().ok())
            .map(str::to_string),
        referer: headers
            .get("referer")
            .and_then(|v| v.to_str().ok())
            .map(str::to_string),
        cookies: extract_cookies(req),
    }
}

pub fn extract_cookies(http_req: &HttpRequest) -> HashMap<String, String> {
    http_req.cookies().map_or_else(
        |_| HashMap::new(),
        |jar| {
            jar.iter()
                .map(|c| (c.name().to_string(), c.value().to_string()))
                .collect()
        },
    )
}

pub fn build_rxid_cookie(uid: &str) -> Cookie<'_> {
    let mut builder = Cookie::build(
        usersync::constants::CONST_REX_COOKIE_ID_PARAM,
        uid.to_owned(),
    )
    .path("/")
    .secure(true)
    .http_only(true)
    .same_site(SameSite::None)
    .max_age(CookieDuration::days(365));

    if let Some(Some(domain)) = COOKIE_DOMAIN.get() {
        builder = builder.domain(domain.clone());
    }

    builder.finish()
}
