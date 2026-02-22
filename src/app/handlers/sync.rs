use crate::app::http::{build_rxid_cookie, extract_cookies};
use crate::app::pipeline::syncing::r#in::context::SyncInContext;
use crate::app::pipeline::syncing::out::context::{SyncOutContext, SyncResponse};
use crate::app::pipeline::syncing::utils;
use crate::core::managers::{DemandManager, PublisherManager};
use crate::core::usersync;
use crate::core::usersync::SyncStore;
use actix_web::{HttpRequest, HttpResponse, Responder};
use anyhow::Error;
use pipeline::Pipeline;
use serde::Serialize;
use std::sync::Arc;
use tracing::{debug, warn};

pub async fn sync_out_handler(
    pubid: String,
    http_req: HttpRequest,
    pipeline: Arc<Pipeline<SyncOutContext, Error>>,
) -> impl Responder {
    let cookies = extract_cookies(&http_req);
    let context = SyncOutContext::new(pubid, cookies);

    if let Err(e) = pipeline.run(&context).await {
        debug!("Sync-out pipeline aborted early: {}", e);
    }

    if context.response.get().is_none() {
        warn!("Cookie init sync returned Ok but no response attached!");
        let mut response = HttpResponse::InternalServerError();
        if let Some(uid) = context.local_uid.get() {
            response.cookie(build_rxid_cookie(uid));
        }
        return response.finish();
    }

    let local_uid = context.local_uid.get();

    match context.response.get().unwrap() {
        SyncResponse::Content(html) => {
            debug!("Sync-out response content: {}", html);
            let mut response = HttpResponse::Ok();
            response.content_type("text/html");
            response.insert_header(("Cache-Control", "no-cache, no-store, must-revalidate"));
            if let Some(uid) = local_uid {
                response.cookie(build_rxid_cookie(uid));
            }
            response.body(html.clone())
        }
        SyncResponse::Error(err) => {
            warn!(
                "Error response encountered during sync out pipeline: {}",
                err
            );
            let mut response = HttpResponse::InternalServerError();
            if let Some(uid) = local_uid {
                response.cookie(build_rxid_cookie(uid));
            }
            response.finish()
        }
        SyncResponse::NoContent => {
            debug!("No content response during sync. Perhaps no bidders?");
            let mut response = HttpResponse::NoContent();
            if let Some(uid) = local_uid {
                response.cookie(build_rxid_cookie(uid));
            }
            response.finish()
        }
    }
}

pub async fn sync_in_handler(
    http_req: HttpRequest,
    pipeline: Arc<Pipeline<SyncInContext, Error>>,
) -> impl Responder {
    let cookies = extract_cookies(&http_req);
    let context = SyncInContext::new(http_req.full_url().to_string(), cookies);

    let pipeline_ok = pipeline.run(&context).await.is_ok();

    let mut response = if pipeline_ok {
        HttpResponse::Ok()
    } else {
        HttpResponse::BadRequest()
    };

    if let Some(uid) = context.local_uid.get() {
        response.cookie(build_rxid_cookie(uid));
    }

    response.finish()
}

#[derive(Serialize)]
struct SyncDebugBidder {
    id: String,
    name: String,
    sync_url: Option<String>,
    sync_kind: Option<String>,
    mapping: Option<SyncDebugMapping>,
}

#[derive(Serialize)]
struct SyncDebugMapping {
    remote_uid: String,
    last_synced: u64,
}

#[derive(Serialize)]
struct SyncDebugPublisher {
    id: String,
    name: String,
    sync_url: Option<String>,
}

#[derive(Serialize)]
struct SyncDebugResponse {
    rxid: String,
    recognized: bool,
    publisher: Option<SyncDebugPublisher>,
    sync_html: String,
    bidder_syncs: Vec<SyncDebugBidder>,
}

const DEBUG_ALLOWED_ORIGINS: &[&str] = &[
    "http://localhost:3000",
    "https://app.neuronicads.com",
    "https://ssp.neuronic.dev",
];

pub fn apply_debug_cors(req: &HttpRequest, response: &mut actix_web::HttpResponseBuilder) {
    let origin = req
        .headers()
        .get("origin")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    if DEBUG_ALLOWED_ORIGINS
        .iter()
        .any(|&allowed| allowed == origin)
    {
        response.insert_header(("Access-Control-Allow-Origin", origin));
        response.insert_header(("Access-Control-Allow-Credentials", "true"));
    }
}

pub async fn sync_debug_handler(
    pubid: String,
    http_req: HttpRequest,
    bidder_manager: Arc<DemandManager>,
    pub_manager: Arc<PublisherManager>,
    sync_store: Arc<dyn SyncStore>,
) -> impl Responder {
    let cookies = extract_cookies(&http_req);
    let (local_uid, recognized) = utils::extract_or_assign_local_uid(&cookies);

    let publisher = pub_manager.get(&pubid);
    let pub_debug = publisher.as_ref().map(|p| SyncDebugPublisher {
        id: pubid.clone(),
        name: p.name.clone(),
        sync_url: p.sync_url.clone(),
    });

    let bidders = bidder_manager.bidders();

    let pub_sync = publisher.as_ref().and_then(|p| {
        p.sync_url
            .as_ref()
            .map(|url| crate::core::models::sync::SyncConfig {
                kind: crate::core::models::sync::SyncKind::Image,
                url: url.clone(),
            })
    });

    let sync_html =
        usersync::utils::generate_sync_iframe_html(&local_uid, bidders.clone(), pub_sync);

    let mappings = sync_store.load(&local_uid).await.unwrap_or_default();

    let bidder_syncs: Vec<SyncDebugBidder> = bidders
        .iter()
        .map(|b| {
            let mapping = mappings.get(&b.id).map(|entry| SyncDebugMapping {
                remote_uid: entry.rid.clone(),
                last_synced: entry.ts,
            });
            SyncDebugBidder {
                id: b.id.clone(),
                name: b.name.clone(),
                sync_url: b.usersync.as_ref().map(|s| s.url.clone()),
                sync_kind: b.usersync.as_ref().map(|s| s.kind.to_string()),
                mapping,
            }
        })
        .collect();

    let body = SyncDebugResponse {
        rxid: local_uid.clone(),
        recognized,
        publisher: pub_debug,
        sync_html,
        bidder_syncs,
    };

    let mut response = HttpResponse::Ok();
    response.cookie(build_rxid_cookie(&local_uid));
    apply_debug_cors(&http_req, &mut response);
    response.json(body)
}

pub async fn sync_debug_preflight(http_req: HttpRequest) -> impl Responder {
    let mut response = HttpResponse::NoContent();
    apply_debug_cors(&http_req, &mut response);
    response.insert_header(("Access-Control-Allow-Methods", "GET, OPTIONS"));
    response.insert_header(("Access-Control-Max-Age", "86400"));
    response.finish()
}
