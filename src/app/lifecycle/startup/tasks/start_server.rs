use crate::app::lifecycle::context::StartupContext;
use crate::app::pipeline::events::billing::context::BillingEventContext;
use crate::app::pipeline::ortb::AuctionContext;
use crate::app::pipeline::syncing::r#in::context::SyncInContext;
use crate::app::pipeline::syncing::out::context::{SyncOutContext, SyncResponse};
use crate::app::pipeline::syncing::utils;
use crate::core::managers::{DemandManager, PublisherManager};
use crate::core::usersync;
use crate::core::usersync::SyncStore;
use actix_web::cookie::time::Duration as CookieDuration;
use actix_web::cookie::{Cookie, SameSite};
use actix_web::web;
use actix_web::{HttpRequest, HttpResponse, Responder};
use anyhow::{Error, anyhow, bail};
use async_trait::async_trait;
use opentelemetry::metrics::{Counter, Histogram};
use opentelemetry::{KeyValue, global};
use pipeline::{AsyncTask, Pipeline};
use rtb::common::bidresponsestate::BidResponseState;
use rtb::server::json::{FastJson, JsonBidResponseState};
use rtb::server::{Server, ServerConfig};
use rtb::{BidRequest, sample_or_attach_root_span};
use serde::Serialize;
use std::collections::HashMap;
use std::sync::{Arc, LazyLock, OnceLock};
use tracing::{Instrument, debug, info, instrument, warn};

static COOKIE_DOMAIN: OnceLock<Option<String>> = OnceLock::new();

static REQUESTS_TOTAL: LazyLock<Counter<u64>> = LazyLock::new(|| {
    global::meter("rex")
        .u64_counter("requests")
        .with_description("All non-broken requests received")
        .with_unit("1")
        .build()
});

static REQUEST_DURATION: LazyLock<Histogram<f64>> = LazyLock::new(|| {
    global::meter("rex")
        .f64_histogram("http.server.duration")
        .with_description("HTTP request duration")
        .with_unit("s")
        .build()
});

pub struct StartServerTask;

fn record_request_metric(
    path: String,
    source: &str,
    pubid: String,
    brs: &BidResponseState,
    pipeline_ok: bool,
    duration: std::time::Duration,
) {
    let mut attrs = vec![
        KeyValue::new("pubid", pubid),
        KeyValue::new("source", source.to_string()),
        KeyValue::new("pipeline_completed", pipeline_ok),
    ];

    let mut status_code = 204;
    match brs {
        BidResponseState::Bid(_) => {
            attrs.push(KeyValue::new("outcome", "bid"));
            status_code = 200;
        }
        BidResponseState::NoBid { desc } => {
            attrs.push(KeyValue::new("outcome", "no_bid"));
            attrs.push(KeyValue::new("no_bid_desc", desc.unwrap_or("none")));
        }
        BidResponseState::NoBidReason { nbr, desc, .. } => {
            attrs.push(KeyValue::new("outcome", "no_bid_reason"));
            attrs.push(KeyValue::new("no_bid_desc", desc.unwrap_or("none")));
            attrs.push(KeyValue::new("no_bid_code", nbr.to_string()));
        }
    }

    REQUESTS_TOTAL.add(1, &attrs);

    attrs.push(KeyValue::new("http.response.status_code", status_code));
    attrs.push(KeyValue::new("http.route", path));

    REQUEST_DURATION.record(duration.as_secs_f64(), &attrs);
}

async fn handle_bid_request_instrumented(
    auction_id: String,
    pubid: String,
    source: String,
    req: BidRequest,
    cookies: Option<HashMap<String, String>>,
    pipeline: Arc<Pipeline<AuctionContext, Error>>,
) -> (BidResponseState, bool) {
    let mut ctx = AuctionContext::new(auction_id, source.clone(), pubid, req, cookies);

    let pipeline_result = pipeline.run(&ctx).await;

    match &pipeline_result {
        Ok(_) => debug!("Request pipeline success"),
        Err(e) => debug!("Request pipeline aborted: {}", e),
    }

    let brs = ctx.res.take().unwrap_or_else(|| BidResponseState::NoBid {
        desc: if pipeline_result.is_err() {
            Some("Failed processing req".into())
        } else {
            Some("No Bid".into())
        },
    });

    (brs, pipeline_result.is_ok())
}

async fn handle_bid_request(
    auction_id: String,
    pubid: String,
    source: String,
    req: BidRequest,
    cookies: Option<HashMap<String, String>>,
    pipeline: Arc<Pipeline<AuctionContext, Error>>,
    span_sample_rate: f32,
) -> (BidResponseState, bool) {
    let root_span = sample_or_attach_root_span!(
        span_sample_rate,
        "handle_bid_request",
        pub_id = pubid,
        source = source,
    );

    handle_bid_request_instrumented(auction_id, pubid, source, req, cookies, pipeline)
        .instrument(root_span)
        .await
}

async fn json_bid_handler(
    auction_id: String,
    pubid: String,
    req: FastJson<BidRequest>,
    http_req: HttpRequest,
    pipeline: Arc<Pipeline<AuctionContext, Error>>,
    span_sample_rate: f32,
) -> JsonBidResponseState {
    let source = http_req.match_pattern().unwrap_or("unknown".to_string());
    let start = std::time::Instant::now();
    let path = http_req.path().to_string();
    let cookies = extract_cookies(&http_req);

    let (brs, pipeline_completed) = handle_bid_request(
        auction_id,
        pubid.clone(),
        path.clone(),
        req.into_inner(),
        Some(cookies),
        pipeline.clone(),
        span_sample_rate,
    )
    .await;

    let duration = start.elapsed();
    record_request_metric(path, &source, pubid, &brs, pipeline_completed, duration);

    JsonBidResponseState(brs)
}

async fn billing_event_handler(
    http_req: HttpRequest,
    event_pipeline: Arc<Pipeline<BillingEventContext, Error>>,
) -> impl Responder {
    debug!("Billing event handler called");

    let context = BillingEventContext::new(http_req.full_url().to_string());

    match event_pipeline.run(&context).await {
        Ok(_) => {
            debug!("Billing event success");
            HttpResponse::Ok().finish()
        }
        Err(e) => {
            warn!("Failed to record billing event: {}", e);
            HttpResponse::BadRequest().finish()
        }
    }
}

fn extract_cookies(http_req: &HttpRequest) -> HashMap<String, String> {
    http_req.cookies().map_or_else(
        |_| HashMap::new(),
        |jar| {
            jar.iter()
                .map(|c| (c.name().to_string(), c.value().to_string()))
                .collect()
        },
    )
}

fn build_rxid_cookie(uid: &str) -> Cookie<'_> {
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

async fn sync_out_handler(
    pubid: String,
    http_req: HttpRequest,
    pipeline: Arc<Pipeline<SyncOutContext, Error>>,
) -> impl Responder {
    let cookies = extract_cookies(&http_req);
    let context = SyncOutContext::new(pubid, cookies);

    if let Err(e) = pipeline.run(&context).await {
        // this isnt an error in and of its self, so just log reason
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

/// Partners syncing in to us their buyeruid value
async fn sync_in_handler(
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

async fn sync_debug_handler(
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

/// Allowed origins for the sync debug endpoint
const DEBUG_ALLOWED_ORIGINS: &[&str] = &["http://localhost:3000", "https://app.neuronicads.com"];

fn apply_debug_cors(req: &HttpRequest, response: &mut actix_web::HttpResponseBuilder) {
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

async fn sync_debug_preflight(http_req: HttpRequest) -> impl Responder {
    let mut response = HttpResponse::NoContent();
    apply_debug_cors(&http_req, &mut response);
    response.insert_header(("Access-Control-Allow-Methods", "GET, OPTIONS"));
    response.insert_header(("Access-Control-Max-Age", "86400"));
    response.finish()
}

#[async_trait]
impl AsyncTask<StartupContext, anyhow::Error> for StartServerTask {
    #[instrument(skip_all, name = "start_server_task")]
    async fn run(&self, ctx: &StartupContext) -> Result<(), Error> {
        let config = match ctx.config.get() {
            Some(config) => config,
            None => bail!("Config missing during start server task"),
        };

        let _ = COOKIE_DOMAIN.set(config.cookie_domain.clone());

        let server_cfg = ServerConfig {
            http_port: Some(80),
            ssl_port: None,
            tls: config.ssl.clone(),
            tcp_backlog: None,
            max_conns: None,
            threads: None,
            tls_rate_per_worker: None,
        };

        let rtb_pipeline = ctx
            .auction_pipeline
            .get()
            .ok_or(anyhow::anyhow!("RTB pipeline not built"))?
            .clone();

        let sync_out_pipeline = ctx
            .sync_out_pipeline
            .get()
            .ok_or(anyhow::anyhow!("Sync-out pipeline not built"))?
            .clone();

        let sync_in_pipeline = ctx
            .sync_in_pipeline
            .get()
            .ok_or(anyhow::anyhow!("Sync-in pipeline not built"))?
            .clone();

        let span_sample_rate = ctx
            .config
            .get()
            .ok_or(anyhow::anyhow!("Server pipeline context missing config!"))?
            .logging
            .span_sample_rate;

        let billing_event_path = config.notifications.billing_path.clone();
        if billing_event_path.is_empty() {
            bail!("Billing event path cannot be empty");
        }

        let billing_event_pipeline = ctx
            .event_pipeline
            .get()
            .ok_or(anyhow::anyhow!("Event pipeline not built"))?
            .clone();

        let bidder_manager = ctx
            .bidder_manager
            .get()
            .ok_or(anyhow::anyhow!("Bidder manager not built"))?
            .clone();

        let pub_manager = ctx
            .pub_manager
            .get()
            .ok_or(anyhow::anyhow!("Publisher manager not built"))?
            .clone();

        let sync_store = ctx
            .sync_store
            .get()
            .ok_or(anyhow::anyhow!("Sync store not built"))?
            .clone();

        let server = Server::listen(server_cfg, move |app| {
            app.route("/hi", web::get().to(|| async { "hi!" }))
                    .route(
                        billing_event_path.as_str(),
                        web::get().to({
                            let pipeline = billing_event_pipeline.clone();
                            move |http_req: HttpRequest| {
                                let p = pipeline.clone();

                                async move { billing_event_handler(http_req, p).await }
                            }
                        }),
                    )
                    .route(
                        "/br/json/{pubid}",
                        web::post().to({
                            let pipeline = rtb_pipeline.clone();
                            move |pubid: web::Path<String>,
                                  req: FastJson<BidRequest>,
                                  http_req: HttpRequest| {
                                let auction_id = req.id.clone();
                                let pubid = pubid.into_inner();
                                let p = pipeline.clone();

                                async move {
                                    json_bid_handler(
                                        auction_id,
                                        pubid,
                                        req,
                                        http_req,
                                        p,
                                        span_sample_rate,
                                    )
                                    .await
                                }
                            }
                        }),
                    )
                    .route(
                        "/sync/out/{pubid}",
                        web::get().to({
                            let pipeline = sync_out_pipeline.clone();

                            move |pubid: web::Path<String>, http_req: HttpRequest| {
                                let pubid = pubid.into_inner();
                                let p = pipeline.clone();
                                async move { sync_out_handler(pubid, http_req, p).await }
                            }
                        }),
                    )
                    .route(
                        "/sync/in",
                        web::get().to({
                            let pipeline = sync_in_pipeline.clone();

                            move |http_req: HttpRequest| {
                                let p = pipeline.clone();
                                async move { sync_in_handler(http_req, p).await }
                            }
                        }),
                    )
                    .route(
                        "/sync/debug/{pubid}",
                        web::get().to({
                            let bm = bidder_manager.clone();
                            let pm = pub_manager.clone();
                            let ss = sync_store.clone();

                            move |pubid: web::Path<String>, http_req: HttpRequest| {
                                let bm = bm.clone();
                                let pm = pm.clone();
                                let ss = ss.clone();
                                let pubid = pubid.into_inner();
                                async move { sync_debug_handler(pubid, http_req, bm, pm, ss).await }
                            }
                        }),
                    )
                    .route(
                        "/sync/debug/{pubid}",
                        web::method(actix_web::http::Method::OPTIONS).to({
                            move |http_req: HttpRequest| async move {
                                sync_debug_preflight(http_req).await
                            }
                        }),
                    );
        })
        .await?;

        // TODO prebid handler should define its own pipeline to record metrics
        // and extract pubid context and adapted bidrequest, then should pass
        // through existing re-useable pipeline stuff

        ctx.server
            .set(server)
            .map_err(|_| anyhow!("Could not set server"))?;

        info!("Started http server, ready for requests");

        Ok(())
    }
}
