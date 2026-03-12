use crate::app::handlers::adtag::{adtag_handler, adtag_preflight};
use crate::app::handlers::billing::billing_event_handler;
use crate::app::handlers::creative_serving::raw_creative_handler;
use crate::app::handlers::profile::profile_handler;
use crate::app::handlers::rtb::json_bid_handler;
use crate::app::handlers::sync::{
    sync_debug_handler, sync_debug_preflight, sync_in_handler, sync_out_handler,
};
use crate::app::http::COOKIE_DOMAIN;
use crate::app::lifecycle::context::StartupContext;
use crate::app::pipeline::adtag::request::AdTagRequest;
use actix_web::HttpRequest;
use actix_web::web;
use anyhow::{Error, anyhow, bail};
use async_trait::async_trait;
use pipeline::AsyncTask;
use rtb::BidRequest;
use rtb::server::json::FastJson;
use rtb::server::{Server, ServerConfig};
use tracing::{info, instrument};

pub struct StartServerTask;

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
            .ok_or(anyhow!("RTB pipeline not built"))?
            .clone();

        let sync_out_pipeline = ctx
            .sync_out_pipeline
            .get()
            .ok_or(anyhow!("Sync-out pipeline not built"))?
            .clone();

        let sync_in_pipeline = ctx
            .sync_in_pipeline
            .get()
            .ok_or(anyhow!("Sync-in pipeline not built"))?
            .clone();

        let span_sample_rate = config.logging.span_sample_rate;

        let billing_event_path = config.notifications.billing_path.clone();
        if billing_event_path.is_empty() {
            bail!("Billing event path cannot be empty");
        }

        let billing_event_pipeline = ctx
            .event_pipeline
            .get()
            .ok_or(anyhow!("Event pipeline not built"))?
            .clone();

        let bidder_manager = ctx
            .bidder_manager
            .get()
            .ok_or(anyhow!("Bidder manager not built"))?
            .clone();

        let pub_manager = ctx
            .pub_manager
            .get()
            .ok_or(anyhow!("Publisher manager not built"))?
            .clone();

        let sync_store = ctx
            .sync_store
            .get()
            .ok_or(anyhow!("Sync store not built"))?
            .clone();

        let adtag_pipeline = ctx
            .adtag_pipeline
            .get()
            .ok_or(anyhow!("Adtag pipeline not built"))?
            .clone();

        let raw_creative_pipeline = ctx.raw_creative_pipeline.get().cloned();

        let server = Server::listen(server_cfg, move |app| {
            app.route("/hi", web::get().to(|| async { "hi!" }))
                    .route(
                        "/publisher/adtag",
                        web::post().to({
                            let pipeline = adtag_pipeline.clone();
                            move |req: web::Json<AdTagRequest>, http_req: HttpRequest| {
                                let p = pipeline.clone();
                                async move { adtag_handler(req, http_req, p, span_sample_rate).await }
                            }
                        }),
                    )
                    .route(
                        "/publisher/adtag",
                        web::method(actix_web::http::Method::OPTIONS).to(adtag_preflight),
                    )
                    .route("/profile", web::get().to(profile_handler))
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
                            let pm = pub_manager.clone();
                            move |pubid: web::Path<String>,
                                  req: FastJson<BidRequest>,
                                  http_req: HttpRequest| {
                                let auction_id = req.id.clone();
                                let pubid = pubid.into_inner();
                                let p = pipeline.clone();
                                let pm = pm.clone();
                                async move {
                                    json_bid_handler(
                                        auction_id,
                                        pubid,
                                        req,
                                        http_req,
                                        p,
                                        pm,
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
                    )
                    .route(
                        "/adserving/raw/{crid}",
                        web::get().to({
                            let pipeline = raw_creative_pipeline.clone();
                            move |crid: web::Path<String>| {
                                let p = pipeline.clone();
                                let crid = crid.into_inner();
                                async move { raw_creative_handler(crid, p, span_sample_rate).await }
                            }
                        }),
                    );
        })
        .await?;

        ctx.server
            .set(server)
            .map_err(|_| anyhow!("Could not set server"))?;

        info!("Started http server, ready for requests");

        Ok(())
    }
}
