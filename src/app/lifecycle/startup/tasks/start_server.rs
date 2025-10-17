use crate::app::lifecycle::context::StartupContext;
use crate::app::pipeline::ortb::AuctionContext;
use actix_web::web::Json;
use actix_web::web;
use anyhow::{anyhow, Error};
use async_trait::async_trait;
use pipeline::{AsyncTask, Pipeline};
use rtb::common::bidresponsestate::BidResponseState;
use rtb::server::json::JsonBidResponseState;
use rtb::server::{Server, ServerConfig};
use rtb::BidRequest;
use std::sync::Arc;

pub struct StartServerTask {
    pipeline: Arc<Pipeline<AuctionContext, anyhow::Error>>
}

impl StartServerTask {
    pub fn new(pipeline: Pipeline<AuctionContext, anyhow::Error>) -> Self {
        Self { pipeline: Arc::new(pipeline) }
    }
}

async fn json_bid_handler(
    req: Json<BidRequest>,
    pipeline: web::Data<Arc<Pipeline<AuctionContext, anyhow::Error>>>
) -> JsonBidResponseState {
    let mut ctx = AuctionContext::new(req.into_inner());

    let pipeline_result = pipeline.run(&ctx).await;

    println!("pipeline result success? {:?}", pipeline_result.is_ok());

    let brs = match ctx.res.take() {
        Some (brs) => brs,
        None => {
            BidResponseState::NoBid {
                desc: if pipeline_result.is_err() { "Failed processing req".into() } else {
                    "No Bid".into()
                }
            }
        }
    };

    JsonBidResponseState(brs)
}

#[async_trait]
impl AsyncTask<StartupContext, anyhow::Error> for StartServerTask {
    async fn run(&self, ctx: &StartupContext) -> Result<(), Error> {
        let cfg = ServerConfig {
            http_port: Some(80),
            ssl_port: None,
            tls: None,
            tcp_backlog: None,
            max_conns: None,
            threads: None,
            tls_rate_per_worker: None,
        };

        // Clone the Arc to share across all request handlers
        let pipeline = Arc::clone(&self.pipeline);

        let server = Server::listen(cfg, move |app| {
            app
                .app_data(web::Data::new(pipeline.clone()))
                .route("/hi", web::get().to(|| async { "hi!" }))
                .route("/br", web::post().to(json_bid_handler));
        }).await?;

        ctx.server.set(server)
            .map_err(|_| anyhow!("Could not set server"))?;

        println!("Started http server on port 80");

        Ok(())
    }
}