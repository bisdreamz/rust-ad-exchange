mod app;
mod core;

use crate::app::context::StartupContext;
use crate::app::shutdown::build_shutdown_pipeline;
use crate::app::startup::build_start_pipeline;
use actix_web::rt::signal;
use mimalloc::MiMalloc;
use rtb::actix_web;
use std::sync::OnceLock;
use tracing::info;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[actix_web::main]
async fn main() {
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");

    let startup_pipeline = build_start_pipeline("rex.yaml".into());
    let startup_ctx = StartupContext {
        server: OnceLock::new(),
        ..Default::default()
    };

    match startup_pipeline.run(&startup_ctx).await {
        Ok(_) => info!("Startup successful"),
        Err(e) => panic!("Startup failed: {:?}", e),
    }

    let shutdown_pipeline = build_shutdown_pipeline();

    signal::ctrl_c().await.expect("Failed to listen for sigint");

    match shutdown_pipeline.run(&startup_ctx).await {
        Ok(_) => info!("Shutdown successful"),
        Err(e) => panic!("Clean shutdown failed {:?}", e),
    }
}
