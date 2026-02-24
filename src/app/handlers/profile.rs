use actix_web::HttpResponse;

pub async fn profile_handler() -> HttpResponse {
    let guard = match pprof::ProfilerGuardBuilder::default()
        .frequency(1000)
        .blocklist(&["libc", "libgcc", "pthread", "vdso"])
        .build()
    {
        Ok(g) => g,
        Err(e) => {
            return HttpResponse::InternalServerError()
                .body(format!("profiler start error: {}", e));
        }
    };

    tokio::time::sleep(std::time::Duration::from_secs(15)).await;

    match guard.report().build() {
        Ok(report) => {
            let mut svg = Vec::new();
            match report.flamegraph(&mut svg) {
                Ok(_) => HttpResponse::Ok().content_type("image/svg+xml").body(svg),
                Err(e) => {
                    HttpResponse::InternalServerError().body(format!("flamegraph error: {}", e))
                }
            }
        }
        Err(e) => HttpResponse::InternalServerError().body(format!("report error: {}", e)),
    }
}
