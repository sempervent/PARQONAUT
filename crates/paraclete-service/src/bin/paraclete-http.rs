//! Standalone HTTP server (bind `0.0.0.0:8080` unless `PARACLETE_HTTP_ADDR` is set).

use std::net::SocketAddr;

use paraclete_service::{build_router, ParacleteService};
use paraclete_store::StoreBackend;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt().with_env_filter(EnvFilter::from_default_env()).init();

    let db_url = std::env::var("PARACLETE_DATABASE_URL").map_err(|_| {
        "PARACLETE_DATABASE_URL must be a SQLite URL (e.g. sqlite:///tmp/paraclete.db) or postgres://…"
    })?;
    let store = StoreBackend::connect(&db_url).await?;
    store.bootstrap_auth_from_env().await.map_err(|e| format!("bootstrap auth token: {e}"))?;
    let service = ParacleteService::new(store);
    let app = build_router(service);

    let addr: SocketAddr =
        std::env::var("PARACLETE_HTTP_ADDR").unwrap_or_else(|_| "0.0.0.0:8080".into()).parse()?;

    tracing::info!(%addr, "listening");
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}
