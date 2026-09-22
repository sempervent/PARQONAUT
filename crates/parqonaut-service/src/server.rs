//! HTTP server lifecycle (bind, graceful shutdown).

use std::net::SocketAddr;
use std::time::Duration;

use axum::Router;
use tokio::signal;
use tracing::info;

/// Binds `router` and serves until SIGINT/SIGTERM.
pub async fn serve(router: Router, listen: SocketAddr) -> Result<(), std::io::Error> {
    if !listen.ip().is_loopback() {
        eprintln!(
            "warning: PARQONAUT is binding to non-loopback address {listen}; use a reverse proxy for TLS on untrusted networks"
        );
    }
    info!(%listen, "PARQONAUT HTTP server listening");
    let listener = tokio::net::TcpListener::bind(listen).await?;
    axum::serve(listener, router).with_graceful_shutdown(shutdown_signal()).await
}

async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c().await.expect("failed to install Ctrl+C handler");
    };
    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
    info!("shutdown signal received; stopping HTTP listener");
    tokio::time::sleep(Duration::from_millis(200)).await;
}
