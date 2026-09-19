//! `prqnt serve` — PARQONAUT HTTP application server.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;

use camino::Utf8PathBuf;
use paraclete_service::{build_router_with_workers, serve, ParacleteService};
use paraclete_store::StoreBackend;
use parqonaut_app::StoragePolicy;
use tracing::info;

#[derive(Debug, clap::Parser)]
pub struct ServeArgs {
    /// Listen address (default loopback-only).
    #[arg(long, default_value = "127.0.0.1:8080")]
    pub listen: String,
    /// SQLite or Postgres database URL.
    #[arg(long)]
    pub database: Option<String>,
    /// Application state directory (database + artifacts).
    #[arg(long)]
    pub state_dir: Option<PathBuf>,
    /// In-process async job workers.
    #[arg(long, default_value_t = 2)]
    pub workers: usize,
    /// Optional TOML config (server + storage policy).
    #[arg(long)]
    pub config: Option<PathBuf>,
    /// Emit OpenAPI JSON to stdout and exit.
    #[arg(long)]
    pub print_openapi: bool,
}

#[derive(Debug, serde::Deserialize, Default)]
struct ServerFileConfig {
    #[serde(default)]
    server: ServerSection,
    #[serde(default)]
    storage: StorageSection,
}

#[derive(Debug, serde::Deserialize, Default)]
struct ServerSection {
    #[serde(default)]
    allowed_local_roots: Vec<Utf8PathBuf>,
}

#[derive(Debug, serde::Deserialize, Default)]
struct StorageSection {
    #[serde(default)]
    allowed_local_roots: Vec<Utf8PathBuf>,
    #[serde(default)]
    allowed_s3_buckets: Vec<String>,
    #[serde(default)]
    allowed_s3_prefixes: Vec<String>,
}

pub async fn run(args: ServeArgs) -> Result<(), Box<dyn std::error::Error>> {
    if args.print_openapi {
        let doc = paraclete_service::openapi_spec();
        println!("{}", serde_json::to_string_pretty(&doc)?);
        return Ok(());
    }

    let file_cfg = load_config(args.config.as_ref())?;
    let state_dir = resolve_state_dir(args.state_dir.as_ref())?;
    std::fs::create_dir_all(&state_dir)?;

    let db_url = args.database.clone().unwrap_or_else(|| default_sqlite_url(&state_dir));

    let policy = storage_policy_from_config(&file_cfg);
    info!(state_dir = %state_dir.display(), workers = args.workers, "starting PARQONAUT server");

    let store = StoreBackend::connect(&db_url).await?;
    store.bootstrap_auth_from_env().await?;

    let listen: SocketAddr = args.listen.parse().map_err(|e| format!("invalid --listen: {e}"))?;
    if listen.ip() == IpAddr::V4(Ipv4Addr::UNSPECIFIED) || !listen.ip().is_loopback() {
        eprintln!(
            "warning: listening on {listen}; bearer tokens over plaintext HTTP are not safe on untrusted networks"
        );
    }

    let service = ParacleteService::with_storage_policy(store, policy);
    let router = build_router_with_workers(service, args.workers);
    serve(router, listen).await?;
    Ok(())
}

fn load_config(path: Option<&PathBuf>) -> Result<ServerFileConfig, Box<dyn std::error::Error>> {
    let Some(path) = path else {
        return Ok(ServerFileConfig::default());
    };
    let text = std::fs::read_to_string(path)?;
    Ok(toml::from_str(&text)?)
}

fn storage_policy_from_config(cfg: &ServerFileConfig) -> StoragePolicy {
    let mut roots = cfg.server.allowed_local_roots.clone();
    roots.extend(cfg.storage.allowed_local_roots.clone());
    StoragePolicy {
        allow_unrestricted_local: false,
        allowed_local_roots: roots,
        allowed_s3_buckets: cfg.storage.allowed_s3_buckets.clone(),
        allowed_s3_prefixes: cfg.storage.allowed_s3_prefixes.clone(),
    }
}

fn resolve_state_dir(explicit: Option<&PathBuf>) -> Result<PathBuf, Box<dyn std::error::Error>> {
    if let Some(p) = explicit {
        return Ok(p.clone());
    }
    if let Ok(p) = std::env::var("PRQNT_STATE_DIR") {
        return Ok(PathBuf::from(p));
    }
    let base = dirs::data_local_dir().ok_or("cannot resolve local data directory")?;
    Ok(base.join("prqnt"))
}

fn default_sqlite_url(state_dir: &PathBuf) -> String {
    let db_dir = state_dir.join("database");
    let _ = std::fs::create_dir_all(&db_dir);
    let path = db_dir.join("parqonaut.sqlite");
    format!("sqlite://{}", path.display())
}
