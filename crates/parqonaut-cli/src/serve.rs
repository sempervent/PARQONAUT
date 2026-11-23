//! `prqnt serve` — PARQONAUT HTTP application server.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;

use camino::Utf8PathBuf;
use parqonaut_app::{ServerPluginPolicy, ServerPluginState, StoragePolicy};
use parqonaut_service::{build_router_with_workers, serve_with_service, ParqonautService};
use parqonaut_store::{PostgresConnectConfig, StoreBackend};
use tracing::info;

#[derive(Debug, clap::Parser)]
pub struct ServeArgs {
    /// Listen address (default loopback-only).
    #[arg(long, default_value = "127.0.0.1:8080")]
    pub listen: String,
    /// SQLite or Postgres database URL.
    #[arg(long, env = "PARQONAUT_DATABASE_URL")]
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
    #[serde(default)]
    plugins: PluginsSection,
}

#[derive(Debug, serde::Deserialize, Default)]
struct PluginsSection {
    #[serde(default)]
    enabled: bool,
    #[serde(default)]
    roots: Vec<Utf8PathBuf>,
    #[serde(default)]
    allowed: Vec<String>,
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
        let doc = parqonaut_service::openapi_spec();
        println!("{}", serde_json::to_string_pretty(&doc)?);
        return Ok(());
    }

    let file_cfg = load_config(args.config.as_deref())?;
    let state_dir = resolve_state_dir(args.state_dir.as_deref())?;
    std::fs::create_dir_all(&state_dir)?;

    let db_target = resolve_database_target(args.database.as_deref(), &state_dir)?;
    let database_backend = db_target.backend_label();

    let policy = storage_policy_from_config(&file_cfg);
    info!(
        state_dir = %state_dir.display(),
        workers = args.workers,
        database_backend,
        "starting PARQONAUT server"
    );

    let store = match &db_target {
        DatabaseTarget::Url { url, pg_password } => {
            StoreBackend::connect_with_password_override(url, pg_password.as_deref()).await?
        }
        DatabaseTarget::PostgresParts(cfg) => StoreBackend::connect_postgres_parts(cfg).await?,
    };
    store.bootstrap_auth_from_env().await?;

    let listen: SocketAddr = args.listen.parse().map_err(|e| format!("invalid --listen: {e}"))?;
    if listen.ip() == IpAddr::V4(Ipv4Addr::UNSPECIFIED) || !listen.ip().is_loopback() {
        eprintln!(
            "warning: listening on {listen}; bearer tokens over plaintext HTTP are not safe on untrusted networks"
        );
    }

    let plugin_policy = plugin_policy_from_config(&file_cfg);
    let server_plugins = ServerPluginState::bootstrap(plugin_policy)
        .map_err(|e| format!("server plugin configuration: {e}"))?;
    let service = ParqonautService::with_storage_policy_and_plugins(store, policy, server_plugins);
    let router = build_router_with_workers(service.clone(), args.workers);
    serve_with_service(router, listen, service).await?;
    Ok(())
}

fn load_config(
    path: Option<&std::path::Path>,
) -> Result<ServerFileConfig, Box<dyn std::error::Error>> {
    let Some(path) = path else {
        return Ok(ServerFileConfig::default());
    };
    let text = std::fs::read_to_string(path)?;
    Ok(toml::from_str(&text)?)
}

fn plugin_policy_from_config(cfg: &ServerFileConfig) -> ServerPluginPolicy {
    let mut policy = ServerPluginPolicy::from_env();
    if cfg.plugins.enabled {
        policy.enabled = true;
    }
    if !cfg.plugins.roots.is_empty() {
        policy.roots = cfg.plugins.roots.iter().map(|p| p.as_std_path().to_path_buf()).collect();
    }
    if !cfg.plugins.allowed.is_empty() {
        policy.allowed = cfg.plugins.allowed.iter().cloned().collect();
    }
    policy
}

fn storage_policy_from_config(cfg: &ServerFileConfig) -> StoragePolicy {
    let mut roots = cfg.server.allowed_local_roots.clone();
    roots.extend(cfg.storage.allowed_local_roots.clone());
    if let Ok(env_roots) = std::env::var("PARQONAUT_ALLOWED_LOCAL_ROOTS") {
        let parsed: Vec<Utf8PathBuf> = env_roots
            .split([':', ','])
            .map(str::trim)
            .filter(|p| !p.is_empty())
            .map(Utf8PathBuf::from)
            .collect();
        if !parsed.is_empty() {
            roots = parsed;
        }
    }

    let mut s3_buckets = cfg.storage.allowed_s3_buckets.clone();
    if let Ok(env_buckets) = std::env::var("PARQONAUT_ALLOWED_S3_BUCKETS") {
        let parsed: Vec<String> = env_buckets
            .split(',')
            .map(str::trim)
            .filter(|p| !p.is_empty())
            .map(str::to_string)
            .collect();
        if !parsed.is_empty() {
            s3_buckets = parsed;
        }
    }

    let mut s3_prefixes = cfg.storage.allowed_s3_prefixes.clone();
    if let Ok(env_prefixes) = std::env::var("PARQONAUT_ALLOWED_S3_PREFIXES") {
        let parsed: Vec<String> = env_prefixes
            .split(',')
            .map(str::trim)
            .filter(|p| !p.is_empty())
            .map(str::to_string)
            .collect();
        if !parsed.is_empty() {
            s3_prefixes = parsed;
        }
    }

    StoragePolicy {
        allow_unrestricted_local: false,
        allowed_local_roots: roots,
        allowed_s3_buckets: s3_buckets,
        allowed_s3_prefixes: s3_prefixes,
    }
}

fn resolve_state_dir(
    explicit: Option<&std::path::Path>,
) -> Result<PathBuf, Box<dyn std::error::Error>> {
    if let Some(p) = explicit {
        return Ok(p.to_path_buf());
    }
    if let Ok(p) = std::env::var("PRQNT_STATE_DIR") {
        return Ok(PathBuf::from(p));
    }
    let base = dirs::data_local_dir().ok_or("cannot resolve local data directory")?;
    Ok(base.join("prqnt"))
}

fn non_empty_env(name: &str) -> Option<String> {
    std::env::var(name).ok().map(|s| s.trim().to_string()).filter(|s| !s.is_empty())
}

fn non_empty_password_env(name: &str) -> Option<String> {
    // Preserve leading/trailing whitespace if an operator intentionally included it in a password.
    std::env::var(name).ok().filter(|s| !s.is_empty())
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum DatabaseTarget {
    Url { url: String, pg_password: Option<String> },
    PostgresParts(PostgresConnectConfig),
}

impl DatabaseTarget {
    fn backend_label(&self) -> &'static str {
        match self {
            DatabaseTarget::Url { url, .. }
                if url.starts_with("postgres://") || url.starts_with("postgresql://") =>
            {
                "postgres"
            }
            DatabaseTarget::Url { .. } => "sqlite",
            DatabaseTarget::PostgresParts(_) => "postgres",
        }
    }
}

fn resolve_database_target(
    explicit_database: Option<&str>,
    state_dir: &std::path::Path,
) -> Result<DatabaseTarget, Box<dyn std::error::Error>> {
    let pg_pw = non_empty_password_env("PARQONAUT_PG_PASSWORD");
    if let Some(url) = explicit_database.map(str::trim).filter(|s| !s.is_empty()) {
        return Ok(DatabaseTarget::Url { url: url.to_string(), pg_password: pg_pw });
    }

    let pg_host = non_empty_env("PARQONAUT_PG_HOST");
    let pg_port_raw = non_empty_env("PARQONAUT_PG_PORT");
    let pg_user = non_empty_env("PARQONAUT_PG_USER");
    let pg_db = non_empty_env("PARQONAUT_PG_DB");

    let pg_configured = pg_host.is_some()
        || pg_port_raw.is_some()
        || pg_user.is_some()
        || pg_pw.is_some()
        || pg_db.is_some();

    if pg_configured {
        let password = pg_pw.ok_or(
            "PARQONAUT_PG_PASSWORD must be set and non-empty when PostgreSQL is configured",
        )?;
        let port = match pg_port_raw {
            Some(raw) => {
                raw.parse::<u16>().map_err(|e| format!("invalid PARQONAUT_PG_PORT '{raw}': {e}"))?
            }
            None => 5432,
        };
        return Ok(DatabaseTarget::PostgresParts(PostgresConnectConfig {
            host: pg_host.unwrap_or_else(|| "localhost".to_string()),
            port,
            username: pg_user.unwrap_or_else(|| "parqonaut".to_string()),
            password,
            database: pg_db.unwrap_or_else(|| "parqonaut".to_string()),
        }));
    }

    Ok(DatabaseTarget::Url { url: default_sqlite_url(state_dir), pg_password: None })
}

fn default_sqlite_url(state_dir: &std::path::Path) -> String {
    let db_dir = state_dir.join("database");
    let _ = std::fs::create_dir_all(&db_dir);
    let path = db_dir.join("parqonaut.sqlite");
    format!("sqlite://{}?mode=rwc", path.display())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn storage_policy_reads_env_and_overrides_toml() {
        let _guard = ENV_LOCK.lock().unwrap();
        let cfg = ServerFileConfig {
            server: ServerSection { allowed_local_roots: vec![Utf8PathBuf::from("/toml/root")] },
            storage: StorageSection {
                allowed_local_roots: vec![],
                allowed_s3_buckets: vec!["toml-bucket".into()],
                allowed_s3_prefixes: vec!["toml-bucket/prefix".into()],
            },
            plugins: PluginsSection::default(),
        };

        std::env::set_var("PARQONAUT_ALLOWED_LOCAL_ROOTS", "/var/lib/parqonaut/data:/tmp/extra");
        std::env::set_var("PARQONAUT_ALLOWED_S3_BUCKETS", "b1, b2");
        std::env::set_var("PARQONAUT_ALLOWED_S3_PREFIXES", "b1/sub/");

        let policy = storage_policy_from_config(&cfg);
        std::env::remove_var("PARQONAUT_ALLOWED_LOCAL_ROOTS");
        std::env::remove_var("PARQONAUT_ALLOWED_S3_BUCKETS");
        std::env::remove_var("PARQONAUT_ALLOWED_S3_PREFIXES");

        assert!(!policy.allow_unrestricted_local);
        assert_eq!(
            policy.allowed_local_roots,
            vec![Utf8PathBuf::from("/var/lib/parqonaut/data"), Utf8PathBuf::from("/tmp/extra")]
        );
        assert_eq!(policy.allowed_s3_buckets, vec!["b1", "b2"]);
        assert_eq!(policy.allowed_s3_prefixes, vec!["b1/sub/"]);
    }

    fn clear_pg_env() {
        for k in [
            "PARQONAUT_PG_HOST",
            "PARQONAUT_PG_PORT",
            "PARQONAUT_PG_USER",
            "PARQONAUT_PG_PASSWORD",
            "PARQONAUT_PG_DB",
        ] {
            std::env::remove_var(k);
        }
    }

    #[test]
    fn resolve_database_target_handles_special_char_pg_password_without_sqlite_fallback() {
        let _guard = ENV_LOCK.lock().unwrap();
        clear_pg_env();
        let dir = tempfile::tempdir().unwrap();

        let special_pw = "p@ss:w/o#rd?100%abc";
        std::env::set_var("PARQONAUT_PG_HOST", "postgres");
        std::env::set_var("PARQONAUT_PG_PORT", "5432");
        std::env::set_var("PARQONAUT_PG_USER", "parqonaut");
        std::env::set_var("PARQONAUT_PG_PASSWORD", special_pw);
        std::env::set_var("PARQONAUT_PG_DB", "parqonaut");

        let target = resolve_database_target(None, dir.path()).unwrap();
        assert_eq!(target.backend_label(), "postgres");
        assert_eq!(
            target,
            DatabaseTarget::PostgresParts(PostgresConnectConfig {
                host: "postgres".into(),
                port: 5432,
                username: "parqonaut".into(),
                password: special_pw.into(),
                database: "parqonaut".into(),
            })
        );

        // Missing PARQONAUT_PG_PASSWORD when PARQONAUT_PG_HOST is set must error (never fall back to SQLite).
        std::env::remove_var("PARQONAUT_PG_PASSWORD");
        let err = resolve_database_target(None, dir.path()).unwrap_err();
        assert!(err.to_string().contains("PARQONAUT_PG_PASSWORD"));

        // Simple legacy URL remains compatible.
        clear_pg_env();
        let legacy =
            resolve_database_target(Some("postgres://u:simple@db:5432/app"), dir.path()).unwrap();
        assert_eq!(legacy.backend_label(), "postgres");

        // Passwordless URL + PARQONAUT_PG_PASSWORD supplies unencoded password cleanly.
        std::env::set_var("PARQONAUT_PG_PASSWORD", special_pw);
        let override_target =
            resolve_database_target(Some("postgres://u@db:5432/app"), dir.path()).unwrap();
        assert_eq!(
            override_target,
            DatabaseTarget::Url {
                url: "postgres://u@db:5432/app".into(),
                pg_password: Some(special_pw.into()),
            }
        );

        clear_pg_env();
    }
}
