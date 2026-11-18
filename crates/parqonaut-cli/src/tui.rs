//! `prqnt tui` — interactive terminal operator console.

use std::path::PathBuf;

use parqonaut_tui::{run_tui, TuiLaunchConfig};

#[derive(Debug, clap::Parser)]
pub struct TuiArgs {
    /// PARQONAUT HTTP server base URL (loopback default).
    #[arg(long, default_value = "http://127.0.0.1:8080")]
    pub url: String,
    /// Environment variable containing the bearer token.
    #[arg(long, default_value = "PARQONAUT_API_TOKEN")]
    pub token_env: String,
    /// File containing the bearer token (trimmed; avoids exposing secrets in process args).
    #[arg(long)]
    pub token_file: Option<PathBuf>,
}

pub async fn run(args: TuiArgs) -> Result<(), Box<dyn std::error::Error>> {
    let token = resolve_token(&args)?;
    run_tui(TuiLaunchConfig { server_url: args.url, token }).await
}

fn resolve_token(args: &TuiArgs) -> Result<Option<String>, Box<dyn std::error::Error>> {
    if let Some(path) = &args.token_file {
        let raw = std::fs::read_to_string(path)
            .map_err(|e| format!("failed to read --token-file {}: {e}", path.display()))?;
        let tok = raw.trim().to_string();
        if tok.is_empty() {
            return Err(format!("--token-file {} is empty", path.display()).into());
        }
        return Ok(Some(tok));
    }
    if let Ok(tok) = std::env::var(&args.token_env) {
        let t = tok.trim().to_string();
        if !t.is_empty() {
            return Ok(Some(t));
        }
    }
    if args.token_env == "PARQONAUT_API_TOKEN" {
        if let Ok(tok) = std::env::var("PRQNT_BOOTSTRAP_ADMIN_TOKEN") {
            let t = tok.trim().to_string();
            if !t.is_empty() {
                return Ok(Some(t));
            }
        }
    }
    Ok(None)
}
