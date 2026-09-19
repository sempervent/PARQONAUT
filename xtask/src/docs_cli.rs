//! Generate `docs/reference/cli.md` from the real `prqnt` Clap help tree.

use std::path::Path;
use std::process::Command;

const OUTPUT: &str = "docs/reference/cli.md";

/// `(argv suffix after `prqnt`, section title)`
const SECTIONS: &[(&[&str], &str)] = &[
    (&[], "Global"),
    (&["scan"], "scan"),
    (&["inspect"], "inspect"),
    (&["rewrite"], "rewrite"),
    (&["convert"], "convert"),
    (&["diagnose"], "diagnose"),
    (&["plan"], "plan"),
    (&["plan-diff"], "plan-diff"),
    (&["check"], "check"),
    (&["repair"], "repair"),
    (&["verify"], "verify"),
    (&["doctor"], "doctor"),
    (&["batch"], "batch"),
    (&["batch", "check"], "batch check"),
    (&["batch", "plan"], "batch plan"),
    (&["batch", "repair"], "batch repair"),
    (&["batch", "status"], "batch status"),
    (&["batch", "resume"], "batch resume"),
    (&["batch", "verify"], "batch verify"),
    (&["serve"], "serve"),
];

fn prqnt_bin(repo_root: &Path) -> camino::Utf8PathBuf {
    let debug = repo_root.join("target/debug/prqnt");
    if debug.is_file() {
        return camino::Utf8PathBuf::from_path_buf(debug).expect("utf8 path");
    }
    repo_root.join("target/release/prqnt").try_into().expect("utf8")
}

fn run_help(prqnt: &Path, args: &[&str]) -> Result<String, Box<dyn std::error::Error>> {
    let mut cmd = Command::new(prqnt);
    for a in args {
        cmd.arg(a);
    }
    cmd.arg("--help");
    let out = cmd.output()?;
    if !out.status.success() {
        let stderr = String::from_utf8_lossy(&out.stderr);
        return Err(format!("prqnt --help {:?} failed: {stderr}", args).into());
    }
    Ok(String::from_utf8(out.stdout)?)
}

fn normalize_help(text: &str) -> String {
    let mut out = String::new();
    for line in text.lines() {
        if line.starts_with("prqnt ") && line.chars().any(|c| c.is_ascii_digit()) {
            out.push_str("prqnt <version>\n");
            continue;
        }
        out.push_str(line);
        out.push('\n');
    }
    out
}

pub fn run(write: bool, check: bool) -> Result<(), Box<dyn std::error::Error>> {
    let repo_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
    let prqnt = prqnt_bin(&repo_root);
    if !prqnt.as_std_path().is_file() {
        return Err(format!(
            "missing prqnt binary at {}; run `cargo build -p parqonaut-cli --bin prqnt` first",
            prqnt
        )
        .into());
    }

    let mut body = String::from(
        "# CLI reference (generated)\n\n\
         This page is generated from the live `prqnt` command tree. \
         Do not edit by hand — run `cargo xtask docs cli`.\n\n",
    );

    for (args, title) in SECTIONS {
        let help = run_help(prqnt.as_std_path(), args)?;
        let normalized = normalize_help(&help);
        body.push_str(&format!("## `{title}`\n\n"));
        body.push_str("```text\n");
        body.push_str(&normalized);
        if !normalized.ends_with('\n') {
            body.push('\n');
        }
        body.push_str("```\n\n");
    }

    let out_path = repo_root.join(OUTPUT);
    if check {
        let existing = std::fs::read_to_string(&out_path).unwrap_or_default();
        if existing != body {
            return Err(format!(
                "CLI reference drift — run `cargo xtask docs cli` (expected {})",
                out_path.display()
            )
            .into());
        }
        eprintln!("CLI reference OK");
        return Ok(());
    }

    if write {
        if let Some(parent) = out_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&out_path, &body)?;
        eprintln!("wrote {}", out_path.display());
    }
    Ok(())
}
