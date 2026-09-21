//! Deliberately minimal child environment (not a secrets sandbox for hostile code).

use std::collections::BTreeMap;
use std::env;

const SECRET_DENYLIST: &[&str] = &[
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
    "PRQNT_BOOTSTRAP_ADMIN_TOKEN",
    "GITHUB_TOKEN",
    "GH_TOKEN",
    "DATABASE_URL",
];

const ALLOWLIST: &[&str] = &[
    "PATH",
    "HOME",
    "USER",
    "LOGNAME",
    "SHELL",
    "LANG",
    "LC_ALL",
    "LC_CTYPE",
    "LC_MESSAGES",
    "TMPDIR",
    "TEMP",
    "TMP",
    "PYTHONHOME",
    "PYTHONUTF8",
    "PYTHONIOENCODING",
    "PYTHONDONTWRITEBYTECODE",
];

/// Builds a scrubbed environment for plugin subprocesses.
pub fn plugin_child_env(extra_pythonpath: &str) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    for key in ALLOWLIST {
        if let Ok(v) = env::var(key) {
            out.insert(key.to_string(), v);
        }
    }
    for (k, v) in env::vars() {
        if k.starts_with("PYTHON") && !out.contains_key(&k) {
            out.insert(k, v);
        }
    }
    for deny in SECRET_DENYLIST {
        out.remove(*deny);
    }
    out.insert("PYTHONPATH".into(), extra_pythonpath.into());
    out.insert("PYTHONDONTWRITEBYTECODE".into(), "1".into());
    out
}
