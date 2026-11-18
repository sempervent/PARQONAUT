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

const TEST_ALLOWLIST: &[&str] = &["PARQONAUT_TEST_SLOW_SCAN_SEC"];

/// Builds a scrubbed environment for plugin subprocesses.
pub fn plugin_child_env(extra_pythonpath: &str) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    for key in ALLOWLIST {
        if let Ok(v) = env::var(key) {
            out.insert(key.to_string(), v);
        }
    }
    for key in TEST_ALLOWLIST {
        if let Ok(v) = env::var(key) {
            out.insert((*key).to_string(), v);
        }
    }
    for deny in SECRET_DENYLIST {
        out.remove(*deny);
    }
    out.insert("PYTHONPATH".into(), extra_pythonpath.into());
    out.insert("PYTHONDONTWRITEBYTECODE".into(), "1".into());
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn env_scrubbing_blocks_pg_url_pythonstartup_and_secrets() {
        env::set_var("PARQONAUT_TEST_PG_URL", "postgres://postgres:secret@db:5432/app");
        env::set_var("PYTHONSTARTUP", "/tmp/evil_startup.py");
        env::set_var("AWS_SECRET_ACCESS_KEY", "aws-secret");
        env::set_var("PARQONAUT_TEST_SLOW_SCAN_SEC", "2");

        let child_env = plugin_child_env("/tmp/plugins");
        assert!(!child_env.contains_key("PARQONAUT_TEST_PG_URL"));
        assert!(!child_env.contains_key("PYTHONSTARTUP"));
        assert!(!child_env.contains_key("AWS_SECRET_ACCESS_KEY"));
        assert_eq!(child_env.get("PARQONAUT_TEST_SLOW_SCAN_SEC").map(String::as_str), Some("2"));
        assert_eq!(child_env.get("PYTHONPATH").map(String::as_str), Some("/tmp/plugins"));
        assert_eq!(child_env.get("PYTHONDONTWRITEBYTECODE").map(String::as_str), Some("1"));
    }
}
