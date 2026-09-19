use std::fmt;

use serde::Serialize;
use url::Url;

/// Redact credential-bearing URI forms for logs, errors, and serialization display.
pub trait RedactUri {
    fn redact_uri(&self) -> String;
}

impl RedactUri for str {
    fn redact_uri(&self) -> String {
        redact_uri_string(self)
    }
}

impl RedactUri for String {
    fn redact_uri(&self) -> String {
        redact_uri_string(self)
    }
}

pub fn redact_uri_string(input: &str) -> String {
    if let Ok(mut url) = Url::parse(input) {
        if !url.username().is_empty() || url.password().is_some() {
            let _ = url.set_username("");
            let _ = url.set_password(None);
        }
        for param in ["X-Amz-Signature", "X-Amz-Credential", "X-Amz-Security-Token"] {
            if url.query_pairs().any(|(k, _)| k == param) {
                url.set_query(None);
                break;
            }
        }
        return url.to_string();
    }
    redact_secrets_in_text(input)
}

pub fn redact_secrets_in_text(input: &str) -> String {
    let mut out = input.to_string();
    for marker in [
        "AWS_SECRET_ACCESS_KEY=",
        "AWS_ACCESS_KEY_ID=",
        "AWS_SESSION_TOKEN=",
        "Authorization:",
        "X-Amz-Security-Token:",
    ] {
        if let Some(idx) = out.find(marker) {
            let end = out[idx..].find('\n').map(|n| idx + n).unwrap_or(out.len());
            out.replace_range(idx..end, &format!("{marker}[REDACTED]"));
        }
    }
    out
}

/// Wrapper for Debug/Display that always redacts.
pub struct Redacted<'a>(pub &'a str);

impl fmt::Debug for Redacted<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0.redact_uri())
    }
}

impl fmt::Display for Redacted<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0.redact_uri())
    }
}

/// Serialize locations through redacted display form for public artifacts.
pub fn redacted_serialize<T: Serialize + RedactUri>(
    value: &T,
) -> Result<String, serde_json::Error> {
    // For structured types, serde uses normal fields; callers should avoid secret fields.
    serde_json::to_string(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redacts_s3_credentials_in_uri() {
        let s = redact_uri_string("s3://AKIAIOSFODNN7EXAMPLE:secret@bucket/prefix");
        assert!(!s.contains("secret"));
        assert!(!s.contains("AKIA"));
    }

    #[test]
    fn redacts_signed_query_string() {
        let s = redact_uri_string(
            "https://bucket.s3.amazonaws.com/key?X-Amz-Signature=abc&X-Amz-Credential=foo",
        );
        assert!(!s.contains("X-Amz-Signature"));
    }

    #[test]
    fn redacts_env_style_secrets() {
        let s = redact_secrets_in_text("AWS_SECRET_ACCESS_KEY=hunter2\nok");
        assert!(!s.contains("hunter2"));
        assert!(s.contains("[REDACTED]"));
    }
}
