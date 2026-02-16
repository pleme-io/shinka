//! Secret redaction utilities for safe logging
//!
//! Provides functions to redact sensitive information before logging.
//! This prevents accidental exposure of database URLs, passwords, and other secrets.

use std::borrow::Cow;

/// Redact a connection string, keeping only the structure visible
///
/// Examples:
/// - `postgres://user:password@host:5432/db` -> `postgres://***:***@host:5432/db`
/// - `postgresql://admin:secret@db.example.com:5432/mydb?sslmode=require` ->
///   `postgresql://***:***@db.example.com:5432/mydb?sslmode=require`
pub fn redact_connection_string(url: &str) -> Cow<'_, str> {
    // Pattern: scheme://user:password@host:port/database
    if let Some(scheme_end) = url.find("://") {
        let after_scheme = &url[scheme_end + 3..];

        if let Some(at_pos) = after_scheme.find('@') {
            // Extract credentials part
            let credentials = &after_scheme[..at_pos];
            let rest = &after_scheme[at_pos..];

            // Check if there are credentials to redact
            if credentials.contains(':') || !credentials.is_empty() {
                let scheme = &url[..scheme_end + 3];
                return Cow::Owned(format!("{}***:***{}", scheme, rest));
            }
        }
    }

    Cow::Borrowed(url)
}

/// Redact all values in a key=value string (e.g., for env vars)
///
/// Examples:
/// - `DATABASE_URL=postgres://...` -> `DATABASE_URL=***`
/// - `password=secret123` -> `password=***`
pub fn redact_value(input: &str) -> Cow<'_, str> {
    if let Some(eq_pos) = input.find('=') {
        let key = &input[..eq_pos];
        Cow::Owned(format!("{}=***", key))
    } else {
        Cow::Borrowed(input)
    }
}

/// Check if a string looks like it might contain a secret
///
/// Returns true for:
/// - Connection strings (contains ://)
/// - Strings containing password-like substrings
/// - Bearer tokens
/// - API keys
pub fn looks_like_secret(s: &str) -> bool {
    let lower = s.to_lowercase();

    // Connection strings
    if s.contains("://") && s.contains('@') {
        return true;
    }

    // Common secret patterns
    let secret_patterns = [
        "password",
        "secret",
        "token",
        "api_key",
        "apikey",
        "auth",
        "bearer",
        "credential",
    ];

    for pattern in &secret_patterns {
        if lower.contains(pattern) {
            return true;
        }
    }

    // JWT-like patterns (three base64 segments)
    if s.chars().filter(|&c| c == '.').count() == 2
        && s.len() > 50
        && s.chars().all(|c| c.is_ascii_alphanumeric() || c == '.' || c == '_' || c == '-')
    {
        return true;
    }

    false
}

/// Redact a string if it looks like a secret
pub fn redact_if_secret(s: &str) -> Cow<'_, str> {
    if looks_like_secret(s) {
        if s.contains("://") {
            redact_connection_string(s)
        } else {
            Cow::Owned("***".to_string())
        }
    } else {
        Cow::Borrowed(s)
    }
}

/// A wrapper that implements Display with redaction
pub struct Redacted<'a>(pub &'a str);

impl std::fmt::Display for Redacted<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", redact_if_secret(self.0))
    }
}

/// A wrapper for connection strings that always redacts
pub struct RedactedUrl<'a>(pub &'a str);

impl std::fmt::Display for RedactedUrl<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", redact_connection_string(self.0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_redact_connection_string() {
        assert_eq!(
            redact_connection_string("postgres://user:password@host:5432/db"),
            "postgres://***:***@host:5432/db"
        );

        assert_eq!(
            redact_connection_string("postgresql://admin:secret@db.example.com:5432/mydb?sslmode=require"),
            "postgresql://***:***@db.example.com:5432/mydb?sslmode=require"
        );

        // No credentials
        assert_eq!(
            redact_connection_string("http://example.com/path"),
            "http://example.com/path"
        );
    }

    #[test]
    fn test_redact_value() {
        assert_eq!(redact_value("DATABASE_URL=postgres://..."), "DATABASE_URL=***");
        assert_eq!(redact_value("password=secret123"), "password=***");
        assert_eq!(redact_value("no_equals"), "no_equals");
    }

    #[test]
    fn test_looks_like_secret() {
        assert!(looks_like_secret("postgres://user:pass@host/db"));
        assert!(looks_like_secret("my_password=secret"));
        assert!(looks_like_secret("Bearer abc123"));
        assert!(looks_like_secret("api_key_here"));

        assert!(!looks_like_secret("normal text"));
        assert!(!looks_like_secret("deployment-name"));
    }

    #[test]
    fn test_redacted_display() {
        let url = "postgres://user:pass@host/db";
        let redacted = Redacted(url);
        assert_eq!(format!("{}", redacted), "postgres://***:***@host/db");

        let normal = "normal-text";
        let redacted = Redacted(normal);
        assert_eq!(format!("{}", redacted), "normal-text");
    }
}
