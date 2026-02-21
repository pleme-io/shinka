//! Shared utility functions used across Shinka modules.

/// Format a duration in seconds as a human-readable string.
///
/// Returns strings like `"30s"`, `"1m 30s"`, `"1h 2m"`.
pub fn format_duration(seconds: u64) -> String {
    if seconds < 60 {
        format!("{}s", seconds)
    } else if seconds < 3600 {
        let mins = seconds / 60;
        let secs = seconds % 60;
        if secs == 0 {
            format!("{}m", mins)
        } else {
            format!("{}m {}s", mins, secs)
        }
    } else {
        let hours = seconds / 3600;
        let mins = (seconds % 3600) / 60;
        format!("{}h {}m", hours, mins)
    }
}

/// Extract the tag (or digest) from a container image string.
///
/// Examples:
/// - `"registry/image:v1.2.3"` → `"v1.2.3"`
/// - `"image@sha256:abc123"` → `"sha256:abc123"`
/// - `"registry:5000/image"` → `"latest"`
pub fn extract_image_tag(image: &str) -> String {
    if let Some(at_pos) = image.rfind('@') {
        // Image uses digest: image@sha256:abc123
        image[at_pos + 1..].to_string()
    } else if let Some(colon_pos) = image.rfind(':') {
        // Image uses tag: image:tag
        // But need to handle port numbers like registry:5000/image:tag
        let after_colon = &image[colon_pos + 1..];
        if after_colon.contains('/') {
            // This colon was a port, no tag
            "latest".to_string()
        } else {
            after_colon.to_string()
        }
    } else {
        "latest".to_string()
    }
}

/// Truncate a string to `max_len` characters, appending `"..."` if truncated.
pub fn truncate_tag(tag: &str, max_len: usize) -> String {
    if tag.len() <= max_len {
        tag.to_string()
    } else {
        format!("{}...", &tag[..max_len - 3])
    }
}

/// Serde default helper that returns `true`.
pub fn default_true() -> bool {
    true
}

/// Serde default helper that returns `false`.
pub fn default_false() -> bool {
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_duration() {
        assert_eq!(format_duration(30), "30s");
        assert_eq!(format_duration(60), "1m");
        assert_eq!(format_duration(90), "1m 30s");
        assert_eq!(format_duration(3600), "1h 0m");
        assert_eq!(format_duration(3720), "1h 2m");
    }

    #[test]
    fn test_extract_image_tag() {
        assert_eq!(extract_image_tag("nginx:1.21"), "1.21");
        assert_eq!(extract_image_tag("registry/app:v1.2.3"), "v1.2.3");
        assert_eq!(
            extract_image_tag("image@sha256:abc123"),
            "sha256:abc123"
        );
        assert_eq!(extract_image_tag("registry:5000/image"), "latest");
        assert_eq!(
            extract_image_tag("registry:5000/image:v1"),
            "v1"
        );
        assert_eq!(extract_image_tag("nginx"), "latest");
    }

    #[test]
    fn test_truncate_tag() {
        assert_eq!(truncate_tag("short", 10), "short");
        assert_eq!(truncate_tag("sha256:abc123def456", 10), "sha256:...");
        assert_eq!(
            truncate_tag("v1.2.3-abc123def456ghi789", 20),
            "v1.2.3-abc123def4..."
        );
    }
}
