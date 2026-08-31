//! RFC 9728 protected-resource metadata -- how an MCP client discovers which
//! authorization server to go to. We are never in the token exchange; this
//! document is the entire handshake we participate in.
//!
//! PROTOCOL-NEUTRAL (invariant 5): this produces a serializable struct and a
//! header *value* as a `String`. Serving the document and attaching the header
//! to a 401 belongs to each inbound adapter.
use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct ProtectedResourceMetadata {
    pub resource: String,
    /// Mirrors the configured issuer allowlist. An array per RFC 9728, because
    /// a resource may trust more than one authorization server -- a workforce
    /// IdP for operators and a workload IdP for agents are both legitimate, and
    /// publishing only the first would make the second undiscoverable even
    /// though the verifier accepts it.
    pub authorization_servers: Vec<String>,
    pub bearer_methods_supported: Vec<String>,
}

impl ProtectedResourceMetadata {
    /// `authorization_servers` should be the verifier's NORMALIZED issuer list
    /// (`Verifier::config()`), so the document advertises exactly the strings
    /// that are actually enforced.
    pub fn new(resource: &str, authorization_servers: &[String]) -> Self {
        Self {
            resource: resource.to_string(),
            authorization_servers: authorization_servers.to_vec(),
            // The only method we accept: `Authorization: Bearer`. Tokens in
            // query strings land in access logs and `Referer` headers, and a
            // form body is not something any of our surfaces read.
            bearer_methods_supported: vec!["header".to_string()],
        }
    }
}

/// The `WWW-Authenticate` value returned with a 401 so a client can find the
/// metadata document without knowing anything about us in advance.
///
/// The URL is interpolated into an RFC 9110 quoted-string, so it is
/// percent-encoded first (see `encode_quoted`) rather than trusted. In practice
/// `metadata_url` is built by the node from its own advertise address, so a
/// hostile value would already mean a compromised config -- this is defence in
/// depth, and it is cheap.
pub fn challenge_header(metadata_url: &str) -> String {
    format!(
        r#"Bearer resource_metadata="{}""#,
        encode_quoted(metadata_url)
    )
}

/// Percent-encode everything that must not appear raw inside a quoted-string
/// header value.
///
/// Three classes are unsafe, and each one is a real failure mode rather than a
/// theoretical one:
///
/// * `"` would close the quoted-string early, so the remainder of the URL is
///   parsed as further auth-params -- a client could be pointed at one metadata
///   URL while the challenge advertises another.
/// * `\` is the quoted-pair escape; a trailing one escapes our own closing
///   quote and swallows the rest of the header.
/// * CR, LF, NUL and the other C0/DEL controls are header injection: a CRLF
///   splits the response, and the stricter HTTP codecs reject the value
///   outright, turning a 401 into a 500 and hiding the challenge entirely.
///
/// Non-ASCII is encoded too: header values are opaque bytes and clients disagree
/// on how to decode them, while a URL is ASCII by definition (RFC 3986).
///
/// `%` is deliberately NOT encoded. Encoding it would be injective but would
/// corrupt any URL that legitimately carries an escape -- `/a%20b` would be
/// advertised as `/a%2520b`, a different resource. A pre-escaped URL is the
/// common case; a literal `%` that is not an escape is not a valid URL to begin
/// with.
fn encode_quoted(url: &str) -> String {
    let mut out = String::with_capacity(url.len());
    for byte in url.bytes() {
        match byte {
            // Printable ASCII, minus the two quoted-string metacharacters.
            b'!'..=b'~' if byte != b'"' && byte != b'\\' => out.push(byte as char),
            other => out.push_str(&format!("%{other:02X}")),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::encode_quoted;

    #[test]
    fn ordinary_urls_pass_through_unchanged() {
        let url = "https://node-1:51402/.well-known/oauth-protected-resource?a=b&c=d#f";
        assert_eq!(encode_quoted(url), url);
    }

    #[test]
    fn existing_percent_escapes_are_not_double_encoded() {
        assert_eq!(
            encode_quoted("https://idp.example/a%20b"),
            "https://idp.example/a%20b"
        );
    }

    #[test]
    fn quoted_string_metacharacters_are_encoded() {
        assert_eq!(encode_quoted("a\"b"), "a%22b");
        assert_eq!(encode_quoted("a\\"), "a%5C");
    }

    #[test]
    fn controls_and_space_are_encoded() {
        assert_eq!(encode_quoted("a\r\nb"), "a%0D%0Ab");
        assert_eq!(encode_quoted("a b"), "a%20b");
        assert_eq!(encode_quoted("a\0b\u{7f}"), "a%00b%7F");
    }

    #[test]
    fn non_ascii_is_encoded_per_utf8_byte() {
        assert_eq!(encode_quoted("café"), "caf%C3%A9");
    }
}
