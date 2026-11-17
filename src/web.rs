use std::{fmt, time::Duration};

use crate::{CopUri, UriError};

/// More than this is not the page of an order.
const MAX_BYTES: usize = 256 * 1024;
const MAX_REDIRECTS: usize = 3;
const TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug)]
pub enum ResolveError {
    Uri(UriError),
    Fetch(String),
    /// The page has no valid `cassis-order` link.
    NoLink,
}

impl fmt::Display for ResolveError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ResolveError::Uri(e) => e.fmt(f),
            ResolveError::Fetch(e) => write!(f, "could not fetch the page: {e}"),
            ResolveError::NoLink => f.write_str("the page does not point to an order"),
        }
    }
}

impl std::error::Error for ResolveError {}

/// Turn what was scanned or typed into a connection URI: a `cop://` URI as it is, a web address
/// (`example.com/order/1`, `https://...`) by fetching it over HTTPS and reading the
/// `<link rel="cassis-order" href="cop://...">` of its `<head>`.
///
/// The page is never rendered and its links never followed. Fetching it shows the payer's
/// address to its host, so only resolve addresses the payer chose.
pub async fn resolve(input: &str) -> Result<CopUri, ResolveError> {
    let input = input.trim();
    if input.len() >= 6 && input[..6].eq_ignore_ascii_case("cop://") {
        return CopUri::parse(input).map_err(ResolveError::Uri);
    }
    let url = match input.split_once("://") {
        Some((scheme, rest)) if scheme.eq_ignore_ascii_case("http") => format!("https://{rest}"),
        Some((scheme, _)) if scheme.eq_ignore_ascii_case("https") => input.to_owned(),
        Some(_) => return Err(ResolveError::Uri(UriError("not an order address".into()))),
        None => format!("https://{input}"),
    };
    let page = fetch(&url).await.map_err(ResolveError::Fetch)?;
    find_link(&page)
        .and_then(|href| CopUri::parse(&href).ok())
        .ok_or(ResolveError::NoLink)
}

async fn fetch(url: &str) -> Result<String, String> {
    let client = reqwest::Client::builder()
        .timeout(TIMEOUT)
        .redirect(reqwest::redirect::Policy::custom(|attempt| {
            if attempt.previous().len() >= MAX_REDIRECTS || attempt.url().scheme() != "https" {
                attempt.stop()
            } else {
                attempt.follow()
            }
        }))
        .https_only(true)
        .build()
        .map_err(|e| e.to_string())?;
    let mut response = client.get(url).send().await.map_err(|e| e.to_string())?;
    if !response.status().is_success() {
        return Err(format!("status {}", response.status()));
    }
    let html = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.to_ascii_lowercase().contains("html"));
    if !html {
        return Err("not an HTML page".into());
    }
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|e| e.to_string())? {
        body.extend_from_slice(&chunk);
        if body.len() > MAX_BYTES {
            // The link is in the head; what is past the limit does not matter.
            body.truncate(MAX_BYTES);
            break;
        }
    }
    Ok(String::from_utf8_lossy(&body).into_owned())
}

/// The `href` of the first `<link rel="... cassis-order ..." href="cop://...">` in the `<head>`.
fn find_link(html: &str) -> Option<String> {
    let lower = html.to_ascii_lowercase();
    let start = lower.find("<head").and_then(|i| lower[i..].find('>').map(|j| i + j + 1))?;
    let end = lower[start..].find("</head").map_or(html.len(), |i| start + i);
    let mut at = start;
    while let Some(i) = lower[at..end].find("<link") {
        let tag_start = at + i + "<link".len();
        // `<linkfoo` is another element.
        if !lower[tag_start..].starts_with(|c: char| c.is_ascii_whitespace() || c == '/') {
            at = tag_start;
            continue;
        }
        let tag_end = lower[tag_start..end].find('>').map_or(end, |j| tag_start + j);
        let attrs = attributes(&html[tag_start..tag_end]);
        let rel = attrs.iter().find(|(k, _)| k == "rel").map(|(_, v)| v.as_str());
        let href = attrs.iter().find(|(k, _)| k == "href").map(|(_, v)| v.as_str());
        if let (Some(rel), Some(href)) = (rel, href) {
            let wanted = rel
                .split_ascii_whitespace()
                .any(|t| t.eq_ignore_ascii_case("cassis-order"));
            if wanted && CopUri::parse(href).is_ok() {
                return Some(href.to_owned());
            }
        }
        at = tag_end;
    }
    None
}

/// The attributes of a tag, with lowercase names, in `name="value"`, `name='value'` and
/// `name=value` form.
fn attributes(tag: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    let mut rest = tag.trim_start_matches(['/', ' ', '\t', '\n', '\r']);
    while !rest.is_empty() {
        let name_end = rest
            .find(|c: char| c == '=' || c.is_ascii_whitespace() || c == '/')
            .unwrap_or(rest.len());
        let name = rest[..name_end].to_ascii_lowercase();
        rest = rest[name_end..].trim_start();
        let mut value = String::new();
        if let Some(after) = rest.strip_prefix('=') {
            let after = after.trim_start();
            let (v, remaining) = match after.chars().next() {
                Some(q @ ('"' | '\'')) => match after[1..].find(q) {
                    Some(i) => (&after[1..1 + i], &after[i + 2..]),
                    None => (&after[1..], ""),
                },
                _ => {
                    let i = after
                        .find(|c: char| c.is_ascii_whitespace())
                        .unwrap_or(after.len());
                    (&after[..i], &after[i..])
                }
            };
            value = v.to_owned();
            rest = remaining;
        }
        if !name.is_empty() {
            out.push((name, value));
        }
        rest = rest.trim_start_matches(['/', ' ', '\t', '\n', '\r']);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    const ID: &str = "5f1c9d0a27e4b6c38d2f71a09e4b5c6d8a3f2e1b0c9d8e7f6a5b4c3d2e1f0a9b";

    #[test]
    fn finds_the_link_in_the_head_only() {
        let page = format!(
            r#"<html><head><title>x</title>
            <link rel="stylesheet" href="a.css">
            <LINK href='cop://{ID}#table=1' REL="alternate cassis-order"/>
            </head><body><link rel="cassis-order" href="cop://{}"></body></html>"#,
            "0".repeat(64)
        );
        assert_eq!(find_link(&page).unwrap(), format!("cop://{ID}#table=1"));
        let in_body = format!(r#"<head></head><body><link rel="cassis-order" href="cop://{ID}">"#);
        assert!(find_link(&in_body).is_none());
        assert!(find_link(r#"<head><link rel="cassis-order" href="https://x"></head>"#).is_none());
    }
}
