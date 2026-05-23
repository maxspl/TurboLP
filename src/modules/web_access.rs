use crate::core::Parser;
use anyhow::Result;
use regex::Regex;
use serde::Serialize;
use std::borrow::Cow;
use time::{format_description::FormatItem, OffsetDateTime, UtcOffset};

/// Set `MULTIPARSE_WEB_FAST_TIME=1` to skip datetime parsing for speed.
fn fast_time_env() -> bool {
    std::env::var("MULTIPARSE_WEB_FAST_TIME")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

pub struct WebAccess {
    ctx: ParserCtx,
}

pub fn new() -> Box<dyn Parser> {
    Box::new(WebAccess {
        ctx: ParserCtx::new(fast_time_env()).expect("init web access ParserCtx"),
    })
}

impl Parser for WebAccess {
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("web-access")
    }

    fn description(&self) -> Cow<'static, str> {
        Cow::Borrowed("Parses Apache/Nginx access logs (common/combined/vhost) -> JSONL")
    }

    fn process_line_to_buf(&self, line: &str, out: &mut Vec<u8>) -> bool {
        let s = trim_cr(line).trim();

        if s.is_empty() {
            return false;
        }

        // Forensic invariant: never drop a non-empty line silently.
        // A line that does not match a known format is emitted as an
        // `unparsed` record so collections remain auditable.
        match self.ctx.parse_line(s) {
            Some(rec) => {
                if serde_json::to_writer(&mut *out, &rec).is_ok() {
                    out.push(b'\n');
                    return true;
                }
            }
            None => {
                let rec = Unparsed {
                    unparsed: true,
                    raw: s,
                };
                if serde_json::to_writer(&mut *out, &rec).is_ok() {
                    out.push(b'\n');
                    return true;
                }
            }
        }

        false
    }
}

/* -------------------- Core parsing logic -------------------- */

#[derive(Serialize)]
struct Record<'a> {
    vhost: Option<&'a str>,
    ip: Option<&'a str>,
    ident: Option<&'a str>,
    user: Option<&'a str>,
    ts: Option<String>,
    ts_raw: Option<&'a str>,
    method: Option<String>,
    target: Option<String>,
    path: Option<String>,
    query: Option<String>,
    protocol: Option<String>,
    status: Option<i64>,
    bytes: Option<i64>,
    referer: Option<Cow<'a, str>>,
    user_agent: Option<Cow<'a, str>>,
    raw: &'a str,
}

/// Fallback record for lines that match no known access-log format.
#[derive(Serialize)]
struct Unparsed<'a> {
    unparsed: bool,
    raw: &'a str,
}

struct ParserCtx {
    re: Regex,
    fmt: &'static [FormatItem<'static>],
    fast_time: bool,
}

impl ParserCtx {
    fn new(fast_time: bool) -> Result<Self> {
        // Single regex covering, in one pass:
        //
        // Common:
        //   IP ident user [time] "request" status size
        //
        // Combined:
        //   IP ident user [time] "request" status size "referer" "agent"
        //
        // Vhost-prefixed (combined or common):
        //   vhost IP ident user [time] "request" status size [...]
        //
        // The referer/agent pair is optional; extra trailing fields are
        // accepted and ignored.
        //
        // Quoted fields use `(?:[^"\\]|\\.)*` (not `[^"]*`) so an Apache
        // backslash-escaped quote inside request/referer/agent does not
        // terminate the field early. The Rust `regex` crate uses a finite
        // automaton, so this alternation cannot cause catastrophic
        // backtracking.
        let re = Regex::new(
            r#"^(?:(?P<vhost>\S+)\s+)?(?P<ip>\S+)\s+(?P<ident>\S+)\s+(?P<user>\S+)\s+\[(?P<time>[^\]]+)\]\s+"(?P<request>(?:[^"\\]|\\.)*)"\s+(?P<status>\d{3}|-)\s+(?P<size>\S+)(?:\s+"(?P<referer>(?:[^"\\]|\\.)*)"\s+"(?P<agent>(?:[^"\\]|\\.)*)")?(?:\s+.*)?$"#,
        )?;

        let fmt = time::macros::format_description!(
            "[day]/[month repr:short]/[year]:[hour]:[minute]:[second] [offset_hour sign:mandatory][offset_minute]"
        );

        Ok(Self {
            re,
            fmt,
            fast_time,
        })
    }

    fn parse_time_iso8601_utc(&self, s: &str) -> Option<String> {
        if self.fast_time {
            return None;
        }

        if let Ok(dt) = OffsetDateTime::parse(s, &self.fmt) {
            let utc = dt.to_offset(UtcOffset::UTC);
            return Some(
                utc.format(&time::format_description::well_known::Rfc3339)
                    .ok()?,
            );
        }

        None
    }

    fn parse_request(
        &self,
        req: &str,
    ) -> (
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
    ) {
        if req.is_empty() || req == "-" {
            return (None, None, None, None, None);
        }

        // `split(' ')` (not `split_whitespace`) is intentional: it keeps
        // empty segments, so the `join` below preserves multi-space
        // targets verbatim, which matters for forensic fidelity.
        let parts: Vec<&str> = req.split(' ').collect();
        let (mut method, mut target, mut protocol) = (None, None, None);

        match parts.len() {
            n if n >= 3 => {
                method = Some(parts[0].to_string());
                protocol = Some(parts[n - 1].to_string());
                target = Some(parts[1..n - 1].join(" "));
            }
            2 => {
                method = Some(parts[0].to_string());
                target = Some(parts[1].to_string());
            }
            1 => {
                target = Some(parts[0].to_string());
            }
            _ => {}
        }

        let mut path: Option<String> = None;
        let mut query: Option<String> = None;

        if let Some(t) = &target {
            if t == "*" {
                path = Some("*".to_string());
            } else if let Some(pos) = t.find('?') {
                path = Some(t[..pos].to_string());

                if pos + 1 < t.len() {
                    query = Some(t[pos + 1..].to_string());
                }
            } else {
                path = Some(t.clone());
            }
        }

        (method, target, path, query, protocol)
    }

    fn to_int(s: Option<&str>) -> Option<i64> {
        let s = s?;

        if s == "-" {
            return None;
        }

        s.parse::<i64>().ok()
    }

    /// Caller must pass an already-trimmed, non-empty line.
    fn parse_line<'a>(&'a self, line: &'a str) -> Option<Record<'a>> {
        let caps = self.re.captures(line)?;

        let vhost = caps.name("vhost").map(|m| m.as_str());
        let ip = caps.name("ip").map(|m| m.as_str());
        let ident = caps.name("ident").map(|m| m.as_str()).filter(|&v| v != "-");
        let user = caps.name("user").map(|m| m.as_str()).filter(|&v| v != "-");

        let time_raw = caps.name("time").map(|m| m.as_str());
        let ts = time_raw.and_then(|t| self.parse_time_iso8601_utc(t));

        let request_raw = caps.name("request").map(|m| m.as_str()).unwrap_or("");
        let request = unescape_logitem(request_raw);
        let (method, target, path, query, protocol) = self.parse_request(&request);

        let status = Self::to_int(caps.name("status").map(|m| m.as_str()));
        let bytes = Self::to_int(caps.name("size").map(|m| m.as_str()));

        let referer = caps
            .name("referer")
            .map(|m| m.as_str())
            .filter(|&v| v != "-")
            .map(unescape_logitem);

        let agent = caps
            .name("agent")
            .map(|m| m.as_str())
            .filter(|&v| v != "-")
            .map(unescape_logitem);

        Some(Record {
            vhost,
            ip,
            ident,
            user,
            ts,
            ts_raw: time_raw,
            method,
            target,
            path,
            query,
            protocol,
            status,
            bytes,
            referer,
            user_agent: agent,
            raw: line,
        })
    }
}

/* -------------------- small helpers -------------------- */

fn trim_cr(s: &str) -> &str {
    if s.as_bytes().last().copied() == Some(b'\r') {
        &s[..s.len() - 1]
    } else {
        s
    }
}

/// Reverse Apache `ap_escape_logitem` escaping inside quoted fields.
/// Handles `\"`, `\\`, `\n`, `\r`, `\t`, `\b`, `\v`, `\f` and `\xHH`.
/// Returns the input borrowed unchanged when it contains no backslash,
/// so the common (unescaped) case allocates nothing.
fn unescape_logitem(s: &str) -> Cow<'_, str> {
    if !s.contains('\\') {
        return Cow::Borrowed(s);
    }

    let mut out = String::with_capacity(s.len());
    let mut it = s.chars();

    while let Some(c) = it.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }

        match it.next() {
            Some('n') => out.push('\n'),
            Some('r') => out.push('\r'),
            Some('t') => out.push('\t'),
            Some('b') => out.push('\u{0008}'),
            Some('v') => out.push('\u{000B}'),
            Some('f') => out.push('\u{000C}'),
            Some('"') => out.push('"'),
            Some('\\') => out.push('\\'),
            Some('x') => {
                let a = it.next();
                let b = it.next();
                match (
                    a.and_then(|c| c.to_digit(16)),
                    b.and_then(|c| c.to_digit(16)),
                ) {
                    (Some(x), Some(y)) => out.push(char::from((x * 16 + y) as u8)),
                    _ => {
                        // Not a valid \xHH sequence: emit literally.
                        out.push('\\');
                        out.push('x');
                        if let Some(a) = a {
                            out.push(a);
                        }
                        if let Some(b) = b {
                            out.push(b);
                        }
                    }
                }
            }
            // Unknown escape: drop the backslash, keep the char.
            Some(other) => out.push(other),
            // Trailing lone backslash.
            None => out.push('\\'),
        }
    }

    Cow::Owned(out)
}

/* -------------------- tests -------------------- */

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx() -> ParserCtx {
        ParserCtx::new(false).unwrap()
    }

    #[test]
    fn parses_combined() {
        let line = r#"1.2.3.4 - alice [10/Oct/2000:13:55:36 -0700] "GET /index.html?a=1 HTTP/1.1" 200 2326 "http://ref/" "Mozilla/5.0""#;
        let r = ctx().parse_line(line).unwrap();
        assert_eq!(r.ip, Some("1.2.3.4"));
        assert_eq!(r.user, Some("alice"));
        assert_eq!(r.method.as_deref(), Some("GET"));
        assert_eq!(r.path.as_deref(), Some("/index.html"));
        assert_eq!(r.query.as_deref(), Some("a=1"));
        assert_eq!(r.protocol.as_deref(), Some("HTTP/1.1"));
        assert_eq!(r.status, Some(200));
        assert_eq!(r.bytes, Some(2326));
        assert_eq!(r.referer.as_deref(), Some("http://ref/"));
        assert_eq!(r.user_agent.as_deref(), Some("Mozilla/5.0"));
        assert_eq!(r.ts.as_deref(), Some("2000-10-10T20:55:36Z"));
    }

    #[test]
    fn parses_common() {
        let line = r#"1.2.3.4 - - [10/Oct/2000:13:55:36 -0700] "GET / HTTP/1.0" 404 -"#;
        let r = ctx().parse_line(line).unwrap();
        assert_eq!(r.status, Some(404));
        assert_eq!(r.bytes, None);
        assert!(r.referer.is_none());
        assert!(r.user_agent.is_none());
    }

    #[test]
    fn parses_vhost_prefixed() {
        let line = r#"www.example.com 1.2.3.4 - - [10/Oct/2000:13:55:36 -0700] "GET / HTTP/1.1" 200 10 "-" "curl""#;
        let r = ctx().parse_line(line).unwrap();
        assert_eq!(r.vhost, Some("www.example.com"));
        assert_eq!(r.ip, Some("1.2.3.4"));
        assert_eq!(r.user_agent.as_deref(), Some("curl"));
    }

    #[test]
    fn handles_escaped_quote_in_request() {
        // The key fix: a backslash-escaped quote must not end the field.
        let line = r#"1.2.3.4 - - [10/Oct/2000:13:55:36 -0700] "GET /a\"b HTTP/1.1" 200 5 "-" "UA""#;
        let r = ctx().parse_line(line).unwrap();
        assert_eq!(r.path.as_deref(), Some(r#"/a"b"#));
        assert_eq!(r.status, Some(200));
    }

    #[test]
    fn empty_request_is_tolerated() {
        let line = r#"1.2.3.4 - - [10/Oct/2000:13:55:36 -0700] "-" 408 0"#;
        let r = ctx().parse_line(line).unwrap();
        assert!(r.method.is_none());
        assert!(r.target.is_none());
        assert_eq!(r.status, Some(408));
    }

    #[test]
    fn unescape_borrows_when_no_backslash() {
        assert!(matches!(unescape_logitem("plain text"), Cow::Borrowed(_)));
        assert_eq!(unescape_logitem(r#"a\"b\\c\x41"#), "a\"b\\cA");
    }

    #[test]
    fn unmatched_line_emits_unparsed_record() {
        let p = new();
        let mut out = Vec::new();
        assert!(p.process_line_to_buf("this is not an access log line", &mut out));
        let s = String::from_utf8(out).unwrap();
        assert!(s.contains(r#""unparsed":true"#));
        assert!(s.ends_with('\n'));
    }

    #[test]
    fn empty_line_emits_nothing() {
        let p = new();
        let mut out = Vec::new();
        assert!(!p.process_line_to_buf("   \r", &mut out));
        assert!(out.is_empty());
    }
}
