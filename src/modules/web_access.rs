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
    fn name(&self) -> Cow<'static, str> { Cow::Borrowed("web-access") }
    fn description(&self) -> Cow<'static, str> { Cow::Borrowed("Parses Apache/Nginx access logs (common/combined) -> JSONL")}

    fn process_line_to_buf(&self, line: &str, out: &mut Vec<u8>) -> bool {
        let s = trim_cr(line);
        if let Some(rec) = self.ctx.parse_line(s) {
            if serde_json::to_writer(&mut *out, &rec).is_ok() {
                out.push(b'\n');
                return true;
            }
        }
        false
    }
}

/* -------------------- Core parsing logic -------------------- */

#[derive(Serialize)]
struct Record<'a> {
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
    referer: Option<&'a str>,
    user_agent: Option<&'a str>,
    raw: &'a str,
}

struct ParserCtx {
    re_combined: Regex,
    re_common: Regex,
    fmt: &'static [FormatItem<'static>],
    fast_time: bool,
}

impl ParserCtx {
    fn new(fast_time: bool) -> Result<Self> {
        let re_combined = Regex::new(
            r#"^(?P<ip>\S+) (?P<ident>\S+) (?P<user>\S+) \[(?P<time>[^\]]+)\] "(?P<request>[^"]*)" (?P<status>\d{3}|-) (?P<size>\S+) "(?P<referer>[^"]*)" "(?P<agent>[^"]*)"$"#,
        )?;
        let re_common = Regex::new(
            r#"^(?P<ip>\S+) (?P<ident>\S+) (?P<user>\S+) \[(?P<time>[^\]]+)\] "(?P<request>[^"]*)" (?P<status>\d{3}|-) (?P<size>\S+)$"#,
        )?;
        let fmt = time::macros::format_description!(
            "[day]/[month repr:short]/[year]:[hour]:[minute]:[second] [offset_hour sign:mandatory][offset_minute]"
        );
        Ok(Self { re_combined, re_common, fmt, fast_time })
    }

    fn parse_time_iso8601_utc(&self, s: &str) -> Option<String> {
        if self.fast_time { return None; }
        if let Ok(dt) = OffsetDateTime::parse(s, &self.fmt) {
            let utc = dt.to_offset(UtcOffset::UTC);
            return Some(utc.format(&time::format_description::well_known::Rfc3339).ok()?);
        }
        None
    }

    fn parse_request(
        &self,
        req: &str,
    ) -> (Option<String>, Option<String>, Option<String>, Option<String>, Option<String>) {
        if req.is_empty() || req == "-" {
            return (None, None, None, None, None);
        }
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
            1 => { target = Some(parts[0].to_string()); }
            _ => {}
        }

        let mut path: Option<String> = None;
        let mut query: Option<String> = None;
        if let Some(t) = &target {
            if t == "*" {
                path = Some("*".to_string());
            } else if let Some(pos) = t.find('?') {
                path = Some(t[..pos].to_string());
                if pos + 1 < t.len() { query = Some(t[pos + 1..].to_string()); }
            } else {
                path = Some(t.clone());
            }
        }
        (method, target, path, query, protocol)
    }

    fn to_int(s: Option<&str>) -> Option<i64> {
        let s = s?;
        if s == "-" { return None; }
        s.parse::<i64>().ok()
    }

    fn parse_line<'a>(&'a self, line: &'a str) -> Option<Record<'a>> {
        let line = line.trim();
        if line.is_empty() { return None; }
        let caps = self.re_combined.captures(line)
            .or_else(|| self.re_common.captures(line))?;

        let ip = caps.name("ip").map(|m| m.as_str());
        let ident = caps.name("ident").map(|m| m.as_str()).filter(|&v| v != "-");
        let user = caps.name("user").map(|m| m.as_str()).filter(|&v| v != "-");
        let time_raw = caps.name("time").map(|m| m.as_str());
        let ts = time_raw.and_then(|t| self.parse_time_iso8601_utc(t));

        let request = caps.name("request").map(|m| m.as_str()).unwrap_or("");
        let (method, target, path, query, protocol) = self.parse_request(request);

        let status = Self::to_int(caps.name("status").map(|m| m.as_str()));
        let bytes = Self::to_int(caps.name("size").map(|m| m.as_str()));

        let referer = caps.name("referer").map(|m| m.as_str()).filter(|&v| v != "-");
        let agent = caps.name("agent").map(|m| m.as_str()).filter(|&v| v != "-");

        Some(Record {
            ip, ident, user, ts, ts_raw: time_raw, method, target, path, query, protocol, status,
            bytes, referer, user_agent: agent, raw: line,
        })
    }
}

/* -------------------- small helpers -------------------- */

fn trim_cr(s: &str) -> &str {
    if s.as_bytes().last().copied() == Some(b'\r') { &s[..s.len()-1] } else { s }
}
