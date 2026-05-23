use crate::core::Parser;
use serde::Serialize;
use std::borrow::Cow;
use time::{format_description::well_known::Rfc3339, OffsetDateTime};

pub fn new() -> Box<dyn Parser> {
    Box::new(MactimeBodyfile)
}

pub struct MactimeBodyfile;

impl Parser for MactimeBodyfile {
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("mactime")
    }

    fn description(&self) -> Cow<'static, str> {
        Cow::Borrowed("Parses UAC bodyfile lines -> compact JSONL, one record per input line")
    }

    fn process_line_to_buf(&self, line: &str, out: &mut Vec<u8>) -> bool {
        let s = trim_cr(line);

        if s.trim().is_empty() || s.starts_with('#') {
            return false;
        }

        match parse_bodyfile_line(s) {
            Some(rec) => {
                if serde_json::to_writer(&mut *out, &rec).is_ok() {
                    out.push(b'\n');
                    return true;
                }
            }
            None => {
                let rec = Unparsed {
                    unparsed: true,
                    parser: "mactime",
                    reason: "invalid_bodyfile_line",
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

#[derive(Serialize)]
struct Record<'a> {
    md5: &'a str,
    path: &'a str,
    inode: NumberOrText<'a>,
    mode: &'a str,
    uid: NumberOrText<'a>,
    gid: NumberOrText<'a>,
    size: NumberOrText<'a>,
    atime: Option<String>,
    mtime: Option<String>,
    ctime: Option<String>,
    crtime: Option<String>,
}

#[derive(Serialize)]
struct Unparsed<'a> {
    unparsed: bool,
    parser: &'static str,
    reason: &'static str,
    raw: &'a str,
}

#[derive(Serialize)]
#[serde(untagged)]
enum NumberOrText<'a> {
    Number(i64),
    Text(&'a str),
}

fn parse_bodyfile_line(line: &str) -> Option<Record<'_>> {
    // UAC bodyfile order:
    // md5|path|inode|mode|uid|gid|size|atime|mtime|ctime|crtime
    //
    // Split once from the left for md5, then from the right for the 9 fixed
    // trailing fields. This keeps paths containing '|' parseable.
    let (md5, rest) = line.split_once('|')?;

    let mut parts: Vec<&str> = rest.rsplitn(10, '|').collect();
    if parts.len() != 10 {
        return None;
    }
    parts.reverse();

    Some(Record {
        md5,
        path: parts[0],
        inode: number_or_text(parts[1]),
        mode: parts[2],
        uid: number_or_text(parts[3]),
        gid: number_or_text(parts[4]),
        size: number_or_text(parts[5]),
        atime: timestamp_to_rfc3339(parts[6]),
        mtime: timestamp_to_rfc3339(parts[7]),
        ctime: timestamp_to_rfc3339(parts[8]),
        crtime: timestamp_to_rfc3339(parts[9]),
    })
}

fn number_or_text(value: &str) -> NumberOrText<'_> {
    match value.parse::<i64>() {
        Ok(n) => NumberOrText::Number(n),
        Err(_) => NumberOrText::Text(value),
    }
}

fn timestamp_to_rfc3339(value: &str) -> Option<String> {
    let epoch = value.parse::<i64>().ok()?;

    // In bodyfiles, 0 and -1 commonly mean "timestamp unavailable".
    // Do not emit misleading 1970-01-01 values for missing timestamps.
    if epoch == 0 || epoch == -1 {
        return None;
    }

    OffsetDateTime::from_unix_timestamp(epoch)
        .ok()?
        .format(&Rfc3339)
        .ok()
}

fn trim_cr(s: &str) -> &str {
    s.strip_suffix('\r').unwrap_or(s)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_standard_bodyfile_line() {
        let line = "0|/var/spool/cron/crontabs/msp|1044499|-rw-------|1031|102|593|1779318061|1688459984|1733323627|1688459984";
        let rec = parse_bodyfile_line(line).expect("bodyfile line should parse");
        assert_eq!(rec.md5, "0");
        assert_eq!(rec.path, "/var/spool/cron/crontabs/msp");
        assert_eq!(rec.mode, "-rw-------");
        assert_eq!(rec.atime.as_deref(), Some("2026-05-20T23:01:01Z"));
        assert_eq!(rec.mtime.as_deref(), Some("2023-07-04T08:39:44Z"));
        assert_eq!(rec.ctime.as_deref(), Some("2024-12-04T14:47:07Z"));
        assert_eq!(rec.crtime.as_deref(), Some("2023-07-04T08:39:44Z"));
    }

    #[test]
    fn parse_path_containing_pipe() {
        let line = "0|/tmp/a|b.txt|42|-rw-r--r--|1000|1000|12|1|2|3|4";
        let rec = parse_bodyfile_line(line).expect("bodyfile line should parse");
        assert_eq!(rec.path, "/tmp/a|b.txt");
    }

    #[test]
    fn missing_timestamps_are_null() {
        let line = "0|/tmp/a.txt|42|-rw-r--r--|1000|1000|12|0|-1|2|3";
        let rec = parse_bodyfile_line(line).expect("bodyfile line should parse");
        assert_eq!(rec.atime, None);
        assert_eq!(rec.mtime, None);
        assert_eq!(rec.ctime.as_deref(), Some("1970-01-01T00:00:02Z"));
    }
}
