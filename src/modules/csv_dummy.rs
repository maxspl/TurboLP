use crate::core::Parser;
use std::borrow::Cow;
use serde::Serialize;

pub fn new() -> Box<dyn Parser> {
    Box::new(CsvDummy::new())
}

pub struct CsvDummy {
    headers: Option<Vec<String>>,
    delim: u8,
}

impl CsvDummy {
    fn new() -> Self {
        let headers = std::env::var("CSV_HEADERS").ok().and_then(|h| {
            let v: Vec<String> = h.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect();
            if v.is_empty() { None } else { Some(v) }
        });

        let delim_env = std::env::var("CSV_DELIM").unwrap_or_else(|_| ",".to_string());
        let delim = if delim_env == r"\t" { b'\t' } else { delim_env.as_bytes().get(0).copied().unwrap_or(b',') };

        Self { headers, delim }
    }

    #[inline]
    fn parse_line<'a>(&self, line: &'a str) -> Option<Record<'a>> {
        let mut rdr = csv::ReaderBuilder::new()
            .has_headers(false)
            .delimiter(self.delim)
            .from_reader(line.as_bytes());

        let mut rec_iter = rdr.records();
        let rec = rec_iter.next()?.ok()?; // one logical CSV record from the line
        let fields: Vec<String> = rec.iter().map(|s| s.to_string()).collect();

        if let Some(hdrs) = &self.headers {
            // map to object; if counts mismatch, we still emit best-effort
            let mut pairs = Vec::with_capacity(fields.len());
            for (i, val) in fields.iter().enumerate() {
                let key = hdrs.get(i).map(|s| s.as_str()).unwrap_or_else(|| "_extra");
                pairs.push((key.to_string(), val.clone()));
            }
            Some(Record::WithHeaders { cols: pairs, raw: line })
        } else {
            Some(Record::Array { cols: fields, raw: line })
        }
    }
}

impl Parser for CsvDummy {
    fn name(&self) -> Cow<'static, str> { Cow::Borrowed("csv-dummy") }
    fn description(&self) -> Cow<'static, str> { Cow::Borrowed("CSV -> JSONL (stateless per-line; optional headers via CSV_HEADERS)") }

    fn process_line_to_buf(&self, line: &str, out: &mut Vec<u8>) -> bool {
        if line.trim().is_empty() { return false; }
        if let Some(rec) = self.parse_line(line) {
            if serde_json::to_writer(&mut *out, &rec).is_ok() {
                out.push(b'\n');
                return true;
            }
        }
        false
    }
}

#[derive(Serialize)]
#[serde(untagged)]
enum Record<'a> {
    // Without headers → array of columns
    Array {
        cols: Vec<String>,
        raw: &'a str,
    },
    // With headers → vector of (key,value) to preserve duplicates/extras cleanly
    WithHeaders {
        cols: Vec<(String, String)>,
        raw: &'a str,
    },
}
