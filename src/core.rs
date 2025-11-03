use std::{
    borrow::Cow,
    fs::File,
    io::{BufRead, BufReader, Read, Seek, Write},
    path::Path,
    sync::Arc,
    thread,
};

use anyhow::{Context, Result};
use crossbeam_channel::{bounded, Receiver, Sender};
use flate2::read::GzDecoder;
use memchr::memchr_iter;

/* -------------------- Parser trait -------------------- */

/// Every module only needs to process one line and append JSONL to `out`.
pub trait Parser: Send + Sync {
    fn name(&self) -> Cow<'static, str>;
    fn description(&self) -> Cow<'static, str>;
    /// Return true if a JSONL record was emitted.
    fn process_line_to_buf(&self, line: &str, out: &mut Vec<u8>) -> bool;
}

/* -------------------- Gzip / IO helpers -------------------- */

const READER_BUF: usize = 1 << 20; // 1 MiB

/// Return a **BufRead** that transparently decompresses `.gz` if needed.
pub fn open_maybe_gz_bufread(path: &Path, buf_size: usize) -> Result<Box<dyn BufRead + Send>> {
    let mut fh = File::open(path).with_context(|| format!("open {}", path.display()))?;

    // Peek gzip magic 0x1F 0x8B
    let mut magic = [0u8; 2];
    let n = fh.read(&mut magic)?;
    fh.rewind()?;

    if n == 2 && magic == [0x1F, 0x8B] {
        let gz = GzDecoder::new(fh);
        Ok(Box::new(BufReader::with_capacity(buf_size, gz)))
    } else {
        Ok(Box::new(BufReader::with_capacity(buf_size, fh)))
    }
}

/// Return a **Read** that transparently decompresses `.gz` if needed
/// (useful for fast scanning / counting).
pub fn open_maybe_gz_read(path: &Path) -> Result<Box<dyn Read + Send>> {
    let mut fh = File::open(path).with_context(|| format!("open {}", path.display()))?;

    let mut magic = [0u8; 2];
    let n = fh.read(&mut magic)?;
    fh.rewind()?;

    if n == 2 && magic == [0x1F, 0x8B] {
        Ok(Box::new(GzDecoder::new(fh)))
    } else {
        Ok(Box::new(fh))
    }
}

/// True if file starts with gzip magic bytes.
pub fn is_gzip(path: &Path) -> Result<bool> {
    let mut fh = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let mut magic = [0u8; 2];
    let n = fh.read(&mut magic)?;
    Ok(n == 2 && magic == [0x1F, 0x8B])
}

/* -------------------- High-throughput streaming runner -------------------- */

/// High-throughput streaming runner (multithreaded only).
pub fn run_streaming_parallel(
    parser: &dyn Parser,
    input: &Path,
    mut writer: Box<dyn Write + Send>,
    workers: usize,
) -> Result<usize> {
    const BYTES_BLOB_TARGET: usize = 4 << 20; // 4 MiB
    const LINES_BLOB_MAX: usize = 16_384;
    const LINES_CHAN_FACTOR: usize = 64;

    let (tx_lines, rx_lines): (Sender<Vec<u8>>, Receiver<Vec<u8>>) =
        bounded(workers * LINES_CHAN_FACTOR);
    let (tx_blobs, rx_blobs): (Sender<Vec<u8>>, Receiver<Vec<u8>>) = bounded(workers * 4);
    let (tx_counts, rx_counts): (Sender<usize>, Receiver<usize>) = bounded(workers);

    // Writer thread
    let writer_handle = thread::spawn(move || -> Result<()> {
        let mut w = std::io::BufWriter::with_capacity(32 << 20, writer);
        for blob in rx_blobs.iter() {
            w.write_all(&blob)?;
        }
        w.flush()?;
        Ok(())
    });

    // Share parser safely (parser lives for the whole run)
    let parser_ref: &'static dyn Parser = unsafe { std::mem::transmute(parser as &dyn Parser) };
    let parser_arc = Arc::new(parser_ref);

    // Workers
    let mut handles = Vec::with_capacity(workers);
    for _ in 0..workers {
        let rx = rx_lines.clone();
        let tx_b = tx_blobs.clone();
        let tx_c = tx_counts.clone();
        let p = parser_arc.clone();

        handles.push(thread::spawn(move || {
            let mut local_count = 0usize;
            let mut blob = Vec::with_capacity(BYTES_BLOB_TARGET);
            let mut lines_in_blob = 0usize;

            for line_bytes in rx.iter() {
                if let Ok(mut s) = std::str::from_utf8(&line_bytes) {
                    if s.as_bytes().last().copied() == Some(b'\n') {
                        s = &s[..s.len() - 1];
                    }
                    if s.as_bytes().last().copied() == Some(b'\r') {
                        s = &s[..s.len() - 1];
                    }
                    if p.process_line_to_buf(s, &mut blob) {
                        local_count += 1;
                        lines_in_blob += 1;
                    }
                }
                if blob.len() >= BYTES_BLOB_TARGET || lines_in_blob >= LINES_BLOB_MAX {
                    if tx_b.send(std::mem::take(&mut blob)).is_err() {
                        break;
                    }
                    blob.reserve(BYTES_BLOB_TARGET);
                    lines_in_blob = 0;
                }
            }

            if !blob.is_empty() {
                let _ = tx_b.send(blob);
            }
            let _ = tx_c.send(local_count);
        }));
    }

    // Reader (supports .gz transparently)
    let path_clone = input.to_path_buf();
    let reader_handle = thread::spawn(move || -> Result<()> {
        let mut r = open_maybe_gz_bufread(&path_clone, READER_BUF)?;
        let mut buf = Vec::<u8>::with_capacity(64 * 1024);
        loop {
            buf.clear();
            let n = r.read_until(b'\n', &mut buf)?;
            if n == 0 {
                break;
            }
            if tx_lines.send(buf.clone()).is_err() {
                break;
            }
        }
        drop(tx_lines);
        Ok(())
    });

    reader_handle
        .join()
        .map_err(|_| anyhow::anyhow!("reader panicked"))??;

    for h in handles {
        let _ = h.join();
    }
    drop(tx_blobs); // close writer channel

    let mut total = 0usize;
    for _ in 0..workers {
        if let Ok(n) = rx_counts.recv() {
            total += n;
        }
    }

    writer_handle
        .join()
        .map_err(|_| anyhow::anyhow!("writer panicked"))??;
    Ok(total)
}

/* -------------------- Registry & utils -------------------- */

pub type ParserFactory = fn() -> Box<dyn Parser>;
pub fn registry() -> &'static [ParserFactory] {
    &[
        crate::modules::web_access::new,
        crate::modules::csv_dummy::new, // keep if useful
    ]
}

pub fn format_size(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut size = bytes as f64;
    let mut unit = 0;
    while size >= 1024.0 && unit < UNITS.len() - 1 {
        size /= 1024.0;
        unit += 1;
    }
    format!("{:.2} {}", size, UNITS[unit])
}

/// Fast line counter for **both plain and .gz** files.
///
/// Uses a big chunked read and `memchr` to count `\n` without per-line allocation.
/// For `.gz`, this does a full decompress pass (inevitable if you want an exact count).
pub fn count_lines_any(path: &Path) -> Result<u64> {
    let mut r = open_maybe_gz_read(path)?;
    let mut buf = vec![0u8; 256 * 1024]; // 256 KiB chunks
    let mut total = 0u64;

    loop {
        let n = r.read(&mut buf)?;
        if n == 0 {
            break;
        }
        total += memchr_iter(b'\n', &buf[..n]).count() as u64;
    }
    Ok(total)
}
