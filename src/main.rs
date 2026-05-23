mod core;
mod modules;

use crate::core::{count_lines_any, format_size, registry, run_streaming_parallel, Parser};
use anyhow::{bail, Context, Result};
use clap::{Parser as ClapParser, Subcommand};
use once_cell::sync::Lazy;
use std::{
    ffi::OsString,
    fs::File,
    io::{self, Write},
    path::{Path, PathBuf},
    time::Instant,
};

#[derive(ClapParser, Debug)]
#[command(name = "minimal-parser", version, about = "Modular file parser (multithreaded only)")]
struct Cli {
    #[command(subcommand)]
    cmd: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Run a specific module on an input file (multithreaded).
    Run {
        /// Module name (see `list`).
        #[arg(long)]
        module: String,

        /// Input file path.
        #[arg(long)]
        input: PathBuf,

        /// Output file path. If omitted, JSONL is written to stdout.
        #[arg(long)]
        output: Option<PathBuf>,

        /// Prefix the output filename with a short deterministic hash of the input path.
        ///
        /// Example:
        ///   --input /a/bodyfile.txt --output /out/bodyfile.jsonl --prefix-input-hash
        ///   => /out/1a2b3c4d-bodyfile.jsonl
        #[arg(long)]
        prefix_input_hash: bool,

        /// Number of worker threads.
        ///
        /// Default: num_cpus::get()
        #[arg(long)]
        workers: Option<usize>,
    },

    /// List available modules and their descriptions.
    List,
}

static PARSERS: Lazy<Vec<Box<dyn Parser>>> = Lazy::new(|| registry().iter().map(|f| f()).collect());

fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.cmd {
        Command::List => {
            println!("Available modules:");
            for p in PARSERS.iter() {
                println!("  {:<16} - {}", p.name(), p.description());
            }
        }

        Command::Run {
            module,
            input,
            output,
            prefix_input_hash,
            workers,
        } => {
            let parser = PARSERS
                .iter()
                .find(|p| p.name() == module)
                .with_context(|| format!("unknown module: {module}"))?;

            let final_output = resolve_output_path(&input, output, prefix_input_hash)?;

            run_with_threads(parser.as_ref(), &input, final_output.as_deref(), workers)?;
        }
    }

    Ok(())
}

fn resolve_output_path(
    input: &Path,
    output: Option<PathBuf>,
    prefix_input_hash: bool,
) -> Result<Option<PathBuf>> {
    let Some(mut output) = output else {
        if prefix_input_hash {
            bail!("--prefix-input-hash requires --output");
        }
        return Ok(None);
    };

    if !prefix_input_hash {
        return Ok(Some(output));
    }

    let filename = output
        .file_name()
        .with_context(|| format!("output path '{}' does not contain a filename", output.display()))?;

    let mut prefixed_filename = OsString::new();
    prefixed_filename.push(short_input_path_hash(input));
    prefixed_filename.push("-");
    prefixed_filename.push(filename);

    output.set_file_name(prefixed_filename);

    Ok(Some(output))
}

/// Deterministic short hash for differentiating files with identical basenames.
fn short_input_path_hash(input: &Path) -> String {
    // FNV-1a 64-bit, truncated to 32 bits for an 8-hex-character prefix.
    // No external dependency, deterministic across platforms/runs.
    let mut hash: u64 = 0xcbf29ce484222325;
    for b in input.as_os_str().to_string_lossy().as_bytes() {
        hash ^= *b as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }

    format!("{:08x}", (hash & 0xffff_ffff) as u32)
}

fn run_with_threads(
    parser: &dyn Parser,
    input: &Path,
    output: Option<&Path>,
    workers: Option<usize>,
) -> Result<()> {
    let meta = std::fs::metadata(input).with_context(|| format!("metadata {}", input.display()))?;
    let file_size = meta.len();

    // Exact line count for both text and .gz.
    let line_count = count_lines_any(input)?;

    let n_workers = workers.unwrap_or_else(num_cpus::get).max(1);

    println!(
        "[INFO] Input file: {} ({}), {} lines",
        input.display(),
        format_size(file_size),
        line_count
    );

    println!(
        "[INFO] Module: {}  |  Threads: {}",
        parser.name(),
        n_workers
    );

    let start = Instant::now();

    let writer: Box<dyn Write + Send> = match output {
        Some(path) => {
            if let Some(parent) = path.parent() {
                if !parent.as_os_str().is_empty() {
                    std::fs::create_dir_all(parent)
                        .with_context(|| format!("create output directory {}", parent.display()))?;
                }
            }

            Box::new(File::create(path).with_context(|| format!("create {}", path.display()))?)
        }
        None => Box::new(io::stdout()),
    };

    let emitted = run_streaming_parallel(parser, input, writer, n_workers)?;

    println!("[INFO] Emitted {} records", emitted);

    let elapsed = start.elapsed().as_secs_f64();

    if let Some(out_path) = output {
        let out_size = std::fs::metadata(out_path)
            .map(|m| format_size(m.len()))
            .unwrap_or_else(|_| "unknown".into());

        println!(
            "[INFO] Output: {} ({}), processed in {:.3}s ({:.1} lines/s)",
            out_path.display(),
            out_size,
            elapsed,
            line_count as f64 / elapsed
        );
    } else {
        println!(
            "[INFO] Output: stdout, processed in {:.3}s ({:.1} lines/s)",
            elapsed,
            line_count as f64 / elapsed
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefixes_output_filename_with_input_path_hash() {
        let out = resolve_output_path(
            Path::new("/cases/a/bodyfile.txt"),
            Some(PathBuf::from("/out/bodyfile.jsonl")),
            true,
        )
        .unwrap()
        .unwrap();

        let name = out.file_name().unwrap().to_string_lossy();
        assert!(name.ends_with("-bodyfile.jsonl"));
        assert_eq!(name.len(), "00000000-bodyfile.jsonl".len());
    }

    #[test]
    fn same_basename_different_input_paths_produce_different_output_names() {
        let a = resolve_output_path(
            Path::new("/cases/a/bodyfile.txt"),
            Some(PathBuf::from("/out/bodyfile.jsonl")),
            true,
        )
        .unwrap()
        .unwrap();
        let b = resolve_output_path(
            Path::new("/cases/b/bodyfile.txt"),
            Some(PathBuf::from("/out/bodyfile.jsonl")),
            true,
        )
        .unwrap()
        .unwrap();

        assert_ne!(a.file_name(), b.file_name());
    }
}
