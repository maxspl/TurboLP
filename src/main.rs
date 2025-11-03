mod core;
mod modules;

use crate::core::{
    count_lines_any, format_size, registry, run_streaming_parallel, Parser,
};
use anyhow::{Context, Result};
use clap::{Parser as ClapParser, Subcommand};
use once_cell::sync::Lazy;
use std::{
    fs::File,
    io::{self, Write},
    path::{Path, PathBuf},
    time::Instant,
};

#[derive(ClapParser, Debug)]
#[command(name="minimal-parser", version, about="Modular file parser (multithreaded only)")]
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
        /// Output file path (stdout if omitted).
        #[arg(long)]
        output: Option<PathBuf>,
        /// Number of worker threads (default: num_cpus::get()).
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
            workers,
        } => {
            let parser = PARSERS
                .iter()
                .find(|p| p.name() == module)
                .with_context(|| format!("unknown module: {module}"))?;

            run_with_threads(parser.as_ref(), &input, output.as_deref(), workers)?;
        }
    }
    Ok(())
}

fn run_with_threads(
    parser: &dyn Parser,
    input: &Path,
    output: Option<&Path>,
    workers: Option<usize>,
) -> Result<()> {
    let meta =
        std::fs::metadata(input).with_context(|| format!("metadata {}", input.display()))?;
    let file_size = meta.len();

    // Exact line count for both text and .gz (fast memchr-based pass).
    let line_count = count_lines_any(input)?;

    let n_workers = workers.unwrap_or_else(num_cpus::get).max(1);

    println!(
        "[INFO] Input file: {} ({}), {} lines",
        input.display(),
        format_size(file_size),
        line_count
    );
    println!("[INFO] Module: {}  |  Threads: {}", parser.name(), n_workers);

    let start = Instant::now();

    let writer: Box<dyn Write + Send> = match output {
        Some(path) => Box::new(
            File::create(path).with_context(|| format!("create {}", path.display()))?,
        ),
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
