# TurboLP
TurboLogParsers is a performance-oriented PoC for parsing large log files into JSONL.

## Features
→ **modular system**: Core processor handles file content, modules only describe how to transform a single line

→ **File streaming**: Never loads the full file into RAM 

→ **Multithreading**: Default behavior

→ **Gzip support**: Automatically handle gzip files

## Modules supported
Currently, only one module exists: **web-access**

## Usage

### List available modules

```bash
./minimal-parser list
```

Example output:

```
Available modules:
  web-access      - Parses Apache/Nginx access logs (common/combined) -> JSONL
  mactime         - Parses UAC bodyfile lines -> JSONL
  cvs-dummy       - Demo CSV parser
```

### Run a module

```bash
./minimal-parser run --module web-access --input data_sample/web_access_sample.log --output out.jsonl
```

### Gzip files work automatically

```bash
./minimal-parser run --module web-access  --input data_sample/web_access_sample.log.gz
```

## Output filename differentiator


Use `--prefix-input-hash` to prefix the output filename with a short hash of the input path.

```bash
./TurboLP run \
  --module web-access \
  --input extract_uac/apache/web_access.log \
  --output web_access.jsonl \
  --prefix-input-hash
```

Example output path:

```text
3f9a21c0-web_access.jsonl
```

This lets multiple inputs with the same basename write into the same output directory without collisions:

```bash
./TurboLP run --module mactime --input extract_uac/apache/web_access.log --output web_access.jsonl  --prefix-input-hash
./TurboLP run --module mactime --input extract_uac/backup/web_access.log --output web_access.jsonl  --prefix-input-hash
```

Possible output files:

```text
/out/7c1e900a-web_access.jsonl
/out/b84ad2fe-web_access.jsonl
```



