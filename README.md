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



