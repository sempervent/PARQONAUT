# CLI reference (generated)

This page is generated from the live `prqnt` command tree. Do not edit by hand — run `cargo xtask docs cli`.

## `Global`

```text
PARQONAUT — Parquet Analysis, Rewriting, Quality, Orchestration, Navigation, Auditing, Unification & Transformation

Usage: prqnt [OPTIONS] <COMMAND>

Commands:
  scan       Forensic scan of local paths or s3:// dataset prefixes
  inspect    Inspect Parquet schema and row-group metadata
  partition  Hive-style partition of a Parquet file
  merge      Merge Parquet files or a dataset directory
  split      Split a Parquet file into smaller parts
  transform  Declarative multi-step transform workflow
  rewrite    Rewrite or recompress a Parquet file
  diagnose   Diagnose dataset repair opportunities from scan evidence
  plan       Generate an evidence-bound repair plan
  plan-diff  Diff two repair plans
  check      CI-oriented policy compliance check (non-mutating)
  repair     Execute a repair plan to a separate output directory or s3:// prefix
  verify     Verify repaired dataset against a before scan
  doctor     Integrated scan → diagnose → plan (optional safe repair)
  batch      Multi-dataset batch orchestration
  serve      Run the PARQONAUT HTTP application server (`/api/v1`)
  convert    Stream-convert CSV/Parquet inputs
  help       Print this message or the help of the given subcommand(s)

Options:
      --json     Emit JSON where supported
  -h, --help     Print help
  -V, --version  Print version
```

## `scan`

```text
Forensic scan of local paths or s3:// dataset prefixes

Usage: prqnt scan [OPTIONS] <PATH>

Arguments:
  <PATH>  Local path or s3:// URI to scan

Options:
      --json               Emit JSON where supported
  -p, --profile <PROFILE>  [default: standard]
  -h, --help               Print help
```

## `inspect`

```text
Inspect Parquet schema and row-group metadata

Usage: prqnt inspect [OPTIONS] <INPUT>

Arguments:
  <INPUT>  

Options:
      --json   Emit JSON where supported
      --stats  Show detailed column statistics
  -h, --help   Print help
```

## `rewrite`

```text
Rewrite or recompress a Parquet file

Usage: prqnt rewrite [OPTIONS] <INPUT> <OUTPUT>

Arguments:
  <INPUT>   
  <OUTPUT>  

Options:
      --compression <COMPRESSION>              Compression codec (snappy, gzip, zstd, uncompressed)
      --json                                   Emit JSON where supported
      --row-group-size-mb <ROW_GROUP_SIZE_MB>  
      --projection <PROJECTION>                
      --filter <FILTER>                        
      --rebuild-stats                          
  -h, --help                                   Print help
```

## `convert`

```text
Stream-convert CSV/Parquet inputs

Usage: prqnt convert [OPTIONS] --out <OUT> <INPUTS>...

Arguments:
  <INPUTS>...  Input file(s), directories, or globs

Options:
      --json
          Emit JSON where supported
  -o, --out <OUT>
          
      --out-format <OUT_FORMAT>
          [possible values: csv, parquet]
      --compression <COMPRESSION>
          [default: none] [possible values: none, snappy, gzip, zstd]
      --zstd-level <ZSTD_LEVEL>
          [default: 3]
      --plan
          
      --dry-run
          
      --state <STATE>
          
      --resume
          
      --schema-conflicts <SCHEMA_CONFLICTS>
          [default: strict]
  -h, --help
          Print help
```

## `diagnose`

```text
Diagnose dataset repair opportunities from scan evidence

Usage: prqnt diagnose [OPTIONS] <PATH>

Arguments:
  <PATH>  Local path or s3:// dataset URI

Options:
      --json  Emit JSON where supported
  -h, --help  Print help
```

## `plan`

```text
Generate an evidence-bound repair plan

Usage: prqnt plan [OPTIONS] <PATH>

Arguments:
  <PATH>  Local path or s3:// dataset URI

Options:
      --json                           Emit JSON where supported
      --output <OUTPUT>                
      --policy <POLICY>                TOML policy file path
      --target-schema <TARGET_SCHEMA>  Explicit target schema JSON
      --canonical                      Emit canonical deterministic plan JSON
  -h, --help                           Print help
```

## `plan-diff`

```text
Diff two repair plans

Usage: prqnt plan-diff [OPTIONS] <LEFT> <RIGHT>

Arguments:
  <LEFT>   
  <RIGHT>  

Options:
      --json  Emit JSON where supported
  -h, --help  Print help
```

## `check`

```text
CI-oriented policy compliance check (non-mutating)

Usage: prqnt check [OPTIONS] <PATH>

Arguments:
  <PATH>  Local path or s3:// dataset URI

Options:
      --json             Emit JSON where supported
      --policy <POLICY>  TOML CI policy file
  -h, --help             Print help
```

## `repair`

```text
Execute a repair plan to a separate output directory or s3:// prefix

Usage: prqnt repair [OPTIONS] --plan <PLAN> --output <OUTPUT> <PATH>

Arguments:
  <PATH>  Source dataset local path or s3:// URI

Options:
      --json                   Emit JSON where supported
      --plan <PLAN>            
      --output <OUTPUT>        Output local directory or s3:// dataset prefix
      --authorize <AUTHORIZE>  Explicitly authorize a ReviewRequired operation ID
  -h, --help                   Print help
```

## `verify`

```text
Verify repaired dataset against a before scan

Usage: prqnt verify [OPTIONS] <BEFORE> <AFTER>

Arguments:
  <BEFORE>  Before dataset local path or s3:// URI
  <AFTER>   

Options:
      --json                 Emit JSON where supported
      --manifest <MANIFEST>  Optional execution manifest path
  -h, --help                 Print help
```

## `doctor`

```text
Integrated scan → diagnose → plan (optional safe repair)

Usage: prqnt doctor [OPTIONS] <PATH>

Arguments:
  <PATH>  Local path or s3:// dataset URI

Options:
      --json                   Emit JSON where supported
      --policy <POLICY>        TOML policy file path (same as plan)
      --repair                 Execute authorized repairs after showing the plan
      --output <OUTPUT>        Output local directory or s3:// dataset prefix
      --authorize <AUTHORIZE>  
  -h, --help                   Print help
```

## `batch`

```text
Multi-dataset batch orchestration

Usage: prqnt batch [OPTIONS] <COMMAND>

Commands:
  check   Validate batch configuration without executing
  plan    Produce a durable batch plan
  repair  Execute an approved batch plan
  status  Read run journal status
  resume  Resume an interrupted batch run
  verify  Verify batch outputs against the plan
  help    Print this message or the help of the given subcommand(s)

Options:
      --json  Emit JSON where supported
  -h, --help  Print help
```

## `batch check`

```text
Validate batch configuration without executing

Usage: prqnt batch check [OPTIONS] --config <CONFIG>

Options:
      --config <CONFIG>  
      --json             Emit JSON where supported
  -h, --help             Print help
```

## `batch plan`

```text
Produce a durable batch plan

Usage: prqnt batch plan [OPTIONS] --config <CONFIG>

Options:
      --config <CONFIG>  
      --json             Emit JSON where supported
      --output <OUTPUT>  
  -h, --help             Print help
```

## `batch repair`

```text
Execute an approved batch plan

Usage: prqnt batch repair [OPTIONS] --plan <PLAN>

Options:
      --json         Emit JSON where supported
      --plan <PLAN>  
      --jobs <JOBS>  
      --dry-run      
  -h, --help         Print help
```

## `batch status`

```text
Read run journal status

Usage: prqnt batch status [OPTIONS] --run-dir <RUN_DIR>

Options:
      --json               Emit JSON where supported
      --run-dir <RUN_DIR>  
  -h, --help               Print help
```

## `batch resume`

```text
Resume an interrupted batch run

Usage: prqnt batch resume [OPTIONS] --run-dir <RUN_DIR>

Options:
      --json               Emit JSON where supported
      --run-dir <RUN_DIR>  
      --jobs <JOBS>        
      --dry-run            
  -h, --help               Print help
```

## `batch verify`

```text
Verify batch outputs against the plan

Usage: prqnt batch verify [OPTIONS] --run-dir <RUN_DIR>

Options:
      --json               Emit JSON where supported
      --run-dir <RUN_DIR>  
  -h, --help               Print help
```

## `serve`

```text
Run the PARQONAUT HTTP application server (`/api/v1`)

Usage: prqnt serve [OPTIONS]

Options:
      --json                   Emit JSON where supported
      --listen <LISTEN>        Listen address (default loopback-only) [default: 127.0.0.1:8080]
      --database <DATABASE>    SQLite or Postgres database URL
      --state-dir <STATE_DIR>  Application state directory (database + artifacts)
      --workers <WORKERS>      In-process async job workers [default: 2]
      --config <CONFIG>        Optional TOML config (server + storage policy)
      --print-openapi          Emit OpenAPI JSON to stdout and exit
  -h, --help                   Print help
```

