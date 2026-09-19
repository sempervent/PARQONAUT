# Scanning

`prqnt scan` performs a **forensic scan** of a local path or an **`s3://`** dataset prefix.

```bash
prqnt scan ./dataset
prqnt scan s3://my-bucket/prefix/ --profile standard
```

Scans produce structured findings, summaries, and dataset identity used by diagnose/plan/repair. On the server, **`POST /api/v1/scans`** submits a **durable async scan job** with the same application semantics as the CLI.

See also [Object storage](./object-storage.md) and [CLI reference](./reference/cli.md).
