# Server storage policy

Network-facing `prqnt serve` must not accept arbitrary local paths or S3 destinations.

Configure allow lists in TOML:

```toml
[server]
allowed_local_roots = ["/data/input", "/data/output"]

[storage]
allowed_local_roots = ["/data/work"]
allowed_s3_buckets = ["my-bucket"]
allowed_s3_prefixes = ["my-bucket/datasets/"]
```

Requests referencing locations outside policy are rejected **before** storage I/O. Job JSON must not contain AWS access keys or bearer secrets.

CLI `prqnt` commands use unrestricted local access by default (interactive/trusted operator model).
