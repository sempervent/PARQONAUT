# Pre-1.0 roadmap (summary)

Detailed planning lives in the repository at `docs/development/pre-1.0-roadmap.md` (not part of the public site nav).

| Release | Direction |
|---------|-----------|
| **v1.0.0-rc.1** (current) | Release candidate qualification: 4-platform release matrix + CycloneDX 1.6 SBOM + build provenance, Rust/Python security audits, sustained Linux `x86_64` libFuzzer campaigns, Docker/Postgres Compose/RustFS S3 validation, public contract freeze, performance envelopes, and 12-scenario failure-recovery drills |
| **v0.12.0** (shipped on `main`) | Production hardening: incremental UTF-8 SSE, atomic job/event transactions, historical DB upgrade & backup/restore, Criterion benchmarks, `proptest`/fuzz/fault-injection, `Cargo.lock` + `deny.toml`, binary release & Docker distribution |
| **v0.11.0** (shipped) | Operational event journal, `prqnt tui`, bundled `/ui/` web dashboard, `Idempotency-Key` safety (`3f5afbe`) |
| **v0.10.0** (shipped) | Plugin execution bridge (`1631a3c`, [v0.10.0 notes](./history/v0.10.0/release-notes.md)) |
| **v0.9.2** | Bounded streaming S3 Parquet publication; full local/S3 routing for convert, transform, and fused specs |
| **v0.9.0** | Unified Arrow/Parquet 54 stack, canonical schema compatibility, in-memory transform fusion, storage columnar I/O |
| **v1.0.0** | Stable API and operational guarantees |

No release dates are committed here.
