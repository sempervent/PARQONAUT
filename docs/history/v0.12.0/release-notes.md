# PARQONAUT v0.12.0 Release Notes

**Theme:** Reliability, Distribution, and 1.0 Readiness

PARQONAUT v0.12.0 hardens execution atomicity, streaming parser boundaries, database upgrade/backup guarantees, supply-chain reproducibility, and binary/container packaging on the path to v1.0.0.

## Highlights

- **Incremental UTF-8 SSE & Resilient Streaming:** Rewrote `parqonaut-client` `SseParser` to frame raw bytes before UTF-8 decoding, enforce explicit line/frame size caps (`512 KiB` / `2 MiB`), reject mid-frame EOFs cleanly, and deduplicate on reconnect; aligned `/ui/sse.js` `TextDecoder` flush handling.
- **Atomic Lifecycle & Event Consistency:** Coupled SQLite and PostgreSQL job enqueue, claim, cancel, completion, and failure state transitions with their durable `application_job_events` writes inside single transactions; added canonical JSON hashing (`sort_json_value`) and bounded expired `idempotency_keys` pruning; wired graceful `SIGINT`/`SIGTERM` worker drain and event flushing.
- **Arrow IPC & Error-Boundary Hardening:** Added `validate_ipc_stream_framing` in `parqonaut-plugin-host` after fuzzing caught a `capacity overflow` panic in `arrow-ipc 54.3.1` on negative metadata lengths; hardened `sanitize_event_message` and HTTP `AppError` responses against secret/path leakage.
- **Historical DB Upgrade & Backup Validation:** Added automated upgrade and WAL `VACUUM INTO` / `pg_dump` recovery verification covering v0.8, v0.9/v0.10, and v0.11 schemas.
- **Benchmarks, Property Testing, Fuzzing & Fault Injection:** Replaced placeholder benchmarks with `parqonaut-bench` (Criterion + RSS baseline + bounded-memory tests); added `proptest` suites, 9 libFuzzer targets + corpora with CI smoke tests, and expanded storage/service fault-injection suites.
- **Reproducible Distribution & Operations Runbook:** Committed root `Cargo.lock` with `--locked` CI gates, `deny.toml`, `.github/dependabot.yml`, `.github/workflows/release.yml` (Linux x86_64/aarch64 & macOS arm64/x86_64 tarballs, SHA-256 checksums, SBOM, build provenance attestations), `Dockerfile`, `compose.yaml` (with optional Postgres profile), and `docs/operations.md`.
