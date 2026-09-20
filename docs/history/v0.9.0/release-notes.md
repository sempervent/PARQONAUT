# PARQONAUT v0.9.0 — Unified columnar pipeline

## Highlights

- Single **Apache Arrow 54.3.1** / **Parquet 54.3.1** stack across stream, transform, repair, and storage columnar I/O.
- Removal of **arrow2** / **parquet2** from product crates (`just columnar-check`).
- Shared **`parqonaut-columnar`** batch boundary: `BatchSource`, `BatchSink`, bounded `BatchStream`, backpressure relay.
- Canonical **schema compatibility** in `parqonaut-columnar::schema` (stream unification + repair classification).
- **In-memory transform spec fusion** for streamable step chains with explicit barriers in dry-run plans.
- **Storage-backed** Parquet read/write (local and S3 range/multipart) for rewrite/merge remote paths.
- Progress event vocabulary extended on `parqonaut-workflow` for future CLI/server/TUI consumers.

## Not in this release

- Plugin execution bridge → **v0.10.0**
- TUI and web dashboard → **v0.11.0**

## Upgrade notes

- Stream checkpoints remain schema version 1 where compatible; stale-source resume behavior unchanged.
- Repair **plan semantics** (safety, authorization, manifest) are unchanged; execution machinery may reuse shared batch primitives.
