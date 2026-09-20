# v0.8.0 parallel audit (read-only)

Seven concurrent read-only reviews were run against the release branch working tree. Summaries below; full agent transcripts are in the session audit log.

## Audit A — Partition safety

- **P1:** High cardinality partitions unbounded in memory maps; spec hardcodes `max_open_partitions = 64`.
- **P1:** Column names containing `/` can nest paths via `PathBuf::push`.
- **P2:** Composite key delimiter `\x1f` not escaped in values.

## Audit B — Merge/split

- **Fixed in release branch:** single-file merge now honors `-o file.parquet` via `exact_output`.
- **P2:** Size caps use in-memory batch estimates, not compressed bytes.
- **P2:** `target_row_groups` still ignored on split CLI/spec.

## Audit C — Spec

- **Fixed:** `partition-by` YAML alias; `--check` runs `compile_plan`; S3 paths rejected at compile.
- **P2:** Non-terminal partition steps cannot predict outputs until directories exist.

## Audit D — Schema

- **Fixed:** same-type coercion clones arrays; stringify uses display conversion.
- **P2:** CSV discovery ignores CLI delimiter/NA overrides during introspection.
- **P2:** Shared vectors exercise stream only, not repair lattice (documented).

## Audit E — Checkpoint

- **Fixed:** fresh runs wipe prior `.staging` directory.
- **P2:** Identity omits CSV parse knobs; file-level resume only (docs aligned).

## Audit F — Progress

- **Fixed:** terminal observer wired when progress enabled; `--json-progress` on convert.
- **P2:** sparse event coverage (start/complete only); extend in v0.8.x.

## Audit G — Storage

- **P1 (disclosed):** Transform v0.8 local-only; repair/storage publication unchanged.
- **Fixed:** docs and compile-time rejection for `s3://` spec paths.

## Disposition

No open **P0** items at tag time. Remaining **P1/P2** items are recorded in acceptance deferrals or follow-up issues.
