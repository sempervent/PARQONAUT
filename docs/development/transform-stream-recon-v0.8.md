# Transform + stream reconnaissance (v0.8.0)

Verified against `feat/transform-stream` branch baseline (post v0.7.1).

## Dependency stacks (unchanged in v0.8)

| Area | Arrow/Parquet |
|------|----------------|
| Scan (`paraclete-core`) | parquet 53 |
| Transform (`parqonaut-transform`) | arrow 54, parquet 54 |
| Stream (`parqonaut-stream`) | arrow2 0.18, parquet2 0.17 |

**v0.8 action:** no wholesale convergence. **v0.9 impact:** all new contracts must be engine-neutral (`parqonaut-workflow`).

## Transform (`parqonaut-transform`)

| Capability | Existing code | Completeness | v0.8 action |
|------------|---------------|--------------|-------------|
| Rewrite | `engine/merge.rs` `rewrite_parquet_file`, `cli/commands.rs` | **Works** (local) | Expose via `prqnt rewrite`; S3 via storage |
| Merge | `merge_parquet_files` | **Engine works**; CLI `run_merge` **stub** | Wire `prqnt merge` |
| Split | `split_parquet_file` | **Engine works**; CLI **stub** | Wire `prqnt split` |
| Partition | `output/partition.rs` | **Placeholder** (`Unsupported`) | Implement Hive writer + LRU |
| Spec | `spec/types.rs`, `spec/parse.rs` | Types exist; no `schema-version`; execution partial | Validate, plan, execute |
| Filter/cast/rename | `engine/*` | Library code present | Spec + rewrite paths |

**Technical debt:** `#![allow(dead_code)]` on crate; internal `parqknife`/`Commands` CLI not exposed on `prqnt`; naive `sanitize_partition_value`.

## Stream (`parqonaut-stream`)

| Capability | Existing code | Completeness | v0.8 action |
|------------|---------------|--------------|-------------|
| Discovery | `discover.rs` | Works | Use in convert |
| CSV/Parquet read | `csv_in`, `parquet_in` | Works | Integrate resume offsets |
| Schema unify | `schema.rs` `from_schemas`, `widen_types` | **Logic exists** | Wire pipeline (not `default()`) |
| Batch align | `coercion.rs` `BatchAligner` | **Implemented** | Use in pipeline |
| Pipeline | `pipeline.rs` | **`UnifiedSchema::default()`**; writer uses `col_N` | Fix discovery + alignment |
| State | `state.rs` | Model exists; **not in pipeline**; truncate write | Atomic save + resume |
| Progress | `progress.rs` | indicatif only | `ProgressObserver` events |

## v0.9 convergence impact

Document boundaries in `columnar-convergence-plan.md` and `in-memory-pipeline-design.md`. Stream/transform must not add new arrow2-only public types in `parqonaut-app`.
