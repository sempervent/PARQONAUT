# Repair policy

Repair plans embed the **effective policy** that produced them. Defaults:

```toml
[repair]
default_compression = "zstd"
target_row_group_mb = 128
small_file_threshold_mb = 16
merge_target_mb = 256
```

Optional override file (TOML) may be passed to `prqnt plan --policy path.toml`.

| Setting | Meaning |
|---------|---------|
| `default_compression` | Target codec for `Recompress` Safe repairs |
| `target_row_group_mb` | Target row-group size for `ResizeRowGroups` |
| `small_file_threshold_mb` | Files below this size count toward `excessive_small_files` |
| `merge_target_mb` | Target output size when merging small compatible files |

PARQONAUT does not claim ZSTD (or any codec) is universally optimal — the policy records the
operator's explicit choice for reproducibility.
