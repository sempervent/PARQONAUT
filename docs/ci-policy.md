# CI policy and `parqonaut check`

`parqonaut check` scans and plans without mutating data, then evaluates CI gates.

## Example policy

```toml
[ci]
fail_on_safe_findings = false
fail_on_review_required = true
fail_on_destructive = true
fail_on_unresolvable_schema = true
```

## Exit codes

| Code | Meaning |
|------|---------|
| 0 | Compliant |
| 2 | Repairable safe issues present |
| 3 | Review-required operations remain |
| 4 | Unresolvable schema conflict or destructive proposal |
| 5 | Operational failure |

```bash
parqonaut check dataset/ --policy ci-policy.toml
parqonaut check dataset/ --policy ci-policy.toml --json
```
