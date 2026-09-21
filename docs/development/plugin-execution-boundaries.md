# Plugin execution boundaries (v0.9)

v0.9 converges **in-process** columnar execution. Plugin loading is planned for **v0.10.0**; this document names stable extension surfaces after convergence.

| Surface | Crate | Notes |
|---------|-------|-------|
| `BatchTransform` (batch in → batch out) | `parqonaut-columnar` / transform engine | Natural hook for user-defined row transforms in fused pipelines |
| Scan / finding providers | `paraclete-core`, `parqonaut-plugin-host` | Versioned via `parqonaut-plugin-protocol` (v1) |
| Schema policy extension | `parqonaut-columnar::schema`, repair `SchemaPolicy` | Widen/stringify and repair safety must stay aligned |
| Post-scan analyzers | `paraclete-report` | Read-only over scan artifacts |

Execution remains **`parqonaut-app`** → engines; plugins must not introduce parallel storage or columnar stacks.
