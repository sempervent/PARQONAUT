# v0.7.1 documentation audit (repository record)

Not published on the public documentation site.

## Scopes reviewed

| Agent scope | Result |
|-------------|--------|
| Getting started / CLI | Examples use `prqnt`; generated CLI reference matches Clap |
| Repair / schema / batch | Conceptual pages link to existing deep docs; boundaries documented |
| S3 / storage | RustFS described as test backend; `S3StorageBackend` named |
| Server / API / security | Matches v0.7 serve, jobs, auth, storage policy |
| Architecture / provenance | Updated architecture diagram; history not in SUMMARY |
| Site / links | SUMMARY-only public set; link checker on active docs |

## Fixes applied

- Architecture page updated for `parqonaut-app` and `prqnt serve`
- v0.7 limitations documented on docs home
- OpenAPI product vs `/api/v1` path version clarified
