# OpenAPI

The authoritative API description is generated at runtime:

```bash
prqnt serve --print-openapi
```

The repository golden file `fixtures/api/openapi-v1.json` is checked in CI (`cargo xtask docs openapi --check`).

## Download from the documentation site

When built for GitHub Pages, the same JSON is published at:

```text
/openapi/openapi.json
```

(relative to the site root, e.g. `https://sempervent.github.io/PARQONAUT/openapi/openapi.json`).

## Versioning

- **Product version** (e.g. **0.7.1**) — PARQONAUT release semver in `info.version`.
- **API path version** — **`/api/v1`** prefix; not incremented with every product patch.

## Authentication

All mutating and most read routes require `Authorization: Bearer <token>`. See [Authentication](./authentication.md) and [HTTP API](./api.md).
