## Summary

- Public documentation site via **mdBook 0.5.4** with deliberate `docs/SUMMARY.md` navigation
- **GitHub Pages** workflow: PR builds validate only; `main` builds and deploys `target/pages/`
- Generated **CLI reference** (`cargo xtask docs cli`) and checked **OpenAPI golden** (`cargo xtask docs openapi`)
- Site publishes **OpenAPI JSON** and **rustdoc**; engineering history stays out of public nav
- Workspace **0.7.1** — documentation and delivery infrastructure; **no intended runtime behavior changes**

## mdBook

- Pinned version: `0.5.4` (`scripts/docs/env.sh`, `docs/MDBOOK_VERSION`)
- Commands: `just docs-check`, `just docs-build`

## Pages

- Build type: GitHub Actions workflow (not `gh-pages` branch)
- Expected URL: `https://sempervent.github.io/PARQONAUT/` (confirm from Pages API after deploy)

## Test plan

- [ ] `documentation` job green on PR
- [ ] Existing `rust`, `api-integration`, `postgres-integration`, `s3-integration` green
- [ ] After merge: Pages deploy succeeds; home, CLI, OpenAPI, rustdoc load under `/PARQONAUT/`
