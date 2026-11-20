# Release Workflow, Artifact Matrix, SBOM & Provenance (`v1.0.0-rc.1`)

## 1. Workflow Hardening (`.github/workflows/release.yml`)

- **Supported Runners:** Replaced retired `macos-14` / `macos-13` runners with current GitHub-hosted runners across all 4 targets:
  - `x86_64-unknown-linux-gnu` (`ubuntu-24.04`)
  - `aarch64-unknown-linux-gnu` (`ubuntu-24.04-arm`)
  - `aarch64-apple-darwin` (`macos-15`)
  - `x86_64-apple-darwin` (`macos-15-intel`)
- **Validation vs Publication Separation:**
  - `workflow_dispatch` executes identity verification, 4-platform `--locked` release builds, 12-step per-platform smoke verification, CycloneDX SBOM generation, SHA-256 checksums, and build provenance attestation — and uploads a single temporary `release-bundle-v<version>` artifact.
  - `Publish Draft GitHub Release (Tag Push Only)` runs strictly on `startsWith(github.ref, 'refs/tags/v') && github.event_name == 'push'`, never on `workflow_dispatch`.
- **Artifact Identity Enforcement (`.github/workflows/release.yml` — `Verify Release Identity` job):**
  - Enforces exact alignment between workspace `Cargo.toml` version, `inputs.version` / tag `v<version>`, `prqnt --version`, `BUILD_METADATA.json`, archive filename, `SHA256SUMS`, and `docs/history/v<version>/release-notes.md`.
- **12-Step Per-Platform Smoke (`scripts/release/verify-artifact.sh`):**
  - Verifies SHA-256 archive checksum, extracts into a clean isolated directory, verifies binary architecture (`file` + `uname -m`), `--version`, `--help`, `plugin list`, `tui --help`, `serve --print-openapi`, streaming CSV -> Parquet conversion + `--json scan`, and live `prqnt serve` startup (`/api/v1/health/ready`, bearer auth, `/api/v1/whoami`, `/ui/` HTML + JS + CSS).

## 2. Validated Matrix Run (Baseline Pre-Bump Validation on Commit `650bae6`)

- **Workflow Run:** [`44658428683`](https://github.com/sempervent/PARQONAUT/actions/runs/44658428683) (`workflow_dispatch`, commit `650bae6`, version `0.12.0`)
- **Job Outcomes:**
  - `Verify Release Identity`: **PASS**
  - `Build & Verify` (`x86_64-unknown-linux-gnu`): **PASS**
  - `Build & Verify` (`aarch64-unknown-linux-gnu`): **PASS**
  - `Build & Verify` (`aarch64-apple-darwin`): **PASS**
  - `Build & Verify` (`x86_64-apple-darwin`): **PASS**
  - `Assemble SBOM, Checksums & Provenance`: **PASS**
  - `Publish Draft GitHub Release (Tag Push Only)`: **SKIPPED** (as designed for `workflow_dispatch`)

### Downloaded Bundle Verification (`release-bundle-v0.12.0`)

- **`shasum -a 256 -c SHA256SUMS`:** **OK** across all 4 archives + SBOM + metadata:
  - `parqonaut-v0.12.0-aarch64-apple-darwin.tar.gz`: `b61f33084c77fa4f5a0c1dca7677af7c6dea95b9b14e7a033f8ce6e98c2ccc52`
  - `parqonaut-v0.12.0-aarch64-unknown-linux-gnu.tar.gz`: `67a0844b6691bd088ca1b93f32281b2225adad23ab52d95cfaa1840c45e89880`
  - `parqonaut-v0.12.0-x86_64-apple-darwin.tar.gz`: `1d41d31071c675ca47429f88ffec3ad433c5e94d4a047737790b925d664ce7fe`
  - `parqonaut-v0.12.0-x86_64-unknown-linux-gnu.tar.gz`: `11556d62d48141f16dc9cdaa023899035898e5b89b93130004d0844beac0031b`
- **CycloneDX 1.6 SBOM (`parqonaut-v0.12.0.cdx.json`):** `633` components, `455` dependency entries, embedded matching SHA-256 hashes for all 4 archives.
- **Provenance Attestation (`gh attestation verify <tarball> --repo sempervent/PARQONAUT`):** **PASS (exit 0)** for all 4 platform archives.
- **`BUILD_METADATA.json`:** Verified accurate `version`, `package_version`, `target`, `runner_os`, `runner_arch`, `git_sha`, `workflow_ref`, `run_id`, `locked: true`.

## 3. Final `1.0.0-rc.1` Matrix Run

- **Workflow Run:** [`44679003417`](https://github.com/sempervent/PARQONAUT/actions/runs/44679003417) (`workflow_dispatch`, `version=v1.0.0-rc.1`, commit `a97e988`)
- **Job Outcomes:**
  - `Verify Release Identity`: **PASS**
  - `Build & Verify` (`x86_64-unknown-linux-gnu`): **PASS**
  - `Build & Verify` (`aarch64-unknown-linux-gnu`): **PASS**
  - `Build & Verify` (`aarch64-apple-darwin`): **PASS**
  - `Build & Verify` (`x86_64-apple-darwin`): **PASS**
  - `Assemble SBOM, Checksums & Provenance`: **PASS**
  - `Publish Draft GitHub Release (Tag Push Only)`: **SKIPPED** (as designed for `workflow_dispatch`)

### Downloaded Bundle Verification (`release-bundle-v1.0.0-rc.1`)

- **`shasum -a 256 -c SHA256SUMS`:** **OK** across all 4 archives + SBOM + metadata:
  - `parqonaut-v1.0.0-rc.1-aarch64-apple-darwin.tar.gz`: `3b6f1e1ec9f07e89af0d007931db6d662ab66ad4ef63e71c2b083a31ecb98d23`
  - `parqonaut-v1.0.0-rc.1-aarch64-unknown-linux-gnu.tar.gz`: `59f9c20b25485ba4849dfc3470a273930ffd49df91e72c2cb2e3367a00be3405`
  - `parqonaut-v1.0.0-rc.1-x86_64-apple-darwin.tar.gz`: `11da3da8ff6cf7293402828da517b406460f656274c18e5eb86ccdc7e1316817`
  - `parqonaut-v1.0.0-rc.1-x86_64-unknown-linux-gnu.tar.gz`: `377fc1e33c48e0f668c945384421eb894d3f460c0481345f406d575b48e957d3`
- **CycloneDX 1.6 SBOM (`parqonaut-v1.0.0-rc.1.cdx.json`):** `633` components, `455` dependency entries, embedded matching SHA-256 hashes for all 4 archives.
- **Provenance Attestation (`gh attestation verify <tarball> --repo sempervent/PARQONAUT`):** **PASS (exit 0)** for all 4 platform archives.
