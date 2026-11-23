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

## 3. Remediated `1.0.0-rc.1` Matrix Run (Pinned Syft SHA-256 + Final Code Commit `3f05280`)

- **Workflow Run:** [`45103052595`](https://github.com/sempervent/PARQONAUT/actions/runs/45103052595) (`workflow_dispatch`, `version=v1.0.0-rc.1`, commit `3f05280`)
- **Syft Supply-Chain Hardening:** Replaced `curl ... install.sh | sh` in `.github/workflows/release.yml` with pinned dual SHA-256 verification of `syft_1.57.2_checksums.txt` and `syft_1.57.2_linux_amd64.tar.gz`.
- **Job Outcomes:**
  - `Verify Release Identity`: **PASS**
  - `Build & Verify` (`x86_64-unknown-linux-gnu`): **PASS**
  - `Build & Verify` (`aarch64-unknown-linux-gnu`): **PASS**
  - `Build & Verify` (`aarch64-apple-darwin`): **PASS**
  - `Build & Verify` (`x86_64-apple-darwin`): **PASS**
  - `Assemble SBOM, Checksums & Provenance`: **PASS**
  - `Publish Draft GitHub Release (Tag Push Only)`: **SKIPPED** (as designed for `workflow_dispatch`)

### Downloaded Bundle Verification (`release-bundle-v1.0.0-rc.1`, Run `45103052595`)

- **`shasum -a 256 -c SHA256SUMS`:** **OK** across all 4 archives + SBOM + metadata:
  - `parqonaut-v1.0.0-rc.1-aarch64-apple-darwin.tar.gz`: `098e60f45d9acbe1739b6518343c0ad2061919838dd48eddbee1169fe9df6a74`
  - `parqonaut-v1.0.0-rc.1-aarch64-unknown-linux-gnu.tar.gz`: `7dabc1e053d70c7bbcc15f6e9fb4e1dacce78a17dd0e3daf06055b720c168651`
  - `parqonaut-v1.0.0-rc.1-x86_64-apple-darwin.tar.gz`: `3c7621aa4e0edc4541042baa95c9379058f48887d594bbeb4e270f2eb0a3c98f`
  - `parqonaut-v1.0.0-rc.1-x86_64-unknown-linux-gnu.tar.gz`: `2c96c02d9effd88b6a1fef0242e11516d9064dab4904d41f9fbcc3a07218b25a`
  - `parqonaut-v1.0.0-rc.1.cdx.json`: `614a3cbf6e45e297879c466483301280412c083200d2eb64033a2f826d23d0f4`
  - `cargo-metadata-v1.0.0-rc.1.json`: `2142ff515ab267279de6f7480a2b76fea80f8d6cbf4d1e9b1f2f6a1447088ed4`
- **CycloneDX 1.6 SBOM (`parqonaut-v1.0.0-rc.1.cdx.json`):** `633` components, `455` dependency entries, tool `syft 1.57.2`, embedded matching SHA-256 hashes for all 4 archives.
- **Provenance Attestation (`gh attestation verify <tarball> --repo sempervent/PARQONAUT`):** **PASS (exit 0)** for all 4 platform archives.

*(Prior initial RC1 run [`44679003417`](https://github.com/sempervent/PARQONAUT/actions/runs/44679003417) on `a97e988` also passed before final defect remediations.)*
