#!/usr/bin/env python3
"""Enrich a Syft CycloneDX 1.6 JSON SBOM with immutable release artifact hashes and validate it."""

from __future__ import annotations

import hashlib
import json
import pathlib
import sys
from cyclonedx.schema import SchemaVersion
from cyclonedx.validation.json import JsonStrictValidator


def sha256_file(path: pathlib.Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def main() -> int:
    if len(sys.argv) != 5:
        print(
            "Usage: build-and-validate-sbom.py <raw-cdx-json> <out-cdx-json> <release-tag> <dist-dir>",
            file=sys.stderr,
        )
        return 2

    raw_path = pathlib.Path(sys.argv[1])
    out_path = pathlib.Path(sys.argv[2])
    release_tag = sys.argv[3]
    dist_dir = pathlib.Path(sys.argv[4])

    bom = json.loads(raw_path.read_text(encoding="utf-8"))
    if bom.get("bomFormat") != "CycloneDX":
        raise SystemExit(f"Unexpected bomFormat: {bom.get('bomFormat')}")

    archives = sorted(dist_dir.glob("parqonaut-*.tar.gz"))
    if not archives:
        raise SystemExit(f"No release archives found in {dist_dir}")

    components = bom.setdefault("components", [])
    existing_refs = {c.get("bom-ref") for c in components if c.get("bom-ref")}
    root_ref = bom.get("metadata", {}).get("component", {}).get("bom-ref")

    artifact_refs: list[str] = []
    for archive in archives:
        digest = sha256_file(archive)
        ref = f"artifact:{archive.name}"
        if ref not in existing_refs:
            components.append(
                {
                    "bom-ref": ref,
                    "type": "application",
                    "name": archive.name,
                    "version": release_tag,
                    "hashes": [{"alg": "SHA-256", "content": digest}],
                    "properties": [
                        {"name": "parqonaut:artifact-type", "value": "release-tarball"},
                    ],
                }
            )
            existing_refs.add(ref)
        artifact_refs.append(ref)

    if root_ref:
        deps = bom.setdefault("dependencies", [])
        root_dep = next((d for d in deps if d.get("ref") == root_ref), None)
        if root_dep is None:
            deps.append({"ref": root_ref, "dependsOn": artifact_refs})
        else:
            existing = set(root_dep.get("dependsOn", []))
            root_dep["dependsOn"] = sorted(existing | set(artifact_refs))

    rendered = json.dumps(bom, indent=2) + "\n"
    validator = JsonStrictValidator(SchemaVersion.V1_6)
    errors = validator.validate_str(rendered)
    if errors is not None:
        raise SystemExit(f"CycloneDX 1.6 schema validation failed: {errors}")

    if len(components) < 10:
        raise SystemExit(f"Suspiciously small component count: {len(components)}")
    if not any(d.get("dependsOn") for d in bom.get("dependencies", [])):
        raise SystemExit("SBOM lacks dependency relationships")

    out_path.write_text(rendered, encoding="utf-8")
    print(
        f"Validated CycloneDX {bom.get('specVersion')} SBOM: "
        f"{len(components)} components, {len(bom.get('dependencies', []))} dependency entries, "
        f"{len(archives)} release artifacts"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
