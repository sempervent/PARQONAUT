"""Pydantic contracts aligned with `paraclete-plugin-protocol` (Rust).

Execution embedding is intentionally out of scope for Phase 0. These models exist so
future bridge code can validate JSON at the boundary without inventing ad hoc dicts.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field, field_validator


class PluginExecutionPhase(StrEnum):
    PRE_SCAN = "pre_scan"
    POST_INVENTORY = "post_inventory"
    POST_RULES = "post_rules"


class DataFormat(StrEnum):
    PARQUET = "parquet"
    CSV = "csv"
    JSON = "json"
    NDJSON = "ndjson"
    UNKNOWN = "unknown"


class FindingSeverity(StrEnum):
    INFO = "info"
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class ScanRequest(BaseModel):
    """Subset of the Rust `ScanRequest` focused on JSON carried into plugins."""

    scan_id: UUID
    target: dict[str, Any]
    profile: str
    options: dict[str, Any] = Field(default_factory=dict)


class PluginCapabilities(BaseModel):
    supported_formats: list[DataFormat]
    supported_phases: list[PluginExecutionPhase]
    tags: list[str] = Field(default_factory=list)


class PluginManifest(BaseModel):
    name: str
    version: str
    entrypoint: str
    capabilities: PluginCapabilities
    metadata: dict[str, str] = Field(default_factory=dict)

    @field_validator("name", "version")
    @classmethod
    def non_blank(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("must not be empty or whitespace")
        return v


class PluginScanContext(BaseModel):
    request: ScanRequest
    discovered_files: list[str] = Field(default_factory=list)
    hints: dict[str, Any] = Field(default_factory=dict)


class PluginFinding(BaseModel):
    code: str
    severity: FindingSeverity
    summary: str
    detail: str
    evidence_ids: list[str] = Field(default_factory=list)


class PluginResult(BaseModel):
    findings: list[PluginFinding] = Field(default_factory=list)
    annotations: dict[str, Any] = Field(default_factory=dict)
