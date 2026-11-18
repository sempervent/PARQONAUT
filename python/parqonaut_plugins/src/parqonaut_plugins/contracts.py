"""Pydantic contracts aligned with `parqonaut-plugin-protocol` (Rust), protocol version 1."""

from __future__ import annotations

from enum import StrEnum
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field, field_validator, model_validator

PLUGIN_PROTOCOL_VERSION = 1


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
    scan_id: UUID
    target: dict[str, Any]
    profile: str
    options: dict[str, Any] = Field(default_factory=dict)


class ScanAnalyzerCapabilities(BaseModel):
    supported_formats: list[DataFormat]
    supported_phases: list[PluginExecutionPhase]
    tags: list[str] = Field(default_factory=list)


class BatchTransformCapabilities(BaseModel):
    tags: list[str] = Field(default_factory=list)


class PluginCapabilities(BaseModel):
    scan: ScanAnalyzerCapabilities | None = None
    batch_transform: BatchTransformCapabilities | None = None

    @model_validator(mode="after")
    def at_least_one_capability(self) -> PluginCapabilities:
        if self.scan is None and self.batch_transform is None:
            msg = "capabilities must include scan and/or batch_transform"
            raise ValueError(msg)
        return self


class PluginManifest(BaseModel):
    protocol_version: int
    name: str
    version: str
    entrypoint: str
    capabilities: PluginCapabilities
    requires_parqonaut: str | None = None
    metadata: dict[str, str] = Field(default_factory=dict)

    @field_validator("protocol_version")
    @classmethod
    def protocol_v1_only(cls, v: int) -> int:
        if v != PLUGIN_PROTOCOL_VERSION:
            raise ValueError(f"unsupported protocol_version {v}")
        return v

    @field_validator("name", "version")
    @classmethod
    def non_blank(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("must not be empty or whitespace")
        return v


class PluginScanContext(BaseModel):
    request: ScanRequest
    discovered_files: list[str] = Field(default_factory=list)
    inventory_summary: dict[str, Any] = Field(default_factory=dict)
    builtin_finding_summaries: list[Any] = Field(default_factory=list)
    hints: dict[str, Any] = Field(default_factory=dict)


class PluginFindingContribution(BaseModel):
    code: str
    severity: FindingSeverity
    summary: str
    detail: str
    evidence_ids: list[str] = Field(default_factory=list)


class PluginResult(BaseModel):
    findings: list[PluginFindingContribution] = Field(default_factory=list)
    annotations: dict[str, Any] = Field(default_factory=dict)


class PluginRequest(BaseModel):
    protocol_version: int
    manifest: PluginManifest
    phase: PluginExecutionPhase
    context: PluginScanContext
    config: dict[str, Any] = Field(default_factory=dict)


class PluginResponse(BaseModel):
    protocol_version: int
    plugin: str
    result: PluginResult
