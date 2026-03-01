"""Typed component models for Atlas Command entities, tasks, and objects.

These models provide type safety for component data
before it is transmitted to the Atlas Command API.
Refactored to use standard Python dataclasses to avoid Pydantic/Rust dependencies
on low-power hardware.
"""

from __future__ import annotations

from dataclasses import dataclass, field, fields
from datetime import datetime
from typing import Any, List, Literal, Optional, Union


def _check_numeric(name: str, value: Any) -> None:
    """Raise TypeError if value is not None and not a real (non-bool) number."""
    if value is None:
        return
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{name} must be a number, got {type(value).__name__}")


def _check_timestamp(name: str, value: Any) -> None:
    """Raise ValueError if value is not None and not a valid ISO 8601 / RFC 3339 string."""
    if value is not None:
        if not isinstance(value, str):
            raise TypeError(f"{name} must be a string, got {type(value).__name__}")
        # Normalize RFC 3339 UTC designator "Z" to "+00:00" for datetime.fromisoformat
        parsed_value = value[:-1] + "+00:00" if value.endswith("Z") else value
        try:
            datetime.fromisoformat(parsed_value)
        except ValueError:
            raise ValueError(f"{name} must be a valid RFC 3339 timestamp, got '{value}'")


def _exclude_none(data: Any) -> Any:
    """Recursively remove None values from a dictionary or list."""
    if isinstance(data, dict):
        return {k: _exclude_none(v) for k, v in data.items() if v is not None}
    elif isinstance(data, list):
        return [_exclude_none(v) for v in data]
    return data


@dataclass
class AtlasModel:
    """Base class providing Pydantic-like model_dump for compatibility."""

    def model_dump(self, exclude_none: bool = False, by_alias: bool = False) -> dict[str, Any]:
        """Convert the model to a dictionary.

        Args:
            exclude_none: Whether to exclude fields with None values.
            by_alias: Included for API compatibility with Pydantic.

        Returns:
            Dictionary representation of the model.
        """

        def serialize(obj: Any) -> Any:
            if isinstance(obj, AtlasModel):
                res = {}
                # Include defined fields
                for f in fields(obj):
                    val = getattr(obj, f.name)
                    res[f.name] = serialize(val)
                # Include extra attributes (for models that allow extra fields)
                for k, v in obj.__dict__.items():
                    if k.startswith("custom_") and k not in res:
                        res[k] = serialize(v)
                return res
            elif isinstance(obj, list):
                return [serialize(i) for i in obj]
            elif isinstance(obj, dict):
                return {k: serialize(v) for k, v in obj.items()}
            return obj

        data = serialize(self)
        if exclude_none:
            return _exclude_none(data)
        return data


# === Entity Components ===


@dataclass
class TelemetryComponent(AtlasModel):
    """Position and motion data for entities."""

    latitude: Optional[float] = None
    longitude: Optional[float] = None
    altitude_m: Optional[float] = None
    speed_m_s: Optional[float] = None
    heading_deg: Optional[float] = None

    def __post_init__(self):
        for name in ("latitude", "longitude", "altitude_m", "speed_m_s", "heading_deg"):
            _check_numeric(name, getattr(self, name))
        if self.latitude is not None and not (-90 <= self.latitude <= 90):
            raise ValueError("latitude must be between -90 and 90")
        if self.longitude is not None and not (-180 <= self.longitude <= 180):
            raise ValueError("longitude must be between -180 and 180")
        if self.speed_m_s is not None and self.speed_m_s < 0:
            raise ValueError("speed_m_s must be non-negative")
        if self.heading_deg is not None and not (0 <= self.heading_deg < 360):
            raise ValueError("heading_deg must be between 0 (inclusive) and 360 (exclusive)")


@dataclass
class GeometryComponent(AtlasModel):
    """GeoJSON geometry for geoentities."""

    type: Literal["Point", "LineString", "Polygon"]
    coordinates: Union[List[float], List[List[float]], List[List[List[float]]]]


@dataclass
class TaskCatalogComponent(AtlasModel):
    """Lists supported task identifiers for an asset."""

    supported_tasks: List[str] = field(default_factory=list)


@dataclass
class MediaRefItem(AtlasModel):
    """A reference to a media object."""

    object_id: str
    role: Literal["camera_feed", "thumbnail", "heatmap_data"]


@dataclass
class MilViewComponent(AtlasModel):
    """Military tactical classification component."""

    classification: Literal["friendly", "hostile", "neutral", "unknown", "civilian"]
    last_seen: Optional[str] = None

    def __post_init__(self):
        _check_timestamp("last_seen", self.last_seen)


@dataclass
class HealthComponent(AtlasModel):
    """Health and vital statistics for entities."""

    battery_percent: Optional[int] = None

    def __post_init__(self):
        _check_numeric("battery_percent", self.battery_percent)
        if self.battery_percent is not None:
            if not (0 <= self.battery_percent <= 100):
                raise ValueError("battery_percent must be between 0 and 100")


@dataclass
class SensorRefItem(AtlasModel):
    """A reference to a sensor with FOV/orientation metadata."""

    sensor_id: str
    type: str
    vertical_fov: Optional[float] = None
    horizontal_fov: Optional[float] = None
    vertical_orientation: Optional[float] = None
    horizontal_orientation: Optional[float] = None

    def __post_init__(self):
        for name in (
            "vertical_fov",
            "horizontal_fov",
            "vertical_orientation",
            "horizontal_orientation",
        ):
            _check_numeric(name, getattr(self, name))


@dataclass
class CommunicationsComponent(AtlasModel):
    """Network link status component."""

    link_state: Literal["connected", "disconnected", "degraded", "unknown"]


@dataclass
class TaskQueueComponent(AtlasModel):
    """Current and queued work items for an entity."""

    current_task_id: Optional[str] = None
    queued_task_ids: List[str] = field(default_factory=list)


@dataclass
class StatusComponent(AtlasModel):
    """Operational status component."""

    value: str
    last_update: Optional[str] = None

    def __post_init__(self):
        if not self.value:
            raise ValueError("StatusComponent.value must be a non-empty string")
        _check_timestamp("last_update", self.last_update)


@dataclass
class HeartbeatComponent(AtlasModel):
    """Heartbeat timing component."""

    last_seen: str

    def __post_init__(self):
        if not self.last_seen:
            raise ValueError("HeartbeatComponent.last_seen must be a non-empty string")
        _check_timestamp("last_seen", self.last_seen)


@dataclass
class EntityComponents(AtlasModel):
    """All supported entity components with optional fields."""

    telemetry: Optional[TelemetryComponent] = None
    geometry: Optional[GeometryComponent] = None
    task_catalog: Optional[TaskCatalogComponent] = None
    media_refs: Optional[List[MediaRefItem]] = None
    mil_view: Optional[MilViewComponent] = None
    health: Optional[HealthComponent] = None
    sensor_refs: Optional[List[SensorRefItem]] = None
    communications: Optional[CommunicationsComponent] = None
    task_queue: Optional[TaskQueueComponent] = None
    status: Optional[StatusComponent] = None
    heartbeat: Optional[HeartbeatComponent] = None

    def __init__(self, **kwargs):
        known_fields = {f.name for f in fields(self)}
        for key, value in kwargs.items():
            if key in known_fields:
                setattr(self, key, value)
            elif key.startswith("custom_"):
                setattr(self, key, value)
            else:
                raise ValueError(
                    f"Unknown component '{key}'. Custom components must be prefixed with 'custom_'"
                )


# === Task Components ===


@dataclass
class CommandComponent(AtlasModel):
    """Command component for tasks. Identifies the work type."""

    type: str

    def __post_init__(self):
        if not self.type:
            raise ValueError("CommandComponent.type must be a non-empty string")


@dataclass
class TaskParametersComponent(AtlasModel):
    """Command parameters for task execution."""

    latitude: Optional[float] = None
    longitude: Optional[float] = None
    altitude_m: Optional[float] = None

    def __init__(self, **kwargs):
        known_fields = {"latitude", "longitude", "altitude_m"}
        for key, value in kwargs.items():
            if key in known_fields or key.startswith("custom_"):
                setattr(self, key, value)
            else:
                raise ValueError(
                    f"Unknown task parameter '{key}'. Custom parameters must be prefixed with 'custom_'"
                )


@dataclass
class TaskProgressComponent(AtlasModel):
    """Runtime telemetry about task execution."""

    percent: Optional[int] = None
    updated_at: Optional[str] = None
    status_detail: Optional[str] = None

    def __post_init__(self):
        _check_numeric("percent", self.percent)
        _check_timestamp("updated_at", self.updated_at)
        if self.percent is not None:
            if not (0 <= self.percent <= 100):
                raise ValueError("percent must be between 0 and 100")


@dataclass
class TaskComponents(AtlasModel):
    """All supported task components."""

    command: Optional[CommandComponent] = None
    parameters: Optional[TaskParametersComponent] = None
    progress: Optional[TaskProgressComponent] = None

    def __init__(self, **kwargs):
        known_fields = {"command", "parameters", "progress"}
        for key, value in kwargs.items():
            if key in known_fields or key.startswith("custom_"):
                setattr(self, key, value)
            else:
                raise ValueError(
                    f"Unknown task component '{key}'. Custom components must be prefixed with 'custom_'"
                )


# === Object Metadata ===


@dataclass
class ObjectReferenceItem(AtlasModel):
    """A reference from an object to an entity or task."""

    entity_id: Optional[str] = None
    task_id: Optional[str] = None


@dataclass
class ObjectMetadata(AtlasModel):
    """Metadata for stored objects (JSON blob fields)."""

    bucket: Optional[str] = None
    size_bytes: Optional[int] = None
    usage_hints: Optional[List[str]] = None
    referenced_by: Optional[List[ObjectReferenceItem]] = None
    checksum: Optional[str] = None
    expiry_time: Optional[str] = None

    def __init__(self, **kwargs):
        known_fields = {
            "bucket",
            "size_bytes",
            "usage_hints",
            "referenced_by",
            "checksum",
            "expiry_time",
        }
        for key, value in kwargs.items():
            if key in known_fields or key.startswith("custom_"):
                setattr(self, key, value)
            else:
                raise ValueError(
                    f"Unknown object metadata field '{key}'. Custom fields must be prefixed with 'custom_'"
                )


# === Helper Functions ===


def components_to_dict(
    components: Optional[Union[EntityComponents, TaskComponents, dict[str, Any]]],
) -> Optional[dict[str, Any]]:
    """Convert typed components to a dictionary for API transmission.

    Args:
        components: Typed component model or raw dict

    Returns:
        Dictionary suitable for JSON serialization
    """
    if components is None:
        return None

    if isinstance(components, dict):
        return components

    if isinstance(components, (EntityComponents, TaskComponents)):
        return components.model_dump(exclude_none=True, by_alias=True)

    raise TypeError(f"Expected EntityComponents or TaskComponents, got {type(components)}")


def object_metadata_to_dict(
    metadata: Optional[ObjectMetadata],
) -> Optional[dict[str, Any]]:
    """Convert typed object metadata to a dictionary for API transmission.

    Args:
        metadata: Typed ObjectMetadata

    Returns:
        Dictionary suitable for JSON serialization
    """
    if metadata is None:
        return None

    if isinstance(metadata, ObjectMetadata):
        return metadata.model_dump(exclude_none=True, by_alias=True)

    raise TypeError(f"Expected ObjectMetadata, got {type(metadata)}")
