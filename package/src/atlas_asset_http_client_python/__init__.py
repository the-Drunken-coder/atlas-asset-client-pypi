"""Atlas Command HTTP client for assets and operators."""

from .components import (
    CommandComponent,
    CommunicationsComponent,
    EntityComponents,
    GeometryComponent,
    HealthComponent,
    HeartbeatComponent,
    MediaRefItem,
    MilViewComponent,
    ObjectMetadata,
    ObjectReferenceItem,
    SensorRefItem,
    StatusComponent,
    TaskCatalogComponent,
    TaskComponents,
    TaskParametersComponent,
    TaskProgressComponent,
    TaskQueueComponent,
    TelemetryComponent,
    components_to_dict,
    object_metadata_to_dict,
)
from .http_client import (
    AtlasCommandHttpClient,
    ChangedSinceResponse,
    DeletedResource,
)

# Back-compat aliases (single tombstone type on the wire).
DeletedEntity = DeletedResource
DeletedTask = DeletedResource
DeletedObject = DeletedResource

__all__ = [
    "AtlasCommandHttpClient",
    "ChangedSinceResponse",
    "DeletedResource",
    "DeletedEntity",
    "DeletedTask",
    "DeletedObject",
    # Entity components
    "EntityComponents",
    "TelemetryComponent",
    "GeometryComponent",
    "TaskCatalogComponent",
    "MediaRefItem",
    "MilViewComponent",
    "HealthComponent",
    "SensorRefItem",
    "CommunicationsComponent",
    "TaskQueueComponent",
    "StatusComponent",
    "HeartbeatComponent",
    # Task components
    "TaskComponents",
    "CommandComponent",
    "TaskParametersComponent",
    "TaskProgressComponent",
    # Object metadata
    "ObjectMetadata",
    "ObjectReferenceItem",
    # Helpers
    "components_to_dict",
    "object_metadata_to_dict",
]
