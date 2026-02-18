"""Atlas Asset Client - Alias for atlas_asset_http_client_python.

This module provides a package-name-matching import path.
When installing 'atlas-asset-client', you can import as 'atlas_asset_client'.
"""

# Note: this package is mirrored and released from the ATLAS monorepo via CI.

# Re-export everything from the actual implementation
from atlas_asset_http_client_python import (
    AtlasCommandHttpClient,
    CommunicationsComponent,
    EntityComponents,
    GeometryComponent,
    HealthComponent,
    MediaRefItem,
    MilViewComponent,
    ObjectMetadata,
    ObjectReferenceItem,
    SensorRefItem,
    TaskCatalogComponent,
    TaskComponents,
    TaskParametersComponent,
    TaskProgressComponent,
    TaskQueueComponent,
    TelemetryComponent,
    components_to_dict,
    object_metadata_to_dict,
)

# Define __all__ to match the original module's exports
__all__ = [
    "AtlasCommandHttpClient",
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
    # Task components
    "TaskComponents",
    "TaskParametersComponent",
    "TaskProgressComponent",
    # Object metadata
    "ObjectMetadata",
    "ObjectReferenceItem",
    # Helpers
    "components_to_dict",
    "object_metadata_to_dict",
]
