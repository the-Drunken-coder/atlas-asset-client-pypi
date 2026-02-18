"""Tests for typed component models."""

from __future__ import annotations

import json

import httpx
import pytest
from atlas_asset_http_client_python import (
    AtlasCommandHttpClient,
    CommandComponent,
    CommunicationsComponent,
    EntityComponents,
    GeometryComponent,
    HealthComponent,
    HeartbeatComponent,
    MediaRefItem,
    MilViewComponent,
    SensorRefItem,
    StatusComponent,
    TaskCatalogComponent,
    TaskComponents,
    TaskParametersComponent,
    TaskProgressComponent,
    TaskQueueComponent,
    TelemetryComponent,
    components_to_dict,
)


class TestTelemetryComponent:
    """Tests for TelemetryComponent."""

    def test_basic_creation(self):
        telemetry = TelemetryComponent(
            latitude=40.7128,
            longitude=-74.0060,
            altitude_m=120,
            speed_m_s=8.2,
            heading_deg=165,
        )
        assert telemetry.latitude == 40.7128
        assert telemetry.longitude == -74.0060
        assert telemetry.altitude_m == 120
        assert telemetry.speed_m_s == 8.2
        assert telemetry.heading_deg == 165

    def test_optional_fields(self):
        telemetry = TelemetryComponent(latitude=40.7128)
        assert telemetry.latitude == 40.7128
        assert telemetry.longitude is None
        assert telemetry.altitude_m is None

    def test_to_dict_excludes_none(self):
        telemetry = TelemetryComponent(latitude=40.7128, longitude=-74.0060)
        result = telemetry.model_dump(exclude_none=True)
        assert result == {"latitude": 40.7128, "longitude": -74.0060}
        assert "altitude_m" not in result


class TestGeometryComponent:
    """Tests for GeometryComponent."""

    def test_point_geometry(self):
        geometry = GeometryComponent(type="Point", coordinates=[-74.0060, 40.7128])
        assert geometry.type == "Point"
        assert geometry.coordinates == [-74.0060, 40.7128]

    def test_linestring_geometry(self):
        geometry = GeometryComponent(
            type="LineString",
            coordinates=[[-74.0060, 40.7128], [-74.0050, 40.7138]],
        )
        assert geometry.type == "LineString"

    def test_polygon_geometry(self):
        geometry = GeometryComponent(
            type="Polygon",
            coordinates=[[[-74.0060, 40.7128], [-74.0050, 40.7138], [-74.0060, 40.7128]]],
        )
        assert geometry.type == "Polygon"


class TestEntityComponents:
    """Tests for EntityComponents with multiple components."""

    def test_full_entity_components(self):
        components = EntityComponents(
            telemetry=TelemetryComponent(
                latitude=40.7128,
                longitude=-74.0060,
                altitude_m=120,
                speed_m_s=8.2,
                heading_deg=165,
            ),
            task_catalog=TaskCatalogComponent(supported_tasks=["move_to_location", "survey_grid"]),
            health=HealthComponent(battery_percent=76),
            communications=CommunicationsComponent(link_state="connected"),
            task_queue=TaskQueueComponent(current_task_id=None, queued_task_ids=[]),
            media_refs=[
                MediaRefItem(object_id="obj-123", role="camera_feed"),
                MediaRefItem(object_id="obj-456", role="thumbnail"),
            ],
            sensor_refs=[
                SensorRefItem(
                    sensor_id="radar-1",
                    type="radar",
                    vertical_fov=60,
                    horizontal_fov=90,
                    vertical_orientation=10,
                    horizontal_orientation=45,
                )
            ],
            mil_view=MilViewComponent(classification="friendly", last_seen="2025-11-23T10:05:00Z"),
        )
        result = components.model_dump(exclude_none=True)
        assert result["telemetry"]["latitude"] == 40.7128
        assert result["task_catalog"]["supported_tasks"] == [
            "move_to_location",
            "survey_grid",
        ]
        assert result["health"]["battery_percent"] == 76
        assert result["communications"]["link_state"] == "connected"
        assert len(result["media_refs"]) == 2
        assert len(result["sensor_refs"]) == 1
        assert result["mil_view"]["classification"] == "friendly"

    def test_custom_components_allowed(self):
        components = EntityComponents(
            telemetry=TelemetryComponent(latitude=40.7128),
            custom_weather={"wind_speed": 12, "gusts": 18},
        )
        result = components.model_dump(exclude_none=True)
        assert result["custom_weather"] == {"wind_speed": 12, "gusts": 18}

    def test_unknown_component_raises_error(self):
        with pytest.raises(ValueError, match="Unknown component"):
            EntityComponents(
                unknown_component={"foo": "bar"},
            )


class TestTaskComponents:
    """Tests for TaskComponents."""

    def test_task_with_parameters_and_progress(self):
        components = TaskComponents(
            parameters=TaskParametersComponent(latitude=40.123, longitude=-74.456, altitude_m=120),
            progress=TaskProgressComponent(
                percent=65,
                updated_at="2025-11-25T08:45:00Z",
                status_detail="En route to destination",
            ),
        )
        result = components.model_dump(exclude_none=True)
        assert result["parameters"]["latitude"] == 40.123
        assert result["progress"]["percent"] == 65
        assert result["progress"]["status_detail"] == "En route to destination"


class TestStatusComponent:
    """Tests for StatusComponent validation."""

    def test_valid_status(self):
        status = StatusComponent(value="active", last_update="2025-11-23T10:05:00Z")
        assert status.value == "active"
        assert status.last_update == "2025-11-23T10:05:00Z"

    def test_empty_value_raises(self):
        with pytest.raises(ValueError, match="non-empty string"):
            StatusComponent(value="")

    def test_invalid_timestamp_raises(self):
        with pytest.raises(ValueError, match="valid RFC 3339 timestamp"):
            StatusComponent(value="ok", last_update="not-a-date")

    def test_z_suffix_accepted(self):
        status = StatusComponent(value="active", last_update="2025-01-01T00:00:00Z")
        assert status.last_update == "2025-01-01T00:00:00Z"


class TestHeartbeatComponent:
    """Tests for HeartbeatComponent validation."""

    def test_valid_heartbeat(self):
        hb = HeartbeatComponent(last_seen="2025-11-23T10:05:00Z")
        assert hb.last_seen == "2025-11-23T10:05:00Z"

    def test_empty_last_seen_raises(self):
        with pytest.raises(ValueError, match="non-empty string"):
            HeartbeatComponent(last_seen="")

    def test_invalid_timestamp_raises(self):
        with pytest.raises(ValueError, match="valid RFC 3339 timestamp"):
            HeartbeatComponent(last_seen="yesterday")


class TestCommandComponent:
    """Tests for CommandComponent validation."""

    def test_valid_command(self):
        cmd = CommandComponent(type="move_to_location")
        assert cmd.type == "move_to_location"

    def test_empty_type_raises(self):
        with pytest.raises(ValueError, match="non-empty string"):
            CommandComponent(type="")


class TestTaskParametersComponent:
    """Tests for TaskParametersComponent validation."""

    def test_known_fields_accepted(self):
        params = TaskParametersComponent(latitude=40.0, longitude=-74.0, altitude_m=100.0)
        assert params.latitude == 40.0
        assert params.longitude == -74.0
        assert params.altitude_m == 100.0

    def test_custom_prefix_accepted(self):
        params = TaskParametersComponent(custom_speed=5.0)
        assert params.custom_speed == 5.0

    def test_unknown_field_without_prefix_raises(self):
        with pytest.raises(ValueError, match="Unknown task parameter"):
            TaskParametersComponent(speed=5.0)


class TestTelemetryValidation:
    """Tests for TelemetryComponent numeric validation."""

    def test_bool_latitude_rejected(self):
        with pytest.raises(TypeError, match="must be a number"):
            TelemetryComponent(latitude=True)

    def test_bool_speed_rejected(self):
        with pytest.raises(TypeError, match="must be a number"):
            TelemetryComponent(speed_m_s=False)

    def test_latitude_out_of_range(self):
        with pytest.raises(ValueError, match="latitude must be between"):
            TelemetryComponent(latitude=91.0)

    def test_negative_speed_rejected(self):
        with pytest.raises(ValueError, match="non-negative"):
            TelemetryComponent(speed_m_s=-1.0)

    def test_heading_out_of_range(self):
        with pytest.raises(ValueError, match="heading_deg must be between"):
            TelemetryComponent(heading_deg=360.0)


class TestComponentsToDict:
    """Tests for the components_to_dict helper."""

    def test_with_typed_components(self):
        components = EntityComponents(
            telemetry=TelemetryComponent(latitude=40.7128, longitude=-74.0060)
        )
        result = components_to_dict(components)
        assert result == {"telemetry": {"latitude": 40.7128, "longitude": -74.0060}}

    def test_with_raw_dict_raises_type_error(self):
        raw_components = {"telemetry": {"latitude": 40.7128}}
        with pytest.raises(TypeError, match="Expected EntityComponents or TaskComponents"):
            components_to_dict(raw_components)

    def test_with_none(self):
        result = components_to_dict(None)
        assert result is None


class TestHttpClientWithTypedComponents:
    """Tests for HTTP client with typed components."""

    @pytest.mark.asyncio
    async def test_create_entity_with_typed_components(self):
        captured: dict[str, httpx.Request] = {}

        async def handler(request: httpx.Request) -> httpx.Response:
            captured["request"] = request
            return httpx.Response(200, json={"entity_id": "asset-1"})

        client = AtlasCommandHttpClient(
            "http://atlas.local",
            transport=httpx.MockTransport(handler),
        )

        components = EntityComponents(
            telemetry=TelemetryComponent(
                latitude=40.7128,
                longitude=-74.0060,
                altitude_m=120,
            ),
            health=HealthComponent(battery_percent=85),
        )

        async with client:
            entity = await client.create_entity(
                entity_id="asset-1",
                entity_type="asset",
                alias="demo",
                subtype="drone",
                components=components,
            )

        assert entity["entity_id"] == "asset-1"
        req = captured["request"]
        payload = json.loads(req.content)
        assert payload["components"]["telemetry"]["latitude"] == 40.7128
        assert payload["components"]["telemetry"]["longitude"] == -74.0060
        assert payload["components"]["health"]["battery_percent"] == 85

    @pytest.mark.asyncio
    async def test_create_task_with_typed_components(self):
        captured: dict[str, httpx.Request] = {}

        async def handler(request: httpx.Request) -> httpx.Response:
            captured["request"] = request
            return httpx.Response(200, json={"task_id": "task-1"})

        client = AtlasCommandHttpClient(
            "http://atlas.local",
            transport=httpx.MockTransport(handler),
        )

        components = TaskComponents(
            parameters=TaskParametersComponent(
                latitude=40.123,
                longitude=-74.456,
                altitude_m=120,
            ),
        )

        async with client:
            task = await client.create_task(
                task_id="task-1",
                entity_id="asset-1",
                components=components,
            )

        assert task["task_id"] == "task-1"
        req = captured["request"]
        payload = json.loads(req.content)
        assert payload["components"]["parameters"]["latitude"] == 40.123
        assert payload["components"]["parameters"]["longitude"] == -74.456

    @pytest.mark.asyncio
    async def test_update_entity_with_typed_components(self):
        captured: dict[str, httpx.Request] = {}

        async def handler(request: httpx.Request) -> httpx.Response:
            captured["request"] = request
            return httpx.Response(200, json={"entity_id": "asset-1"})

        client = AtlasCommandHttpClient(
            "http://atlas.local",
            transport=httpx.MockTransport(handler),
        )

        components = EntityComponents(
            telemetry=TelemetryComponent(latitude=41.0, longitude=-75.0),
        )

        async with client:
            await client.update_entity("asset-1", components=components)

        req = captured["request"]
        payload = json.loads(req.content)
        assert payload["components"]["telemetry"]["latitude"] == 41.0

    @pytest.mark.asyncio
    async def test_raw_dict_rejected_by_create_entity(self):
        """Verify raw dict components are rejected (no longer supported)."""

        async def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json={"entity_id": "asset-1"})

        client = AtlasCommandHttpClient(
            "http://atlas.local",
            transport=httpx.MockTransport(handler),
        )

        raw_components = {"telemetry": {"latitude": 40.7128, "longitude": -74.0060}}

        async with client:
            with pytest.raises(TypeError, match="Expected EntityComponents or TaskComponents"):
                await client.create_entity(
                    entity_id="asset-1",
                    entity_type="asset",
                    alias="demo",
                    subtype="drone",
                    components=raw_components,
                )
