"""Tests for the Atlas Command HTTP client."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

import httpx
import pytest
from atlas_asset_http_client_python import AtlasCommandHttpClient


def build_client(json_map: dict[tuple[str, str], tuple[int, Any]]) -> AtlasCommandHttpClient:
    async def handler(request: httpx.Request) -> httpx.Response:
        key = (request.method, request.url.path)
        if key not in json_map:
            return httpx.Response(404)
        status_code, payload = json_map[key]
        return httpx.Response(status_code, json=payload)

    transport = httpx.MockTransport(handler)
    return AtlasCommandHttpClient("http://atlas.local", transport=transport)


@pytest.mark.asyncio
async def test_list_entities_calls_endpoint():
    payload = [{"entity_id": "one"}]
    client: Any = build_client({("GET", "/entities"): (200, payload)})
    async with client:
        result = await client.list_entities()
    assert result == payload


@pytest.mark.asyncio
async def test_create_entity_posts_payload():
    captured: dict[str, httpx.Request] = {}

    async def handler(request: httpx.Request) -> httpx.Response:
        captured["request"] = request
        return httpx.Response(200, json={"entity_id": "asset-1"})

    client: Any = AtlasCommandHttpClient(
        "http://atlas.local",
        transport=httpx.MockTransport(handler),
        token="secret",
    )
    async with client:
        entity = await client.create_entity(
            entity_id="asset-1", entity_type="asset", alias="demo", subtype="drone"
        )

    assert entity["entity_id"] == "asset-1"
    req = captured["request"]
    assert req.headers["authorization"] == "Bearer secret"
    assert json.loads(req.content)["alias"] == "demo"


@pytest.mark.asyncio
async def test_create_object_uploads_file_and_references():
    upload_requests: list[httpx.Request] = []
    reference_requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/objects/upload":
            upload_requests.append(request)
            return httpx.Response(201, json={"object_id": "obj-123"})
        if request.url.path == "/objects/obj-123/references":
            reference_requests.append(request)
            return httpx.Response(200, json={})
        return httpx.Response(404)

    client: Any = AtlasCommandHttpClient(
        "http://atlas.local", transport=httpx.MockTransport(handler)
    )
    async with client:
        stored = await client.create_object(
            file=b"binary-data",
            object_id="obj-123",
            content_type="application/octet-stream",
            usage_hint="mission_video",
            object_type="heatmap",
            referenced_by=[{"entity_id": "asset-1", "task_id": "task-alpha"}],
        )

    assert stored["object_id"] == "obj-123"
    assert len(upload_requests) == 1
    upload_request = upload_requests[0]
    assert upload_request.method == "POST"
    assert upload_request.url.path == "/objects/upload"
    assert b'name="object_id"' in upload_request.content
    assert b"obj-123" in upload_request.content
    assert b'name="type"' in upload_request.content
    assert b"heatmap" in upload_request.content
    assert b'name="usage_hint"' in upload_request.content
    assert b"mission_video" in upload_request.content
    assert len(reference_requests) == 1
    ref_request = reference_requests[0]
    payload = json.loads(ref_request.content)
    assert payload["entity_id"] == "asset-1"
    assert payload["task_id"] == "task-alpha"


@pytest.mark.asyncio
async def test_create_object_with_references_requires_object_id_in_response():
    upload_requests: list[httpx.Request] = []
    reference_requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/objects/upload":
            upload_requests.append(request)
            return httpx.Response(201)
        if request.url.path.startswith("/objects/"):
            reference_requests.append(request)
        return httpx.Response(404)

    client: Any = AtlasCommandHttpClient(
        "http://atlas.local", transport=httpx.MockTransport(handler)
    )
    async with client:
        with pytest.raises(RuntimeError):
            await client.create_object(
                file=b"binary-data",
                object_id="obj-123",
                content_type="application/octet-stream",
                referenced_by=[{"entity_id": "asset-1"}],
            )

    assert len(upload_requests) == 1
    assert reference_requests == []


@pytest.mark.asyncio
async def test_get_changed_since_passes_params():
    since = datetime(2025, 1, 1, tzinfo=timezone.utc).isoformat()

    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.params["since"] == since
        assert request.url.params["limit_per_type"] == "10"
        return httpx.Response(
            200,
            json={
                "entities": [],
                "tasks": [],
                "objects": [],
                "deleted_entities": [{"entity_id": "entity-1", "deleted_at": since}],
                "deleted_tasks": [{"task_id": "task-1", "deleted_at": since}],
                "deleted_objects": [{"object_id": "obj-1", "deleted_at": since}],
            },
        )

    client: Any = AtlasCommandHttpClient(
        "http://atlas.local",
        transport=httpx.MockTransport(handler),
    )
    async with client:
        snapshot = await client.get_changed_since(since, limit_per_type=10)
    assert snapshot == {
        "entities": [],
        "tasks": [],
        "objects": [],
        "deleted_entities": [{"entity_id": "entity-1", "deleted_at": since}],
        "deleted_tasks": [{"task_id": "task-1", "deleted_at": since}],
        "deleted_objects": [{"object_id": "obj-1", "deleted_at": since}],
    }


@pytest.mark.asyncio
async def test_checkin_entity_sends_payload_and_params():
    captured: dict[str, httpx.Request] = {}

    async def handler(request: httpx.Request) -> httpx.Response:
        captured["request"] = request
        return httpx.Response(200, json={"tasks": []})

    client: Any = AtlasCommandHttpClient(
        "http://atlas.local",
        transport=httpx.MockTransport(handler),
    )
    async with client:
        await client.checkin_entity(
            "asset-1",
            latitude=1.0,
            longitude=2.0,
            status_filter="pending",
            limit=5,
            since="2025-01-01T00:00:00Z",
            fields="minimal",
        )

    req = captured["request"]
    assert req.url.path == "/entities/asset-1/checkin"
    assert req.url.params["status_filter"] == "pending"
    assert req.url.params["limit"] == "5"
    assert req.url.params["since"] == "2025-01-01T00:00:00Z"
    assert req.url.params["fields"] == "minimal"
    payload = json.loads(req.content)
    assert payload == {"latitude": 1.0, "longitude": 2.0}


@pytest.mark.asyncio
async def test_transition_task_status_posts_body():
    captured: dict[str, httpx.Request] = {}

    async def handler(request: httpx.Request) -> httpx.Response:
        captured["request"] = request
        return httpx.Response(200, json={"task_id": "task-1", "status": "in_progress"})

    client: Any = AtlasCommandHttpClient(
        "http://atlas.local", transport=httpx.MockTransport(handler)
    )
    async with client:
        await client.transition_task_status(
            "task-1", "in_progress", validate=False, extra={"note": "go"}
        )

    req = captured["request"]
    assert req.url.path == "/tasks/task-1/status"
    payload = json.loads(req.content)
    assert payload == {"status": "in_progress", "validate": False, "extra": {"note": "go"}}


@pytest.mark.asyncio
async def test_complete_task_accepts_result():
    captured: dict[str, httpx.Request] = {}

    async def handler(request: httpx.Request) -> httpx.Response:
        captured["request"] = request
        return httpx.Response(200, json={"ok": True})

    client: Any = AtlasCommandHttpClient(
        "http://atlas.local", transport=httpx.MockTransport(handler)
    )
    async with client:
        await client.complete_task("task-9", result={"done": True})

    req = captured["request"]
    assert req.url.path == "/tasks/task-9/complete"
    assert json.loads(req.content) == {"result": {"done": True}}


@pytest.mark.asyncio
async def test_download_object_returns_bytes_and_metadata():
    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/objects/obj-123/download":
            return httpx.Response(
                200,
                content=b"payload",
                headers={"content-type": "video/mp4", "content-length": "7"},
            )
        return httpx.Response(404)

    client: Any = AtlasCommandHttpClient(
        "http://atlas.local", transport=httpx.MockTransport(handler)
    )
    async with client:
        data, content_type, content_length = await client.download_object("obj-123")

    assert data == b"payload"
    assert content_type == "video/mp4"
    assert content_length == 7
