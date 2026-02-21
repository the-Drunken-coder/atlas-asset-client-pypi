"""Async HTTP client for Atlas Command."""

from __future__ import annotations

from datetime import datetime
from typing import Any, BinaryIO, Mapping, Optional, TypedDict

import httpx

from .components import (
    EntityComponents,
    TaskComponents,
    components_to_dict,
)


class DeletedEntity(TypedDict):
    entity_id: str
    deleted_at: str | None


class DeletedTask(TypedDict):
    task_id: str
    deleted_at: str | None


class DeletedObject(TypedDict):
    object_id: str
    deleted_at: str | None


class ChangedSinceResponse(TypedDict, total=False):
    entities: list[dict[str, Any]]
    tasks: list[dict[str, Any]]
    objects: list[dict[str, Any]]
    deleted_entities: list[DeletedEntity]
    deleted_tasks: list[DeletedTask]
    deleted_objects: list[DeletedObject]


class AtlasCommandHttpClient:
    """Minimal async HTTP client for Atlas Command's REST API."""

    def __init__(
        self,
        base_url: str,
        *,
        token: Optional[str] = None,
        timeout: float = 10.0,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._token = token
        self._client: httpx.AsyncClient | None = httpx.AsyncClient(
            base_url=self._base_url,
            timeout=timeout,
            transport=transport,
        )

    async def __aenter__(self) -> "AtlasCommandHttpClient":
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    @property
    def _http(self) -> httpx.AsyncClient:
        if self._client is None:
            raise RuntimeError("Client is closed")
        return self._client

    def _headers(self) -> dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if self._token:
            headers["Authorization"] = f"Bearer {self._token}"
        return headers

    async def _request(self, method: str, path: str, **kwargs: Any) -> Any:
        response = await self._http.request(method, path, headers=self._headers(), **kwargs)
        response.raise_for_status()
        if response.content:
            return response.json()
        return None

    def _multipart_headers(self) -> dict[str, str]:
        headers: dict[str, str] = {}
        if self._token:
            headers["Authorization"] = f"Bearer {self._token}"
        return headers

    async def _multipart_request(
        self,
        path: str,
        *,
        files: dict[str, Any],
        data: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        response = await self._http.post(
            path, headers=self._multipart_headers(), files=files, data=data
        )
        response.raise_for_status()
        if response.content:
            return response.json()
        return {}

    # Service -----------------------------------------------------------------

    async def get_root(self) -> dict[str, Any]:
        return await self._request("GET", "/")

    async def get_health(self) -> dict[str, Any]:
        return await self._request("GET", "/health")

    async def get_readiness(self) -> dict[str, Any]:
        return await self._request("GET", "/readiness")

    # Entities -----------------------------------------------------------------

    async def list_entities(self, *, limit: int = 100, offset: int = 0) -> list[dict[str, Any]]:
        return await self._request("GET", "/entities", params={"limit": limit, "offset": offset})

    async def get_entity(self, entity_id: str) -> dict[str, Any]:
        return await self._request("GET", f"/entities/{entity_id}")

    async def get_entity_by_alias(self, alias: str) -> dict[str, Any]:
        return await self._request("GET", f"/entities/alias/{alias}")

    async def create_entity(
        self,
        *,
        entity_id: str,
        entity_type: str,
        alias: str,
        subtype: str,
        components: Optional[EntityComponents] = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "entity_id": entity_id,
            "entity_type": entity_type,
            "alias": alias,
            "subtype": subtype,
            "components": components_to_dict(components),
        }
        return await self._request("POST", "/entities", json=payload)

    async def update_entity(
        self,
        entity_id: str,
        *,
        components: Optional[EntityComponents] = None,
        subtype: Optional[str] = None,
    ) -> dict[str, Any]:
        if components is None and subtype is None:
            raise ValueError("update_entity requires at least one of: components, subtype")
        payload: dict[str, Any] = {}
        if components is not None:
            payload["components"] = components_to_dict(components)
        if subtype is not None:
            payload["subtype"] = subtype
        return await self._request("PATCH", f"/entities/{entity_id}", json=payload)

    async def delete_entity(self, entity_id: str) -> None:
        await self._request("DELETE", f"/entities/{entity_id}")

    async def checkin_entity(
        self,
        entity_id: str,
        *,
        status: Optional[str] = None,
        latitude: Optional[float] = None,
        longitude: Optional[float] = None,
        altitude_m: Optional[float] = None,
        speed_m_s: Optional[float] = None,
        heading_deg: Optional[float] = None,
        status_filter: str = "pending,acknowledged",
        limit: int = 10,
        since: Optional[str] = None,
        fields: Optional[str] = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {}
        if latitude is not None:
            payload["latitude"] = latitude
        if longitude is not None:
            payload["longitude"] = longitude
        if altitude_m is not None:
            payload["altitude_m"] = altitude_m
        if speed_m_s is not None:
            payload["speed_m_s"] = speed_m_s
        if heading_deg is not None:
            payload["heading_deg"] = heading_deg
        params: dict[str, Any] = {"status_filter": status_filter, "limit": limit}
        if since is not None:
            params["since"] = since
        if fields is not None:
            params["fields"] = fields
        if status is not None:
            payload["status"] = status
        return await self._request(
            "POST", f"/entities/{entity_id}/checkin", json=payload, params=params
        )

    async def update_entity_telemetry(
        self,
        entity_id: str,
        *,
        latitude: Optional[float] = None,
        longitude: Optional[float] = None,
        altitude_m: Optional[float] = None,
        speed_m_s: Optional[float] = None,
        heading_deg: Optional[float] = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {}
        if latitude is not None:
            payload["latitude"] = latitude
        if longitude is not None:
            payload["longitude"] = longitude
        if altitude_m is not None:
            payload["altitude_m"] = altitude_m
        if speed_m_s is not None:
            payload["speed_m_s"] = speed_m_s
        if heading_deg is not None:
            payload["heading_deg"] = heading_deg
        return await self._request("PATCH", f"/entities/{entity_id}/telemetry", json=payload)

    # Tasks --------------------------------------------------------------------

    async def list_tasks(
        self, *, status: Optional[str] = None, limit: int = 25, offset: int = 0
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {"limit": limit, "offset": offset}
        if status:
            params["status"] = status
        return await self._request("GET", "/tasks", params=params)

    async def get_task(self, task_id: str) -> dict[str, Any]:
        return await self._request("GET", f"/tasks/{task_id}")

    async def create_task(
        self,
        *,
        task_id: str,
        status: str = "pending",
        entity_id: Optional[str] = None,
        components: Optional[TaskComponents] = None,
        extra: Optional[Mapping[str, Any]] = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "task_id": task_id,
            "status": status,
        }
        if entity_id is not None:
            payload["entity_id"] = entity_id
        if components is not None:
            payload["components"] = components_to_dict(components)
        if extra is not None:
            payload["extra"] = extra
        return await self._request("POST", "/tasks", json=payload)

    async def update_task(
        self,
        task_id: str,
        *,
        status: Optional[str] = None,
        entity_id: Optional[str] = None,
        components: Optional[TaskComponents] = None,
        extra: Optional[Mapping[str, Any]] = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {}
        if status is not None:
            payload["status"] = status
        if entity_id is not None:
            payload["entity_id"] = entity_id
        if components is not None:
            payload["components"] = components_to_dict(components)
        if extra is not None:
            payload["extra"] = extra
        return await self._request("PATCH", f"/tasks/{task_id}", json=payload)

    async def delete_task(self, task_id: str) -> None:
        await self._request("DELETE", f"/tasks/{task_id}")

    async def get_tasks_by_entity(
        self, entity_id: str, *, status: Optional[str] = None, limit: int = 25, offset: int = 0
    ) -> list[dict]:
        params: dict[str, Any] = {"limit": limit, "offset": offset}
        if status:
            params["status"] = status
        return await self._request("GET", f"/entities/{entity_id}/tasks", params=params)

    async def acknowledge_task(self, task_id: str) -> dict[str, Any]:
        return await self._request("POST", f"/tasks/{task_id}/acknowledge", json={})

    async def start_task(self, task_id: str) -> dict[str, Any]:
        return await self.acknowledge_task(task_id)

    async def complete_task(
        self,
        task_id: str,
        *,
        result: Optional[Mapping[str, Any]] = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {}
        if result is not None:
            payload["result"] = result
        return await self._request("POST", f"/tasks/{task_id}/complete", json=payload)

    async def transition_task_status(
        self,
        task_id: str,
        status: str,
        *,
        validate: bool = True,
        extra: Optional[Mapping[str, Any]] = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {"status": status, "validate": validate}
        if extra is not None:
            payload["extra"] = extra
        return await self._request("POST", f"/tasks/{task_id}/status", json=payload)

    async def fail_task(
        self,
        task_id: str,
        *,
        error_message: Optional[str] = None,
        error_details: Optional[Mapping[str, Any]] = None,
    ) -> dict[str, Any]:
        return await self._request(
            "POST",
            f"/tasks/{task_id}/fail",
            json={"error_message": error_message, "error_details": error_details},
        )

    # Objects ------------------------------------------------------------------

    async def download_object(self, object_id: str) -> tuple[bytes, Optional[str], Optional[int]]:
        """Download raw object bytes with content metadata."""
        response = await self._http.get(f"/objects/{object_id}/download", headers=self._headers())
        response.raise_for_status()
        content_type = response.headers.get("content-type")
        content_length_header = response.headers.get("content-length")
        content_length = (
            int(content_length_header)
            if content_length_header and content_length_header.isdigit()
            else None
        )
        return response.content, content_type, content_length

    async def view_object(self, object_id: str) -> tuple[str, Optional[str], Optional[int]]:
        """Return viewable object content as text with content metadata."""
        response = await self._http.get(f"/objects/{object_id}/view", headers=self._headers())
        response.raise_for_status()
        content_type = response.headers.get("content-type")
        content_length_header = response.headers.get("content-length")
        content_length = (
            int(content_length_header)
            if content_length_header and content_length_header.isdigit()
            else None
        )
        return response.text, content_type, content_length

    async def list_objects(
        self,
        *,
        content_type: Optional[str] = None,
        type: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {"limit": limit, "offset": offset}
        if content_type:
            params["content_type"] = content_type
        if type:
            params["type"] = type
        return await self._request("GET", "/objects", params=params)

    async def get_object(self, object_id: str) -> dict[str, Any]:
        return await self._request("GET", f"/objects/{object_id}")

    async def create_object(
        self,
        file: bytes | BinaryIO,
        *,
        object_id: str,
        usage_hint: Optional[str] = None,
        referenced_by: Optional[list[dict[str, Any]]] = None,
        content_type: str,
        object_type: Optional[str] = None,
    ) -> dict[str, Any]:
        if not content_type:
            raise ValueError("create_object requires 'content_type'")
        filename = (
            getattr(file, "name", "upload.bin")
            if not isinstance(file, (bytes, bytearray))
            else "upload.bin"
        )
        files = {"file": (filename, file, content_type)}
        data = {"object_id": object_id}
        if usage_hint:
            data["usage_hint"] = usage_hint
        if object_type:
            data["type"] = object_type

        stored = await self._multipart_request("/objects/upload", files=files, data=data)

        stored_object_id = stored.get("object_id")
        if referenced_by:
            if not stored_object_id:
                raise RuntimeError(
                    "AtlasCommandHttpClient.create_object expected the upload response to include "
                    "an object_id before attaching references."
                )
            for reference in referenced_by:
                await self.add_object_reference(
                    stored_object_id,
                    entity_id=reference.get("entity_id"),
                    task_id=reference.get("task_id"),
                )

        return stored

    async def create_object_metadata(
        self,
        *,
        object_id: str,
        path: Optional[str] = None,
        bucket: Optional[str] = None,
        size_bytes: Optional[int] = None,
        content_type: Optional[str] = None,
        object_type: Optional[str] = None,
        usage_hints: Optional[list[str]] = None,
        referenced_by: Optional[list[dict[str, Any]]] = None,
        extra: Optional[Mapping[str, Any]] = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {"object_id": object_id}
        if path is not None:
            payload["path"] = path
        if bucket is not None:
            payload["bucket"] = bucket
        if size_bytes is not None:
            payload["size_bytes"] = size_bytes
        if content_type is not None:
            payload["content_type"] = content_type
        if object_type is not None:
            payload["type"] = object_type
        if usage_hints is not None:
            payload["usage_hints"] = usage_hints
        if referenced_by is not None:
            payload["referenced_by"] = referenced_by
        if extra is not None:
            payload["extra"] = extra
        return await self._request("POST", "/objects", json=payload)

    async def update_object(
        self,
        object_id: str,
        *,
        usage_hints: Optional[list[str]] = None,
        referenced_by: Optional[list[dict[str, Any]]] = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {}
        if usage_hints is not None:
            payload["usage_hints"] = usage_hints
        if referenced_by is not None:
            payload["referenced_by"] = referenced_by
        if not payload:
            raise ValueError("update_object requires at least one field to update")
        return await self._request("PATCH", f"/objects/{object_id}", json=payload)

    async def delete_object(self, object_id: str) -> None:
        await self._request("DELETE", f"/objects/{object_id}")

    async def get_objects_by_entity(
        self, entity_id: str, *, limit: int = 50, offset: int = 0
    ) -> list[dict[str, Any]]:
        return await self._request(
            "GET", f"/entities/{entity_id}/objects", params={"limit": limit, "offset": offset}
        )

    async def get_objects_by_task(
        self, task_id: str, *, limit: int = 50, offset: int = 0
    ) -> list[dict[str, Any]]:
        return await self._request(
            "GET", f"/tasks/{task_id}/objects", params={"limit": limit, "offset": offset}
        )

    async def add_object_reference(
        self,
        object_id: str,
        *,
        entity_id: Optional[str] = None,
        task_id: Optional[str] = None,
    ) -> dict[str, Any]:
        payload = {"entity_id": entity_id, "task_id": task_id}
        return await self._request("POST", f"/objects/{object_id}/references", json=payload)

    async def remove_object_reference(
        self,
        object_id: str,
        *,
        entity_id: Optional[str] = None,
        task_id: Optional[str] = None,
    ) -> dict[str, Any]:
        payload = {"entity_id": entity_id, "task_id": task_id}
        return await self._request("DELETE", f"/objects/{object_id}/references", json=payload)

    async def find_orphaned_objects(
        self, *, limit: int = 100, offset: int = 0
    ) -> list[dict[str, Any]]:
        return await self._request(
            "GET", "/objects/orphaned", params={"limit": limit, "offset": offset}
        )

    async def get_object_references(self, object_id: str) -> dict[str, Any]:
        return await self._request("GET", f"/objects/{object_id}/references/info")

    async def validate_object_references(self, object_id: str) -> list[dict[str, Any]]:
        return await self._request("GET", f"/objects/{object_id}/references/validate")

    async def cleanup_object_references(self, object_id: str) -> dict[str, Any]:
        return await self._request("POST", f"/objects/{object_id}/references/cleanup")

    # Queries ------------------------------------------------------------------

    async def get_changed_since(
        self,
        since: datetime | str,
        *,
        limit_per_type: Optional[int] = None,
    ) -> ChangedSinceResponse:
        params: dict[str, Any] = {"since": since}
        if limit_per_type is not None:
            params["limit_per_type"] = limit_per_type
        return await self._request("GET", "/queries/changed-since", params=params)

    async def get_full_dataset(
        self,
        *,
        entity_limit: Optional[int] = None,
        task_limit: Optional[int] = None,
        object_limit: Optional[int] = None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {}
        if entity_limit is not None:
            params["entity_limit"] = entity_limit
        if task_limit is not None:
            params["task_limit"] = task_limit
        if object_limit is not None:
            params["object_limit"] = object_limit
        return await self._request("GET", "/queries/full", params=params or None)
