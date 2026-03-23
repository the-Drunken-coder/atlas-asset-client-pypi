"""Async HTTP client for Atlas Command."""

from __future__ import annotations

import warnings
from datetime import datetime
from typing import Any, BinaryIO, Mapping, Optional, TypedDict, cast

import httpx
from typing_extensions import NotRequired

from .components import (
    EntityComponents,
    TaskComponents,
    components_to_dict,
)


class _DeletedResourceCore(TypedDict):
    id: str
    type: str


class DeletedResource(_DeletedResourceCore, total=False):
    """Tombstone from changed-since; id and type are always present in API JSON.

    ``get_changed_since`` also injects legacy ``entity_id`` / ``task_id`` / ``object_id``
    aliases (mirroring the npm helper) so callers can use either shape.
    """

    deleted_at: str
    entity_id: NotRequired[str]
    task_id: NotRequired[str]
    object_id: NotRequired[str]


class ChangedSinceResponse(TypedDict, total=False):
    entities: list[dict[str, Any]]
    tasks: list[dict[str, Any]]
    objects: list[dict[str, Any]]
    deleted_entities: list[DeletedResource]
    deleted_tasks: list[DeletedResource]
    deleted_objects: list[DeletedResource]
    timestamp: str
    has_more_entities: bool
    has_more_tasks: bool
    has_more_objects: bool
    has_more_deleted_entities: bool
    has_more_deleted_tasks: bool
    has_more_deleted_objects: bool
    next_entity_cursor: str
    next_task_cursor: str
    next_object_cursor: str
    next_deleted_entity_cursor: str
    next_deleted_task_cursor: str
    next_deleted_object_cursor: str


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
        # Weak ETags from GET /objects/{id}; used as If-Match on PATCH for safe reference updates.
        self._object_etags: dict[str, str] = {}
        self._max_etag_cache_size = 10_000

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

    def _cache_etag(self, object_id: str, etag: str) -> None:
        """Store an ETag with LRU eviction.

        Deletes and re-inserts the key so dict iteration order (insertion order)
        reflects most-recent access, then evicts the oldest entry when the cache
        exceeds its cap.
        """
        self._object_etags.pop(object_id, None)
        self._object_etags[object_id] = etag
        if len(self._object_etags) > self._max_etag_cache_size:
            oldest = next(iter(self._object_etags))
            del self._object_etags[oldest]

    @staticmethod
    def _with_legacy_deleted_aliases(rows: Any, legacy_key: str) -> list[dict[str, Any]] | None:
        if not isinstance(rows, list):
            return None
        out: list[dict[str, Any]] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            entry = dict(row)
            rid = entry.get("id")
            if isinstance(rid, str):
                entry[legacy_key] = rid
            out.append(entry)
        return out

    def _normalize_changed_since_response(self, payload: Any) -> ChangedSinceResponse:
        if not isinstance(payload, dict):
            return cast(ChangedSinceResponse, payload)
        result = dict(payload)
        pairs = (
            ("deleted_entities", "entity_id"),
            ("deleted_tasks", "task_id"),
            ("deleted_objects", "object_id"),
        )
        for key, legacy in pairs:
            normalized = self._with_legacy_deleted_aliases(result.get(key), legacy)
            if normalized is not None:
                result[key] = normalized
        return cast(ChangedSinceResponse, result)

    async def _request(self, method: str, path: str, **kwargs: Any) -> Any:
        response = await self._http.request(method, path, headers=self._headers(), **kwargs)
        response.raise_for_status()
        if response.content:
            return response.json()
        return None

    async def _get_json_allow_not_found(self, path: str) -> dict[str, Any] | None:
        """GET JSON body or None when the server returns 404."""
        response = await self._http.get(path, headers=self._headers())
        if response.status_code == 404:
            return None
        response.raise_for_status()
        if response.content:
            body = response.json()
            if isinstance(body, dict):
                return body
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
    ) -> tuple[dict[str, Any], Optional[str]]:
        response = await self._http.post(
            path, headers=self._multipart_headers(), files=files, data=data
        )
        response.raise_for_status()
        etag = response.headers.get("etag")
        if response.content:
            body = response.json()
            if isinstance(body, dict):
                return body, etag
        return {}, etag

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
        if isinstance(components, dict):
            components = EntityComponents(**components)
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
        if isinstance(components, dict):
            components = EntityComponents(**components)
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
        self,
        *,
        limit: int = 25,
        offset: int = 0,
        status: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        """List tasks. ``status`` is deprecated and ignored (API no longer filters server-side)."""
        if status is not None:
            warnings.warn(
                "list_tasks(status=...) is deprecated and ignored; remove the status argument.",
                DeprecationWarning,
                stacklevel=2,
            )
        params: dict[str, Any] = {"limit": limit, "offset": offset}
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
        self,
        entity_id: str,
        *,
        status: Optional[str] = None,
        limit: int = 25,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        if status is not None:
            warnings.warn(
                "get_tasks_by_entity(status=...) is deprecated and ignored; "
                "the API does not filter tasks by status on this endpoint.",
                DeprecationWarning,
                stacklevel=2,
            )
        params: dict[str, Any] = {"limit": limit, "offset": offset}
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
        progress: Optional[float] = None,
        message: Optional[str] = None,
        validate: Optional[bool] = None,
        extra: Optional[Mapping[str, Any]] = None,
    ) -> dict[str, Any]:
        if validate is not None or extra is not None:
            warnings.warn(
                "transition_task_status(validate=..., extra=...) is deprecated and ignored; "
                "use progress/message only.",
                DeprecationWarning,
                stacklevel=2,
            )
        payload: dict[str, Any] = {"status": status}
        if progress is not None:
            payload["progress"] = progress
        if message is not None:
            payload["message"] = message
        return await self._request("POST", f"/tasks/{task_id}/status", json=payload)

    async def fail_task(
        self,
        task_id: str,
        *,
        error_message: Optional[str] = None,
        error_details: Optional[Mapping[str, Any]] = None,
    ) -> dict[str, Any]:
        err: dict[str, Any] = {}
        if error_message is not None:
            err["message"] = error_message
        if error_details is not None:
            err["details"] = error_details
        payload: dict[str, Any] = {}
        if err:
            payload["error"] = err
        return await self._request("POST", f"/tasks/{task_id}/fail", json=payload)

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
        limit: int = 100,
        offset: int = 0,
        content_type: Optional[str] = None,
        type: Optional[str] = None,  # noqa: A002
        validate: Optional[Any] = None,
    ) -> list[dict[str, Any]]:
        """List objects. ``content_type``, ``type``, and ``validate`` are deprecated and ignored."""
        if content_type is not None:
            warnings.warn(
                "list_objects(content_type=...) is deprecated and ignored; remove the argument.",
                DeprecationWarning,
                stacklevel=2,
            )
        if type is not None:
            warnings.warn(
                "list_objects(type=...) is deprecated and ignored; remove the argument.",
                DeprecationWarning,
                stacklevel=2,
            )
        if validate is not None:
            warnings.warn(
                "list_objects(validate=...) is deprecated and ignored; remove the argument.",
                DeprecationWarning,
                stacklevel=2,
            )
        params: dict[str, Any] = {"limit": limit, "offset": offset}
        return await self._request("GET", "/objects", params=params)

    async def get_object(self, object_id: str) -> dict[str, Any]:
        data, _etag = await self._get_object_with_etag(object_id)
        return data

    async def _get_object_with_etag(self, object_id: str) -> tuple[dict[str, Any], str | None]:
        """Fetch object JSON and ETag atomically from a single response."""
        response = await self._http.get(f"/objects/{object_id}", headers=self._headers())
        response.raise_for_status()
        etag = response.headers.get("etag")
        if etag:
            self._cache_etag(object_id, etag)
        else:
            self._object_etags.pop(object_id, None)
        data = response.json() if response.content else {}
        return data, etag

    async def _patch_object(
        self,
        object_id: str,
        payload: dict[str, Any],
        *,
        if_match: Optional[str] = None,
    ) -> dict[str, Any]:
        """PATCH object with If-Match when an ETag is cached.

        If ``if_match`` is set, it is sent as ``If-Match`` instead of the cached ETag
        from :meth:`get_object`. A 412 is always surfaced to the caller (no silent retry
        with a stale payload).
        """
        headers = dict(self._headers())
        etag = if_match if if_match is not None else self._object_etags.get(object_id)
        if etag:
            headers["If-Match"] = etag
        response = await self._http.patch(f"/objects/{object_id}", headers=headers, json=payload)
        response.raise_for_status()
        new_etag = response.headers.get("etag")
        if new_etag:
            self._cache_etag(object_id, new_etag)
        else:
            self._object_etags.pop(object_id, None)
        if response.content:
            return response.json()
        return {}

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

        stored, upload_etag = await self._multipart_request(
            "/objects/upload", files=files, data=data
        )

        stored_object_id = stored.get("object_id")
        etag = upload_etag or stored.get("etag") or stored.get("object_etag")
        if stored_object_id:
            if etag:
                self._cache_etag(stored_object_id, etag)
            else:
                self._object_etags.pop(stored_object_id, None)
        if referenced_by:
            if not stored_object_id:
                raise RuntimeError(
                    "AtlasCommandHttpClient.create_object expected the upload response to include "
                    "an object_id before attaching references."
                )
            if not etag:
                raise RuntimeError(
                    "AtlasCommandHttpClient.create_object cannot attach references safely because "
                    "the upload response did not include an ETag."
                )
            return await self.update_object(
                stored_object_id,
                referenced_by=referenced_by,
                if_match=etag,
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
        response = await self._http.post("/objects", headers=self._headers(), json=payload)
        response.raise_for_status()
        body: dict[str, Any] = response.json() if response.content else {}
        oid = body.get("object_id") or payload.get("object_id")
        if isinstance(oid, str):
            etag = response.headers.get("etag")
            if etag:
                self._cache_etag(oid, etag)
        return body

    async def update_object(
        self,
        object_id: str,
        *,
        usage_hints: Optional[list[str]] = None,
        referenced_by: Optional[list[dict[str, Any]]] = None,
        if_match: Optional[str] = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {}
        if usage_hints is not None:
            payload["usage_hints"] = usage_hints
        if referenced_by is not None:
            payload["referenced_by"] = referenced_by
        if not payload:
            raise ValueError("update_object requires at least one field to update")
        return await self._patch_object(object_id, payload, if_match=if_match)

    async def delete_object(self, object_id: str) -> None:
        await self._request("DELETE", f"/objects/{object_id}")
        self._object_etags.pop(object_id, None)

    async def get_objects_by_entity(
        self, entity_id: str, *, limit: int = 50, offset: int = 0
    ) -> list[dict[str, Any]]:
        return await self._request(
            "GET",
            f"/entities/{entity_id}/objects",
            params={"limit": limit, "offset": offset},
        )

    async def get_objects_by_task(
        self, task_id: str, *, limit: int = 50, offset: int = 0
    ) -> list[dict[str, Any]]:
        return await self._request(
            "GET",
            f"/tasks/{task_id}/objects",
            params={"limit": limit, "offset": offset},
        )

    async def add_object_reference(
        self,
        object_id: str,
        *,
        entity_id: Optional[str] = None,
        task_id: Optional[str] = None,
    ) -> dict[str, Any]:
        """Append a reference (GET + PATCH). Uses If-Match when the server returns ETags."""
        new_ref = {k: v for k, v in (("entity_id", entity_id), ("task_id", task_id)) if v}
        if not new_ref:
            raise ValueError("add_object_reference requires entity_id and/or task_id")

        for attempt in range(2):
            obj, etag = await self._get_object_with_etag(object_id)
            refs_any = obj.get("referenced_by")
            refs: list[dict[str, Any]] = []
            if isinstance(refs_any, list):
                for item in refs_any:
                    if isinstance(item, dict):
                        refs.append(dict(item))

            def _same_ref(r: dict[str, Any]) -> bool:
                return r.get("entity_id") == new_ref.get("entity_id") and r.get(
                    "task_id"
                ) == new_ref.get("task_id")

            if any(_same_ref(r) for r in refs):
                return obj
            refs.append(new_ref)
            try:
                return await self.update_object(object_id, referenced_by=refs, if_match=etag)
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code == 412 and attempt == 0:
                    continue
                raise

        raise RuntimeError("add_object_reference retry exhausted")

    async def remove_object_reference(
        self,
        object_id: str,
        *,
        entity_id: Optional[str] = None,
        task_id: Optional[str] = None,
    ) -> dict[str, Any]:
        """Remove refs that exactly match the given dimensions (pair, entity-only, or task-only).

        Entity-only removes only references with that entity and no task; task-only removes
        only references with that task and no entity. See add_object_reference re: concurrency.
        """

        def should_remove(r: dict[str, Any]) -> bool:
            re = r.get("entity_id")
            rt = r.get("task_id")
            entity_unset = re is None
            task_unset = rt is None
            if entity_id is not None and task_id is not None:
                return re == entity_id and rt == task_id
            if entity_id is not None:
                return re == entity_id and task_unset
            return rt == task_id and entity_unset

        if entity_id is None and task_id is None:
            raise ValueError("remove_object_reference requires entity_id and/or task_id")

        for attempt in range(2):
            obj, etag = await self._get_object_with_etag(object_id)
            refs_any = obj.get("referenced_by")
            refs: list[dict[str, Any]] = []
            if isinstance(refs_any, list):
                for item in refs_any:
                    if isinstance(item, dict):
                        refs.append(dict(item))

            new_refs = [r for r in refs if not should_remove(r)]
            if len(new_refs) == len(refs):
                return obj
            try:
                return await self.update_object(object_id, referenced_by=new_refs, if_match=etag)
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code == 412 and attempt == 0:
                    continue
                raise

        raise RuntimeError("remove_object_reference retry exhausted")

    async def find_orphaned_objects(
        self, *, limit: int = 100, offset: int = 0
    ) -> list[dict[str, Any]]:
        return await self._request(
            "GET", "/objects/orphaned", params={"limit": limit, "offset": offset}
        )

    async def get_object_references(self, object_id: str) -> dict[str, Any]:
        obj = await self.get_object(object_id)
        rb = obj.get("referenced_by")
        if rb is None:
            rb = []
        if not isinstance(rb, list):
            rb = []
        oid = obj.get("object_id", object_id)
        return {"object_id": oid, "referenced_by": rb}

    async def validate_object_references(self, object_id: str) -> list[dict[str, Any]]:
        """Resolve each reference via GET entity/task; no dedicated validate route on the API."""
        checks, _ = await self._validate_object_references_snapshot(object_id)
        return checks

    async def _validate_object_references_snapshot(
        self, object_id: str
    ) -> tuple[list[dict[str, Any]], Optional[str]]:
        response = await self._http.get(f"/objects/{object_id}", headers=self._headers())
        response.raise_for_status()
        etag = response.headers.get("etag")
        if etag:
            self._cache_etag(object_id, etag)
        else:
            self._object_etags.pop(object_id, None)
        obj = response.json() if response.content else {}
        refs_any = obj.get("referenced_by")
        out: list[dict[str, Any]] = []
        if not isinstance(refs_any, list):
            return out, etag
        for item in refs_any:
            if not isinstance(item, dict):
                out.append(
                    {
                        "status": "invalid_format",
                        "reason": "reference_not_object",
                    }
                )
                continue
            ref = dict(item)
            eid = ref.get("entity_id")
            tid = ref.get("task_id")
            if eid is None and tid is None:
                ref["status"] = "invalid_format"
                ref["reason"] = "missing_entity_and_task"
                out.append(ref)
                continue
            valid = True
            if eid is not None:
                ent = await self._get_json_allow_not_found(f"/entities/{eid}")
                if ent is None:
                    valid = False
                    ref.setdefault("reason", "entity_not_found")
            if valid and tid is not None:
                task = await self._get_json_allow_not_found(f"/tasks/{tid}")
                if task is None:
                    valid = False
                    ref.setdefault("reason", "task_not_found")
            ref["status"] = "valid" if valid else "invalid"
            out.append(ref)
        return out, etag

    async def cleanup_object_references(self, object_id: str) -> dict[str, Any]:
        """Drop references whose entity/task no longer exists (PATCH referenced_by)."""
        checks, etag = await self._validate_object_references_snapshot(object_id)
        kept: list[dict[str, Any]] = []
        for row in checks:
            if row.get("status") == "valid":
                kept.append({k: v for k, v in row.items() if k not in ("status", "reason")})
        removed = sum(1 for r in checks if r.get("status") != "valid")
        if removed == 0:
            return {"object_id": object_id, "cleaned": 0}
        await self.update_object(object_id, referenced_by=kept, if_match=etag)
        return {"object_id": object_id, "cleaned": removed}

    # Queries ------------------------------------------------------------------

    async def get_changed_since(
        self,
        since: datetime | str,
        *,
        limit_per_type: Optional[int] = None,
        entity_cursor: Optional[str] = None,
        task_cursor: Optional[str] = None,
        object_cursor: Optional[str] = None,
        deleted_entity_cursor: Optional[str] = None,
        deleted_task_cursor: Optional[str] = None,
        deleted_object_cursor: Optional[str] = None,
    ) -> ChangedSinceResponse:
        if isinstance(since, datetime):
            if since.tzinfo is None:
                raise ValueError("since must be timezone-aware")
            since = since.isoformat()
        params: dict[str, Any] = {"since": since}
        if limit_per_type is not None:
            params["limit_per_type"] = limit_per_type
        if entity_cursor is not None:
            params["entity_cursor"] = entity_cursor
        if task_cursor is not None:
            params["task_cursor"] = task_cursor
        if object_cursor is not None:
            params["object_cursor"] = object_cursor
        if deleted_entity_cursor is not None:
            params["deleted_entity_cursor"] = deleted_entity_cursor
        if deleted_task_cursor is not None:
            params["deleted_task_cursor"] = deleted_task_cursor
        if deleted_object_cursor is not None:
            params["deleted_object_cursor"] = deleted_object_cursor
        raw = await self._request("GET", "/queries/changed-since", params=params)
        return self._normalize_changed_since_response(raw)

    async def get_full_dataset(
        self,
        *,
        entity_limit: Optional[int] = None,
        task_limit: Optional[int] = None,
        object_limit: Optional[int] = None,
        entity_cursor: Optional[str] = None,
        task_cursor: Optional[str] = None,
        object_cursor: Optional[str] = None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {}
        if entity_limit is not None:
            params["entity_limit"] = entity_limit
        if task_limit is not None:
            params["task_limit"] = task_limit
        if object_limit is not None:
            params["object_limit"] = object_limit
        if entity_cursor is not None:
            params["entity_cursor"] = entity_cursor
        if task_cursor is not None:
            params["task_cursor"] = task_cursor
        if object_cursor is not None:
            params["object_cursor"] = object_cursor
        return await self._request("GET", "/queries/full", params=params or None)
