"""Add-only and bounded redacted queries through the existing owner worker."""

from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
import re
import stat
from typing import Final

try:
    from mcp import types
    from mcp.server import InitializationOptions, NotificationOptions, Server
    from mcp.server.stdio import stdio_server
except ModuleNotFoundError as error:
    if error.name != "mcp":
        raise
    _MCP_SDK_AVAILABLE = False
else:
    _MCP_SDK_AVAILABLE = True

from hermes_downloads.ipc import (AddBatchCommand, add_batch, request_health,
    request_query_list, request_query_status, request_query_events, validate_query_cursor)


_SERVER_NAME: Final = "hermes-downloads-query"
_SERVER_VERSION: Final = "0.1.0"
_TOOL_NAME: Final = "downloads_query"
_SAFE_INVALID_INPUT: Final = "downloads_query_invalid_input"
_SAFE_UNAVAILABLE: Final = "downloads_query_unavailable"
_SOCKET_ENVIRONMENT_VARIABLE: Final = "HERMES_DOWNLOADS_SOCKET"
_IDENTIFIER: Final = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}\Z")
_OWNER_ONLY_SOCKET_MODE: Final = 0o600

_QUERY_INPUT_SCHEMA: Final[dict[str, object]] = {
    "type": "object",
    "additionalProperties": False,
    "required": ["scope"],
    "properties": {
        "scope": {"type": "string", "enum": ["health", "list", "status", "events"]},
        "id": {"type": "string", "pattern": "^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$"},
        "cursor": {
            "anyOf": [
                {"type": "null"},
                {
                    "type": "string",
                    "maxLength": 1024,
                    "pattern": "^(?:[A-Za-z0-9][A-Za-z0-9._:-]{0,127}|~q1:[A-Za-z0-9_-]+)$",
                },
            ]
        },
    },
    "oneOf": [
        {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "scope": {"const": "health"},
                "cursor": {"type": "null"},
            },
            "required": ["scope"],
        },
        {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "scope": {"const": "list"},
                "cursor": {"anyOf": [
                    {"type": "null"},
                    {"type": "string", "maxLength": 1024,
                     "pattern": "^(?:[A-Za-z0-9][A-Za-z0-9._:-]{0,127}|~q1:[A-Za-z0-9_-]+)$"},
                ]},
            },
            "required": ["scope"],
        },
        {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "scope": {"const": "status"},
                "id": {"type": "string", "pattern": "^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$"},
            },
            "required": ["scope", "id"],
        },
        {
            "type": "object",
            "additionalProperties": False,
            "properties": {"scope": {"const": "events"}, "cursor": {"anyOf": [
                {"type": "null"}, {"type": "string", "maxLength": 1024, "pattern": "^~q1:[A-Za-z0-9_-]+$"}]}},
            "required": ["scope"],
        },
    ],
}

_ADD_ITEM_KEYS: Final = frozenset({'job', 'source_kind', 'source_url', 'priority',
    'category', 'partial_filename', 'selected_final_filename', 'expected_sha256'})
_ADD_INPUT_SCHEMA: Final = {
    'type': 'object', 'additionalProperties': False, 'required': ['items', 'request_id'],
    'properties': {
        'request_id': {'type': 'string', 'pattern': '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'},
        'collection': {'anyOf': [{'type': 'null'}, {'type': 'string'}]},
        'start': {'type': 'boolean', 'default': False, 'description': 'Only false is supported; true fails before IPC.'},
        'items': {'type': 'array', 'minItems': 1, 'maxItems': 500, 'items': {
            'type': 'object', 'additionalProperties': False,
            'required': sorted(_ADD_ITEM_KEYS - {'expected_sha256'}),
            'properties': {**{key: {'type': 'string'} for key in _ADD_ITEM_KEYS - {'priority', 'expected_sha256'}},
                'priority': {'type': 'integer'},
                'expected_sha256': {'anyOf': [{'type': 'null'}, {'type': 'string'}]}},
        }},
    },
}


def _error_result(code: str) -> types.CallToolResult:
    return types.CallToolResult(
        content=[types.TextContent(type="text", text=code)],
        isError=True,
    )


def _success_result(record: dict[str, object]) -> types.CallToolResult:
    return types.CallToolResult(
        content=[
            types.TextContent(
                type="text",
                text=json.dumps(record, separators=(",", ":"), sort_keys=True, allow_nan=False),
            )
        ],
        structuredContent=record,
    )


def _parse_query_arguments(arguments: object) -> tuple[str, str | None] | None:
    """Return the closed query request or reject it before worker IPC."""

    if type(arguments) is not dict:
        return None
    scope = arguments.get("scope")
    if type(scope) is not str or scope not in {"health", "list", "status", "events"}:
        return None
    if scope == 'status':
        id = arguments.get('id')
        return (scope, id) if set(arguments) == {'scope', 'id'} and type(id) is str and _IDENTIFIER.fullmatch(id) else None
    if set(arguments) not in ({'scope'}, {'scope', 'cursor'}):
        return None
    cursor = arguments.get("cursor")
    if scope == "health":
        return (scope, None) if cursor is None else None
    try:
        validate_query_cursor(cursor, scope)
    except (TypeError, ValueError, UnicodeError, RecursionError):
        return None
    return scope, cursor


def _parse_add_arguments(arguments):
    if (type(arguments) is not dict or not {'items', 'request_id'} <= set(arguments) <= {'items', 'request_id', 'collection', 'start'}
            or type(arguments['request_id']) is not str or type(arguments.get('start', False)) is not bool
            or arguments.get('collection') is not None and type(arguments['collection']) is not str):
        raise ValueError('invalid add envelope')
    items = arguments['items']
    if type(items) is not list or not 1 <= len(items) <= 500:
        raise ValueError('invalid add count')
    for item in items:
        if type(item) is not dict or not _ADD_ITEM_KEYS - {'expected_sha256'} <= set(item) <= _ADD_ITEM_KEYS:
            raise ValueError('invalid add item')
        if (any(type(item[key]) is not str for key in _ADD_ITEM_KEYS - {'priority', 'expected_sha256'})
                or type(item['priority']) is not int
                or item.get('expected_sha256') is not None and type(item['expected_sha256']) is not str):
            raise ValueError('invalid add scalar')
    return items


def _worker_socket_from_environment() -> Path | None:
    """Accept only the existing owner-only absolute worker endpoint."""

    value = os.environ.get(_SOCKET_ENVIRONMENT_VARIABLE)
    if not value:
        return None
    try:
        socket_path = Path(value)
        if not socket_path.is_absolute():
            return None
        socket_stat = socket_path.lstat()
    except (OSError, ValueError):
        return None
    if (
        not stat.S_ISSOCK(socket_stat.st_mode)
        or socket_stat.st_uid != os.geteuid()
        or stat.S_IMODE(socket_stat.st_mode) != _OWNER_ONLY_SOCKET_MODE
    ):
        return None
    return socket_path


def create_server() -> Server:
    """Register exactly the two implemented tools through the official SDK."""

    if not _MCP_SDK_AVAILABLE:
        raise ModuleNotFoundError("No module named 'mcp'", name="mcp")
    server = Server(
        _SERVER_NAME,
        version=_SERVER_VERSION,
        instructions=(
            "Version 1 add-only creation and redacted health/list/status/audit queries. "
            "Add requires stable explicit IDs and managed filenames; semantic refusals are ordered per item. "
            "Successful direct entries accept optional lowercase SHA256; no start or transfer is supported. "
            "List pages are finite live keysets bounded by a captured maximum, not immutable membership. "
            "List/events use fixed 100 pages and cold-sensitive cursors; new counters are JSON safe integers. "
            "Creation receipt order_key is always a decimal string or null. Other five tools remain unavailable."
        ),
    )

    @server.list_tools()
    async def list_tools() -> list[types.Tool]:
        return [
            types.Tool(name='downloads_add', title='Add downloads',
                description='Atomically create 1..500 inactive direct jobs with stable replay; never authorize or start.',
                inputSchema=_ADD_INPUT_SCHEMA),
            types.Tool(
                name=_TOOL_NAME,
                title="Downloads query",
                description=(
                    "Read compatible health, a v1 lifecycle page/status, or whitelisted audit events; no private source fields."
                ),
                inputSchema=_QUERY_INPUT_SCHEMA,
            )
        ]

    @server.call_tool(validate_input=False)
    async def call_tool(name: str, arguments: dict[str, object]) -> types.CallToolResult:
        if name == 'downloads_add':
            try:
                items = _parse_add_arguments(arguments)
            except (TypeError, ValueError, UnicodeError, RecursionError):
                return _error_result('downloads_add_invalid_input')
            if arguments.get('start', False):
                return _error_result('downloads_add_start_unsupported')
            try:
                command = AddBatchCommand(arguments['request_id'], arguments.get('collection'), items)
            except (TypeError, ValueError, UnicodeError, RecursionError):
                return _error_result('downloads_add_invalid_input')
            try:
                socket_path = _worker_socket_from_environment()
                if socket_path is None:
                    return _error_result('downloads_add_unavailable')
                wire = command.to_record()
                receipt = add_batch(socket_path, request_id=command.request_id,
                    collection=command.collection, entries=wire['entries']).to_record()
                for result in receipt['results']:
                    if result['order_key'] is not None:
                        result['order_key'] = str(result['order_key'])
                if len(json.dumps(receipt, separators=(',', ':'), sort_keys=True, allow_nan=False).encode('utf-8')) > 512 * 1024:
                    return _error_result('downloads_add_unavailable')
                return _success_result(receipt)
            except Exception:
                return _error_result('downloads_add_unavailable')
        if name != _TOOL_NAME:
            return _error_result(_SAFE_INVALID_INPUT)
        parsed = _parse_query_arguments(arguments)
        if parsed is None:
            return _error_result(_SAFE_INVALID_INPUT)
        scope, cursor = parsed
        try:
            socket_path = _worker_socket_from_environment()
            if socket_path is None:
                return _error_result(_SAFE_UNAVAILABLE)
            if scope == "health":
                health = request_health(socket_path)
                return _success_result(
                    {
                        "worker_epoch": health.worker_epoch,
                        "queue_gate": health.queue_gate,
                    }
                )
            if scope == 'status':
                result = request_query_status(socket_path, id=cursor)
            elif scope == 'events':
                result = request_query_events(socket_path, cursor=cursor)
            else:
                result = request_query_list(socket_path, cursor=cursor)
            return _success_result(result.to_record())
        except Exception:
            return _error_result(_SAFE_UNAVAILABLE)

    return server


def _initialization_options(server: Server) -> InitializationOptions:
    """Publish only the registered tools capability through mcp==1.29.1 APIs.

    In this SDK, sampling is a client capability rather than a server capability.
    The low-level Server has no sampling handler or client session, and this exact
    capability construction advertises only the registered tools surface.
    """

    return InitializationOptions(
        server_name=_SERVER_NAME,
        server_version=_SERVER_VERSION,
        capabilities=server.get_capabilities(
            notification_options=NotificationOptions(
                prompts_changed=False,
                resources_changed=False,
                tools_changed=False,
            ),
            experimental_capabilities={},
        ),
        instructions=server.instructions,
    )


async def _serve_stdio() -> None:
    server = create_server()
    async with stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            _initialization_options(server),
        )


def main() -> int:
    """Run the read-only adapter over the official SDK stdio transport."""

    try:
        asyncio.run(_serve_stdio())
    except ModuleNotFoundError as error:
        if error.name != "mcp":
            raise
    return 0
