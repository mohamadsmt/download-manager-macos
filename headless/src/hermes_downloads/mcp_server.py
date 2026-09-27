"""Read-only stdio MCP foundation for worker-owned download queries.

This deliberately exposes only redacted health and job-page projections through the
existing worker IPC socket. It does not own worker, queue, engine, SQLite, or
payload lifecycle.
"""

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

from hermes_downloads.ipc import request_health, request_jobs_page


_SERVER_NAME: Final = "hermes-downloads-query"
_SERVER_VERSION: Final = "0.0.0"
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
        "scope": {"type": "string", "enum": ["health", "list"]},
        "cursor": {
            "anyOf": [
                {"type": "null"},
                {
                    "type": "string",
                    "pattern": "^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$",
                },
            ]
        },
    },
    "oneOf": [
        {
            "properties": {
                "scope": {"const": "health"},
                "cursor": {"type": "null"},
            },
            "required": ["scope"],
        },
        {
            "properties": {"scope": {"const": "list"}},
            "required": ["scope"],
        },
    ],
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
                text=json.dumps(record, separators=(",", ":"), sort_keys=True),
            )
        ],
        structuredContent=record,
    )


def _parse_query_arguments(arguments: object) -> tuple[str, str | None] | None:
    """Return the closed query request or reject it before worker IPC."""

    if type(arguments) is not dict or set(arguments) not in ({"scope"}, {"scope", "cursor"}):
        return None
    scope = arguments["scope"]
    if type(scope) is not str or scope not in {"health", "list"}:
        return None
    cursor = arguments.get("cursor")
    if scope == "health":
        return (scope, None) if cursor is None else None
    if cursor is not None and (
        type(cursor) is not str or _IDENTIFIER.fullmatch(cursor) is None
    ):
        return None
    return scope, cursor


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
    """Build the official-SDK server for the intentionally read-only T18 slice."""

    if not _MCP_SDK_AVAILABLE:
        raise ModuleNotFoundError("No module named 'mcp'", name="mcp")
    server = Server(
        _SERVER_NAME,
        version=_SERVER_VERSION,
        instructions=(
            "Read-only downloads worker queries only. This server cannot start, stop, "
            "create, edit, or otherwise mutate downloads."
        ),
    )

    @server.list_tools()
    async def list_tools() -> list[types.Tool]:
        return [
            types.Tool(
                name=_TOOL_NAME,
                title="Downloads query",
                description=(
                    "Read the worker health projection or one redacted bounded job page."
                ),
                inputSchema=_QUERY_INPUT_SCHEMA,
            )
        ]

    @server.call_tool(validate_input=False)
    async def call_tool(name: str, arguments: dict[str, object]) -> types.CallToolResult:
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
            page = request_jobs_page(socket_path, cursor=cursor)
            return _success_result(page.to_record())
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
