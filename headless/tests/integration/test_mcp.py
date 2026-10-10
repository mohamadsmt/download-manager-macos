"""Real stdio MCP coverage for the read-only downloads query adapter."""

from __future__ import annotations

import asyncio
import builtins
import json
import multiprocessing
import os
from pathlib import Path
from queue import Empty
import tempfile
import tomllib
from typing import Any

import pytest
from mcp import ClientSession, types
from mcp.client.stdio import StdioServerParameters, stdio_client

from hermes_downloads import worker
from hermes_downloads.models import DownloadIntent
from hermes_downloads.store import SQLiteStore


_WATCHDOG_SECONDS = 5.0
_HEADLESS_ROOT = Path(__file__).resolve().parents[2]
_SAFE_INVALID_INPUT = "downloads_query_invalid_input"
_SAFE_UNAVAILABLE = "downloads_query_unavailable"
_EXPECTED_INPUT_SCHEMA: dict[str, object] = {
    "type": "object",
    "additionalProperties": False,
    "required": ["scope"],
    "properties": {
        "scope": {"type": "string", "enum": ["health", "list", "status", "events", "queue"]},
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
            "type": "object", "additionalProperties": False,
            "properties": {"scope": {"const": "queue"}}, "required": ["scope"],
        },
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


def _run_worker_process(
    state_root: str,
    socket_path: str,
    ready_event: object,
    shutdown_event: object,
    stopped_event: object,
    results: object,
) -> None:
    try:
        outcome = worker.run_worker(
            Path(state_root),
            socket_path=Path(socket_path),
            ready_event=ready_event,
            shutdown_event=shutdown_event,
            stopped_event=stopped_event,
        )
    except BaseException as error:
        results.put(("error", type(error).__name__, str(error)))
    else:
        results.put(("result", outcome))


def _run_worker_process_with_engine_import_guard(
    state_root: str,
    socket_path: str,
    ready_event: object,
    shutdown_event: object,
    stopped_event: object,
    results: object,
) -> None:
    """Keep real worker queries from lazily importing download engines."""

    original_import = builtins.__import__

    def guarded_import(
        name: str,
        globals: object = None,
        locals: object = None,
        fromlist: object = (),
        level: int = 0,
    ) -> object:
        if name in {"hermes_downloads.direct", "hermes_downloads.video"}:
            raise AssertionError("read-only MCP query imported a download engine")
        return original_import(name, globals, locals, fromlist, level)

    builtins.__import__ = guarded_import
    try:
        _run_worker_process(
            state_root,
            socket_path,
            ready_event,
            shutdown_event,
            stopped_event,
            results,
        )
    finally:
        builtins.__import__ = original_import


def _worker_result(results: Any) -> tuple[object, ...]:
    try:
        result = results.get(timeout=_WATCHDOG_SECONDS)
    except Empty:
        pytest.fail("worker did not report an outcome")
    assert type(result) is tuple
    return result


def _join(process: multiprocessing.Process) -> None:
    process.join(_WATCHDOG_SECONDS)
    assert not process.is_alive()
    assert process.exitcode == 0


def _seed_private_jobs(state_root: Path, *, count: int) -> None:
    store = SQLiteStore(state_root / "state.db")
    try:
        for index in range(count):
            store.apply_add(
                DownloadIntent(
                    job_id=f"job-{index:03d}",
                    request_id=f"request-{index:03d}",
                    payload_digest=f"{index:064x}",
                    source_url=(
                        "https://example.invalid/private-source-"
                        f"{index}?token=synthetic-secret"
                    ).encode("utf-8"),
                    generation=index,
                    revision=index,
                )
            )
    finally:
        store.close()


def _worker_snapshot(state_root: Path) -> tuple[object, ...]:
    store = SQLiteStore(state_root / "state.db")
    try:
        return (
            store.worker_epoch(),
            store.queue_gate(),
            tuple(
                (job.job, job.generation, job.revision, job.state)
                for job in store.list_jobs()
            ),
            store.get_direct_engine_record(),
            store.get_direct_engine_activation_fence(),
        )
    finally:
        store.close()


def _server_parameters(
    *, socket_value: str | None, state_root: Path
) -> StdioServerParameters:
    environment = {
        "HERMES_DOWNLOADS_STATE_ROOT": str(state_root),
        "PYTHONHOME": "/definitely-not-a-python-home",
        "PYTHONPATH": "/definitely-not-a-python-path",
    }
    if socket_value is not None:
        environment["HERMES_DOWNLOADS_SOCKET"] = socket_value
    return StdioServerParameters(
        command=str(_HEADLESS_ROOT / "scripts" / "run-mcp"),
        env=environment,
    )


async def _query_worker(
    *, socket_value: str | None, state_root: Path, arguments: list[dict[str, object]]
) -> tuple[types.InitializeResult, types.ListToolsResult, list[types.CallToolResult], str]:
    with tempfile.TemporaryFile(mode="w+t", encoding="utf-8") as error_log:
        async with stdio_client(
            _server_parameters(socket_value=socket_value, state_root=state_root),
            errlog=error_log,
        ) as (read_stream, write_stream):
            async with ClientSession(
                read_stream,
                write_stream,
                sampling_capabilities=None,
            ) as session:
                initialized = await session.initialize()
                tools = await session.list_tools()
                results = [
                    await session.call_tool("downloads_query", arguments=arguments_for_call)
                    for arguments_for_call in arguments
                ]
        error_log.seek(0)
        errors = error_log.read()
    return initialized, tools, results, errors


def _result_text(result: types.CallToolResult) -> str:
    assert len(result.content) == 1
    content = result.content[0]
    assert isinstance(content, types.TextContent)
    return content.text


def test_mcp_sdk_is_pinned_runtime_dependency() -> None:
    """The MCP server's official SDK ships with the production package."""

    with (_HEADLESS_ROOT / "pyproject.toml").open("rb") as manifest_file:
        manifest = tomllib.load(manifest_file)

    runtime_mcp_requirements = [
        dependency
        for dependency in manifest["project"]["dependencies"]
        if dependency.split("[", 1)[0] == "mcp"
    ]
    assert runtime_mcp_requirements == ["mcp[cli]==1.29.1"]


def test_downloads_query_server_factory_is_available() -> None:
    """The adapter exposes a constructible official-SDK server factory."""

    from hermes_downloads.mcp_server import create_server

    assert callable(create_server)


def test_downloads_query_stdio_round_trip_is_read_only_and_redacted() -> None:
    """The official SDK discovers exactly three tools and real redacted reads."""

    with tempfile.TemporaryDirectory(dir="/tmp", prefix="hd-mcp-") as temporary_root:
        state_root = Path(temporary_root) / "state"
        state_root.mkdir(mode=0o700)
        _seed_private_jobs(state_root, count=101)
        socket_path = state_root / "worker.sock"
        context = multiprocessing.get_context("spawn")
        ready = context.Event()
        shutdown = context.Event()
        stopped = context.Event()
        results = context.Queue()
        process = context.Process(
            target=_run_worker_process_with_engine_import_guard,
            args=(
                str(state_root),
                str(socket_path),
                ready,
                shutdown,
                stopped,
                results,
            ),
        )
        process.start()
        try:
            assert ready.wait(_WATCHDOG_SECONDS), _worker_result(results)
            assert socket_path.is_socket()
            assert os.stat(socket_path).st_mode & 0o777 == 0o600
            before = _worker_snapshot(state_root)
            assert before[3:] == (None, None)
            initialized, listed, queried, error_log = asyncio.run(
                _query_worker(
                    socket_value=str(socket_path),
                    state_root=state_root,
                    arguments=[
                        {"scope": "health"},
                        {"scope": "list"},
                        {"scope": "list", "cursor": "job-099"},
                    ],
                )
            )
            assert error_log == ""
            assert initialized.capabilities.model_dump(by_alias=True, exclude_none=True) == {
                "experimental": {},
                "tools": {"listChanged": False},
            }
            assert sorted(tool.name for tool in listed.tools) == ['downloads_add', 'downloads_control', 'downloads_query']
            tool = next(tool for tool in listed.tools if tool.name == 'downloads_query')
            assert tool.name == "downloads_query"
            assert tool.inputSchema == _EXPECTED_INPUT_SCHEMA

            health, first_page, second_page = queried
            assert not health.isError
            assert health.structuredContent == {"worker_epoch": 1, "queue_gate": "paused"}
            assert json.loads(_result_text(health)) == health.structuredContent

            assert not first_page.isError
            assert first_page.structuredContent is not None
            assert set(first_page.structuredContent) == {"jobs", "next_cursor", "has_more"}
            assert len(first_page.structuredContent["jobs"]) == 100
            assert first_page.structuredContent["next_cursor"].startswith('~q1:')
            assert first_page.structuredContent['has_more'] is True
            assert first_page.structuredContent["jobs"][0] == {
                "job": "job-000",
                "generation": 1,
                "revision": 1,
                "state": "paused",
            }
            assert set(first_page.structuredContent["jobs"][0]) == {
                "job",
                "generation",
                "revision",
                "state",
            }
            assert json.loads(_result_text(first_page)) == first_page.structuredContent

            assert not second_page.isError
            assert second_page.structuredContent == {
                "jobs": [
                    {
                        "job": "job-100",
                        "generation": 101,
                        "revision": 101,
                        "state": "paused",
                    }
                ],
                "next_cursor": None,
                "has_more": False,
            }
            assert json.loads(_result_text(second_page)) == second_page.structuredContent
            exposed = json.dumps(
                [
                    health.structuredContent,
                    first_page.structuredContent,
                    second_page.structuredContent,
                ],
                sort_keys=True,
            )
            assert "private-source" not in exposed
            assert "synthetic-secret" not in exposed
            assert "state.db" not in exposed
            assert "worker.sock" not in exposed

            assert process.is_alive()
            assert socket_path.exists()
            assert not (state_root / "direct-runtime").exists()
            assert _worker_snapshot(state_root) == before
        finally:
            shutdown.set()
            if process.is_alive():
                assert stopped.wait(_WATCHDOG_SECONDS)
                _join(process)
            assert _worker_result(results) == ("result", None)


@pytest.mark.parametrize(
    "arguments",
    [
        [{"scope": "health", "cursor": "private-cursor"}],
        [{"scope": "health", "unexpected": "private-value"}],
        [{"scope": "list", "cursor": "not/an-identifier"}],
        [{"scope": "unexpected"}],
        [{"scope": "list", "cursor": 1}],
    ],
)
def test_downloads_query_rejects_invalid_input_before_socket_access(
    arguments: list[dict[str, object]],
) -> None:
    """Invalid combinations have a fixed error even without a worker socket."""

    with tempfile.TemporaryDirectory(dir="/tmp", prefix="hd-mcp-") as temporary_root:
        state_root = Path(temporary_root) / "must-not-be-created"
        _initialized, _listed, results, error_log = asyncio.run(
            _query_worker(socket_value=None, state_root=state_root, arguments=arguments)
        )
        assert error_log == ""
        assert len(results) == 1
        result = results[0]
        assert result.isError
        assert result.structuredContent is None
        assert _result_text(result) == _SAFE_INVALID_INPUT
        assert not state_root.exists()


@pytest.mark.parametrize("socket_kind", ["missing", "unreachable", "relative", "invalid"])
def test_downloads_query_fails_closed_without_bootstrapping(socket_kind: str) -> None:
    """Missing, unreachable, non-absolute, and invalid sockets are safe errors."""

    with tempfile.TemporaryDirectory(dir="/tmp", prefix="hd-mcp-") as temporary_root:
        root = Path(temporary_root)
        state_root = root / "must-not-be-created"
        if socket_kind == "missing":
            socket_value: str | None = None
        elif socket_kind == "unreachable":
            socket_value = str(root / "missing.sock")
        elif socket_kind == "relative":
            socket_value = "worker.sock"
        else:
            invalid_socket = root / "not-a-socket"
            invalid_socket.write_text("not a socket", encoding="utf-8")
            invalid_socket.chmod(0o600)
            socket_value = str(invalid_socket)

        _initialized, _listed, results, error_log = asyncio.run(
            _query_worker(
                socket_value=socket_value,
                state_root=state_root,
                arguments=[{"scope": "health"}],
            )
        )
        assert error_log == ""
        assert len(results) == 1
        result = results[0]
        assert result.isError
        assert result.structuredContent is None
        assert _result_text(result) == _SAFE_UNAVAILABLE
        assert not state_root.exists()
