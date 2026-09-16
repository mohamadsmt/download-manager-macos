"""Metadata-adapter integration checks with no external video endpoint."""
from __future__ import annotations

import importlib
import importlib.util
import json
import os
from pathlib import Path
import shlex
import subprocess
import sys
import textwrap

import pytest
from yt_dlp.utils import DownloadError, UnsupportedError

from hermes_downloads import network, processes


def _video():
    spec = importlib.util.find_spec("hermes_downloads.video")
    assert spec is not None, "hermes_downloads.video must provide the yt-dlp metadata adapter"
    return importlib.import_module("hermes_downloads.video")


def _source(url: str) -> network.SourceURL:
    return network.validate_source_url(url)


def _metadata_json(**overrides: object) -> str:
    metadata: dict[str, object] = {
        "id": "content-11",
        "title": "Metadata only",
        "formats": [
            {
                "format_id": "18",
                "height": 360,
                "vcodec": "avc1.42001E",
                "acodec": "mp4a.40.2",
                "ext": "mp4",
            }
        ],
    }
    metadata.update(overrides)
    return json.dumps(metadata)


def _engine_result(stdout: str) -> processes.EngineResult:
    return processes.EngineResult(
        identity=processes.EngineIdentity(
            leader_pid=1,
            process_group_id=1,
            started_monotonic_ns=1,
            argv_sha256="0" * 64,
        ),
        returncode=0,
        stdout=stdout,
        stderr="",
    )


def _playlist_metadata_json(*positions: object) -> str:
    return json.dumps(
        {
            "_type": "playlist",
            "entries": [
                {
                    "id": f"content-{position}",
                    "title": f"Playlist item {position}",
                    "playlist_index": position,
                    "formats": [
                        {
                            "format_id": f"format-{position}",
                            "height": 360,
                            "vcodec": "avc1.42001E",
                            "acodec": "mp4a.40.2",
                            "ext": "mp4",
                            "url": (
                                "https://media.example.test/video?"
                                f"token=playlist-private-{position}"
                            ),
                        }
                    ],
                }
                for position in positions
            ],
        }
    )


class _MetadataRunner:
    def __init__(self, response: object) -> None:
        self.response = response
        self.commands: list[tuple[str, ...]] = []
        self.payload_attempts = 0

    def __call__(self, command: tuple[str, ...]) -> object:
        self.commands.append(command)
        return self.response


def test_metadata_adapter_uses_only_ytdlp_metadata_flags_and_no_payload_runner() -> None:
    video = _video()
    source = _source(
        "https://video.example.test/watch?v=one&list=playlist-1&signature=source-private"
    )
    runner = _MetadataRunner(_metadata_json())

    result = video.YtDlpMetadataClient(
        runner=runner,
        python_executable="/fixed/python",
    ).resolve(video.VideoRequest(source=source), job_id="job-11")

    assert result.status is video.VideoStatus.READY
    assert len(runner.commands) == 1
    assert runner.payload_attempts == 0
    command = runner.commands[0]
    assert command[:3] == ("/fixed/python", "-m", "yt_dlp")
    assert {
        "--dump-single-json",
        "--skip-download",
        "--ignore-config",
        "--no-plugin-dirs",
        "--no-update",
        "--no-remote-components",
        "--no-netrc",
        "--no-cookies",
        "--no-playlist",
    }.issubset(command)
    assert not any(argument.startswith("--playlist-items=") for argument in command)
    assert command[-1] == source.raw_url.decode("utf-8")
    assert "source-private" not in repr(result)


def test_metadata_adapter_requires_bounded_explicit_playlist_selection() -> None:
    video = _video()
    ambiguous = _source("https://video.example.test/watch?v=one&list=playlist-1")
    ambiguous_runner = _MetadataRunner(_metadata_json())

    ambiguous_result = video.YtDlpMetadataClient(runner=ambiguous_runner).resolve(
        video.VideoRequest(source=ambiguous), job_id="job-video"
    )

    assert ambiguous_result.status is video.VideoStatus.READY
    assert "--no-playlist" in ambiguous_runner.commands[0]
    assert not any(
        argument.startswith("--playlist-items=")
        for argument in ambiguous_runner.commands[0]
    )

    pure_playlist = _source("https://video.example.test/playlist?list=playlist-1")
    blocked_runner = _MetadataRunner(_metadata_json())
    blocked_result = video.YtDlpMetadataClient(runner=blocked_runner).resolve(
        video.VideoRequest(source=pure_playlist), job_id="job-playlist"
    )

    assert blocked_result.status is video.VideoStatus.PLAYLIST_SELECTION_REQUIRED
    assert blocked_runner.commands == []

    single_selected_runner = _MetadataRunner(_playlist_metadata_json(2))
    single_selected = video.YtDlpMetadataClient(runner=single_selected_runner).resolve(
        video.VideoRequest(
            source=pure_playlist,
            playlist_selection=video.PlaylistSelection((2,)),
        ),
        job_id="job-single-selected",
    )

    assert single_selected.status is video.VideoStatus.READY
    assert single_selected.content_id == "content-2"
    assert "--playlist-items=2" in single_selected_runner.commands[0]

    selected_runner = _MetadataRunner(_playlist_metadata_json(2, 4))
    selected_results = video.YtDlpMetadataClient(runner=selected_runner).resolve_many(
        video.VideoRequest(
            source=pure_playlist,
            playlist_selection=video.PlaylistSelection((2, 4)),
        ),
        job_id="job-selected",
    )

    assert type(selected_results) is tuple
    assert [result.status for result in selected_results] == [
        video.VideoStatus.READY,
        video.VideoStatus.READY,
    ]
    assert [result.content_id for result in selected_results] == ["content-2", "content-4"]
    assert [result.provisional_filename for result in selected_results] == [
        "job-selected--playlist-2--metadata-pending",
        "job-selected--playlist-4--metadata-pending",
    ]
    assert len({result.provisional_filename for result in selected_results}) == 2
    assert "playlist-private" not in repr(selected_results)
    assert "--playlist-items=2,4" in selected_runner.commands[0]
    assert "--no-playlist" not in selected_runner.commands[0]

    ambiguous_multi_runner = _MetadataRunner(_playlist_metadata_json(2, 4))
    with pytest.raises(ValueError, match="multiple playlist items require resolve_many"):
        video.YtDlpMetadataClient(runner=ambiguous_multi_runner).resolve(
            video.VideoRequest(
                source=pure_playlist,
                playlist_selection=video.PlaylistSelection((2, 4)),
            ),
            job_id="job-ambiguous-multi",
        )
    assert ambiguous_multi_runner.commands == []

    malformed_runner = _MetadataRunner(_metadata_json())
    with pytest.raises(ValueError, match="playlist metadata does not match selected items"):
        video.YtDlpMetadataClient(runner=malformed_runner).resolve_many(
            video.VideoRequest(
                source=pure_playlist,
                playlist_selection=video.PlaylistSelection((2, 4)),
            ),
            job_id="job-malformed-playlist",
        )
    assert len(malformed_runner.commands) == 1

    with pytest.raises(ValueError):
        video.PlaylistSelection(tuple(range(1, 27)))
    with pytest.raises(ValueError):
        video.PlaylistSelection((1, 1))


@pytest.mark.parametrize(
    "metadata_positions",
    (
        pytest.param((2,), id="missing-selected-entry"),
        pytest.param((2, 4, 6), id="extra-entry-cardinality-mismatch"),
        pytest.param((2, 2), id="duplicate-position"),
        pytest.param((2, 6), id="unselected-position"),
        pytest.param((2, "4"), id="non-integer-position"),
    ),
)
def test_metadata_adapter_rejects_malformed_playlist_shapes_before_ready(
    metadata_positions: tuple[object, ...],
) -> None:
    video = _video()
    runner = _MetadataRunner(_playlist_metadata_json(*metadata_positions))
    resolutions: tuple[object, ...] | None = None

    with pytest.raises(
        ValueError,
        match=r"^playlist metadata does not match selected items$",
    ):
        resolutions = video.YtDlpMetadataClient(runner=runner).resolve_many(
            video.VideoRequest(
                source=_source("https://video.example.test/playlist?list=playlist-1"),
                playlist_selection=video.PlaylistSelection((2, 4)),
            ),
            job_id="job-malformed-playlist-shape",
        )

    assert resolutions is None
    assert len(runner.commands) == 1
    assert "--playlist-items=2,4" in runner.commands[0]


def test_metadata_adapter_returns_requested_order_from_reversed_playlist_metadata() -> None:
    video = _video()
    runner = _MetadataRunner(_playlist_metadata_json(4, 2))

    resolutions = video.YtDlpMetadataClient(runner=runner).resolve_many(
        video.VideoRequest(
            source=_source("https://video.example.test/playlist?list=playlist-1"),
            playlist_selection=video.PlaylistSelection((2, 4)),
        ),
        job_id="job-reversed-playlist-metadata",
    )

    assert [resolution.status for resolution in resolutions] == [
        video.VideoStatus.READY,
        video.VideoStatus.READY,
    ]
    assert [resolution.content_id for resolution in resolutions] == [
        "content-2",
        "content-4",
    ]
    assert [resolution.provisional_filename for resolution in resolutions] == [
        "job-reversed-playlist-metadata--playlist-2--metadata-pending",
        "job-reversed-playlist-metadata--playlist-4--metadata-pending",
    ]
    assert len(runner.commands) == 1
    assert "--playlist-items=2,4" in runner.commands[0]


def test_default_metadata_client_uses_bounded_contained_child_api_envelope(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    video = _video()
    source = _source("https://video.example.test/watch?v=one&signature=source-private")
    calls: list[tuple[tuple[str, ...], Path, float, int]] = []

    def contained(
        command: tuple[str, ...], *, cwd: Path, timeout: float, output_limit: int
    ) -> object:
        calls.append((command, cwd, timeout, output_limit))
        return _engine_result(
            json.dumps(
                {
                    "outcome": "metadata",
                    "metadata": json.loads(_metadata_json()),
                }
            )
        )

    class UnexpectedInProcessYoutubeDL:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            raise AssertionError("default metadata resolution must use the child API")

    monkeypatch.delenv("HERMES_DOWNLOADS_DISABLE_NETWORK", raising=False)
    monkeypatch.setattr(video, "YoutubeDL", UnexpectedInProcessYoutubeDL)
    monkeypatch.setattr(video, "run_contained", contained, raising=False)

    result = video.YtDlpMetadataClient(python_executable="/fixed/python").resolve(
        video.VideoRequest(source=source), job_id="job-contained-default"
    )

    assert result.status is video.VideoStatus.READY
    assert result.selection is not None
    assert len(calls) == 1
    command, cwd, timeout, output_limit = calls[0]
    assert command == (
        "/fixed/python",
        "-m",
        "hermes_downloads.video",
        "--metadata-helper",
        "--no-playlist",
        source.raw_url.decode("utf-8"),
    )
    module_path = video.__file__
    assert module_path is not None
    assert cwd == Path(module_path).resolve().parent
    assert timeout == 30.0
    assert output_limit == video.MAX_METADATA_BYTES
    assert "source-private" not in repr(result)


def test_metadata_adapter_classifies_trusted_ytdlp_unsupported_error_without_diagnostics() -> None:
    video = _video()

    def api_runner(_command: tuple[str, ...]) -> object:
        try:
            raise UnsupportedError("https://video.example.test/unsupported?token=error-private")
        except UnsupportedError as error:
            assert error.__traceback__ is not None
            raise DownloadError(
                "error-private", (UnsupportedError, error, error.__traceback__)
            )

    result = video.YtDlpMetadataClient(runner=api_runner).resolve(
        video.VideoRequest(source=_source("https://video.example.test/watch?v=one")),
        job_id="job-unsupported",
    )

    assert result.status is video.VideoStatus.UNSUPPORTED
    assert result.selection is None
    assert "error-private" not in repr(result)


def test_default_metadata_client_maps_contained_wrapped_unsupported_error_without_diagnostics(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    video = _video()
    child = tmp_path / "wrapped_unsupported_child.py"
    child.write_text(
        textwrap.dedent(
            """\
            import sys

            import hermes_downloads.video as video


            class WrappedUnsupportedYoutubeDL:
                def __init__(self, *_args: object, **_kwargs: object) -> None:
                    pass

                def __enter__(self) -> object:
                    return self

                def __exit__(self, *_args: object) -> None:
                    return None

                def extract_info(self, *_args: object, **_kwargs: object) -> object:
                    try:
                        raise video.UnsupportedError(
                            "https://video.example.test/unsupported?token=child-source-private"
                        )
                    except video.UnsupportedError as error:
                        raise video.DownloadError(
                            "child-error-private",
                            (video.UnsupportedError, error, error.__traceback__),
                        )


            if tuple(sys.argv[1:3]) != ("-m", "hermes_downloads.video"):
                raise SystemExit(1)
            video.YoutubeDL = WrappedUnsupportedYoutubeDL
            sys.argv = ["hermes_downloads.video", *sys.argv[3:]]
            raise SystemExit(video.main())
            """
        ),
        encoding="utf-8",
    )
    runner = tmp_path / "run-wrapped-unsupported-child"
    runner.write_text(
        "#!/bin/sh\n"
        f"exec {shlex.quote(sys.executable)} {shlex.quote(str(child))} \"$@\"\n",
        encoding="utf-8",
    )
    runner.chmod(0o700)

    monkeypatch.delenv("HERMES_DOWNLOADS_DISABLE_NETWORK", raising=False)
    result = video.YtDlpMetadataClient(python_executable=str(runner)).resolve(
        video.VideoRequest(
            source=_source(
                "https://video.example.test/watch?v=one&signature=source-private"
            )
        ),
        job_id="job-contained-wrapped-unsupported",
    )

    assert result.status is video.VideoStatus.UNSUPPORTED
    assert result.selection is None
    for marker in (
        "child-error-private",
        "child-source-private",
        "source-private",
    ):
        assert marker not in repr(result)


@pytest.mark.parametrize(
    ("envelope", "status"),
    (
        pytest.param(
            {"outcome": "failure", "status": "unsupported"},
            "UNSUPPORTED",
            id="typed-unsupported",
        ),
        pytest.param(
            {"outcome": "failure", "status": "transient"},
            "TRANSIENT",
            id="typed-transient",
        ),
        pytest.param(
            {
                "outcome": "metadata",
                "metadata": json.loads(_metadata_json(_has_drm=True)),
            },
            "DRM",
            id="structured-drm",
        ),
        pytest.param(
            {
                "outcome": "metadata",
                "metadata": json.loads(_metadata_json(availability="needs_auth")),
            },
            "AUTH_NEEDED",
            id="structured-auth-needed",
        ),
    ),
)
def test_default_metadata_client_maps_contained_engine_result_envelopes(
    monkeypatch: pytest.MonkeyPatch,
    envelope: dict[str, object],
    status: str,
) -> None:
    video = _video()
    calls: list[tuple[str, ...]] = []

    def contained(
        command: tuple[str, ...], *, cwd: Path, timeout: float, output_limit: int
    ) -> object:
        del cwd, timeout, output_limit
        calls.append(command)
        return _engine_result(json.dumps(envelope))

    class UnexpectedInProcessYoutubeDL:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            raise AssertionError("default metadata resolution must use the child API")

    monkeypatch.delenv("HERMES_DOWNLOADS_DISABLE_NETWORK", raising=False)
    monkeypatch.setattr(video, "YoutubeDL", UnexpectedInProcessYoutubeDL)
    monkeypatch.setattr(video, "run_contained", contained, raising=False)

    result = video.YtDlpMetadataClient().resolve(
        video.VideoRequest(
            source=_source("https://video.example.test/watch?v=one&signature=source-private")
        ),
        job_id="job-contained-envelope",
    )

    assert len(calls) == 1
    assert result.status is video.VideoStatus[status]
    assert result.selection is None
    assert "source-private" not in repr(result)


def test_default_metadata_client_rejects_raw_metadata_without_child_envelope(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    video = _video()
    calls: list[tuple[str, ...]] = []

    def contained(
        command: tuple[str, ...], *, cwd: Path, timeout: float, output_limit: int
    ) -> object:
        del cwd, timeout, output_limit
        calls.append(command)
        return _engine_result(_metadata_json())

    monkeypatch.delenv("HERMES_DOWNLOADS_DISABLE_NETWORK", raising=False)
    monkeypatch.setattr(video, "run_contained", contained, raising=False)

    result = video.YtDlpMetadataClient().resolve(
        video.VideoRequest(source=_source("https://video.example.test/watch?v=one")),
        job_id="job-raw-metadata",
    )

    assert len(calls) == 1
    assert result.status is video.VideoStatus.TRANSIENT
    assert result.selection is None


def test_metadata_helper_invalid_protocol_emits_fixed_transient_envelope(
    tmp_path: Path,
) -> None:
    completed = subprocess.run(
        (sys.executable, "-m", "hermes_downloads.video", "--metadata-helper"),
        capture_output=True,
        check=False,
        cwd=tmp_path,
        env={"PATH": os.environ.get("PATH", "")},
        text=True,
        timeout=5,
    )

    assert completed.returncode == 0
    assert completed.stderr == ""
    assert json.loads(completed.stdout) == {
        "outcome": "failure",
        "status": "transient",
    }


def test_metadata_adapter_leaves_generic_ytdlp_errors_transient_without_diagnostics() -> None:
    video = _video()

    def api_runner(_command: tuple[str, ...]) -> object:
        raise DownloadError("unsupported?token=generic-private")

    result = video.YtDlpMetadataClient(runner=api_runner).resolve(
        video.VideoRequest(source=_source("https://video.example.test/watch?v=one")),
        job_id="job-generic-error",
    )

    assert result.status is video.VideoStatus.TRANSIENT
    assert result.selection is None
    assert "generic-private" not in repr(result)


def test_metadata_adapter_bounds_response_before_decoding_and_honors_network_disable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    video = _video()
    source = _source("https://video.example.test/watch?v=one")
    oversized_runner = _MetadataRunner("x" * (video.MAX_METADATA_BYTES + 1))

    oversized = video.YtDlpMetadataClient(runner=oversized_runner).resolve(
        video.VideoRequest(source=source), job_id="job-large"
    )

    assert oversized.status is video.VideoStatus.TRANSIENT
    assert len(oversized_runner.commands) == 1

    attempted = False

    def unexpected_session(*_args: object, **_kwargs: object) -> object:
        nonlocal attempted
        attempted = True
        raise AssertionError("metadata API must not launch while network is disabled")

    monkeypatch.setenv("HERMES_DOWNLOADS_DISABLE_NETWORK", "1")
    monkeypatch.setattr(video, "YoutubeDL", unexpected_session)
    disabled = video.YtDlpMetadataClient().resolve(
        video.VideoRequest(source=source), job_id="job-disabled"
    )

    assert disabled.status is video.VideoStatus.TRANSIENT
    assert attempted is False
