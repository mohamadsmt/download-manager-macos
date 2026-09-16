"""Metadata-adapter integration checks with no external video endpoint."""
from __future__ import annotations

import importlib
import importlib.util
import json

import pytest

from hermes_downloads import network


def _video():
    spec = importlib.util.find_spec("hermes_downloads.video")
    assert spec is not None, "hermes_downloads.video must provide the yt-dlp metadata adapter"
    return importlib.import_module("hermes_downloads.video")


def _source(url: str) -> network.SourceURL:
    return network.validate_source_url(url)


def _metadata_json() -> str:
    return json.dumps(
        {
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

    selected_runner = _MetadataRunner(_metadata_json())
    selected_result = video.YtDlpMetadataClient(runner=selected_runner).resolve(
        video.VideoRequest(
            source=pure_playlist,
            playlist_selection=video.PlaylistSelection((2, 4)),
        ),
        job_id="job-selected",
    )

    assert selected_result.status is video.VideoStatus.READY
    assert "--playlist-items=2,4" in selected_runner.commands[0]
    assert "--no-playlist" not in selected_runner.commands[0]
    with pytest.raises(ValueError):
        video.PlaylistSelection(tuple(range(1, 27)))
    with pytest.raises(ValueError):
        video.PlaylistSelection((1, 1))


@pytest.mark.parametrize(
    "status",
    ("UNSUPPORTED", "DRM", "AUTH_NEEDED", "TRANSIENT"),
)
def test_metadata_adapter_distinguishes_typed_unavailable_outcomes(status: str) -> None:
    video = _video()
    expected = video.VideoStatus[status]
    runner = _MetadataRunner(video.MetadataFailure(status=expected))

    result = video.YtDlpMetadataClient(runner=runner).resolve(
        video.VideoRequest(source=_source("https://video.example.test/watch?v=one")),
        job_id="job-failure",
    )

    assert result.status is expected
    assert result.selection is None
    assert len(runner.commands) == 1


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

    def unexpected_runner(*_args: object, **_kwargs: object) -> object:
        nonlocal attempted
        attempted = True
        raise AssertionError("metadata runner must not launch while network is disabled")

    monkeypatch.setenv("HERMES_DOWNLOADS_DISABLE_NETWORK", "1")
    monkeypatch.setattr(video, "run_contained", unexpected_runner)
    disabled = video.YtDlpMetadataClient().resolve(
        video.VideoRequest(source=source), job_id="job-disabled"
    )

    assert disabled.status is video.VideoStatus.TRANSIENT
    assert attempted is False
