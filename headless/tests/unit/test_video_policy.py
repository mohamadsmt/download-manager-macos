"""Behavioral contract for metadata-only video selection."""
from __future__ import annotations

from dataclasses import fields
import importlib
import importlib.util

import pytest

from hermes_downloads import network


def _video():
    spec = importlib.util.find_spec("hermes_downloads.video")
    assert spec is not None, "hermes_downloads.video must provide metadata-only policy"
    return importlib.import_module("hermes_downloads.video")


def _source(url: str = "https://video.example.test/watch?v=one&signature=source-private"):
    return network.validate_source_url(url)


def _metadata(*, formats: list[dict[str, object]], **overrides: object) -> dict[str, object]:
    value: dict[str, object] = {
        "id": "content-42",
        "title": "Course / lesson",
        "formats": formats,
    }
    value.update(overrides)
    return value


def test_default_policy_selects_actual_separate_tracks_and_retains_page_source() -> None:
    video = _video()
    source = _source()

    resolution = video.resolve_metadata(
        video.VideoRequest(source=source),
        _metadata(
            formats=[
                {
                    "format_id": "137",
                    "height": 1080,
                    "vcodec": "avc1.640028",
                    "acodec": "none",
                    "ext": "mp4",
                    "tbr": 4500,
                    "url": "https://media.example.test/video?token=video-private",
                },
                {
                    "format_id": "136",
                    "height": 720,
                    "vcodec": "avc1.4d401f",
                    "acodec": "none",
                    "ext": "mp4",
                    "tbr": 2500,
                },
                {
                    "format_id": "251",
                    "vcodec": "none",
                    "acodec": "opus",
                    "ext": "webm",
                    "abr": 160,
                    "url": "https://media.example.test/audio?token=audio-private",
                },
            ],
            subtitles={
                "en": [
                    {
                        "ext": "vtt",
                        "url": "https://media.example.test/subtitle?token=subtitle-private",
                    }
                ]
            },
        ),
        job_id="job-9",
    )

    assert video.provisional_filename("job-9") == "job-9--metadata-pending"
    assert resolution.status is video.VideoStatus.READY
    assert resolution.original_page is source
    assert resolution.original_page.raw_url == source.raw_url
    assert resolution.content_id == "content-42"
    assert resolution.final_filename == "Course lesson.mkv"
    assert resolution.selection == video.FormatSelection(
        video_format_id="137",
        audio_format_id="251",
        container="mkv",
        extension="mkv",
        subtitles=(),
    )
    rendered = repr(resolution)
    for secret in ("source-private", "video-private", "audio-private", "subtitle-private"):
        assert secret not in rendered


def test_default_policy_falls_back_to_an_available_progressive_format() -> None:
    video = _video()

    resolution = video.resolve_metadata(
        video.VideoRequest(source=_source()),
        _metadata(
            formats=[
                {
                    "format_id": "22",
                    "height": 720,
                    "vcodec": "avc1.64001F",
                    "acodec": "mp4a.40.2",
                    "ext": "mp4",
                    "tbr": 1800,
                }
            ]
        ),
        job_id="job-22",
    )

    assert resolution.status is video.VideoStatus.READY
    assert resolution.selection == video.FormatSelection(
        video_format_id="22",
        audio_format_id=None,
        container="mp4",
        extension="mp4",
        subtitles=(),
    )
    assert resolution.final_filename == "Course lesson.mp4"


def test_quality_cap_reports_unavailable_when_no_actual_video_format_fits() -> None:
    video = _video()

    resolution = video.resolve_metadata(
        video.VideoRequest(
            source=_source(),
            options=video.VideoOptions(quality=video.VideoQuality.UP_TO_720P),
        ),
        _metadata(
            formats=[
                {
                    "format_id": "401",
                    "height": 1440,
                    "vcodec": "av01.0.12M.08",
                    "acodec": "none",
                    "ext": "webm",
                },
                {
                    "format_id": "251",
                    "vcodec": "none",
                    "acodec": "opus",
                    "ext": "webm",
                },
            ]
        ),
        job_id="job-720",
    )

    assert resolution.status is video.VideoStatus.UNAVAILABLE
    assert resolution.selection is None
    assert resolution.final_filename is None
    assert resolution.available_qualities == (1440,)


def test_typed_audio_and_subtitle_choices_select_only_declared_metadata() -> None:
    video = _video()

    resolution = video.resolve_metadata(
        video.VideoRequest(
            source=_source(),
            options=video.VideoOptions(
                quality=video.VideoQuality.UP_TO_1080P,
                audio=video.AudioChoice.AUDIO_ONLY,
                subtitles=video.SubtitleChoice.AVAILABLE,
            ),
        ),
        _metadata(
            formats=[
                {
                    "format_id": "251",
                    "vcodec": "none",
                    "acodec": "opus",
                    "ext": "webm",
                    "abr": 160,
                }
            ],
            subtitles={
                "en": [
                    {
                        "ext": "vtt",
                        "url": "https://media.example.test/subtitle?token=subtitle-private",
                    }
                ]
            },
        ),
        job_id="job-audio",
    )

    assert resolution.status is video.VideoStatus.READY
    assert resolution.selection == video.FormatSelection(
        video_format_id=None,
        audio_format_id="251",
        container="webm",
        extension="webm",
        subtitles=(video.SubtitleTrack(language="en", extension="vtt"),),
    )
    with pytest.raises(TypeError):
        video.VideoOptions(quality="up_to_1080p")
    assert "subtitle-private" not in repr(resolution)


def test_cookie_grant_requires_explicit_origin_scoped_consent_without_cookie_material() -> None:
    video = _video()
    source = _source()
    other_source = _source("https://other.example.test/watch?v=one")

    with pytest.raises(network.CredentialPolicyError):
        video.CookieGrant.for_source(source, user_consented=False)

    grant = video.CookieGrant.for_source(source, user_consented=True)

    assert grant.permits(source) is True
    assert grant.permits(other_source) is False
    assert {field.name for field in fields(video.CookieGrant)} == {"scope"}
    with pytest.raises(network.CredentialPolicyError):
        video.VideoRequest(source=other_source, cookie_grant=grant)
