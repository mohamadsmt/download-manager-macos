"""Metadata-only video policy; this module never starts a media payload."""
from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from enum import Enum
import json
import math
import os
from pathlib import Path
import re
import sys
from typing import Final, Protocol
import unicodedata
from urllib.parse import urlsplit

from yt_dlp.utils import DownloadError, UnsupportedError

from hermes_downloads.network import CredentialPolicyError, CredentialScope, SourceURL
from hermes_downloads.processes import EngineResult, run_contained

__all__ = [
    "AudioChoice",
    "CookieGrant",
    "FormatSelection",
    "MAX_METADATA_BYTES",
    "MetadataFailure",
    "MetadataRunner",
    "PlaylistSelection",
    "SubtitleChoice",
    "SubtitleTrack",
    "VideoOptions",
    "VideoQuality",
    "VideoRequest",
    "VideoResolution",
    "VideoStatus",
    "YtDlpMetadataClient",
    "provisional_filename",
    "resolve_metadata",
]

_IDENTIFIER: Final = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}\Z")
_FORMAT_ID: Final = re.compile(r"[A-Za-z0-9._-]{1,128}\Z")
_EXTENSION: Final = re.compile(r"[A-Za-z0-9]{1,16}\Z")
_MAX_FORMATS: Final = 128
_MAX_TITLE_CHARACTERS: Final = 180
MAX_METADATA_BYTES: Final = 64 * 1024
_MAX_PLAYLIST_ITEMS: Final = 25
_MAX_EXCEPTION_GRAPH_NODES: Final = 16
_METADATA_TIMEOUT_SECONDS: Final = 15.0
_METADATA_CWD: Final = Path("/")
_METADATA_FLAGS: Final = (
    "--dump-single-json",
    "--skip-download",
    "--ignore-config",
    "--no-plugin-dirs",
    "--no-update",
    "--no-remote-components",
    "--no-netrc",
    "--no-cookies",
)


class VideoQuality(str, Enum):
    """Maximum selected video height; the default never exceeds 1080p."""

    UP_TO_360P = "up_to_360p"
    UP_TO_480P = "up_to_480p"
    UP_TO_720P = "up_to_720p"
    UP_TO_1080P = "up_to_1080p"
    UP_TO_1440P = "up_to_1440p"
    UP_TO_2160P = "up_to_2160p"

    @property
    def maximum_height(self) -> int:
        return int(self.value.removeprefix("up_to_").removesuffix("p"))


class AudioChoice(str, Enum):
    """Whether selected metadata must include, contain only, or omit audio."""

    INCLUDE = "include"
    AUDIO_ONLY = "audio_only"
    VIDEO_ONLY = "video_only"


class SubtitleChoice(str, Enum):
    """Whether available subtitle tracks are selected as metadata."""

    NONE = "none"
    AVAILABLE = "available"


class VideoStatus(str, Enum):
    """Bounded, public metadata-resolution states."""

    READY = "ready"
    UNAVAILABLE = "unavailable"
    PLAYLIST_SELECTION_REQUIRED = "playlist_selection_required"
    UNSUPPORTED = "unsupported"
    DRM = "drm"
    AUTH_NEEDED = "auth_needed"
    TRANSIENT = "transient"


@dataclass(frozen=True, slots=True)
class VideoOptions:
    """Typed media choices that can be resolved without any body transfer."""

    quality: VideoQuality = VideoQuality.UP_TO_1080P
    audio: AudioChoice = AudioChoice.INCLUDE
    subtitles: SubtitleChoice = SubtitleChoice.NONE

    def __post_init__(self) -> None:
        if type(self.quality) is not VideoQuality:
            raise TypeError("quality must be a VideoQuality")
        if type(self.audio) is not AudioChoice:
            raise TypeError("audio must be an AudioChoice")
        if type(self.subtitles) is not SubtitleChoice:
            raise TypeError("subtitles must be a SubtitleChoice")


@dataclass(frozen=True, slots=True)
class CookieGrant:
    """Explicit origin-scoped consent, deliberately without cookie material."""

    scope: CredentialScope

    def __post_init__(self) -> None:
        if type(self.scope) is not CredentialScope:
            raise TypeError("scope must be a CredentialScope")

    @classmethod
    def for_source(cls, source: SourceURL, *, user_consented: bool) -> CookieGrant:
        return cls(CredentialScope.for_source(source, user_consented=user_consented))

    def permits(self, source: SourceURL) -> bool:
        return self.scope.permits(source)


@dataclass(frozen=True, slots=True)
class PlaylistSelection:
    """An explicit, bounded set of one-based playlist positions."""

    positions: tuple[int, ...]

    def __post_init__(self) -> None:
        if type(self.positions) is not tuple:
            raise TypeError("positions must be a tuple")
        if not self.positions or len(self.positions) > _MAX_PLAYLIST_ITEMS:
            raise ValueError("playlist selection must contain 1 to 25 items")
        if any(type(position) is not int or position < 1 for position in self.positions):
            raise ValueError("playlist positions must be positive integers")
        if len(set(self.positions)) != len(self.positions):
            raise ValueError("playlist positions must be unique")


@dataclass(frozen=True, slots=True)
class VideoRequest:
    """One original page and its typed non-payload metadata preferences."""

    source: SourceURL
    options: VideoOptions = VideoOptions()
    cookie_grant: CookieGrant | None = None
    playlist_selection: PlaylistSelection | None = None

    def __post_init__(self) -> None:
        if type(self.source) is not SourceURL:
            raise TypeError("source must be a validated SourceURL")
        if type(self.options) is not VideoOptions:
            raise TypeError("options must be a VideoOptions")
        if self.cookie_grant is not None:
            if type(self.cookie_grant) is not CookieGrant:
                raise TypeError("cookie_grant must be a CookieGrant")
            if not self.cookie_grant.permits(self.source):
                raise CredentialPolicyError("cookie consent is not scoped to this source")
        if self.playlist_selection is not None:
            if type(self.playlist_selection) is not PlaylistSelection:
                raise TypeError("playlist_selection must be a PlaylistSelection")
            if not _is_pure_playlist(self.source):
                raise ValueError("playlist selection requires a pure playlist URL")


@dataclass(frozen=True, slots=True)
class SubtitleTrack:
    """A selected subtitle descriptor without its private media URL."""

    language: str
    extension: str


@dataclass(frozen=True, slots=True)
class FormatSelection:
    """Actual yt-dlp format IDs and the truthful final output container."""

    video_format_id: str | None
    audio_format_id: str | None
    container: str
    extension: str
    subtitles: tuple[SubtitleTrack, ...]


@dataclass(frozen=True, slots=True)
class VideoResolution:
    """Metadata outcome retaining the original page but never media URLs."""

    status: VideoStatus
    original_page: SourceURL
    provisional_filename: str
    content_id: str | None
    final_filename: str | None
    selection: FormatSelection | None
    available_qualities: tuple[int, ...]


@dataclass(frozen=True, slots=True)
class MetadataFailure:
    """A typed, non-diagnostic metadata-adapter failure."""

    status: VideoStatus

    def __post_init__(self) -> None:
        if type(self.status) is not VideoStatus:
            raise TypeError("status must be a VideoStatus")
        if self.status not in {
            VideoStatus.UNSUPPORTED,
            VideoStatus.DRM,
            VideoStatus.AUTH_NEEDED,
            VideoStatus.TRANSIENT,
        }:
            raise ValueError("metadata failures require an unavailable status")


class MetadataRunner(Protocol):
    """A testable metadata-only command boundary."""

    def __call__(self, command: tuple[str, ...]) -> object: ...


@dataclass(frozen=True, slots=True)
class _Format:
    format_id: str
    height: int | None
    video_codec: str
    audio_codec: str
    extension: str
    bitrate: float

    @property
    def has_video(self) -> bool:
        return self.video_codec.casefold() != "none"

    @property
    def has_audio(self) -> bool:
        return self.audio_codec.casefold() != "none"


def provisional_filename(job_id: str) -> str:
    """Return the explicit pre-title marker used before metadata arrives."""

    if type(job_id) is not str or _IDENTIFIER.fullmatch(job_id) is None:
        raise ValueError("job_id must be a nonblank identifier")
    return f"{job_id}--metadata-pending"


def resolve_metadata(
    request: VideoRequest,
    metadata: dict[str, object],
    *,
    job_id: str,
) -> VideoResolution:
    """Choose declared formats from bounded metadata without fetching any body."""

    if type(request) is not VideoRequest:
        raise TypeError("request must be a VideoRequest")
    if type(metadata) is not dict:
        raise TypeError("metadata must be a dictionary")
    return _resolve_parsed_metadata(
        request,
        metadata,
        provisional=provisional_filename(job_id),
    )


def _resolve_parsed_metadata(
    request: VideoRequest,
    metadata: dict[str, object],
    *,
    provisional: str,
) -> VideoResolution:
    content_id = _content_id(metadata.get("id"))
    title = metadata.get("title")
    available_qualities = _available_qualities(metadata)

    unavailable_status = _structured_metadata_status(metadata)
    if unavailable_status is not None:
        return _unavailable(
            unavailable_status,
            request.source,
            provisional,
            content_id,
            available_qualities,
        )

    formats = _formats(metadata)
    selection = _select_formats(formats, request.options, metadata.get("subtitles"))
    if selection is None:
        return _unavailable(
            VideoStatus.UNAVAILABLE,
            request.source,
            provisional,
            content_id,
            available_qualities,
        )
    return VideoResolution(
        status=VideoStatus.READY,
        original_page=request.source,
        provisional_filename=provisional,
        content_id=content_id,
        final_filename=_final_filename(title, content_id, selection.extension),
        selection=selection,
        available_qualities=available_qualities,
    )


class YtDlpMetadataClient:
    """Resolve yt-dlp metadata through a fixed no-payload command only."""

    __slots__ = ("_python_executable", "_runner", "_uses_default_runner")

    def __init__(
        self,
        *,
        runner: MetadataRunner | None = None,
        python_executable: str = sys.executable,
    ) -> None:
        if runner is not None and not callable(runner):
            raise TypeError("runner must be callable")
        if type(python_executable) is not str or not os.path.isabs(python_executable):
            raise ValueError("python_executable must be an absolute path")
        self._uses_default_runner = runner is None
        self._runner: MetadataRunner = _run_ytdlp_metadata if runner is None else runner
        self._python_executable = python_executable

    def resolve(self, request: VideoRequest, *, job_id: str) -> VideoResolution:
        """Return metadata selection without starting a media payload."""

        if type(request) is not VideoRequest:
            raise TypeError("request must be a VideoRequest")
        provisional = provisional_filename(job_id)
        is_playlist = _is_pure_playlist(request.source)
        selection = request.playlist_selection
        if is_playlist:
            if selection is None:
                return _unavailable(
                    VideoStatus.PLAYLIST_SELECTION_REQUIRED,
                    request.source,
                    provisional,
                    None,
                    (),
                )
            if len(selection.positions) > 1:
                raise ValueError("multiple playlist items require resolve_many")
            provisional = _playlist_provisional_filename(job_id, selection.positions[0])
        if (
            self._uses_default_runner
            and os.environ.get("HERMES_DOWNLOADS_DISABLE_NETWORK") == "1"
        ):
            return _unavailable(
                VideoStatus.TRANSIENT,
                request.source,
                provisional,
                None,
                (),
            )
        try:
            response = self._runner(_metadata_command(self._python_executable, request))
        except UnsupportedError:
            return _unavailable(
                VideoStatus.UNSUPPORTED,
                request.source,
                provisional,
                None,
                (),
            )
        except DownloadError as error:
            return _unavailable(
                _download_error_status(error),
                request.source,
                provisional,
                None,
                (),
            )
        except Exception:
            return _unavailable(
                VideoStatus.TRANSIENT,
                request.source,
                provisional,
                None,
                (),
            )
        if type(response) is MetadataFailure:
            return _unavailable(response.status, request.source, provisional, None, ())
        metadata = _parse_metadata_response(response)
        if metadata is None:
            return _unavailable(
                VideoStatus.TRANSIENT,
                request.source,
                provisional,
                None,
                (),
            )
        if is_playlist:
            unavailable_status = _structured_metadata_status(metadata)
            if unavailable_status is not None:
                return _unavailable(
                    unavailable_status,
                    request.source,
                    provisional,
                    None,
                    (),
                )
            assert selection is not None
            metadata = _selected_playlist_metadata(metadata, selection.positions)[0]
        return _resolve_metadata_or_transient(request, metadata, provisional=provisional)

    def resolve_many(
        self, request: VideoRequest, *, job_id: str
    ) -> tuple[VideoResolution, ...]:
        """Resolve every explicitly selected pure-playlist item independently."""

        if type(request) is not VideoRequest:
            raise TypeError("request must be a VideoRequest")
        if not _is_pure_playlist(request.source):
            raise ValueError("resolve_many requires a pure playlist URL")
        selection = request.playlist_selection
        if selection is None:
            raise ValueError("playlist selection is required")
        positions = selection.positions
        provisional_filenames = tuple(
            _playlist_provisional_filename(job_id, position) for position in positions
        )
        if (
            self._uses_default_runner
            and os.environ.get("HERMES_DOWNLOADS_DISABLE_NETWORK") == "1"
        ):
            return _playlist_unavailable(
                VideoStatus.TRANSIENT,
                request.source,
                provisional_filenames,
            )
        try:
            response = self._runner(_metadata_command(self._python_executable, request))
        except UnsupportedError:
            return _playlist_unavailable(
                VideoStatus.UNSUPPORTED,
                request.source,
                provisional_filenames,
            )
        except DownloadError as error:
            return _playlist_unavailable(
                _download_error_status(error),
                request.source,
                provisional_filenames,
            )
        except Exception:
            return _playlist_unavailable(
                VideoStatus.TRANSIENT,
                request.source,
                provisional_filenames,
            )
        if type(response) is MetadataFailure:
            return _playlist_unavailable(
                response.status,
                request.source,
                provisional_filenames,
            )
        metadata = _parse_metadata_response(response)
        if metadata is None:
            return _playlist_unavailable(
                VideoStatus.TRANSIENT,
                request.source,
                provisional_filenames,
            )
        unavailable_status = _structured_metadata_status(metadata)
        if unavailable_status is not None:
            return _playlist_unavailable(
                unavailable_status,
                request.source,
                provisional_filenames,
            )
        entries = _selected_playlist_metadata(metadata, positions)
        return tuple(
            _resolve_metadata_or_transient(
                request,
                entry,
                provisional=provisional,
            )
            for entry, provisional in zip(entries, provisional_filenames, strict=True)
        )


def _resolve_metadata_or_transient(
    request: VideoRequest,
    metadata: dict[str, object],
    *,
    provisional: str,
) -> VideoResolution:
    try:
        return _resolve_parsed_metadata(request, metadata, provisional=provisional)
    except Exception:
        return _unavailable(
            VideoStatus.TRANSIENT,
            request.source,
            provisional,
            None,
            (),
        )


def _playlist_provisional_filename(job_id: str, position: int) -> str:
    provisional_filename(job_id)
    if type(position) is not int or position < 1:
        raise ValueError("playlist position must be a positive integer")
    return f"{job_id}--playlist-{position}--metadata-pending"


def _playlist_unavailable(
    status: VideoStatus,
    source: SourceURL,
    provisional_filenames: tuple[str, ...],
) -> tuple[VideoResolution, ...]:
    return tuple(
        _unavailable(status, source, provisional, None, ())
        for provisional in provisional_filenames
    )


def _selected_playlist_metadata(
    metadata: dict[str, object], positions: tuple[int, ...]
) -> tuple[dict[str, object], ...]:
    if metadata.get("_type") != "playlist":
        raise ValueError("playlist metadata does not match selected items")
    entries = metadata.get("entries")
    if type(entries) is not list or len(entries) != len(positions):
        raise ValueError("playlist metadata does not match selected items")
    selected: dict[int, dict[str, object]] = {}
    selected_positions = set(positions)
    for entry in entries:
        if type(entry) is not dict:
            raise ValueError("playlist metadata does not match selected items")
        position = entry.get("playlist_index")
        if (
            type(position) is not int
            or position not in selected_positions
            or position in selected
        ):
            raise ValueError("playlist metadata does not match selected items")
        selected[position] = entry
    if len(selected) != len(positions):
        raise ValueError("playlist metadata does not match selected items")
    return tuple(selected[position] for position in positions)


def _structured_metadata_status(metadata: dict[str, object]) -> VideoStatus | None:
    if metadata.get("has_drm") is True or metadata.get("_has_drm") is True:
        return VideoStatus.DRM
    if _needs_auth(metadata.get("availability")):
        return VideoStatus.AUTH_NEEDED
    if metadata.get("availability") == "unsupported":
        return VideoStatus.UNSUPPORTED
    return None


def _download_error_status(error: DownloadError) -> VideoStatus:
    try:
        if _contains_unsupported_error(error):
            return VideoStatus.UNSUPPORTED
    except Exception:
        pass
    return VideoStatus.TRANSIENT


def _contains_unsupported_error(error: DownloadError) -> bool:
    pending: list[BaseException] = [error]
    seen: set[int] = set()
    while pending and len(seen) < _MAX_EXCEPTION_GRAPH_NODES:
        current = pending.pop()
        identity = id(current)
        if identity in seen:
            continue
        seen.add(identity)
        if isinstance(current, UnsupportedError):
            return True
        for related in (current.__cause__, current.__context__):
            if isinstance(related, BaseException):
                pending.append(related)
        if not isinstance(current, DownloadError):
            continue
        exc_info = getattr(current, "exc_info", None)
        if type(exc_info) is not tuple or len(exc_info) != 3:
            continue
        exception_type, nested, _ = exc_info
        if (
            type(exception_type) is type
            and isinstance(nested, BaseException)
            and isinstance(nested, exception_type)
        ):
            pending.append(nested)
    return False


def _run_ytdlp_metadata(command: tuple[str, ...]) -> EngineResult:
    return run_contained(
        command,
        cwd=_METADATA_CWD,
        timeout=_METADATA_TIMEOUT_SECONDS,
        output_limit=MAX_METADATA_BYTES,
        environment={},
    )


def _metadata_command(python_executable: str, request: VideoRequest) -> tuple[str, ...]:
    playlist_option = "--no-playlist"
    if _is_pure_playlist(request.source):
        selection = request.playlist_selection
        if selection is None:
            raise ValueError("playlist selection is required")
        playlist_option = "--playlist-items=" + ",".join(
            str(position) for position in selection.positions
        )
    return (
        python_executable,
        "-m",
        "yt_dlp",
        *_METADATA_FLAGS,
        playlist_option,
        request.source.raw_url.decode("utf-8"),
    )


def _is_pure_playlist(source: SourceURL) -> bool:
    return urlsplit(source.raw_url.decode("utf-8")).path.rstrip("/").casefold().endswith(
        "/playlist"
    )


def _parse_metadata_response(response: object) -> dict[str, object] | None:
    if type(response) is EngineResult:
        if response.returncode != 0:
            return None
        response = response.stdout
    if type(response) is str:
        try:
            encoded = response.encode("utf-8", "strict")
        except UnicodeEncodeError:
            return None
    elif type(response) is bytes:
        encoded = response
    else:
        return None
    if len(encoded) > MAX_METADATA_BYTES:
        return None
    try:
        parsed = json.loads(
            encoded.decode("utf-8", "strict"),
            object_pairs_hook=_strict_json_object,
            parse_constant=_reject_json_constant,
        )
    except (UnicodeError, ValueError, RecursionError):
        return None
    if type(parsed) is not dict:
        return None
    return parsed


def _strict_json_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    parsed: dict[str, object] = {}
    for key, value in pairs:
        if key in parsed:
            raise ValueError("duplicate metadata key")
        parsed[key] = value
    return parsed


def _reject_json_constant(_: str) -> object:
    raise ValueError("non-finite JSON value")


def _unavailable(
    status: VideoStatus,
    source: SourceURL,
    provisional: str,
    content_id: str | None,
    available_qualities: tuple[int, ...],
) -> VideoResolution:
    return VideoResolution(
        status=status,
        original_page=source,
        provisional_filename=provisional,
        content_id=content_id,
        final_filename=None,
        selection=None,
        available_qualities=available_qualities,
    )


def _content_id(value: object) -> str | None:
    if type(value) is str and _FORMAT_ID.fullmatch(value) is not None:
        return value
    return None


def _needs_auth(value: object) -> bool:
    return type(value) is str and value.casefold() in {
        "needs_auth",
        "needs_login",
        "needs_subscription",
        "premium_only",
        "private",
        "subscriber_only",
    }


def _formats(metadata: dict[str, object]) -> tuple[_Format, ...]:
    values = metadata.get("formats")
    if type(values) is not list or len(values) > _MAX_FORMATS:
        return ()
    formats: list[_Format] = []
    for value in values:
        if type(value) is not dict:
            continue
        parsed = _format_from_metadata(value)
        if parsed is not None:
            formats.append(parsed)
    return tuple(formats)


def _format_from_metadata(value: dict[object, object]) -> _Format | None:
    format_id = value.get("format_id")
    video_codec = value.get("vcodec")
    audio_codec = value.get("acodec")
    extension = value.get("ext")
    if (
        type(format_id) is not str
        or _FORMAT_ID.fullmatch(format_id) is None
        or type(video_codec) is not str
        or type(audio_codec) is not str
        or type(extension) is not str
        or _EXTENSION.fullmatch(extension) is None
    ):
        return None
    height = value.get("height")
    if type(height) is not int or height < 1:
        height = None
    bitrate = _bitrate(value.get("abr"))
    if bitrate == 0.0:
        bitrate = _bitrate(value.get("tbr"))
    return _Format(
        format_id=format_id,
        height=height,
        video_codec=video_codec,
        audio_codec=audio_codec,
        extension=extension.casefold(),
        bitrate=bitrate,
    )


def _bitrate(value: object) -> float:
    if type(value) is int:
        return float(value) if value >= 0 else 0.0
    if type(value) is float and math.isfinite(value) and value >= 0:
        return value
    return 0.0


def _available_qualities(metadata: dict[str, object]) -> tuple[int, ...]:
    qualities: set[int] = set()
    for format in _formats(metadata):
        if format.has_video and format.height is not None:
            qualities.add(format.height)
    return tuple(sorted(qualities))


def _select_formats(
    formats: tuple[_Format, ...], options: VideoOptions, subtitles: object
) -> FormatSelection | None:
    selected_subtitles = _select_subtitles(subtitles, options.subtitles)
    if options.audio is AudioChoice.AUDIO_ONLY:
        audio = _best(format for format in formats if format.has_audio and not format.has_video)
        if audio is None:
            return None
        return FormatSelection(
            video_format_id=None,
            audio_format_id=audio.format_id,
            container=audio.extension,
            extension=audio.extension,
            subtitles=selected_subtitles,
        )

    videos = tuple(
        format
        for format in formats
        if format.has_video
        and format.height is not None
        and format.height <= options.quality.maximum_height
    )
    if options.audio is AudioChoice.VIDEO_ONLY:
        video = _best(format for format in videos if not format.has_audio)
        if video is None:
            return None
        return FormatSelection(
            video_format_id=video.format_id,
            audio_format_id=None,
            container=video.extension,
            extension=video.extension,
            subtitles=selected_subtitles,
        )

    video = _best(format for format in videos if not format.has_audio)
    audio = _best(format for format in formats if format.has_audio and not format.has_video)
    if video is not None and audio is not None:
        container = _merge_container(video, audio)
        return FormatSelection(
            video_format_id=video.format_id,
            audio_format_id=audio.format_id,
            container=container,
            extension=container,
            subtitles=selected_subtitles,
        )

    progressive = _best(format for format in videos if format.has_audio)
    if progressive is None:
        return None
    return FormatSelection(
        video_format_id=progressive.format_id,
        audio_format_id=None,
        container=progressive.extension,
        extension=progressive.extension,
        subtitles=selected_subtitles,
    )


def _best(formats: Iterable[_Format]) -> _Format | None:
    candidates = tuple(formats)
    if not candidates:
        return None
    return max(
        candidates,
        key=lambda format: (
            -1 if format.height is None else format.height,
            format.bitrate,
            format.format_id,
        ),
    )


def _merge_container(video: _Format, audio: _Format) -> str:
    if (
        video.extension == "mp4"
        and audio.extension in {"m4a", "mp4"}
        and _matches_prefix(video.video_codec, ("avc", "av01", "hev", "hvc"))
        and _matches_prefix(audio.audio_codec, ("aac", "alac", "mp4a"))
    ):
        return "mp4"
    if (
        video.extension == "webm"
        and audio.extension == "webm"
        and _matches_prefix(video.video_codec, ("av01", "vp8", "vp9"))
        and _matches_prefix(audio.audio_codec, ("opus", "vorbis"))
    ):
        return "webm"
    return "mkv"


def _matches_prefix(value: str, prefixes: tuple[str, ...]) -> bool:
    normalized = value.casefold()
    return normalized.startswith(prefixes)


def _select_subtitles(value: object, choice: SubtitleChoice) -> tuple[SubtitleTrack, ...]:
    if choice is SubtitleChoice.NONE or type(value) is not dict:
        return ()
    selected: list[SubtitleTrack] = []
    for language in sorted(
        language for language in value if type(language) is str and language
    ):
        tracks = value[language]
        if type(tracks) is not list:
            continue
        for track in tracks:
            if type(track) is not dict:
                continue
            extension = track.get("ext")
            if type(extension) is str and _EXTENSION.fullmatch(extension) is not None:
                selected.append(
                    SubtitleTrack(language=language, extension=extension.casefold())
                )
                break
    return tuple(selected)


def _final_filename(title: object, content_id: str | None, extension: str) -> str:
    stem = _safe_title(title)
    if not stem:
        stem = f"video-{content_id or 'download'}"
    return f"{stem}.{extension}"


def _safe_title(value: object) -> str:
    if type(value) is not str:
        return ""
    safe = "".join(
        " "
        if character in {"/", "\\"} or unicodedata.category(character) in {"Cc", "Cs"}
        else character
        for character in value
    )
    return " ".join(safe.split()).strip(". ")[:_MAX_TITLE_CHARACTERS].rstrip(". ")
