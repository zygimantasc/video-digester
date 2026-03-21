from __future__ import annotations

import logging
import os
import random
import re
import sqlite3
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
from faster_whisper import WhisperModel


YOUTUBE_PLAYLIST_ITEMS_URL = "https://www.googleapis.com/youtube/v3/playlistItems"
YOUTUBE_VIDEOS_URL = "https://www.googleapis.com/youtube/v3/videos"
NEXOS_CHAT_URL = "https://api.nexos.ai/v1/chat/completions"
_WHISPER_MODEL: WhisperModel | None = None
_LOGGER: logging.Logger | None = None


@dataclass
class Video:
    channel_id: str
    channel_title: str
    video_id: str
    title: str
    description: str
    published_at: str
    video_url: str
    duration_seconds: int | None = None


@dataclass
class PreparedSummary:
    video: Video
    caption_source: str
    summary_title: str
    summary_text: str


def get_logger() -> logging.Logger:
    global _LOGGER
    if _LOGGER is not None:
        return _LOGGER

    log_path = Path(os.getenv("LOG_PATH", "/logs/runner.log"))
    log_path.parent.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("summary_runner")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    logger.propagate = False

    formatter = logging.Formatter("%(message)s")

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    _LOGGER = logger
    return logger


def log(message: str) -> None:
    get_logger().info(message)


def log_error(message: str) -> None:
    get_logger().error(message)


def getenv_required(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def parse_channel_ids(raw: str) -> list[str]:
    parts = [part.strip() for part in raw.replace("\n", ",").split(",")]
    return [part for part in parts if part]


def get_db() -> sqlite3.Connection:
    db_path = Path(os.getenv("STATE_DB_PATH", "/data/state.db"))
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS processed_videos (
            video_id TEXT PRIMARY KEY,
            channel_id TEXT NOT NULL,
            channel_title TEXT NOT NULL,
            title TEXT NOT NULL,
            published_at TEXT NOT NULL,
            video_url TEXT NOT NULL,
            caption_source TEXT NOT NULL,
            parent_ts TEXT NOT NULL,
            thread_ts TEXT NOT NULL,
            processed_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS video_attempts (
            video_id TEXT PRIMARY KEY,
            channel_id TEXT NOT NULL,
            channel_title TEXT NOT NULL,
            title TEXT NOT NULL,
            published_at TEXT NOT NULL,
            video_url TEXT NOT NULL,
            status TEXT NOT NULL,
            reason TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.commit()
    return conn


def uploads_playlist_id(channel_id: str) -> str:
    if channel_id.startswith("UC") and len(channel_id) > 2:
        return "UU" + channel_id[2:]
    return channel_id


def request_with_retries(method: str, url: str, **kwargs: Any) -> requests.Response:
    retries = int(os.getenv("HTTP_RETRY_COUNT", "3"))
    backoff = float(os.getenv("HTTP_RETRY_BACKOFF_SECONDS", "2"))
    timeout = kwargs.get("timeout")

    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            response = requests.request(method, url, **kwargs)
            if response.status_code == 429 or response.status_code >= 500:
                if attempt < retries:
                    log(
                        f"[http] retrying {method.upper()} {url} after status {response.status_code} "
                        f"(attempt {attempt}/{retries}, timeout={timeout})"
                    )
                    time.sleep(backoff * attempt)
                    continue
            return response
        except requests.RequestException as exc:
            last_error = exc
            if attempt >= retries:
                break
            log(
                f"[http] retrying {method.upper()} {url} after {exc.__class__.__name__} "
                f"(attempt {attempt}/{retries}, timeout={timeout})"
            )
            time.sleep(backoff * attempt)

    if last_error is not None:
        raise last_error
    raise RuntimeError(f"Request failed without response for {method.upper()} {url}")


def parse_iso8601_duration(raw: str) -> int | None:
    match = re.fullmatch(
        r"P(?:(?P<days>\d+)D)?(?:T(?:(?P<hours>\d+)H)?(?:(?P<minutes>\d+)M)?(?:(?P<seconds>\d+)S)?)?",
        raw.strip(),
    )
    if not match:
        return None
    days = int(match.group("days") or 0)
    hours = int(match.group("hours") or 0)
    minutes = int(match.group("minutes") or 0)
    seconds = int(match.group("seconds") or 0)
    return days * 86400 + hours * 3600 + minutes * 60 + seconds


def format_duration(seconds: int) -> str:
    days, remainder = divmod(seconds, 86400)
    hours, remainder = divmod(remainder, 3600)
    minutes, secs = divmod(remainder, 60)
    parts: list[str] = []
    if days:
        parts.append(f"{days}d")
    if hours:
        parts.append(f"{hours}h")
    if minutes:
        parts.append(f"{minutes}m")
    if secs or not parts:
        parts.append(f"{secs}s")
    return " ".join(parts)


def fetch_video_durations(
    youtube_api_key: str, video_ids: list[str], timeout: int
) -> dict[str, int | None]:
    if not video_ids:
        return {}
    response = request_with_retries(
        "get",
        YOUTUBE_VIDEOS_URL,
        params={
            "part": "contentDetails",
            "id": ",".join(video_ids),
            "maxResults": len(video_ids),
            "key": youtube_api_key,
        },
        timeout=timeout,
    )
    if response.status_code >= 500:
        log_error(
            f"[youtube] videos error {response.status_code} while loading durations for {len(video_ids)} videos"
        )
        return {}
    response.raise_for_status()
    durations: dict[str, int | None] = {}
    for item in response.json().get("items", []):
        video_id = (item.get("id") or "").strip()
        if not video_id:
            continue
        durations[video_id] = parse_iso8601_duration(
            item.get("contentDetails", {}).get("duration", "")
        )
    return durations


def fetch_recent_videos(
    youtube_api_key: str, channel_id: str, max_results: int, timeout: int
) -> list[Video]:
    playlist_id = uploads_playlist_id(channel_id)
    log(
        f"[youtube] fetching recent videos for channel {channel_id} via playlistItems (max={max_results})"
    )
    response = request_with_retries(
        "get",
        YOUTUBE_PLAYLIST_ITEMS_URL,
        params={
            "part": "snippet,contentDetails",
            "playlistId": playlist_id,
            "maxResults": max_results,
            "key": youtube_api_key,
        },
        timeout=timeout,
    )
    if response.status_code == 404:
        log_error(
            f"[youtube] uploads playlist not found for channel {channel_id}; skipping this channel"
        )
        return []
    if response.status_code >= 500:
        log_error(
            f"[youtube] playlistItems error {response.status_code} for channel {channel_id}; skipping this channel for now"
        )
        return []
    response.raise_for_status()
    payload = response.json()
    videos: list[Video] = []
    items = payload.get("items", [])
    log(
        f"[youtube] playlistItems returned {len(items)} items for {channel_id}"
    )
    for item in items[:max_results]:
        snippet = item.get("snippet", {})
        content_details = item.get("contentDetails", {})
        video_id = (content_details.get("videoId") or snippet.get("resourceId", {}).get("videoId") or "").strip()
        if not video_id:
            continue
        videos.append(
            Video(
                channel_id=channel_id,
                channel_title=snippet.get("videoOwnerChannelTitle")
                or snippet.get("channelTitle")
                or channel_id,
                video_id=video_id,
                title=snippet.get("title", "(untitled video)"),
                description=snippet.get("description", ""),
                published_at=snippet.get("publishedAt", ""),
                video_url=f"https://www.youtube.com/watch?v={video_id}",
            )
        )
    durations = fetch_video_durations(
        youtube_api_key,
        [video.video_id for video in videos],
        timeout,
    )
    max_duration_seconds = int(os.getenv("MAX_VIDEO_DURATION_MINUTES", "40")) * 60
    filtered_videos: list[Video] = []
    skipped_for_duration = 0
    for video in videos:
        duration_seconds = durations.get(video.video_id)
        video.duration_seconds = duration_seconds
        if duration_seconds is not None and duration_seconds > max_duration_seconds:
            skipped_for_duration += 1
            log(
                f"[youtube] skip {video.video_id}: duration {format_duration(duration_seconds)} exceeds "
                f"max {format_duration(max_duration_seconds)}"
            )
            continue
        filtered_videos.append(video)
    log(
        f"[youtube] fetched {len(filtered_videos)} videos for channel {channel_id}"
        + (f" after skipping {skipped_for_duration} long videos" if skipped_for_duration else "")
    )
    return filtered_videos


def filter_unprocessed(conn: sqlite3.Connection, videos: list[Video]) -> list[Video]:
    if not videos:
        return []
    placeholders = ",".join("?" for _ in videos)
    rows = conn.execute(
        f"SELECT video_id FROM processed_videos WHERE video_id IN ({placeholders})",
        [video.video_id for video in videos],
    ).fetchall()
    seen_ids = {row[0] for row in rows}
    unseen = [video for video in videos if video.video_id not in seen_ids]
    log(f"[state] unseen videos after dedupe: {len(unseen)} of {len(videos)}")
    return unseen


def get_processed_video_ids(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute("SELECT video_id FROM processed_videos").fetchall()
    return {row[0] for row in rows}


def build_fallback_candidates(
    conn: sqlite3.Connection,
    youtube_api_key: str,
    channel_ids: list[str],
    max_results: int,
    timeout: int,
    exclude_ids: set[str] | None = None,
    limit: int | None = None,
) -> list[Video]:
    selected: list[Video] = []
    seen_ids = get_processed_video_ids(conn) | (exclude_ids or set())
    for channel_id in channel_ids:
        if limit is not None and len(selected) >= limit:
            break
        pool = fetch_recent_videos(youtube_api_key, channel_id, max_results, timeout)
        if not pool:
            continue
        unique_pool = list({video.video_id: video for video in pool}.values())
        candidates = [video for video in unique_pool if video.video_id not in seen_ids]
        if not candidates:
            continue
        random.shuffle(candidates)
        log(
            f"[fallback] prepared {len(candidates)} fallback candidates for {channel_id} from a pool of {len(unique_pool)}"
        )
        for candidate in candidates:
            if candidate.video_id in seen_ids:
                continue
            selected.append(candidate)
            seen_ids.add(candidate.video_id)
            if limit is not None and len(selected) >= limit:
                break
    return selected


def post_slack_message(
    token: str, channel: str, text: str, timeout: int, thread_ts: str | None = None
) -> dict[str, Any]:
    if thread_ts:
        log(f"[slack] posting thread reply to channel={channel} thread_ts={thread_ts}")
    else:
        log(f"[slack] posting parent message to channel={channel}")
    payload: dict[str, Any] = {
        "channel": channel,
        "text": text,
        "unfurl_links": False,
        "unfurl_media": False,
    }
    if thread_ts:
        payload["thread_ts"] = thread_ts

    response = request_with_retries(
        "post",
        "https://slack.com/api/chat.postMessage",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json; charset=utf-8",
        },
        json=payload,
        timeout=timeout,
    )
    response.raise_for_status()
    body = response.json()
    if not body.get("ok"):
        raise RuntimeError(f"Slack API error: {body.get('error', 'unknown_error')}")
    log(f"[slack] posted message ts={body.get('ts', '?')}")
    return body


def clean_vtt(text: str) -> str:
    lines = []
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line == "WEBVTT" or "-->" in line or line.isdigit():
            continue
        lines.append(line)
    return re.sub(r"\s+", " ", " ".join(lines)).strip()


def read_caption_file(base_path: Path, language: str) -> str | None:
    patterns = [
        f"{base_path.name}.{language}.vtt",
        f"{base_path.name}.{language}.orig.vtt",
        f"{base_path.name}.{language}-*.vtt",
        f"{base_path.name}.{language}-*.orig.vtt",
    ]
    for pattern in patterns:
        for candidate in sorted(base_path.parent.glob(pattern)):
            if candidate.exists():
                return candidate.read_text(encoding="utf-8", errors="ignore")
    return None


def run_ytdlp(video: Video, language: str, auto: bool, timeout: int) -> str | None:
    with tempfile.TemporaryDirectory(prefix="captions-") as tmp_dir:
        base_path = Path(tmp_dir) / video.video_id
        mode = "auto" if auto else "manual"
        log(f"[captions] trying {mode} captions for {video.video_id} lang={language}")
        cmd = [
            "yt-dlp",
            "--skip-download",
            "--sub-langs",
            f"{language}.*,{language}",
            "--sub-format",
            "vtt",
            "--output",
            str(base_path),
        ]
        cmd.append("--write-auto-sub" if auto else "--write-sub")
        cmd.append(video.video_url)
        subprocess.run(
            cmd,
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=timeout,
        )
        text = read_caption_file(base_path, language)
        if text:
            log(f"[captions] found {mode} captions for {video.video_id}")
        else:
            log(f"[captions] no {mode} captions for {video.video_id}")
        return text


def download_audio(video: Video, timeout: int) -> Path | None:
    with tempfile.TemporaryDirectory(prefix="audio-") as tmp_dir:
        base_path = Path(tmp_dir) / video.video_id
        log(f"[audio] downloading audio for {video.video_id}")
        cmd = [
            "yt-dlp",
            "--extract-audio",
            "--audio-format",
            "mp3",
            "--output",
            f"{base_path}.%(ext)s",
            video.video_url,
        ]
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

        inactivity_timeout = int(os.getenv("YTDLP_INACTIVITY_TIMEOUT_SECONDS", str(timeout)))
        poll_interval = float(os.getenv("YTDLP_POLL_INTERVAL_SECONDS", "2"))
        last_progress_at = time.monotonic()
        last_sizes: dict[Path, int] = {}

        while True:
            candidates = sorted(base_path.parent.glob(f"{base_path.name}.*"))
            progress_seen = False
            for candidate in candidates:
                if not candidate.is_file():
                    continue
                try:
                    size = candidate.stat().st_size
                except FileNotFoundError:
                    continue
                previous = last_sizes.get(candidate)
                if previous is None or size > previous:
                    last_sizes[candidate] = size
                    progress_seen = True

            if progress_seen:
                last_progress_at = time.monotonic()

            returncode = process.poll()
            if returncode is not None:
                break

            if time.monotonic() - last_progress_at > inactivity_timeout:
                process.kill()
                process.wait()
                log(
                    f"[audio] yt-dlp stalled for {video.video_id} after {inactivity_timeout}s without file growth"
                )
                raise subprocess.TimeoutExpired(cmd=cmd, timeout=inactivity_timeout)

            time.sleep(poll_interval)

        candidates = sorted(base_path.parent.glob(f"{base_path.name}.*"))
        log(
            f"[audio] yt-dlp finished for {video.video_id} returncode={returncode} files={[candidate.name for candidate in candidates]}"
        )
        for candidate in candidates:
            if candidate.is_file() and candidate.suffix != ".part":
                copied = Path(tempfile.mkdtemp(prefix="audio-ready-")) / candidate.name
                copied.write_bytes(candidate.read_bytes())
                log(f"[audio] downloaded audio for {video.video_id} to {copied.name}")
                return copied
    log(f"[audio] failed to download audio for {video.video_id}")
    return None


def get_whisper_model() -> WhisperModel:
    global _WHISPER_MODEL
    if _WHISPER_MODEL is None:
        log(
            f"[whisper] loading model={os.getenv('WHISPER_MODEL', 'small')} device={os.getenv('WHISPER_DEVICE', 'cpu')} compute_type={os.getenv('WHISPER_COMPUTE_TYPE', 'int8')}"
        )
        _WHISPER_MODEL = WhisperModel(
            os.getenv("WHISPER_MODEL", "small"),
            device=os.getenv("WHISPER_DEVICE", "cpu"),
            compute_type=os.getenv("WHISPER_COMPUTE_TYPE", "int8"),
        )
        log("[whisper] model ready")
    return _WHISPER_MODEL


def transcribe_audio(video: Video, timeout: int) -> dict[str, Any]:
    try:
        audio_path = download_audio(video, timeout=timeout)
        if not audio_path:
            return {"ok": False, "reason": "audio_download_failed"}
        model = get_whisper_model()
        log(f"[whisper] transcribing {video.video_id}")
        segments, info = model.transcribe(
            str(audio_path),
            beam_size=1,
            vad_filter=True,
            language=os.getenv("WHISPER_LANGUAGE", "en"),
        )
        text = " ".join(segment.text.strip() for segment in segments).strip()
        try:
            audio_path.unlink(missing_ok=True)
            audio_path.parent.rmdir()
        except OSError:
            pass
        if not text:
            return {"ok": False, "reason": "empty_transcription"}
        log(f"[whisper] transcription complete for {video.video_id} chars={len(text)}")
        return {
            "ok": True,
            "source": "whisper",
            "videoId": video.video_id,
            "language": getattr(info, "language", os.getenv("WHISPER_LANGUAGE", "en")),
            "text": text,
        }
    except subprocess.TimeoutExpired:
        return {"ok": False, "reason": "audio_download_timeout"}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "reason": f"transcription_failed:{exc}"}


def fetch_captions(video: Video, language: str, allow_auto: bool, timeout: int) -> dict[str, Any]:
    try:
        manual_raw = run_ytdlp(video, language=language, auto=False, timeout=timeout)
        if manual_raw:
            return {
                "ok": True,
                "source": "manual",
                "videoId": video.video_id,
                "language": language,
                "text": clean_vtt(manual_raw),
            }

        if allow_auto:
            auto_raw = run_ytdlp(video, language=language, auto=True, timeout=timeout)
            if auto_raw:
                return {
                    "ok": True,
                    "source": "auto",
                    "videoId": video.video_id,
                    "language": language,
                    "text": clean_vtt(auto_raw),
                }
    except subprocess.TimeoutExpired:
        return {"ok": False, "reason": "timeout"}

    return {"ok": False, "reason": "no_captions"}


def summarize_video(
    nexos_api_key: str,
    model: str,
    video: Video,
    caption_payload: dict[str, Any],
    timeout: int,
) -> PreparedSummary:
    log(f"[summary] requesting summary for {video.video_id}")
    prompt = "\n".join(
        [
            "Summarize this YouTube video using only the supplied YouTube text.",
            (
                "The source text is auto-generated captions. Mention possible recognition errors briefly only if they materially affect meaning."
                if caption_payload.get("source") == "auto"
                else (
                    "The source text is a local speech-to-text transcription. It may contain recognition errors; mention this briefly only if it materially affects meaning."
                    if caption_payload.get("source") == "whisper"
                    else "The source text is manual captions."
                )
            ),
            "",
            f"Channel: {video.channel_title}",
            f"Title: {video.title}",
            f"Published At: {video.published_at}",
            f"URL: {video.video_url}",
            "",
            "Return plain text for Slack with exactly these sections in this order:",
            "Summary title",
            "Write exactly 1 short line that says what the video is really about.",
            "This must be a rewritten summary title, not a copy of the original YouTube title.",
            "It should be more informative than the original YouTube title.",
            "Do not repeat or lightly paraphrase the original YouTube title.",
            "Maximum 100 characters including spaces.",
            "Do not use quotes, markdown, bullets, or labels in the title.",
            "Main topics",
            "Use 1 to 3 short bullet points starting with `- `.",
            "Keep each bullet brief and substantive.",
            "Order bullets by importance. The first bullet must be the most important or interesting point.",
            "Do not include filler, introductions, hype, or generic announcements unless they are the actual point of the video.",
            "Detailed summary",
            "Write 1 short factual paragraph, around 80 to 160 words.",
            "Do not use Markdown headings like ##.",
            "Do not include intro or outro text.",
            "",
            "Source Text:",
            caption_payload["text"][:120000],
        ]
    )

    response = request_with_retries(
        "post",
        NEXOS_CHAT_URL,
        headers={
            "Authorization": f"Bearer {nexos_api_key}",
            "Content-Type": "application/json",
        },
        json={
            "model": model,
            "temperature": 0.1,
            "max_tokens": 500,
            "messages": [
                {
                    "role": "system",
                    "content": "You summarize YouTube videos into concise Slack-ready notes.",
                },
                {"role": "user", "content": prompt},
            ],
        },
        timeout=timeout,
    )
    response.raise_for_status()
    body = response.json()
    choices = body.get("choices", [])
    if not choices:
        raise RuntimeError("Nexos response did not include choices")
    content = choices[0].get("message", {}).get("content", "").strip()
    if not content:
        raise RuntimeError("Nexos response did not include summary content")
    normalized = normalize_summary(content)
    summary_title, summary_body = parse_summary_sections(normalized)
    if title_is_too_similar(summary_title, video.title):
        summary_title = fallback_summary_title(video, summary_body)
    summary_title = shorten_text(summary_title, 100)
    log(f"[summary] prepared summary for {video.video_id}: {summary_title}")
    return PreparedSummary(
        video=video,
        caption_source=caption_payload.get("source", "unknown"),
        summary_title=summary_title,
        summary_text=summary_body,
    )


def normalize_summary(text: str) -> str:
    lines = [line.rstrip() for line in text.replace("\r\n", "\n").split("\n")]
    normalized: list[str] = []
    for line in lines:
        stripped = line.strip()
        if stripped.lower().startswith("## "):
            stripped = stripped[3:]
        if stripped.startswith("**") and stripped.endswith("**") and len(stripped) > 4:
            stripped = stripped[2:-2]
        normalized.append(stripped if stripped else "")
    return "\n".join(normalized).strip()


def parse_summary_sections(text: str) -> tuple[str, str]:
    lines = [line.strip() for line in text.split("\n")]
    summary_title = ""
    body_lines: list[str] = []
    mode = "body"
    for line in lines:
        if not line:
            body_lines.append("")
            continue
        lowered = line.lower()
        if lowered == "summary title":
            mode = "title"
            continue
        if lowered == "main topics":
            mode = "body"
            body_lines.append("Main topics")
            continue
        if lowered == "detailed summary":
            mode = "body"
            body_lines.append("")
            body_lines.append("Detailed summary")
            continue
        if mode == "title" and not summary_title:
            summary_title = line.lstrip("-• ").strip()
            mode = "body"
            continue
        body_lines.append(line)

    if not summary_title:
        for line in lines:
            stripped = line.strip()
            if stripped and stripped.lower() not in {"main topics", "detailed summary"}:
                summary_title = stripped.lstrip("-• ").strip()
                break

    summary_body = "\n".join(body_lines).strip()
    return summary_title or "Video summary", summary_body


def shorten_text(text: str, max_chars: int) -> str:
    normalized = " ".join(text.split()).strip()
    if len(normalized) <= max_chars:
        return normalized
    if max_chars <= 1:
        return normalized[:max_chars]
    return normalized[: max_chars - 1].rstrip() + "…"


def normalize_for_similarity(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", text.lower()).strip()


def title_is_too_similar(summary_title: str, original_title: str) -> bool:
    normalized_summary = normalize_for_similarity(summary_title)
    normalized_original = normalize_for_similarity(original_title)
    if not normalized_summary or not normalized_original:
        return False
    if normalized_summary == normalized_original:
        return True
    return normalized_summary in normalized_original or normalized_original in normalized_summary


def fallback_summary_title(video: Video, summary_body: str) -> str:
    for line in summary_body.split("\n"):
        stripped = line.strip()
        if stripped.startswith("- "):
            return stripped[2:].strip().rstrip(".")
    if "Detailed summary" in summary_body:
        detail = summary_body.split("Detailed summary", 1)[1].strip()
        first_sentence = re.split(r"(?<=[.!?])\s+", detail, maxsplit=1)[0].strip()
        if first_sentence:
            return first_sentence.rstrip(".")
    return video.title


def format_thread_body(text: str) -> str:
    lines = text.split("\n")
    formatted: list[str] = []
    for line in lines:
        stripped = line.strip()
        lowered = stripped.lower()
        if lowered == "main topics":
            formatted.append("*Main topics*")
        elif lowered == "detailed summary":
            formatted.append("*Detailed summary*")
        else:
            formatted.append(line)
    return "\n".join(formatted).strip()


def dedupe_thread_title_from_body(summary_title: str, body: str) -> str:
    body_lines = body.split("\n")
    normalized_title = normalize_for_similarity(summary_title)
    while body_lines:
        first = body_lines[0].strip()
        if not first:
            body_lines.pop(0)
            continue
        if normalize_for_similarity(first) == normalized_title:
            body_lines.pop(0)
            continue
        break
    return "\n".join(body_lines).strip()


def mark_processed(
    conn: sqlite3.Connection,
    video: Video,
    caption_source: str,
    parent_ts: str,
    thread_ts: str,
) -> None:
    conn.execute(
        """
        INSERT OR REPLACE INTO processed_videos (
            video_id, channel_id, channel_title, title, published_at, video_url,
            caption_source, parent_ts, thread_ts, processed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            video.video_id,
            video.channel_id,
            video.channel_title,
            video.title,
            video.published_at,
            video.video_url,
            caption_source,
            parent_ts,
            thread_ts,
            datetime.now(timezone.utc).isoformat(),
        ),
    )
    conn.commit()


def mark_attempt(conn: sqlite3.Connection, video: Video, status: str, reason: str) -> None:
    conn.execute(
        """
        INSERT OR REPLACE INTO video_attempts (
            video_id, channel_id, channel_title, title, published_at, video_url,
            status, reason, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            video.video_id,
            video.channel_id,
            video.channel_title,
            video.title,
            video.published_at,
            video.video_url,
            status,
            reason,
            datetime.now(timezone.utc).isoformat(),
        ),
    )
    conn.commit()


def clear_attempt(conn: sqlite3.Connection, video_id: str) -> None:
    conn.execute("DELETE FROM video_attempts WHERE video_id = ?", (video_id,))
    conn.commit()


def build_parent_text_from_summaries(summaries: list[PreparedSummary]) -> str:
    ordered = sorted(
        summaries,
        key=lambda summary: extract_interest_rank(summary.summary_text),
    )
    return "\n".join(
        f"• <{summary.video.video_url}|{shorten_text(summary.summary_title, 100)}>"
        for summary in ordered
    )


def build_thread_text(summary: PreparedSummary) -> str:
    thread_title = shorten_text(summary.summary_title, 100)
    body = format_thread_body(summary.summary_text)
    body = dedupe_thread_title_from_body(thread_title, body)
    return f"*<{summary.video.video_url}|{thread_title}>*\n──────────\n{body}"


def extract_interest_rank(summary_text: str) -> tuple[int, str]:
    lines = [line.strip() for line in summary_text.split("\n")]
    bullet = ""
    for line in lines:
        if line.startswith("- "):
            bullet = line[2:].strip()
            break
    rank_basis = bullet or summary_text
    return (0 if bullet else 1, normalize_for_similarity(rank_basis))


def main() -> int:
    youtube_api_key = getenv_required("YOUTUBE_API_KEY")
    nexos_api_key = getenv_required("NEXOS_API_KEY")
    slack_bot_token = getenv_required("SLACK_BOT_TOKEN")
    slack_channel = getenv_required("SLACK_CHANNEL")
    channel_ids = parse_channel_ids(getenv_required("YOUTUBE_CHANNEL_IDS"))
    if not channel_ids:
        raise RuntimeError("YOUTUBE_CHANNEL_IDS is empty")
    fallback_channel_ids = parse_channel_ids(os.getenv("FALLBACK_CHANNEL_IDS", ""))
    if not fallback_channel_ids:
        fallback_channel_ids = channel_ids

    nexos_model = os.getenv("NEXOS_MODEL", "GPT 5.4 (Public)")
    caption_language = os.getenv("CAPTION_LANGUAGE", "en")
    allow_auto = os.getenv("ALLOW_AUTO_CAPTIONS", "true").lower() != "false"
    transcribe_on_missing = os.getenv("TRANSCRIBE_ON_MISSING_CAPTIONS", "true").lower() != "false"
    random_fallback_enabled = os.getenv("RANDOM_FALLBACK_ENABLED", "true").lower() != "false"
    random_fallback_pool = int(os.getenv("RANDOM_FALLBACK_POOL", "25"))
    youtube_max_results = int(os.getenv("YOUTUBE_MAX_RESULTS", "5"))
    min_summaries_per_run = int(os.getenv("MIN_SUMMARIES_PER_RUN", "3"))
    max_summaries_per_run = int(os.getenv("MAX_SUMMARIES_PER_RUN", "6"))
    fallback_attempt_multiplier = int(os.getenv("FALLBACK_ATTEMPT_MULTIPLIER", "3"))
    http_timeout = int(os.getenv("HTTP_TIMEOUT_SECONDS", "60"))

    conn = get_db()

    fetched: list[Video] = []
    for channel_id in channel_ids:
        fetched.extend(
            fetch_recent_videos(
                youtube_api_key=youtube_api_key,
                channel_id=channel_id,
                max_results=youtube_max_results,
                timeout=http_timeout,
            )
        )

    if not fetched:
        log("[run] no videos fetched")
        return 0

    unique_by_id: dict[str, Video] = {video.video_id: video for video in fetched}
    unseen = filter_unprocessed(
        conn,
        sorted(
            unique_by_id.values(),
            key=lambda video: video.published_at,
            reverse=True,
        ),
    )

    candidate_videos = list(unseen)
    if random_fallback_enabled and len(candidate_videos) < min_summaries_per_run:
        missing_to_min = max(0, min_summaries_per_run - len(candidate_videos))
        topup_limit = max(
            missing_to_min,
            min(
                max_summaries_per_run,
                missing_to_min * fallback_attempt_multiplier,
            ),
        )
        fallback_candidates = build_fallback_candidates(
            conn=conn,
            youtube_api_key=youtube_api_key,
            channel_ids=fallback_channel_ids,
            max_results=random_fallback_pool,
            timeout=http_timeout,
            exclude_ids={video.video_id for video in candidate_videos},
            limit=topup_limit,
        )
        if fallback_candidates:
            log(
                f"[fallback] topping up candidate pool with {len(fallback_candidates)} videos "
                f"(limit {topup_limit}) to try to reach the minimum summary target of {min_summaries_per_run}"
            )
            candidate_videos.extend(fallback_candidates)

    if not candidate_videos:
        log("[run] no unseen videos")
        return 0

    prepared_summaries: list[PreparedSummary] = []
    skipped = 0
    log(f"[run] processing {len(candidate_videos)} candidate videos")
    for video in candidate_videos:
        if len(prepared_summaries) >= max_summaries_per_run:
            log(
                f"[run] reached max summaries for this run ({max_summaries_per_run}); stopping candidate processing"
            )
            break
        try:
            log(f"[video] start {video.video_id} {video.title}")
            captions = fetch_captions(
                video=video,
                language=caption_language,
                allow_auto=allow_auto,
                timeout=http_timeout,
            )
            if (not captions.get("ok") or not captions.get("text")) and transcribe_on_missing:
                log(f"[video] falling back to whisper for {video.video_id}")
                captions = transcribe_audio(video, timeout=http_timeout * 4)
            if not captions.get("ok") or not captions.get("text"):
                skipped += 1
                mark_attempt(conn, video, "skipped", captions.get("reason", "no_captions"))
                log(f"[video] skip {video.video_id}: {captions.get('reason', 'no_captions')}")
                continue

            prepared_summaries.append(
                summarize_video(
                    nexos_api_key=nexos_api_key,
                    model=nexos_model,
                    video=video,
                    caption_payload=captions,
                    timeout=http_timeout,
                )
            )
            log(f"[video] summary prepared for {video.video_id}")
        except Exception as exc:  # noqa: BLE001
            skipped += 1
            mark_attempt(conn, video, "failed", str(exc))
            log_error(f"[video] failed {video.video_id}: {exc}")

    if not prepared_summaries:
        log(f"[run] done candidates={len(candidate_videos)} prepared=0 posted=0 skipped={skipped}")
        return 0

    if len(prepared_summaries) < min_summaries_per_run:
        log(
            f"[run] prepared only {len(prepared_summaries)} summaries; minimum required is {min_summaries_per_run}. "
            "Skipping Slack post for this run."
        )
        return 0

    log(f"[slack] creating parent message for {len(prepared_summaries)} summaries")
    parent = post_slack_message(
        token=slack_bot_token,
        channel=slack_channel,
        text=build_parent_text_from_summaries(prepared_summaries),
        timeout=http_timeout,
    )
    parent_ts = parent["ts"]
    parent_channel = parent["channel"]

    posted = 0
    for prepared in prepared_summaries:
        try:
            log(f"[slack] posting thread summary for {prepared.video.video_id}")
            thread = post_slack_message(
                token=slack_bot_token,
                channel=parent_channel,
                text=build_thread_text(prepared),
                timeout=http_timeout,
                thread_ts=parent_ts,
            )
            mark_processed(
                conn=conn,
                video=prepared.video,
                caption_source=prepared.caption_source,
                parent_ts=parent_ts,
                thread_ts=thread.get("ts", parent_ts),
            )
            clear_attempt(conn, prepared.video.video_id)
            posted += 1
            log(f"[slack] posted thread summary for {prepared.video.video_id}")
        except Exception as exc:  # noqa: BLE001
            skipped += 1
            mark_attempt(conn, prepared.video, "failed", f"thread_post:{exc}")
            log_error(f"[slack] failed thread post for {prepared.video.video_id}: {exc}")

    log(
        f"[run] done candidates={len(candidate_videos)} prepared={len(prepared_summaries)} posted={posted} skipped={skipped}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
