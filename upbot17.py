#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
=====================================================================
 Telegram Multi-Type URL & Streaming Video Uploader Bot — upbot17.py
=====================================================================
 Configured for Bot #17 from token.txt: @razubot1_bot
 Ready for Google Colab (70 GB disk) + Linux servers.

 Core Capabilities:
 1. Auto-installs all required Python packages (telethon, aiohttp,
    tqdm, cryptg, yt-dlp, pypdf, nest_asyncio) and system tools
    (aria2, ffmpeg/ffprobe) on startup before running.
 2. Downloads BOTH:
    • Streaming videos (Vimeo, player.vimeo.com, YouTube, HLS/DASH,
      embedded pages) via yt-dlp (+ aria2c/concurrent fragments) as
      MP4 capped at <= 1080p (or user-specified 480/720/1080/etc.).
    • Direct files (PDF, images, audio, WMV, ZIP, DOCX, direct MP4)
      via high-speed aria2c (with aiohttp fallback).
 3. Uploads files to Telegram according to their exact type/extension:
    • Video (.mp4/.mkv/.mov/.webm/.m4v) → streamable video + thumb +
      duration/resolution metadata (ffprobe/ffmpeg)
    • Image (.jpg/.jpeg/.png/.webp/.gif) → photo/GIF (auto document
      fallback if > 10 MB or oversized dimensions)
    • Audio (.mp3/.m4a/.aac/.ogg/.opus/.wav/.flac) → audio player
    • PDF / Documents / Archives / WMV / AVI → document preserving name
 4. Auto-splits any file exceeding Telegram's 2000 MB limit into the
    lowest required parts (<= 2000 MB each) named:
      <name>_part1.<ext>, <name>_part2.<ext>, ...
    and uploads them sequentially in order:
    • Videos: fast lossless ffmpeg stream-copy split (playable parts!)
    • PDFs: valid page-range split via pypdf (readable PDF parts!)
    • Other files: exact binary part split
 5. Supports ALL input formats (both .txt/.csv files & direct chat text):
    • name,link,format,resolution   (e.g. cardio,https://...,mp4,720)
    • name,link,extension           (e.g. nephropathy,https://...,pdf)
    • name,link,resolution          (e.g. cardio,https://...,720)
    • name,link                     (default: auto ext / <=1080p mp4)
    • index.csv format              (Topicid,Name,videoid,link)
    • URL,name | name<TAB>URL | bare URL | numbered lines
 6. Secure Download Headers, Cookies & Referer:
    • Default Referer: https://medsparkle-drhalim.com/
    • Configurable via env vars OR live Telegram commands:
      /referer, /header, /cookies (or upload cookies.txt)
 7. Colab 70 GB Disk Protection & Speed:
    • Simultaneous downloads + parallel MTProto uploads (cryptg)
    • Instant cleanup of raw files, split parts & temp folders right
      after upload (and in finally blocks), plus storage guard
    • Source rate-limit brain (403/410/429 backoff) & resumable memory

 Commands:
   /start /help   /status /settings   /cancel   /clean   /forget
   /referer <url|clear>   /header <Key: Value|clear>   /res <1080|720|480>
   /cookies <cookie_str|clear>  (or send a cookies.txt file directly)
=====================================================================
"""

import os
import sys
import shutil
import subprocess
import importlib


# ----------------------------------------------------------------------
# 0. AUTO-BOOTSTRAP: Check & install requirements before main imports
# ----------------------------------------------------------------------
REQUIRED_PY_PACKAGES = [
    ("telethon", "telethon"),
    ("aiohttp", "aiohttp"),
    ("tqdm", "tqdm"),
    ("cryptg", "cryptg"),
    ("curl_cffi", "curl-cffi"),
    ("yt_dlp", "yt-dlp[default,curl-cffi]"),
    ("pypdf", "pypdf"),
    ("nest_asyncio", "nest_asyncio"),
]


def _run_quiet(cmd):
    return subprocess.run(cmd, check=False)


def ensure_dependencies():
    if os.environ.get("SKIP_AUTO_INSTALL") == "1" or "--skip-install" in sys.argv:
        return

    missing_py = []
    for mod_name, pip_name in REQUIRED_PY_PACKAGES:
        try:
            importlib.import_module(mod_name)
        except ImportError:
            missing_py.append(pip_name)

    if missing_py:
        print(f"[setup] Installing Python packages: {', '.join(missing_py)} ...")
        cmd = [sys.executable, "-m", "pip", "install", "-q", "--upgrade", *missing_py]
        res = subprocess.run(cmd, check=False)
        if res.returncode != 0:
            # Retry without cryptg/optional if wheel build fails on exotic platform
            core = [p for p in missing_py if p != "cryptg"]
            if core:
                subprocess.run([sys.executable, "-m", "pip", "install", "-q", *core], check=False)

    # Ensure yt-dlp stays reasonably current in Colab if requested
    if os.environ.get("UPGRADE_YTDLP", "0") == "1":
        _run_quiet([sys.executable, "-m", "pip", "install", "-q", "-U", "yt-dlp"])

    # Check system tools: aria2c and ffmpeg/ffprobe
    need_aria2 = shutil.which("aria2c") is None
    need_ffmpeg = shutil.which("ffmpeg") is None or shutil.which("ffprobe") is None
    if need_aria2 or need_ffmpeg:
        apt_pkgs = []
        if need_aria2:
            apt_pkgs.append("aria2")
        if need_ffmpeg:
            apt_pkgs.append("ffmpeg")
        if shutil.which("apt-get"):
            print(f"[setup] Installing system tools via apt-get: {' '.join(apt_pkgs)} ...")
            prefix = [] if os.geteuid() == 0 else (["sudo"] if shutil.which("sudo") else [])
            subprocess.run([*prefix, "apt-get", "update", "-qq"], check=False)
            subprocess.run([*prefix, "apt-get", "install", "-y", "-qq", *apt_pkgs], check=False)
        else:
            print(f"[setup] WARNING: Missing system tools ({', '.join(apt_pkgs)}). "
                  "Please install them manually.")


ensure_dependencies()


# ----------------------------------------------------------------------
# Standard & third-party imports
# ----------------------------------------------------------------------
import re
import csv
import math
import time
import json
import glob
import shlex
import random
import hashlib
import asyncio
import logging
import tempfile
import mimetypes
import http.cookiejar
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlsplit, unquote

import aiohttp
from tqdm import tqdm

from telethon import TelegramClient, events
from telethon.sessions import MemorySession, SQLiteSession
from telethon.errors import (
    FloodWaitError,
    MessageNotModifiedError,
    PhotoInvalidDimensionsError,
    RPCError,
)
from telethon.tl.functions.upload import SaveFilePartRequest, SaveBigFilePartRequest
from telethon.tl.types import (
    DocumentAttributeAudio,
    DocumentAttributeFilename,
    DocumentAttributeVideo,
    InputFile,
    InputFileBig,
)

# Register common MIME types cleanly
mimetypes.init()
EXTRA_MIMES = {
    ".mp4": "video/mp4",
    ".mkv": "video/x-matroska",
    ".webm": "video/webm",
    ".mov": "video/quicktime",
    ".m4v": "video/x-m4v",
    ".wmv": "video/x-ms-wmv",
    ".avi": "video/x-msvideo",
    ".flv": "video/x-flv",
    ".ts": "video/mp2t",
    ".pdf": "application/pdf",
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".png": "image/png",
    ".webp": "image/webp",
    ".gif": "image/gif",
    ".mp3": "audio/mpeg",
    ".m4a": "audio/mp4",
    ".aac": "audio/aac",
    ".ogg": "audio/ogg",
    ".opus": "audio/opus",
    ".wav": "audio/wav",
    ".flac": "audio/flac",
    ".zip": "application/zip",
    ".rar": "application/vnd.rar",
    ".7z": "application/x-7z-compressed",
}
for _ext, _mt in EXTRA_MIMES.items():
    mimetypes.add_type(_mt, _ext)


# ----------------------------------------------------------------------
# CONFIG (Bot #17 from token.txt: @razubot1_bot)
# ----------------------------------------------------------------------
BOT_NAME   = os.environ.get("BOT_NAME", "upbot17")
BOT_HANDLE = os.environ.get("BOT_HANDLE", "@razubot1_bot")
API_ID     = int(os.environ.get("API_ID", "17074961"))
API_HASH   = os.environ.get("API_HASH", "f0077a7fb095171c22b0172dc9856b9a")
BOT_TOKEN  = os.environ.get("BOT_TOKEN", "5183801187:AAHEUSf3qXTh_ZUPu_l0OutYxGxSBBcrhic")
OWNER_ID   = int(os.environ.get("OWNER_ID", "1081431445"))

# ---- Default Download Headers / Referer / Cookies ---------------------
DEFAULT_REFERER    = os.environ.get("REFERER", "https://medsparkle-drhalim.com/")
DEFAULT_ORIGIN     = os.environ.get("ORIGIN", "https://medsparkle-drhalim.com")
DEFAULT_UA         = os.environ.get(
    "USER_AGENT",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:132.0) Gecko/20100101 Firefox/132.0",
)
DEFAULT_RESOLUTION = int(os.environ.get("DEFAULT_RESOLUTION", "1080"))
COOKIES_FILE       = Path(os.environ.get("COOKIES_FILE", f"{BOT_NAME}_cookies.txt"))
HTTP_CONFIG_FILE   = Path(os.environ.get("HTTP_CONFIG_FILE", f"{BOT_NAME}_http_config.json"))

# ---- Speed & Concurrency Tuning (fast without exhausting bot/source) --
MAX_DOWNLOADS      = int(os.environ.get("MAX_DOWNLOADS", "4"))      # concurrent downloads (aria2/yt-dlp)
MAX_UPLOADS        = int(os.environ.get("MAX_UPLOADS", "4"))        # concurrent uploads to Telegram
UPLOAD_SESSIONS    = int(os.environ.get("UPLOAD_SESSIONS", "1"))    # 1 persistent session avoids ImportBotAuth FloodWait!
UPLOAD_WORKERS     = int(os.environ.get("UPLOAD_WORKERS", "100"))    # parallel 512 KB parts per upload
ARIA2_CONNS        = int(os.environ.get("ARIA2_CONNS", "100"))       # aria2c -x / -s for direct files
YTDLP_ARIA2_CONNS  = int(os.environ.get("YTDLP_ARIA2_CONNS", "100"))  # aria2c conns inside yt-dlp
YTDLP_FRAG_CONNS   = int(os.environ.get("YTDLP_FRAG_CONNS", "100"))   # concurrent HLS/DASH fragments
PARALLEL_UPLOAD_MIN = int(os.environ.get("PARALLEL_UPLOAD_MIN", str(10 * 1024 * 1024)))
BIG_FILE_MIN       = 10 * 1024 * 1024                               # Telegram >= 10 MB -> saveBigFilePart
DL_SPEED_LIMIT_MBPS = float(os.environ.get("DL_SPEED_LIMIT_MBPS", "0"))  # 0 = unlimited

# ---- Colab 70 GB Disk Guard & Cleanup ---------------------------------
MIN_FREE_GB        = float(os.environ.get("MIN_FREE_GB", "5.0"))
MIN_FREE_BYTES     = int(MIN_FREE_GB * 1024 ** 3)
MAX_DISK_USE_PCT   = float(os.environ.get("MAX_DISK_USE_PCT", "93"))
HARD_FLOOR_BYTES   = 512 * 1024 * 1024

# ---- Batch Waves & Retries --------------------------------------------
WAVE_SIZE          = int(os.environ.get("WAVE_SIZE", "0"))          # 0 = continuous simultaneous pipeline
UPLOAD_RETRIES     = int(os.environ.get("UPLOAD_RETRIES", "3"))
UPLOAD_RETRY_BASE  = float(os.environ.get("UPLOAD_RETRY_BASE", "15"))
FAILED_REQUEUE     = int(os.environ.get("FAILED_REQUEUE", "2"))

# ---- Source Rate-Limit Brain (403/410/429 protection) -----------------
RATE_LIMIT_STATUSES = {403, 410, 429}
RATE_LIMIT_RETRIES  = int(os.environ.get("RATE_LIMIT_RETRIES", "3"))
RATE_LIMIT_BASE     = int(os.environ.get("RATE_LIMIT_BASE", "180"))
GLOBAL_PAUSE_AFTER  = int(os.environ.get("GLOBAL_PAUSE_AFTER", "4"))
GLOBAL_PAUSE_WINDOW = int(os.environ.get("GLOBAL_PAUSE_WINDOW", "120"))
GLOBAL_PAUSE_BASE   = int(os.environ.get("GLOBAL_PAUSE_BASE", "300"))
GLOBAL_PAUSE_MAX    = int(os.environ.get("GLOBAL_PAUSE_MAX", "3600"))

# ---- Upload Memory (per bot) ------------------------------------------
SKIP_UPLOADED      = os.environ.get("SKIP_UPLOADED", "1") == "1"
HISTORY_FILE       = Path(os.environ.get("HISTORY_FILE", f"uploaded_history_{BOT_NAME}.json"))

# ---- Telegram Limits & Splitting --------------------------------------
# Telegram MTProto max upload for bots is 2000 MiB (4000 * 512 KiB = 2,097,152,000 B).
MAX_UPLOAD         = 2000 * 1024 * 1024
VIDEO_SPLIT_TARGET = 1940 * 1024 * 1024   # safety margin for ffmpeg keyframes & moov atom
PDF_SPLIT_TARGET   = 1950 * 1024 * 1024
BIN_SPLIT_TARGET   = 1990 * 1024 * 1024
PHOTO_MAX_BYTES    = 10 * 1024 * 1024     # Telegram photo limit: 10 MB

CHUNK_SIZE         = 1024 * 1024
MAX_RETRIES        = 3
RETRY_BACKOFF      = 4
STATUS_EVERY       = 4.0                  # seconds between Telegram status edits

# ---- Extension Categories ---------------------------------------------
STREAMABLE_VIDEO_EXTS = {".mp4", ".mov", ".m4v", ".mkv", ".webm"}
DOC_VIDEO_EXTS        = {".avi", ".wmv", ".flv", ".ts", ".3gp", ".mpg", ".mpeg", ".m2ts", ".ogv"}
VIDEO_EXTS            = STREAMABLE_VIDEO_EXTS | DOC_VIDEO_EXTS
PHOTO_EXTS            = {".jpg", ".jpeg", ".png", ".webp"}
ANIM_EXTS             = {".gif"}
OTHER_IMAGE_EXTS      = {".bmp", ".tiff", ".tif", ".svg", ".heic", ".ico"}
IMAGE_EXTS            = PHOTO_EXTS | ANIM_EXTS | OTHER_IMAGE_EXTS
AUDIO_EXTS            = {".mp3", ".m4a", ".aac", ".ogg", ".opus", ".wav", ".flac", ".wma"}

STREAMING_DOMAINS = (
    "vimeo.com", "player.vimeo.com", "youtube.com", "youtu.be",
    "dailymotion.com", "dai.ly", "wistia.com", "wistia.net",
    "loom.com", "twitch.tv", "facebook.com", "fb.watch",
    "streamable.com", "bitchute.com", "rumble.com", "odysee.com",
    "mediadelivery.net", "iframe.mediadelivery.net", "bunnycdn.com",
)

MIME_TO_EXT = {
    "application/pdf": ".pdf",
    "video/mp4": ".mp4",
    "video/quicktime": ".mov",
    "video/x-matroska": ".mkv",
    "video/webm": ".webm",
    "video/x-ms-wmv": ".wmv",
    "video/x-msvideo": ".avi",
    "video/x-flv": ".flv",
    "video/mp2t": ".ts",
    "image/jpeg": ".jpg",
    "image/jpg": ".jpg",
    "image/png": ".png",
    "image/webp": ".webp",
    "image/gif": ".gif",
    "image/bmp": ".bmp",
    "image/tiff": ".tiff",
    "image/svg+xml": ".svg",
    "audio/mpeg": ".mp3",
    "audio/mp4": ".m4a",
    "audio/aac": ".aac",
    "audio/ogg": ".ogg",
    "audio/opus": ".opus",
    "audio/wav": ".wav",
    "audio/x-wav": ".wav",
    "audio/flac": ".flac",
    "application/zip": ".zip",
    "application/x-rar-compressed": ".rar",
    "application/vnd.rar": ".rar",
    "application/x-7z-compressed": ".7z",
    "application/msword": ".doc",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": ".docx",
    "application/vnd.ms-powerpoint": ".ppt",
    "application/vnd.openxmlformats-officedocument.presentationml.presentation": ".pptx",
    "application/vnd.ms-excel": ".xls",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": ".xlsx",
    "text/plain": ".txt",
    "text/csv": ".csv",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(BOT_NAME)

current_batch = None


# ----------------------------------------------------------------------
# Runtime HTTP / Header / Cookie / Resolution Settings
# ----------------------------------------------------------------------
class RuntimeConfig:
    def __init__(self):
        self.referer = DEFAULT_REFERER
        self.origin = DEFAULT_ORIGIN
        self.user_agent = DEFAULT_UA
        self.extra_headers = {}
        self.cookie_string = os.environ.get("COOKIE_STRING", "")
        self.default_resolution = DEFAULT_RESOLUTION
        self._load_env_headers()
        self.load()

    def _load_env_headers(self):
        raw = os.environ.get("CUSTOM_HEADERS_JSON", "").strip()
        if raw:
            try:
                data = json.loads(raw)
                if isinstance(data, dict):
                    for k, v in data.items():
                        self.extra_headers[str(k)] = str(v)
            except Exception as e:
                log.warning("Invalid CUSTOM_HEADERS_JSON: %s", e)

    def load(self):
        if HTTP_CONFIG_FILE.exists():
            try:
                data = json.loads(HTTP_CONFIG_FILE.read_text(encoding="utf-8"))
                self.referer = data.get("referer", self.referer)
                self.origin = data.get("origin", self.origin)
                self.user_agent = data.get("user_agent", self.user_agent)
                self.extra_headers = data.get("extra_headers", self.extra_headers)
                self.cookie_string = data.get("cookie_string", self.cookie_string)
                self.default_resolution = int(data.get("default_resolution", self.default_resolution))
            except Exception as e:
                log.warning("Could not load %s: %s", HTTP_CONFIG_FILE, e)

    def save(self):
        try:
            HTTP_CONFIG_FILE.write_text(
                json.dumps(
                    {
                        "referer": self.referer,
                        "origin": self.origin,
                        "user_agent": self.user_agent,
                        "extra_headers": self.extra_headers,
                        "cookie_string": self.cookie_string,
                        "default_resolution": self.default_resolution,
                    },
                    indent=2,
                ),
                encoding="utf-8",
            )
        except OSError as e:
            log.warning("Could not save %s: %s", HTTP_CONFIG_FILE, e)

    def set_referer(self, ref: str):
        ref = ref.strip()
        if not ref or ref.lower() in ("none", "clear", "off"):
            self.referer = ""
            self.origin = ""
        else:
            self.referer = ref
            sp = urlsplit(ref)
            if sp.scheme and sp.netloc:
                self.origin = f"{sp.scheme}://{sp.netloc}"
        self.save()

    def has_cookies_file(self) -> bool:
        return COOKIES_FILE.exists() and COOKIES_FILE.stat().st_size > 0

    def build_headers(self, url: str = "", include_origin: bool = False) -> dict:
        h = {"User-Agent": self.user_agent}
        if self.referer:
            h["Referer"] = self.referer
        if include_origin and self.origin:
            h["Origin"] = self.origin
        for k, v in self.extra_headers.items():
            h[k] = v
        if self.cookie_string:
            h["Cookie"] = self.cookie_string
        elif self.has_cookies_file() and url:
            ck = cookies_for_url(COOKIES_FILE, url)
            if ck:
                h["Cookie"] = ck
        return h

    def summary_text(self) -> str:
        ck_file = f"Yes ({COOKIES_FILE.name})" if self.has_cookies_file() else "None"
        ck_str = f"Set ({len(self.cookie_string)} chars)" if self.cookie_string else "None"
        hdrs = ", ".join(f"{k}: {v[:30]}" for k, v in self.extra_headers.items()) or "None"
        return (
            f"⚙ {BOT_NAME} ({BOT_HANDLE})\n"
            f"• Referer: {self.referer or 'None'}\n"
            f"• Origin: {self.origin or 'None'}\n"
            f"• Default max resolution: {self.default_resolution}p\n"
            f"• Custom headers: {hdrs}\n"
            f"• Cookies file: {ck_file} | Cookie string: {ck_str}\n"
            f"• Concurrency: {MAX_DOWNLOADS}⬇ / {MAX_UPLOADS}⬆ • {UPLOAD_SESSIONS} sessions × {UPLOAD_WORKERS} workers\n"
            f"• Disk: {disk_info()}"
        )


CFG = RuntimeConfig()


def cookies_for_url(cookie_path: Path, url: str) -> str:
    """Extract matching Cookie header string from Netscape cookies.txt for aiohttp/aria2 fallback."""
    try:
        cj = http.cookiejar.MozillaCookieJar()
        cj.load(str(cookie_path), ignore_discard=True, ignore_expires=True)
        host = (urlsplit(url).hostname or "").lower()
        path = urlsplit(url).path or "/"
        pairs = []
        for c in cj:
            dom = c.domain.lstrip(".").lower()
            if (host == dom or host.endswith("." + dom)) and path.startswith(c.path):
                pairs.append(f"{c.name}={c.value}")
        return "; ".join(pairs)
    except Exception:
        return ""


# ----------------------------------------------------------------------
# Exceptions & Helpers
# ----------------------------------------------------------------------
class JobError(Exception):
    pass


class BatchCancelled(Exception):
    pass


class SourceLimited(Exception):
    def __init__(self, status, retry_after=0):
        super().__init__(f"HTTP {status}")
        self.status = status
        self.retry_after = retry_after


def fmt_bytes(n: float) -> str:
    if n is None:
        return "?"
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024 or unit == "TB":
            return f"{n:.1f} {unit}" if unit != "B" else f"{int(n)} B"
        n /= 1024
    return "?"


def fmt_eta(seconds) -> str:
    if seconds is None or seconds < 0 or seconds != seconds:
        return "--:--"
    seconds = int(seconds)
    h, r = divmod(seconds, 3600)
    m, s = divmod(r, 60)
    return f"{h}:{m:02d}:{s:02d}" if h else f"{m}:{s:02d}"


def bar(frac: float, width: int = 10) -> str:
    frac = 0.0 if frac is None else max(0.0, min(1.0, frac))
    n = int(round(frac * width))
    return "▓" * n + "░" * (width - n)


def sanitize_filename(name: str) -> str:
    name = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", name)
    name = re.sub(r"\s+", " ", name).strip(" .")
    return name[:170] or "file"


def decode_txt(raw: bytes) -> str:
    if raw.startswith((b"\xff\xfe", b"\xfe\xff")):
        return raw.decode("utf-16", errors="replace")
    if b"\x00" in raw[:200]:
        for enc in ("utf-16-le", "utf-16-be"):
            try:
                t = raw.decode(enc)
                if "http" in t.lower():
                    return t
            except UnicodeDecodeError:
                continue
    try:
        return raw.decode("utf-8-sig")
    except UnicodeDecodeError:
        return raw.decode("cp1252", errors="replace")


def looks_like_link_list(text: str) -> bool:
    return "http://" in text.lower() or "https://" in text.lower()


def parse_size_token(tok: str) -> int:
    """Convert aria2c size tokens like 128MiB, 1.2GiB, 512KiB, 400B -> bytes."""
    m = re.match(r"^\s*([0-9]+(?:\.[0-9]+)?)\s*(KiB|MiB|GiB|TiB|KB|MB|GB|TB|B)?\s*$", tok, re.I)
    if not m:
        return 0
    val = float(m.group(1))
    unit = (m.group(2) or "B").lower()
    mult = {
        "b": 1,
        "kb": 1000, "mb": 1000**2, "gb": 1000**3, "tb": 1000**4,
        "kib": 1024, "mib": 1024**2, "gib": 1024**3, "tib": 1024**4,
    }.get(unit, 1)
    return int(val * mult)


# ----------------------------------------------------------------------
# Storage Management (Colab 70 GB protection)
# ----------------------------------------------------------------------
TEMP_PREFIXES = ("tgupload_", "tglist_", f"{BOT_NAME}_batch_", f"{BOT_NAME}_test_")


def sweep_temp_dirs(skip=None) -> int:
    removed = 0
    skip_abs = os.path.abspath(skip) if skip else None
    for root in {tempfile.gettempdir(), os.getcwd()}:
        for prefix in TEMP_PREFIXES:
            for p in glob.glob(os.path.join(root, prefix + "*")):
                if skip_abs and os.path.abspath(p) == skip_abs:
                    continue
                if p.endswith((".session", ".session-journal", ".json", ".txt", ".py")):
                    continue
                try:
                    if os.path.isdir(p):
                        shutil.rmtree(p, ignore_errors=True)
                    else:
                        os.remove(p)
                    removed += 1
                except OSError:
                    pass
    return removed


def disk_info(path: str = ".") -> str:
    try:
        st = shutil.disk_usage(path)
        pct = (st.free / st.total * 100) if st.total else 0
        return f"{fmt_bytes(st.used)} used / {fmt_bytes(st.total)} ({pct:.0f}% free)"
    except OSError:
        return "usage unknown"


def free_bytes(path: str = ".") -> int:
    try:
        return shutil.disk_usage(path).free
    except OSError:
        return 1 << 60


def disk_used_pct() -> float:
    try:
        du = shutil.disk_usage(".")
        return (du.total - du.free) * 100.0 / du.total if du.total else 0.0
    except OSError:
        return 0.0


def projected_free(tracker, job=None) -> int:
    free = free_bytes()
    admitted = getattr(tracker, "admitted", None)
    if admitted:
        free -= sum(
            max(0, (j.total or 0) - j.done)
            for j in admitted.values()
            if j is not job and j.status == "downloading"
        )
    return max(0, free)


def storage_paused(tracker=None, job=None, need=None) -> bool:
    if need is not None:
        return projected_free(tracker, job) < need
    return free_bytes() < MIN_FREE_BYTES or disk_used_pct() >= MAX_DISK_USE_PCT


def _all_downloads_stalled(tracker, job) -> bool:
    jobs = getattr(tracker, "jobs", [])
    if any(j.status == "uploading" for j in jobs):
        return False
    others = [j for j in jobs if j.status == "downloading" and j is not job]
    return all(j.note.startswith(("⏸", "💾", "🚦")) for j in others)


async def wait_for_storage(tracker, job=None, need=None):
    escape = False
    warned = False
    while True:
        if not storage_paused(tracker, job, need):
            break
        if tracker.cancelled:
            raise BatchCancelled
        if job is not None and not escape:
            if getattr(tracker, "escape_job", None) is job:
                escape = True
            elif getattr(tracker, "escape_job", None) is None and _all_downloads_stalled(tracker, job):
                escape = True
                tracker.escape_job = job
                log.info("💾 Disk at limit and stalled — letting %s finish to free space", job.name)
        if escape and free_bytes() > HARD_FLOOR_BYTES:
            if job is not None:
                job.note = "💾 disk low — finishing to free space"
            return
        if not warned:
            log.info(
                "💾 Waiting for disk space (free: %s, used: %.0f%%)",
                fmt_bytes(free_bytes()),
                disk_used_pct(),
            )
            warned = True
        if job is not None:
            if need is not None:
                job.note = f"🚦 needs {fmt_bytes(need)} ({fmt_bytes(projected_free(tracker, job))} free)"
            else:
                job.note = f"⏸ disk full ({fmt_bytes(free_bytes())} free)"
        await asyncio.sleep(3)
    if job is not None:
        job.note = ""


# ----------------------------------------------------------------------
# Upload Memory
# ----------------------------------------------------------------------
HISTORY = {}


def load_history():
    global HISTORY
    try:
        HISTORY = json.loads(HISTORY_FILE.read_text(encoding="utf-8")) if HISTORY_FILE.exists() else {}
    except Exception:
        HISTORY = {}
    return len(HISTORY)


def save_history():
    try:
        HISTORY_FILE.write_text(json.dumps(HISTORY), encoding="utf-8")
    except OSError as e:
        log.warning("Could not save history: %s", e)


def mark_uploaded(name: str):
    HISTORY[name] = int(time.time())
    save_history()


# ----------------------------------------------------------------------
# Source Rate-Limit Brain (403 / 410 / 429)
# ----------------------------------------------------------------------
class RateLimitState:
    def __init__(self):
        self.global_until = 0.0
        self.global_count = 0
        self.recent = deque()


rl = RateLimitState()


def rate_limit_hint(status: int) -> str:
    if status == 410:
        return "HTTP 410 Gone — signed URL expired or blocked. Re-send a fresh link list."
    if status == 429:
        return f"HTTP 429 — source rate limit persisted after {RATE_LIMIT_RETRIES} waits."
    return f"HTTP {status} — source refused download (check Referer / Cookies / Headers)."


def parse_retry_after(headers) -> int:
    try:
        return max(0, int(headers.get("Retry-After", "0")))
    except (TypeError, ValueError):
        return 0


def note_rate_limit(status: int, retry_after: int = 0):
    now = time.monotonic()
    rl.recent.append(now)
    while rl.recent and now - rl.recent[0] > GLOBAL_PAUSE_WINDOW:
        rl.recent.popleft()
    if len(rl.recent) >= GLOBAL_PAUSE_AFTER:
        rl.global_count += 1
        dur = min(GLOBAL_PAUSE_BASE * (2 ** (rl.global_count - 1)), GLOBAL_PAUSE_MAX)
        dur = max(dur, retry_after)
        rl.global_until = max(rl.global_until, now + dur)
        rl.recent.clear()
        log.info("⏸ Multiple %d errors — pausing all downloads for %s", status, fmt_eta(dur))


async def wait_global(tracker):
    while time.monotonic() < rl.global_until:
        if tracker.cancelled:
            raise BatchCancelled
        await asyncio.sleep(3)


async def wait_out(status: int, retry_after: int, attempt: int, tracker, job):
    note_rate_limit(status, retry_after)
    await wait_global(tracker)
    d = min(max(RATE_LIMIT_BASE * (2 ** (attempt - 1)), retry_after), GLOBAL_PAUSE_MAX)
    job.note = f"⏸ HTTP {status} — retry in {fmt_eta(d)}"
    log.info("⏸ %s: HTTP %d — waiting %s (retry %d/%d)",
             job.name, status, fmt_eta(d), attempt, RATE_LIMIT_RETRIES)
    end = time.monotonic() + d
    while time.monotonic() < end:
        if tracker.cancelled:
            raise BatchCancelled
        await asyncio.sleep(min(5, max(0.1, end - time.monotonic())))
    job.note = ""


# ----------------------------------------------------------------------
# Optional Global Download Speed Throttle
# ----------------------------------------------------------------------
class GlobalThrottle:
    def __init__(self, mbps: float):
        self.limit = int(mbps * 1024 * 1024) if mbps else 0
        self.window = deque()

    async def consume(self, n: int):
        if not self.limit:
            return
        now = time.monotonic()
        while self.window and now - self.window[0][0] >= 1.0:
            self.window.popleft()
        total = sum(b for _, b in self.window)
        if total + n > self.limit:
            await asyncio.sleep(min(5.0, (total + n - self.limit) / self.limit))
        self.window.append((time.monotonic(), n))


THROTTLE = GlobalThrottle(DL_SPEED_LIMIT_MBPS)


# ----------------------------------------------------------------------
# Job Definition & Multi-Format Parser
# ----------------------------------------------------------------------
@dataclass
class Job:
    index: int
    name: str                  # filename with ext, e.g. nephropathy.pdf
    caption: str               # clean title for Telegram caption
    url: str
    path: Path
    ext: str = ""              # normalized lowercase with dot, e.g. ".pdf", ".mp4"
    explicit_ext: bool = False # True when user explicitly forced extension
    resolution: int = 1080     # max video height (480, 720, 1080, ...)
    topic_id: str = ""
    status: str = "queued"     # queued|downloading|uploading|done|failed|cancelled|skipped
    phase: str = "download"
    done: int = 0
    total: int = 0
    error: str = ""
    note: str = ""
    downloaded: int = 0
    requeues: int = 0
    speed: float = None
    bar: object = None
    samples: deque = field(default_factory=deque)


URL_START = re.compile(r"https?://", re.I)
RES_TOKEN = re.compile(r"^(\d{3,4})p?$", re.I)
EXT_TOKEN = re.compile(r"^\.?([a-z0-9]{1,8})$", re.I)
KNOWN_ALL_EXTS = (
    VIDEO_EXTS | IMAGE_EXTS | AUDIO_EXTS |
    {".pdf", ".doc", ".docx", ".ppt", ".pptx", ".xls", ".xlsx",
     ".txt", ".csv", ".zip", ".rar", ".7z", ".tar", ".gz", ".epub", ".apk", ".exe", ".iso"}
)


def _clean(s: str) -> str:
    return (s.replace("\u00a0", " ").replace("\u200b", "").replace("\ufeff", "")
            .strip().strip("\"'<>").strip())


def _is_url(s: str) -> bool:
    return s.lower().startswith(("http://", "https://"))


def _parse_fmt_res_tokens(tokens: list, default_res: int):
    """Parse optional trailing tokens [format, resolution] or [resolution] or [extension]."""
    fmt = ""
    res = default_res
    for tok in tokens:
        t = _clean(tok)
        if not t:
            continue
        m_res = RES_TOKEN.fullmatch(t)
        if m_res:
            val = int(m_res.group(1))
            if 144 <= val <= 4320:
                res = val
                continue
        m_ext = EXT_TOKEN.fullmatch(t)
        if m_ext and not t.isdigit():
            fmt = "." + m_ext.group(1).lower()
    return fmt, res


def _is_trailing_meta_field(tok: str) -> bool:
    t = _clean(tok)
    if not t:
        return True
    if any(ch in t for ch in ("=", "&", "/", "?", ":")):
        return False
    if RES_TOKEN.fullmatch(t):
        return True
    if EXT_TOKEN.fullmatch(t) and not t.isdigit():
        return True
    return False


def _parse_csv_fields(fields: list, default_res: int):
    """Interpret tokenized CSV row fields -> (name, url, fmt, res, topic_id, is_header)."""
    cleaned = [f.replace("\u00a0", " ").replace("\u200b", "").strip() for f in fields]
    if not cleaned:
        return "", "", "", default_res, "", False

    # Skip index.csv / standard header row
    lower_cols = [c.lower() for c in cleaned]
    if ("link" in lower_cols or "url" in lower_cols) and not any(_is_url(c) for c in cleaned):
        return "", "", "", default_res, "", True

    # Find first column starting with http:// or https://
    u_idx = next((i for i, c in enumerate(cleaned) if _is_url(c)), None)
    if u_idx is None:
        return "", "", "", default_res, "", False

    # Re-attach URL commas if trailing fields are actually part of a comma-containing URL
    url_parts = [cleaned[u_idx]]
    tail_idx = u_idx + 1
    while tail_idx < len(cleaned) and not _is_trailing_meta_field(cleaned[tail_idx]):
        # Special case: URL,name (u_idx == 0 and tail looks like a title rather than URL params)
        if u_idx == 0 and len(cleaned) >= 2 and not any(ch in cleaned[tail_idx] for ch in ("=", "&", "/", "?")):
            break
        url_parts.append(cleaned[tail_idx])
        tail_idx += 1

    url = ",".join(url_parts).strip()
    trailing = cleaned[tail_idx:]

    topic_id = ""
    name = ""

    if u_idx == 0:
        # Format: URL [, name [, fmt [, res]]]
        if trailing and not _is_trailing_meta_field(trailing[0]):
            name = trailing[0]
            fmt, res = _parse_fmt_res_tokens(trailing[1:], default_res)
        else:
            fmt, res = _parse_fmt_res_tokens(trailing, default_res)
    elif u_idx == 1:
        # Format: name, link [, fmt [, res]]
        name = cleaned[0]
        fmt, res = _parse_fmt_res_tokens(trailing, default_res)
    elif u_idx == 3:
        # Format: Topicid, Name, videoid, link [, fmt [, res]] (matches index.csv!)
        topic_id = cleaned[0]
        name = cleaned[1] or cleaned[2]
        fmt, res = _parse_fmt_res_tokens(trailing, default_res)
    else:
        # u_idx == 2 or >3: e.g. id, name, link [, fmt [, res]]
        if cleaned[0].isdigit():
            topic_id = cleaned[0]
        non_empty_head = [c for c in cleaned[:u_idx] if c]
        name = non_empty_head[1] if len(non_empty_head) >= 2 and non_empty_head[0].isdigit() else " - ".join(non_empty_head)
        fmt, res = _parse_fmt_res_tokens(trailing, default_res)

    return _clean(name), _clean(url), fmt, res, topic_id, False


def parse_line(raw_line: str, default_res: int):
    """Parse one line -> (name, url, fmt, res, topic_id, is_header)."""
    line = raw_line.replace("\u00a0", " ").replace("\u200b", "").lstrip("\ufeff").strip()
    if not line or line.startswith("#"):
        return "", "", "", default_res, "", True

    # 1. If it looks comma-delimited (CSV or name,link,ext,res)
    if "," in line:
        try:
            row = next(csv.reader([line]))
            if any(_is_url(c.strip()) for c in row) or (
                any(k in [c.strip().lower() for c in row] for k in ("link", "url"))
                and "topicid" in [c.strip().lower() for c in row]
            ):
                return _parse_csv_fields(row, default_res)
        except Exception:
            pass

    # 2. Tab / pipe / semicolon / whitespace fallback
    m = URL_START.search(line)
    if not m:
        return line, "", "", default_res, "", False

    before = _clean(line[:m.start()])
    after_all = line[m.start():].strip()

    # Check if separated by tab or pipe
    if "\t" in line or "|" in line:
        sep = "\t" if "\t" in line else "|"
        parts = [p.strip() for p in line.split(sep)]
        return _parse_csv_fields(parts, default_res)

    # Line starts with URL (possibly followed by space + name)
    if m.start() == 0:
        parts = after_all.split(None, 1)
        url = parts[0]
        name = _clean(parts[1]) if len(parts) == 2 else ""
        return name, url, "", default_res, "", False

    before = re.sub(r"[\s|;:=\-–—]+$", "", before).strip()
    # Strip trailing url space if any
    url_parts = after_all.split(None, 1)
    url = url_parts[0]
    fmt, res = ("", default_res)
    if len(url_parts) > 1:
        extra_toks = re.split(r"[\s,]+", url_parts[1])
        fmt, res = _parse_fmt_res_tokens(extra_toks, default_res)
    return before, url, fmt, res, "", False


def is_streaming_url(url: str) -> bool:
    sp = urlsplit(url)
    host = (sp.hostname or "").lower()
    path = (sp.path or "").lower()
    if any(host == d or host.endswith("." + d) for d in STREAMING_DOMAINS):
        return True
    if path.endswith((".m3u8", ".mpd")) or ".m3u8" in url.lower() or ".mpd" in url.lower():
        return True
    return False


def parse_jobs(text: str, batch_dir: Path, default_res: int = None):
    if default_res is None:
        default_res = CFG.default_resolution
    jobs = []
    used = {}
    text = text.lstrip("\ufeff")

    # Handle multi-line quoted CSV cleanly if entire text parses as CSV with a link header
    lines = text.splitlines()
    for raw in lines:
        s = raw.strip()
        if not s or s.startswith("#"):
            continue
        name, url, fmt, res, topic_id, is_hdr = parse_line(raw, default_res)
        if is_hdr:
            continue
        if not url:
            jobs.append(Job(
                index=len(jobs),
                name=sanitize_filename(name or s)[:60],
                caption=name or s,
                url="",
                path=batch_dir / "invalid",
                status="failed",
                error="Invalid line (no URL found)",
            ))
            continue

        # Clean up name
        if not name:
            url_base = os.path.basename(unquote(urlsplit(url).path)) or "file"
            name = os.path.splitext(url_base)[0] or "file"
        name = re.sub(r"^\s*\d+\s*[\).\-\u2013]\s*", "", name).strip() or "file"
        raw_title = name
        name = sanitize_filename(name)

        stem, name_ext = os.path.splitext(name)
        name_ext_low = name_ext.lower()
        explicit_ext = False

        if fmt:
            ext = fmt.lower()
            explicit_ext = True
            if name_ext_low == ext:
                name = stem
        elif name_ext_low in KNOWN_ALL_EXTS:
            ext = name_ext_low
            explicit_ext = True
            name = stem
        else:
            url_ext = os.path.splitext(urlsplit(url).path)[1].lower()
            if url_ext in KNOWN_ALL_EXTS or (re.fullmatch(r"\.[a-z0-9]{2,5}", url_ext or "") and not is_streaming_url(url)):
                ext = url_ext
            elif is_streaming_url(url):
                ext = ".mp4"
            else:
                ext = ""   # will be resolved via Content-Disposition / Content-Type / fallback

        # Deduplicate stem in this batch
        base_stem = name
        k = 2
        key_ext = ext or ".mp4"
        while f"{name.lower()}{key_ext}" in used:
            name = f"{base_stem} ({k})"
            k += 1
        used[f"{name.lower()}{key_ext}"] = True

        full_name = name + (ext if ext else "")
        caption = f"[Topic {topic_id}] {raw_title}" if topic_id else raw_title
        jobs.append(Job(
            index=len(jobs),
            name=full_name,
            caption=caption,
            url=url,
            path=batch_dir / (full_name if ext else (name + ".tmp")),
            ext=ext,
            explicit_ext=explicit_ext,
            resolution=res,
            topic_id=topic_id,
        ))
    return jobs


# ----------------------------------------------------------------------
# Download Engine: Probe + yt-dlp (Streaming) + aria2c (Direct)
# ----------------------------------------------------------------------
def ext_from_headers(headers, url: str) -> str:
    cd = headers.get("Content-Disposition", "")
    m = re.search(r"filename\*\s*=\s*(?:UTF-8''|utf-8'')?([^;\r\n]+)", cd)
    if not m:
        m = re.search(r'filename\s*=\s*"?([^";\r\n]+)"?', cd)
    if m:
        fn = unquote(m.group(1).strip().strip('"'))
        e = os.path.splitext(fn)[1].lower()
        if re.fullmatch(r"\.[a-z0-9]{1,8}", e):
            return e

    ct = (headers.get("Content-Type") or "").split(";")[0].strip().lower()
    if ct in MIME_TO_EXT:
        return MIME_TO_EXT[ct]
    if ct:
        guessed = mimetypes.guess_extension(ct)
        if guessed:
            return ".jpg" if guessed == ".jpe" else guessed.lower()

    url_ext = os.path.splitext(urlsplit(url).path)[1].lower()
    if re.fullmatch(r"\.[a-z0-9]{1,8}", url_ext):
        return url_ext
    return ""


async def probe_url(session, url: str):
    """Probe URL headers without downloading body. Returns (size, content_type, detected_ext, range_ok)."""
    headers = CFG.build_headers(url)
    probe_hdrs = dict(headers)
    probe_hdrs["Range"] = "bytes=0-0"
    try:
        async with session.get(url, headers=probe_hdrs, allow_redirects=True) as resp:
            if resp.status in RATE_LIMIT_STATUSES:
                raise SourceLimited(resp.status, parse_retry_after(resp.headers))
            ct = (resp.headers.get("Content-Type") or "").split(";")[0].strip().lower()
            det_ext = ext_from_headers(resp.headers, str(resp.url))
            if resp.status == 206:
                cr = resp.headers.get("Content-Range", "")
                size = 0
                if "/" in cr:
                    tail = cr.split("/")[-1].strip()
                    size = int(tail) if tail.isdigit() else 0
                await resp.content.read(8)
                return size, ct, det_ext, True
            if resp.status == 200:
                size = int(resp.headers.get("Content-Length") or 0)
                return size, ct, det_ext, False
            if resp.status in (400, 405, 416, 501):
                # Some servers reject Range probes — retry plain HEAD or let aria2c handle it
                async with session.head(url, headers=headers, allow_redirects=True) as hresp:
                    if hresp.status in RATE_LIMIT_STATUSES:
                        raise SourceLimited(hresp.status, parse_retry_after(hresp.headers))
                    hct = (hresp.headers.get("Content-Type") or "").split(";")[0].strip().lower()
                    hext = ext_from_headers(hresp.headers, str(hresp.url))
                    hsize = int(hresp.headers.get("Content-Length") or 0)
                    return hsize, hct or ct, hext or det_ext, False
            raise JobError(f"HTTP {resp.status} {resp.reason}")
    except (JobError, SourceLimited):
        raise
    except Exception as e:
        log.warning("Probe warning for %s (%s) — letting downloader attempt directly", url[:80], e)
        return 0, "", "", False


def _finalize_job_ext(job: Job, new_ext: str, batch_dir: Path):
    if not new_ext:
        new_ext = ".mp4"
    if not new_ext.startswith("."):
        new_ext = "." + new_ext
    new_ext = new_ext.lower()
    stem = os.path.splitext(job.name)[0] if job.ext else job.name
    job.ext = new_ext
    job.name = stem + new_ext
    job.path = batch_dir / job.name


async def _terminate_proc(proc):
    if proc and proc.returncode is None:
        try:
            proc.terminate()
            await asyncio.wait_for(proc.wait(), timeout=3)
        except Exception:
            try:
                proc.kill()
                await proc.wait()
            except Exception:
                pass


# ---- 1) aria2c Direct Downloader --------------------------------------
ARIA2_PROG_RE = re.compile(
    r"\[#\S+\s+([0-9.]+\w+)/([0-9.]+\w+)\(\d+%\).*?DL:([0-9.]+\w+)",
    re.I,
)


async def download_with_aria2(job: Job, tracker, out_path: Path) -> bool:
    """Download direct file with aria2c. Returns True on success."""
    if not shutil.which("aria2c"):
        return False

    out_path.parent.mkdir(parents=True, exist_ok=True)
    headers = CFG.build_headers(job.url)
    ua = headers.pop("User-Agent", CFG.user_agent)
    ref = headers.pop("Referer", "")

    cmd = [
        "aria2c",
        "--dir", str(out_path.parent),
        "--out", out_path.name,
        "--max-connection-per-server", str(min(16, max(1, ARIA2_CONNS))),
        "--split", str(ARIA2_CONNS),
        "--min-split-size", "1M",
        "--max-tries", "5",
        "--retry-wait", "3",
        "--continue=true",
        "--allow-overwrite=true",
        "--auto-file-renaming=false",
        "--file-allocation=none",
        "--summary-interval=1",
        "--console-log-level=notice",
        "--user-agent", ua,
    ]
    if ref:
        cmd.extend(["--referer", ref])
    if CFG.has_cookies_file() and not CFG.cookie_string:
        cmd.extend(["--load-cookies", str(COOKIES_FILE)])
    for k, v in headers.items():
        cmd.extend(["--header", f"{k}: {v}"])
    if DL_SPEED_LIMIT_MBPS > 0:
        cmd.extend(["--max-overall-download-limit", f"{int(DL_SPEED_LIMIT_MBPS * 1024)}K"])
    cmd.append(job.url)

    log.info("⬇ [aria2c ×%d] %s", ARIA2_CONNS, job.name)
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )

    last_lines = deque(maxlen=15)
    detected_rl = None
    try:
        while True:
            if tracker.cancelled:
                await _terminate_proc(proc)
                raise BatchCancelled
            try:
                raw_line = await asyncio.wait_for(proc.stdout.readline(), timeout=1.0)
            except asyncio.TimeoutError:
                if proc.returncode is not None:
                    break
                if storage_paused():
                    await wait_for_storage(tracker, job)
                continue
            if not raw_line:
                break
            line = raw_line.decode("utf-8", errors="replace").strip()
            if not line:
                continue
            last_lines.append(line)

            m = ARIA2_PROG_RE.search(line)
            if m:
                done_b = parse_size_token(m.group(1))
                tot_b = parse_size_token(m.group(2))
                spd_b = parse_size_token(m.group(3))
                if done_b:
                    job.done = done_b
                if tot_b:
                    job.total = tot_b
                if spd_b:
                    job.speed = float(spd_b)

            m_http = re.search(r"status=(403|410|429)\b|HTTP/\S+\s+(403|410|429)\b", line)
            if m_http:
                detected_rl = int(m_http.group(1) or m_http.group(2))

        rc = await proc.wait()
        if tracker.cancelled:
            raise BatchCancelled
        if detected_rl and rc != 0:
            raise SourceLimited(detected_rl)
        if rc == 0 and out_path.exists() and out_path.stat().st_size > 0:
            job.done = job.total = out_path.stat().st_size
            # Remove any stray .aria2 control file
            Path(str(out_path) + ".aria2").unlink(missing_ok=True)
            return True

        tail = " | ".join(last_lines)[-250:]
        log.warning("aria2c failed (rc=%s) for %s: %s", rc, job.name, tail)
        return False
    except (BatchCancelled, SourceLimited):
        await _terminate_proc(proc)
        raise
    finally:
        Path(str(out_path) + ".aria2").unlink(missing_ok=True)


async def download_with_aiohttp(job: Job, session, tracker, out_path: Path):
    """Fallback direct HTTP downloader using aiohttp."""
    net_attempt = 0
    while True:
        if tracker.cancelled:
            raise BatchCancelled
        resume = out_path.stat().st_size if out_path.exists() else 0
        headers = CFG.build_headers(job.url)
        if resume > 0:
            headers["Range"] = f"bytes={resume}-"
        try:
            await wait_global(tracker)
            async with session.get(job.url, headers=headers, allow_redirects=True) as resp:
                if resp.status in RATE_LIMIT_STATUSES:
                    raise SourceLimited(resp.status, parse_retry_after(resp.headers))
                if resp.status == 416:
                    job.done = job.total = resume
                    return
                if resp.status == 206:
                    mode = "ab"
                    cr = resp.headers.get("Content-Range", "")
                    if "/" in cr and cr.split("/")[-1].strip().isdigit():
                        job.total = int(cr.split("/")[-1])
                elif resp.status == 200:
                    mode, resume = "wb", 0
                    job.total = int(resp.headers.get("Content-Length") or 0)
                else:
                    raise JobError(f"HTTP {resp.status} {resp.reason}")

                got = resume
                job.done = got
                since_check = 0
                out_path.parent.mkdir(parents=True, exist_ok=True)
                with out_path.open(mode) as f:
                    async for chunk in resp.content.iter_chunked(CHUNK_SIZE):
                        if tracker.cancelled:
                            raise BatchCancelled
                        await THROTTLE.consume(len(chunk))
                        f.write(chunk)
                        got += len(chunk)
                        job.done = got
                        since_check += len(chunk)
                        if since_check >= 16 * 1024 * 1024:
                            since_check = 0
                            if storage_paused():
                                await wait_for_storage(tracker, job)
                return
        except (BatchCancelled, SourceLimited, JobError):
            raise
        except Exception as e:
            net_attempt += 1
            if net_attempt > MAX_RETRIES:
                raise JobError(f"Direct download failed: {type(e).__name__}: {e}")
            await asyncio.sleep(RETRY_BACKOFF * net_attempt)


# ---- 2) yt-dlp Streaming Video Downloader -----------------------------
def _build_ytdlp_cmd(url: str, job_dir: Path, job: Job, use_aria2: bool, test_seconds: int = 0) -> list:
    res = job.resolution or CFG.default_resolution
    target_ext = (job.ext.lstrip(".") if job.ext else "mp4").lower()
    is_audio = (job.ext in AUDIO_EXTS)

    out_tmpl = str(job_dir / "media.%(ext)s")
    cmd = [
        sys.executable, "-m", "yt_dlp",
        "--no-playlist",
        "--newline",
        "--progress",
        "--progress-template",
        "download:YTDLP|%(progress.downloaded_bytes)s|%(progress.total_bytes)s|%(progress.total_bytes_estimate)s|%(progress.speed)s|%(progress.eta)s",
        "--retries", "5",
        "--fragment-retries", "5",
        "--concurrent-fragments", str(YTDLP_FRAG_CONNS),
        "--user-agent", CFG.user_agent,
        "--output", out_tmpl,
    ]

    if CFG.referer:
        cmd.extend(["--referer", CFG.referer])
    if CFG.origin:
        cmd.extend(["--add-header", f"Origin:{CFG.origin}"])
    for k, v in CFG.extra_headers.items():
        cmd.extend(["--add-header", f"{k}:{v}"])
    if CFG.cookie_string:
        cmd.extend(["--add-header", f"Cookie:{CFG.cookie_string}"])
    elif CFG.has_cookies_file():
        cmd.extend(["--cookies", str(COOKIES_FILE)])

    if is_audio:
        audio_fmt = target_ext if target_ext in {"mp3", "m4a", "aac", "opus", "wav", "flac"} else "mp3"
        cmd.extend(["-f", "ba/b", "-x", "--audio-format", audio_fmt])
    else:
        # Strict cap: <= requested resolution (default 1080p)
        fmt_sel = (
            f"bv*[height<={res}][ext=mp4]+ba[ext=m4a]/"
            f"bv*[height<={res}]+ba/"
            f"b[height<={res}][ext=mp4]/"
            f"b[height<={res}]/"
            f"bv*[height<=?{res}]+ba/"
            f"b[height<=?{res}]"
        )
        cmd.extend([
            "-f", fmt_sel,
            "--format-sort", f"res:{res},vcodec:h264,acodec:aac,ext:mp4:m4a",
        ])
        merge_fmt = target_ext if target_ext in {"mp4", "mkv", "webm", "mov"} else "mp4"
        cmd.extend(["--merge-output-format", merge_fmt])

    if use_aria2 and shutil.which("aria2c"):
        cmd.extend([
            "--external-downloader", "aria2c",
            "--external-downloader-args",
            f"aria2c:-x {min(16, max(1, YTDLP_ARIA2_CONNS))} -s {max(1, YTDLP_ARIA2_CONNS)} -k 1M --file-allocation=none",
        ])
    if DL_SPEED_LIMIT_MBPS > 0:
        cmd.extend(["--limit-rate", f"{DL_SPEED_LIMIT_MBPS}M"])
    if test_seconds and test_seconds > 0:
        cmd.extend(["--download-sections", f"*00:00:00-00:00:{int(test_seconds):02d}"])

    cmd.append(url)
    return cmd


async def _run_ytdlp_once(url: str, job: Job, tracker, job_dir: Path, use_aria2: bool, test_seconds: int = 0):
    job_dir.mkdir(parents=True, exist_ok=True)
    cmd = _build_ytdlp_cmd(url, job_dir, job, use_aria2=use_aria2, test_seconds=test_seconds)
    log.info("⬇ [yt-dlp%s <=%dp] %s (%s)",
             "+aria2c" if (use_aria2 and shutil.which("aria2c")) else "",
             job.resolution, job.name, url[:65])

    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )

    tail = deque(maxlen=25)
    detected_rl = None
    try:
        while True:
            if tracker.cancelled:
                await _terminate_proc(proc)
                raise BatchCancelled
            try:
                raw = await asyncio.wait_for(proc.stdout.readline(), timeout=1.0)
            except asyncio.TimeoutError:
                if proc.returncode is not None:
                    break
                continue
            if not raw:
                break
            line = raw.decode("utf-8", errors="replace").strip()
            if not line:
                continue

            if line.startswith("YTDLP|"):
                parts = line.split("|")
                if len(parts) >= 5:
                    def _num(x):
                        try:
                            return float(x) if x and x not in ("NA", "None") else 0.0
                        except ValueError:
                            return 0.0
                    dl = int(_num(parts[1]))
                    tot = int(_num(parts[2]) or _num(parts[3]))
                    spd = _num(parts[4])
                    if dl > 0:
                        job.done = dl
                    if tot > 0:
                        job.total = tot
                    if spd > 0:
                        job.speed = spd
                continue

            # Also parse aria2c progress inside yt-dlp if external downloader runs
            m_a2 = ARIA2_PROG_RE.search(line)
            if m_a2:
                job.done = parse_size_token(m_a2.group(1)) or job.done
                job.total = parse_size_token(m_a2.group(2)) or job.total
                job.speed = float(parse_size_token(m_a2.group(3)) or 0) or job.speed
                continue

            tail.append(line)
            m_rl = re.search(r"HTTP Error (403|410|429)\b", line, re.I)
            if m_rl:
                detected_rl = int(m_rl.group(1))

        rc = await proc.wait()
        if tracker.cancelled:
            raise BatchCancelled
        if rc == 0:
            files = [
                p for p in job_dir.iterdir()
                if p.is_file() and not p.name.endswith((".part", ".ytdl", ".aria2", ".temp"))
            ]
            if files:
                # Pick largest media file produced
                best = max(files, key=lambda p: p.stat().st_size)
                return best, ""
        err_msg = "\n".join(tail)
        return None, (f"HTTP {detected_rl}: " if detected_rl else "") + err_msg[-300:]
    except BatchCancelled:
        await _terminate_proc(proc)
        raise


def _vimeo_embed_candidate(url: str) -> str:
    """Convert vimeo.com/<id>/<hash> or vimeo.com/<id>?h=<hash> to player.vimeo.com embed URL."""
    m = re.match(r"https?://(?:www\.)?vimeo\.com/(?:channels/[^/]+/|groups/[^/]+/videos/|video/)?(\d+)/([a-f0-9]+)", url, re.I)
    if m:
        return f"https://player.vimeo.com/video/{m.group(1)}?h={m.group(2)}"
    m2 = re.match(r"https?://(?:www\.)?vimeo\.com/(?:channels/[^/]+/|groups/[^/]+/videos/|video/)?(\d+)(?:\?.*?\bh=([a-f0-9]+))?", url, re.I)
    if m2:
        vid, h = m2.group(1), m2.group(2)
        return f"https://player.vimeo.com/video/{vid}?h={h}" if h else f"https://player.vimeo.com/video/{vid}"
    return ""


async def download_with_ytdlp(job: Job, tracker, batch_dir: Path, test_seconds: int = 0):
    """Download streaming video using yt-dlp with automatic fallback for Vimeo embed / aria2c."""
    job_dir = batch_dir / f"ytdl_{job.index}_{random.randint(1000, 9999)}"
    try:
        # Recent yt-dlp requires login cookies for vimeo.com/<id>/<hash> web URLs,
        # whereas player.vimeo.com/video/<id>?h=<hash> with Referer works without cookies!
        urls_to_try = []
        embed_url = _vimeo_embed_candidate(job.url)
        has_cookies = CFG.has_cookies_file() or bool(CFG.cookie_string)
        if embed_url and not has_cookies:
            urls_to_try = [embed_url, job.url]
        elif embed_url:
            urls_to_try = [job.url, embed_url]
        else:
            urls_to_try = [job.url]

        last_err = ""
        for u in urls_to_try:
            for use_aria2 in (True, False):
                if job_dir.exists():
                    shutil.rmtree(job_dir, ignore_errors=True)
                out_file, err = await _run_ytdlp_once(
                    u, job, tracker, job_dir, use_aria2=use_aria2, test_seconds=test_seconds
                )
                if out_file is not None:
                    # Convert extension if user explicitly asked for e.g. wmv/avi
                    desired_ext = job.ext or ".mp4"
                    actual_ext = out_file.suffix.lower() or ".mp4"
                    if job.explicit_ext and desired_ext != actual_ext and shutil.which("ffmpeg"):
                        conv_path = job_dir / f"converted{desired_ext}"
                        job.note = f"⚙ converting to {desired_ext}"
                        p = await asyncio.create_subprocess_exec(
                            "ffmpeg", "-y", "-i", str(out_file), "-c", "copy", str(conv_path),
                            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
                        )
                        await p.communicate()
                        if p.returncode != 0 or not conv_path.exists() or conv_path.stat().st_size == 0:
                            # Re-encode if stream copy incompatible with container (e.g. wmv)
                            p2 = await asyncio.create_subprocess_exec(
                                "ffmpeg", "-y", "-i", str(out_file), str(conv_path),
                                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
                            )
                            await p2.communicate()
                        if conv_path.exists() and conv_path.stat().st_size > 0:
                            out_file = conv_path
                            actual_ext = desired_ext
                        job.note = ""

                    _finalize_job_ext(job, actual_ext if not job.explicit_ext else desired_ext, batch_dir)
                    shutil.move(str(out_file), str(job.path))
                    job.done = job.total = job.path.stat().st_size
                    return

                last_err = err
                if "Unsupported URL" in err:
                    # Not a yt-dlp page — signal caller to fall back to aria2 direct download
                    raise ValueError("YTDLP_UNSUPPORTED_URL")
                if not use_aria2:
                    break  # don't loop endlessly on same URL

        m_rl = re.search(r"HTTP (?:Error )?(403|410|429)\b", last_err)
        if m_rl:
            raise SourceLimited(int(m_rl.group(1)))
        raise JobError(f"yt-dlp failed: {last_err[-240:]}")
    finally:
        shutil.rmtree(job_dir, ignore_errors=True)


async def download_job(job: Job, session, tracker, test_seconds: int = 0):
    """Route job to yt-dlp or aria2c with source rate-limit retries & storage guard."""
    attempt = 0
    while True:
        try:
            await wait_global(tracker)
            await wait_for_storage(tracker, job)

            # Decide download strategy
            force_direct = job.explicit_ext and job.ext not in VIDEO_EXTS and job.ext not in AUDIO_EXTS
            streaming = (not force_direct) and is_streaming_url(job.url)

            if streaming:
                try:
                    await download_with_ytdlp(job, tracker, tracker.batch_dir, test_seconds=test_seconds)
                    return
                except ValueError as ve:
                    if str(ve) != "YTDLP_UNSUPPORTED_URL":
                        raise
                    streaming = False

            # Probe direct URL to get size, MIME type, filename, and detect HTML video pages
            size, ct, det_ext, _ = await probe_url(session, job.url)
            if ct == "text/html" and not force_direct:
                # Webpage containing embedded video! Hand off to yt-dlp
                try:
                    await download_with_ytdlp(job, tracker, tracker.batch_dir, test_seconds=test_seconds)
                    return
                except ValueError:
                    pass  # proceed as direct download if yt-dlp doesn't recognize it

            if size:
                du = shutil.disk_usage(".")
                if size > du.total - MIN_FREE_BYTES:
                    raise JobError(f"File ({fmt_bytes(size)}) exceeds usable disk ({fmt_bytes(du.total)})")
                await wait_for_storage(tracker, job, size + MIN_FREE_BYTES)
                tracker.admitted[id(job)] = job

            if not job.explicit_ext:
                resolved_ext = job.ext or det_ext or ".mp4"
                _finalize_job_ext(job, resolved_ext, tracker.batch_dir)
            else:
                _finalize_job_ext(job, job.ext, tracker.batch_dir)

            job.total = size
            ok = await download_with_aria2(job, tracker, job.path)
            if not ok:
                log.info("⬇ [aiohttp fallback] %s", job.name)
                await download_with_aiohttp(job, session, tracker, job.path)
            return
        except SourceLimited as sl:
            attempt += 1
            if attempt > RATE_LIMIT_RETRIES:
                raise JobError(rate_limit_hint(sl.status))
            await wait_out(sl.status, sl.retry_after, attempt, tracker, job)


# ----------------------------------------------------------------------
# Smart Auto-Splitter (> 2000 MB -> lowest required parts <= 2000 MB)
# ----------------------------------------------------------------------
async def ffprobe_info(path: Path) -> dict:
    """Return {duration, width, height} via ffprobe."""
    if not shutil.which("ffprobe"):
        return {"duration": 0, "width": 0, "height": 0}
    try:
        proc = await asyncio.create_subprocess_exec(
            "ffprobe", "-v", "quiet", "-print_format", "json",
            "-show_format", "-show_streams", str(path),
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        )
        out, _ = await proc.communicate()
        data = json.loads(out.decode("utf-8", errors="replace"))
        dur = float(data.get("format", {}).get("duration") or 0)
        w = h = 0
        for st in data.get("streams", []):
            if st.get("codec_type") == "video":
                w = int(st.get("width") or 0)
                h = int(st.get("height") or 0)
                if not dur and st.get("duration"):
                    dur = float(st["duration"])
                break
        return {"duration": int(round(dur)), "width": w, "height": h}
    except Exception:
        return {"duration": 0, "width": 0, "height": 0}


async def make_video_thumb(path: Path, thumb_path: Path, duration: int) -> Path:
    if not shutil.which("ffmpeg"):
        return None
    seek = str(min(5, max(1, duration // 4))) if duration > 2 else "0"
    try:
        proc = await asyncio.create_subprocess_exec(
            "ffmpeg", "-y", "-ss", seek, "-i", str(path),
            "-vframes", "1", "-vf", "scale='min(320,iw)':-2",
            "-q:v", "4", str(thumb_path),
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        )
        await proc.communicate()
        if thumb_path.exists() and thumb_path.stat().st_size > 0:
            return thumb_path
    except Exception:
        pass
    return None


def _binary_split(path: Path, stem: str, ext: str, target_bytes: int) -> list:
    """Split file into lowest required binary parts <= target_bytes."""
    size = path.stat().st_size
    n_parts = max(2, math.ceil(size / target_bytes))
    part_size = math.ceil(size / n_parts)
    parts = []
    buf_size = 4 * 1024 * 1024
    with open(path, "rb") as src:
        for i in range(1, n_parts + 1):
            part_path = path.parent / f"{stem}_part{i}{ext}"
            remaining = part_size if i < n_parts else size
            with open(part_path, "wb") as dst:
                while remaining > 0:
                    chunk = src.read(min(buf_size, remaining))
                    if not chunk:
                        break
                    dst.write(chunk)
                    remaining -= len(chunk)
            if part_path.stat().st_size > 0:
                parts.append(part_path)
    return parts


def _pdf_split(path: Path, stem: str, ext: str, target_bytes: int) -> list:
    """Split oversized PDF by pages so every part is a valid readable PDF <= target_bytes."""
    try:
        from pypdf import PdfReader, PdfWriter
    except ImportError:
        return _binary_split(path, stem, ext, target_bytes)

    try:
        reader = PdfReader(str(path))
        total_pages = len(reader.pages)
        if total_pages <= 1:
            return _binary_split(path, stem, ext, target_bytes)

        size = path.stat().st_size
        est_parts = max(2, math.ceil(size / target_bytes))
        pages_per_part = max(1, math.ceil(total_pages / est_parts))

        parts = []
        idx = 0
        part_num = 1
        while idx < total_pages:
            end = min(total_pages, idx + pages_per_part)
            while True:
                writer = PdfWriter()
                for p in range(idx, end):
                    writer.add_page(reader.pages[p])
                out_p = path.parent / f"{stem}_part{part_num}{ext}"
                with open(out_p, "wb") as f:
                    writer.write(f)
                if out_p.stat().st_size <= MAX_UPLOAD or (end - idx) == 1:
                    break
                # Overshot target -> shrink page slice and retry
                out_p.unlink(missing_ok=True)
                end = idx + max(1, (end - idx) * 8 // 10)

            if out_p.stat().st_size > MAX_UPLOAD:
                # Single page itself > 2000 MB -> fall back to binary split
                for p in parts:
                    p.unlink(missing_ok=True)
                out_p.unlink(missing_ok=True)
                return _binary_split(path, stem, ext, BIN_SPLIT_TARGET)

            parts.append(out_p)
            idx = end
            part_num += 1
        return parts
    except Exception as e:
        log.warning("PDF page-split failed (%s), falling back to binary split", e)
        return _binary_split(path, stem, ext, BIN_SPLIT_TARGET)


async def _video_split(path: Path, stem: str, ext: str) -> list:
    """Lossless ffmpeg stream-copy split so every part is <= MAX_UPLOAD and independently playable."""
    if not shutil.which("ffmpeg"):
        return await asyncio.to_thread(_binary_split, path, stem, ext, BIN_SPLIT_TARGET)

    info = await ffprobe_info(path)
    total_dur = info["duration"]
    if total_dur <= 1:
        return await asyncio.to_thread(_binary_split, path, stem, ext, BIN_SPLIT_TARGET)

    parts = []
    cur_sec = 0.0
    part_num = 1
    fs_limit = str(VIDEO_SPLIT_TARGET)
    faststart = ["-movflags", "+faststart"] if ext in {".mp4", ".mov", ".m4v"} else []

    try:
        while cur_sec < total_dur - 1.0:
            out_p = path.parent / f"{stem}_part{part_num}{ext}"
            cmd = ["ffmpeg", "-y"]
            if cur_sec > 0:
                cmd.extend(["-ss", f"{cur_sec:.3f}"])
            cmd.extend([
                "-i", str(path),
                "-c", "copy",
                "-map", "0",
                "-fs", fs_limit,
                *faststart,
                str(out_p),
            ])
            proc = await asyncio.create_subprocess_exec(
                *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            )
            await proc.communicate()
            if proc.returncode != 0 or not out_p.exists() or out_p.stat().st_size == 0:
                raise RuntimeError("ffmpeg split returned empty/error")

            p_size = out_p.stat().st_size
            if p_size > MAX_UPLOAD:
                # Rare keyframe overshoot: slice slightly shorter by time
                p_info = await ffprobe_info(out_p)
                out_p.unlink(missing_ok=True)
                safe_dur = max(10, int(p_info["duration"] * 0.90))
                cmd2 = ["ffmpeg", "-y"]
                if cur_sec > 0:
                    cmd2.extend(["-ss", f"{cur_sec:.3f}"])
                cmd2.extend(["-i", str(path), "-t", str(safe_dur), "-c", "copy", "-map", "0", *faststart, str(out_p)])
                p2 = await asyncio.create_subprocess_exec(
                    *cmd2, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
                )
                await p2.communicate()
                if not out_p.exists() or out_p.stat().st_size > MAX_UPLOAD or out_p.stat().st_size == 0:
                    raise RuntimeError("ffmpeg time-bounded split failed")

            p_info = await ffprobe_info(out_p)
            step = p_info["duration"]
            if step <= 0:
                raise RuntimeError("split part has 0 duration")

            parts.append(out_p)
            cur_sec += step
            part_num += 1
            if part_num > 100:
                raise RuntimeError("too many video parts")

        return parts
    except Exception as e:
        log.warning("ffmpeg video split failed for %s (%s) — falling back to binary split", path.name, e)
        for p in parts:
            p.unlink(missing_ok=True)
        return await asyncio.to_thread(_binary_split, path, stem, ext, BIN_SPLIT_TARGET)


async def split_if_needed(job: Job) -> list:
    """Return list of paths to upload sequentially. Deletes original if split."""
    size = job.path.stat().st_size
    if size <= MAX_UPLOAD:
        return [job.path]

    ext = job.path.suffix.lower() or job.ext
    stem = job.path.stem
    job.note = f"✂ splitting {fmt_bytes(size)} into <=2000 MB parts..."
    log.info("✂ %s (%s) > 2000 MB — auto-splitting into lowest required parts",
             job.name, fmt_bytes(size))

    if ext in VIDEO_EXTS:
        parts = await _video_split(job.path, stem, ext)
    elif ext == ".pdf":
        parts = await asyncio.to_thread(_pdf_split, job.path, stem, ext, PDF_SPLIT_TARGET)
    else:
        parts = await asyncio.to_thread(_binary_split, job.path, stem, ext, BIN_SPLIT_TARGET)

    # Free original file space immediately once split succeeds
    if len(parts) > 1 and all(p.exists() and 0 < p.stat().st_size <= MAX_UPLOAD for p in parts):
        job.path.unlink(missing_ok=True)
    elif len(parts) == 1:
        parts = [job.path]
    else:
        # Safety fallback if any part still exceeded MAX_UPLOAD
        for p in parts:
            if p != job.path:
                p.unlink(missing_ok=True)
        parts = await asyncio.to_thread(_binary_split, job.path, stem, ext, BIN_SPLIT_TARGET)
        job.path.unlink(missing_ok=True)

    job.note = ""
    log.info("✂ Split %s into %d part(s): %s",
             job.name, len(parts), ", ".join(p.name for p in parts))
    return parts


# ----------------------------------------------------------------------
# Extension-Aware High-Speed Telegram Upload Pipeline
# ----------------------------------------------------------------------
def _file_md5(path: Path) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


async def build_upload_kwargs(path: Path, caption: str, thumb_dir: Path):
    """Determine optimal Telegram send_file arguments by file extension."""
    ext = path.suffix.lower()
    size = path.stat().st_size
    mime = mimetypes.guess_type(path.name)[0] or "application/octet-stream"
    kwargs = {
        "caption": caption[:1000],
        "mime_type": mime,
    }
    thumb_path = None

    if ext in STREAMABLE_VIDEO_EXTS:
        info = await ffprobe_info(path)
        thumb_path = await make_video_thumb(path, thumb_dir / f"{path.stem}_thumb.jpg", info["duration"])
        attrs = [
            DocumentAttributeVideo(
                duration=info["duration"],
                w=info["width"] or 1280,
                h=info["height"] or 720,
                supports_streaming=True,
            ),
            DocumentAttributeFilename(path.name),
        ]
        kwargs.update({
            "supports_streaming": True,
            "force_document": False,
            "attributes": attrs,
            "is_photo": False,
        })
        if thumb_path:
            kwargs["thumb"] = str(thumb_path)

    elif ext in DOC_VIDEO_EXTS:
        # e.g. .wmv, .avi, .flv — upload as document to preserve filename & extension reliably
        kwargs.update({
            "force_document": True,
            "attributes": [DocumentAttributeFilename(path.name)],
            "is_photo": False,
        })

    elif ext in PHOTO_EXTS and size <= PHOTO_MAX_BYTES:
        kwargs.update({
            "force_document": False,
            "is_photo": True,
        })

    elif ext in ANIM_EXTS and size <= 50 * 1024 * 1024:
        kwargs.update({
            "force_document": False,
            "is_photo": False,
            "attributes": [DocumentAttributeFilename(path.name)],
        })

    elif ext in AUDIO_EXTS:
        info = await ffprobe_info(path)
        kwargs.update({
            "force_document": False,
            "is_photo": False,
            "attributes": [
                DocumentAttributeAudio(duration=info["duration"], title=path.stem),
                DocumentAttributeFilename(path.name),
            ],
        })

    else:
        # PDFs, large images (>10 MB), docs, archives, unknown types
        kwargs.update({
            "force_document": True,
            "is_photo": False,
            "attributes": [DocumentAttributeFilename(path.name)],
        })

    return kwargs, thumb_path


async def parallel_upload(job: Job, path: Path, up_kwargs: dict, client, chat_id, tracker):
    """Parallel 512 KB chunk upload over MTProto (fasttelethon style)."""
    size = path.stat().st_size
    part_size = 512 * 1024
    part_count = math.ceil(size / part_size)
    big = size >= BIG_FILE_MIN
    file_id = random.randint(-2**63, 2**63 - 1)

    tb = tqdm(total=size, unit="B", unit_scale=True, unit_divisor=1024,
              desc=f"⬆ {path.name[:40]} ×{UPLOAD_WORKERS}", leave=False)
    md5_hex = None if big else await asyncio.to_thread(_file_md5, path)
    fd = os.open(str(path), os.O_RDONLY)
    sem = asyncio.Semaphore(UPLOAD_WORKERS)
    sender = client._sender

    async def send_part(i):
        async with sem:
            for attempt in range(1, MAX_RETRIES + 1):
                if tracker.cancelled:
                    raise BatchCancelled
                data = await asyncio.to_thread(os.pread, fd, part_size, i * part_size)
                if not data:
                    return
                req = (
                    SaveBigFilePartRequest(file_id=file_id, file_part=i, file_total_parts=part_count, bytes=data)
                    if big else
                    SaveFilePartRequest(file_id=file_id, file_part=i, bytes=data)
                )
                try:
                    await sender.send(req)
                    job.done += len(data)
                    tb.update(len(data))
                    return
                except (BatchCancelled, asyncio.CancelledError):
                    raise
                except FloodWaitError as e:
                    log.info("FloodWait %ss on upload part — sleeping", e.seconds)
                    await asyncio.sleep(e.seconds + 1)
                except Exception as e:
                    if attempt == MAX_RETRIES:
                        raise JobError(f"Part {i+1}/{part_count} failed: {type(e).__name__}: {e}")
                    await asyncio.sleep(RETRY_BACKOFF * attempt)

    try:
        tasks = [asyncio.create_task(send_part(i)) for i in range(part_count)]
        try:
            await asyncio.gather(*tasks)
        except BaseException:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise
    finally:
        os.close(fd)
        tb.close()

    if tracker.cancelled:
        raise BatchCancelled

    input_file = (
        InputFileBig(id=file_id, parts=part_count, name=path.name)
        if big else
        InputFile(id=file_id, parts=part_count, name=path.name, md5_checksum=md5_hex)
    )

    send_opts = {k: v for k, v in up_kwargs.items() if k != "is_photo"}
    for attempt in range(1, 4):
        try:
            await client.send_file(chat_id, file=input_file, **send_opts)
            return
        except PhotoInvalidDimensionsError:
            send_opts["force_document"] = True
            send_opts["attributes"] = [DocumentAttributeFilename(path.name)]
        except FloodWaitError as e:
            await asyncio.sleep(e.seconds + 1)
        except RPCError as e:
            if attempt == 3:
                raise JobError(f"send_file failed: {e}")
            await asyncio.sleep(3)


async def upload_single_path(
    job: Job, path: Path, caption: str, client, chat_id, tracker, part_base_done: int = 0
):
    size = path.stat().st_size
    up_kwargs, thumb_path = await build_upload_kwargs(path, caption, tracker.batch_dir)
    is_photo = up_kwargs.get("is_photo", False)
    try:
        # Photos (<10 MB) and small files use standard send_file; >=10 MB use parallel MTProto
        if UPLOAD_WORKERS > 1 and size >= PARALLEL_UPLOAD_MIN and not is_photo:
            await parallel_upload(job, path, up_kwargs, client, chat_id, tracker)
            return

        tb = tqdm(total=size, unit="B", unit_scale=True, unit_divisor=1024,
                  desc=f"⬆ {path.name[:40]}", leave=False)

        def cb(current, total):
            job.done = part_base_done + current
            tb.update(max(0, current - tb.n))
            if tracker.cancelled:
                raise BatchCancelled

        send_opts = {k: v for k, v in up_kwargs.items() if k != "is_photo"}
        try:
            try:
                await client.send_file(
                    chat_id,
                    file=str(path),
                    progress_callback=cb,
                    part_size_kb=512,
                    **send_opts,
                )
            except PhotoInvalidDimensionsError:
                send_opts["force_document"] = True
                send_opts["attributes"] = [DocumentAttributeFilename(path.name)]
                await client.send_file(
                    chat_id,
                    file=str(path),
                    progress_callback=cb,
                    part_size_kb=512,
                    **send_opts,
                )
        finally:
            tb.close()
    finally:
        if thumb_path:
            thumb_path.unlink(missing_ok=True)


# ----------------------------------------------------------------------
# Per-Job Processing Pipeline
# ----------------------------------------------------------------------
async def process_job(job: Job, session, chat_id, tracker) -> None:
    if job.status in ("failed", "skipped"):
        return
    parts = []
    try:
        async with tracker.dl_sem:
            if tracker.cancelled:
                raise BatchCancelled
            await wait_for_storage(tracker, job)
            job.phase, job.status, job.done, job.total = "download", "downloading", 0, 0
            await download_job(job, session, tracker)

        if tracker.escape_job is job:
            tracker.escape_job = None
        if tracker.cancelled:
            raise BatchCancelled

        size = job.path.stat().st_size if job.path.exists() else 0
        job.downloaded = size
        if size == 0:
            raise JobError("Downloaded file is empty (0 bytes)")

        # Auto-split if > 2000 MB
        parts = await split_if_needed(job)
        total_upload_bytes = sum(p.stat().st_size for p in parts)

        async with tracker.up_sem:
            if tracker.cancelled:
                raise BatchCancelled
            job.phase, job.status, job.done, job.total = "upload", "uploading", 0, total_upload_bytes
            n_parts = len(parts)

            # Upload part 1, 2, 3... strictly sequentially
            completed_bytes = 0
            for idx, part_path in enumerate(parts, 1):
                if tracker.cancelled:
                    raise BatchCancelled
                part_size = part_path.stat().st_size
                part_caption = (
                    f"{job.caption} (Part {idx}/{n_parts})"
                    if n_parts > 1 else job.caption
                )
                if n_parts > 1:
                    job.note = f"Part {idx}/{n_parts}"

                up_client = tracker.upload_clients[(job.index + idx - 1) % len(tracker.upload_clients)]
                target = getattr(tracker, "target_peer", None) or chat_id
                attempt = 0
                while True:
                    try:
                        job.done = completed_bytes
                        await upload_single_path(
                            job, part_path, part_caption, up_client, target, tracker,
                            part_base_done=completed_bytes,
                        )
                        break
                    except (BatchCancelled, asyncio.CancelledError):
                        raise
                    except Exception as e:
                        attempt += 1
                        if attempt > UPLOAD_RETRIES:
                            raise JobError(f"Upload failed ({part_path.name}): {type(e).__name__}: {e}") from e
                        secs = min(UPLOAD_RETRY_BASE * attempt, 300)
                        fw = getattr(e, "seconds", 0)
                        if fw:
                            secs = max(secs, min(int(fw) + 5, 600))
                        log.warning("🔁 %s upload error (%s) — retry %d/%d in %ss",
                                    part_path.name, e, attempt, UPLOAD_RETRIES, int(secs))
                        job.note = f"🔁 retry {attempt}/{UPLOAD_RETRIES} in {int(secs)}s"
                        await asyncio.sleep(secs)
                        job.note = f"Part {idx}/{n_parts}" if n_parts > 1 else ""
                        # Always fall back to primary client on retry if extra session failed
                        up_client = tracker.upload_clients[0] if isinstance(e, ValueError) else tracker.upload_clients[
                            (job.index + idx + attempt) % len(tracker.upload_clients)
                        ]

                completed_bytes += part_size
                job.done = completed_bytes
                # Immediately delete uploaded part to free Colab disk space
                part_path.unlink(missing_ok=True)

        job.status = "done"
        if SKIP_UPLOADED:
            mark_uploaded(job.name)
        log.info("✔ Uploaded %s (%s%s)",
                 job.name, fmt_bytes(size), f", {len(parts)} parts" if len(parts) > 1 else "")
    except BatchCancelled:
        job.status = "cancelled"
    except asyncio.CancelledError:
        job.status = "cancelled"
        raise
    except Exception as e:
        job.status = "failed"
        job.error = str(e)[:300]
        log.error("✖ %s failed: %s", job.name, e)
    finally:
        if getattr(tracker, "escape_job", None) is job:
            tracker.escape_job = None
        tracker.admitted.pop(id(job), None)
        job.note = ""
        # Delete all raw data & any remaining parts immediately
        try:
            job.path.unlink(missing_ok=True)
        except OSError:
            pass
        for p in parts:
            try:
                p.unlink(missing_ok=True)
            except OSError:
                pass


# ----------------------------------------------------------------------
# Live Progress Message
# ----------------------------------------------------------------------
def speed_of(samples: deque, now_done: float):
    samples.append((time.monotonic(), now_done))
    while samples and now_done - samples[0][1] > 0 and time.monotonic() - samples[0][0] > 15:
        samples.popleft()
    if len(samples) < 2:
        return None
    t0, b0 = samples[0]
    dt = time.monotonic() - t0
    return ((now_done - b0) / dt) if dt > 0.1 else None


def render_status(tracker) -> str:
    jobs = tracker.jobs
    n_done = sum(j.status == "done" for j in jobs)
    n_fail = sum(j.status == "failed" for j in jobs)
    n_canc = sum(j.status == "cancelled" for j in jobs)
    n_skip = sum(j.status == "skipped" for j in jobs)
    n_dl   = sum(j.status == "downloading" for j in jobs)
    n_up   = sum(j.status == "uploading" for j in jobs)
    n_wait = sum(j.status == "queued" for j in jobs)

    total = sum(j.total for j in jobs if j.total > 0)
    done = sum(j.done for j in jobs if j.status in ("downloading", "uploading", "done"))
    frac = done / total if total else None
    speed = tracker.overall_speed
    eta = ((total - done) / speed) if (speed and total and frac is not None and frac < 1) else None

    stats = f"📊 {fmt_bytes(done)} / {fmt_bytes(total) if total else '?'}"
    if frac is not None:
        stats += f"  ({frac*100:.1f}%)"
    if speed:
        stats += f"  •  {fmt_bytes(speed)}/s"
    if eta:
        stats += f"  •  ETA {fmt_eta(eta)}"

    head = [
        f"🤖 {BOT_NAME} ({BOT_HANDLE}) — {tracker.label} ({len(jobs)} item(s))",
        f"⚙ {MAX_DOWNLOADS}⬇/{MAX_UPLOADS}⬆ • aria2×{ARIA2_CONNS} • {len(tracker.upload_clients)}tx×{UPLOAD_WORKERS}w",
        stats,
        f"✅ {n_done}   ⬇ {n_dl}   ⬆ {n_up}   ⏳ {n_wait}   ⏭ {n_skip}   ❌ {n_fail}"
        + (f"   ⛔ {n_canc}" if n_canc else "")
        + f"   ⏱ {fmt_eta(time.monotonic() - tracker.t0)}"
        + f"   💾 {fmt_bytes(free_bytes())} free ({disk_used_pct():.0f}% used)",
    ]

    paused_for = rl.global_until - time.monotonic()
    if paused_for > 0:
        head.append(f"⏸ Source cooldown — resumes in {fmt_eta(paused_for)}")

    lines = []
    active = [j for j in jobs if j.status in ("downloading", "uploading")]
    for j in sorted(active, key=lambda x: x.index)[:8]:
        f = j.done / j.total if j.total else None
        icon = "⬇" if j.phase == "download" else "⬆"
        pct = f" {f*100:5.1f}%" if f is not None else "     ?"
        spd_txt = f" {fmt_bytes(j.speed)}/s" if j.speed else ""
        label = j.name[:28] + (f" ({j.note})" if j.note else "")
        lines.append(f"{icon} {bar(f)}{pct} {fmt_bytes(j.done)}/{fmt_bytes(j.total) if j.total else '?'}{spd_txt} {label}")
    if len(active) > 8:
        lines.append(f"… and {len(active) - 8} more active")
    return "\n".join(head + lines)


async def progress_updater(tracker) -> None:
    last_text = None
    while not tracker.finished:
        await asyncio.sleep(STATUS_EVERY)
        done_now = sum(j.done for j in tracker.jobs if j.status in ("downloading", "uploading", "done"))
        tracker.overall_speed = speed_of(tracker.samples, done_now)
        for j in tracker.jobs:
            if j.status == "uploading":
                j.speed = speed_of(j.samples, j.done)
            elif j.status != "downloading":
                j.speed = None
        text = render_status(tracker)
        if text != last_text:
            try:
                await tracker.message.edit(text)
                last_text = text
            except MessageNotModifiedError:
                pass
            except FloodWaitError as e:
                await asyncio.sleep(e.seconds + 1)
            except RPCError as e:
                log.warning("Status edit failed: %s", e)


# ----------------------------------------------------------------------
# Batch Runner & Simultaneous Pipeline
# ----------------------------------------------------------------------
class Tracker:
    def __init__(self, label, message, upload_clients):
        self.jobs = []
        self.label = label
        self.message = message
        self.upload_clients = upload_clients
        self.dl_sem = asyncio.Semaphore(MAX_DOWNLOADS)
        self.up_sem = asyncio.Semaphore(MAX_UPLOADS)
        self.batch_dir = None
        self.session = None
        self.cancelled = False
        self.finished = False
        self.t0 = time.monotonic()
        self.samples = deque()
        self.overall_speed = None
        self.outstanding = 0
        self.tasks = set()
        self.pending = deque()
        self.admitted = {}
        self.escape_job = None

    def enqueue(self, jobs):
        for j in jobs:
            j.path = self.batch_dir / (j.name if j.ext else (j.name + ".tmp"))
            self.jobs.append(j)
            if j.status == "queued":
                self.pending.append(j)

    def start_jobs(self, jobs, session, chat_id):
        for j in jobs:
            self.outstanding += 1
            t = asyncio.create_task(self._run_job(j, session, chat_id))
            self.tasks.add(t)
            t.add_done_callback(self.tasks.discard)

    async def _run_job(self, job, session, chat_id):
        try:
            await process_job(job, session, chat_id, self)
        finally:
            self.outstanding -= 1


def apply_skip_memory(jobs) -> int:
    n = 0
    if SKIP_UPLOADED:
        for j in jobs:
            if j.status != "failed" and j.name in HISTORY:
                j.status = "skipped"
                n += 1
    return n


async def add_to_running_batch(event, jobs, source_label) -> bool:
    b = current_batch
    if not b or b.finished or b.cancelled:
        return False
    n_skipped = apply_skip_memory(jobs)
    n_add = sum(1 for j in jobs if j.status == "queued")
    b.enqueue(jobs)
    await event.respond(
        f"➕ Added {n_add} item(s) from {source_label} to running queue"
        + (f" • ⏭ {n_skipped} already uploaded" if n_skipped else "")
        + f"\nQueue total: {len(b.jobs)}"
    )
    return True


async def queue_feeder(tracker, chat_id):
    """Feed downloads & uploads continuously (WAVE_SIZE=0) or in waves (WAVE_SIZE>0)."""
    while True:
        if tracker.cancelled:
            for j in tracker.pending:
                j.status = "cancelled"
            tracker.pending.clear()
            return

        if WAVE_SIZE > 0:
            if not tracker.pending:
                if tracker.outstanding == 0:
                    return
                await asyncio.sleep(0.5)
                continue
            n = min(WAVE_SIZE, len(tracker.pending))
            wave = [tracker.pending.popleft() for _ in range(n)]
            tracker.start_jobs(wave, tracker.session, chat_id)
            await asyncio.gather(*list(tracker.tasks), return_exceptions=True)
            retry = []
            for j in wave:
                if j.status == "failed" and j.url and j.requeues < FAILED_REQUEUE:
                    j.requeues += 1
                    j.status = "queued"
                    j.error = ""
                    j.done = j.total = j.downloaded = 0
                    retry.append(j)
            if retry:
                tracker.pending.extendleft(reversed(retry))
        else:
            # Continuous simultaneous download + upload mode (bounded task pool)
            max_inflight = max(MAX_DOWNLOADS + MAX_UPLOADS, 2)
            slots = max(0, max_inflight - tracker.outstanding)
            if tracker.pending and slots > 0:
                batch = [tracker.pending.popleft() for _ in range(min(slots, len(tracker.pending)))]
                tracker.start_jobs(batch, tracker.session, chat_id)

            if tracker.tasks:
                await asyncio.wait(
                    list(tracker.tasks), timeout=0.5, return_when=asyncio.FIRST_COMPLETED
                )
                # Requeue failed jobs eligible for retry
                for j in list(tracker.jobs):
                    if j.status == "failed" and j.url and j.requeues < FAILED_REQUEUE:
                        j.requeues += 1
                        j.status = "queued"
                        j.error = ""
                        j.done = j.total = j.downloaded = 0
                        tracker.pending.append(j)
            elif not tracker.pending and tracker.outstanding == 0:
                return
            else:
                await asyncio.sleep(0.3)


async def run_batch(client, chat_id, jobs, label, upload_clients=None) -> None:
    global current_batch
    if not jobs:
        await client.send_message(chat_id, "❌ No valid links found.")
        return

    n_skipped = apply_skip_memory(jobs)
    upload_clients = upload_clients or [client]
    n_queued = sum(1 for j in jobs if j.status == "queued")
    status = await client.send_message(
        chat_id,
        f"🚀 Queued {n_queued} item(s) from {label}"
        + (f" • ⏭ {n_skipped} skipped (already uploaded)" if n_skipped else "")
        + f"\n⚙ {MAX_DOWNLOADS}⬇ (yt-dlp/aria2c) • {MAX_UPLOADS}⬆ ({len(upload_clients)}tx×{UPLOAD_WORKERS}w)"
        + f"\n🔗 Referer: {CFG.referer or 'None'} • Max res: {CFG.default_resolution}p"
        + f"\n💾 Colab Disk Guard: auto-deletes raw data after upload (keeps {MIN_FREE_GB:.0f} GB free)",
    )

    tracker = Tracker(label, status, upload_clients)
    try:
        tracker.target_peer = await client.get_input_entity(chat_id)
    except Exception:
        tracker.target_peer = chat_id
    current_batch = tracker
    rl.global_until = 0.0
    rl.global_count = 0
    rl.recent.clear()

    tracker.batch_dir = Path(tempfile.mkdtemp(prefix=f"{BOT_NAME}_batch_"))
    timeout = aiohttp.ClientTimeout(total=None, connect=30, sock_read=120)
    tracker.session = aiohttp.ClientSession(timeout=timeout, connector=aiohttp.TCPConnector(limit=0))

    updater = None
    feeder = None
    try:
        updater = asyncio.create_task(progress_updater(tracker))
        tracker.enqueue(jobs)
        feeder = asyncio.create_task(queue_feeder(tracker, chat_id))
        while tracker.outstanding > 0 or tracker.tasks or tracker.pending or not feeder.done():
            await asyncio.sleep(0.5)
    finally:
        tracker.finished = True
        if current_batch is tracker:
            current_batch = None
        for tsk in (updater, feeder):
            if tsk:
                tsk.cancel()
                try:
                    await tsk
                except (asyncio.CancelledError, Exception):
                    pass
        await tracker.session.close()
        shutil.rmtree(tracker.batch_dir, ignore_errors=True)

    all_jobs = tracker.jobs
    n_done = [j for j in all_jobs if j.status == "done"]
    n_fail = [j for j in all_jobs if j.status == "failed"]
    n_canc = [j for j in all_jobs if j.status == "cancelled"]
    n_skip = [j for j in all_jobs if j.status == "skipped"]
    freed = sum(j.downloaded for j in all_jobs)
    n_swept = sweep_temp_dirs()

    summary = (
        f"{'✅' if not n_fail and not n_canc else '⚠️'} Batch finished: {label}\n"
        f"✅ {len(n_done)} uploaded • ⏭ {len(n_skip)} skipped • ❌ {len(n_fail)} failed • ⛔ {len(n_canc)} cancelled\n"
        f"⏱ Time: {fmt_eta(time.monotonic() - tracker.t0)}\n"
        f"🧹 Cleaned ~{fmt_bytes(freed)} raw data"
        + (f" + {n_swept} temp item(s)" if n_swept else "")
        + f"\n💾 Disk now: {disk_info()}"
    )
    if n_fail:
        summary += "\n\nFailures:"
        for j in n_fail[:15]:
            summary += f"\n❌ {j.name[:55]} — {j.error[:150]}"

    try:
        await tracker.message.edit(render_status(tracker) + "\n\n✔ Finished")
    except (RPCError, MessageNotModifiedError, FloodWaitError):
        pass
    await client.send_message(chat_id, summary)


# ----------------------------------------------------------------------
# Telegram Command & Message Handlers
# ----------------------------------------------------------------------
HELP_TEXT = (
    f"👋 {BOT_NAME} ({BOT_HANDLE}) — Multi-Type URL & Streaming Video Bot\n\n"
    "📄 Send a .txt / .csv file OR paste links directly:\n"
    "  • name,link,format,resolution   (e.g. Cardio,https://vimeo.com/...,mp4,720)\n"
    "  • name,link,extension           (e.g. nephropathy,https://...,pdf)\n"
    "  • name,link,resolution          (e.g. Cardio,https://...,480)\n"
    "  • name,link                     (default: auto ext / <=1080p mp4)\n"
    "  • index.csv format              (Topicid,Name,videoid,link)\n\n"
    "⚡ Features:\n"
    "  • Streaming videos via yt-dlp (<=1080p mp4) + Direct files via aria2c\n"
    "  • Auto-uploads as Video / Photo / Audio / PDF-Document by extension\n"
    "  • Files >2000 MB auto-split into lowest required parts (_part1, _part2...)\n"
    "  • Auto-deletes all raw data after upload (Colab 70 GB safe)\n\n"
    "🔧 Commands:\n"
    "  /status — show referer, headers, cookies, disk & speed settings\n"
    "  /referer <url|clear> — set or clear Referer header\n"
    "  /header <Key: Value|clear> — add or clear custom HTTP header\n"
    "  /cookies <cookie_str|clear> — set cookie string (or upload cookies.txt)\n"
    "  /res <480|720|1080> — change default max video resolution\n"
    "  /cancel — stop running batch\n"
    "  /clean — sweep leftover temp files + show disk\n"
    "  /forget — reset uploaded-files memory"
)


def document_name(doc) -> str:
    try:
        for attr in (doc.attributes if doc else None) or []:
            name = getattr(attr, "file_name", None)
            if name:
                return name
    except Exception:
        pass
    return ""


async def cmd_start(event):
    if event.sender_id != OWNER_ID:
        await event.respond("⛔ Unauthorized.")
        return
    await event.respond(HELP_TEXT)


async def cmd_status(event):
    if event.sender_id != OWNER_ID:
        return
    await event.respond(CFG.summary_text())


async def cmd_referer(event):
    if event.sender_id != OWNER_ID:
        return
    arg = (event.message.text or "").split(None, 1)
    if len(arg) < 2:
        await event.respond(f"Current Referer: {CFG.referer or 'None'}\nUsage: /referer https://medsparkle-drhalim.com/  (or /referer clear)")
        return
    CFG.set_referer(arg[1])
    await event.respond(f"✅ Referer updated: {CFG.referer or 'None'}\nOrigin: {CFG.origin or 'None'}")


async def cmd_header(event):
    if event.sender_id != OWNER_ID:
        return
    parts = (event.message.text or "").split(None, 1)
    if len(parts) < 2:
        hdrs = "\n".join(f"  {k}: {v}" for k, v in CFG.extra_headers.items()) or "  (none)"
        await event.respond(f"Custom Headers:\n{hdrs}\nUsage: /header Key: Value  (or /header clear)")
        return
    arg = parts[1].strip()
    if arg.lower() in ("clear", "reset", "none"):
        CFG.extra_headers.clear()
        CFG.save()
        await event.respond("🧹 Cleared all custom headers.")
        return
    if ":" not in arg:
        await event.respond("❌ Format must be: /header Key: Value")
        return
    k, v = arg.split(":", 1)
    CFG.extra_headers[k.strip()] = v.strip()
    CFG.save()
    await event.respond(f"✅ Added header `{k.strip()}`.")


async def cmd_cookies(event):
    if event.sender_id != OWNER_ID:
        return
    parts = (event.message.text or "").split(None, 1)
    if len(parts) < 2:
        await event.respond(
            f"Cookies file: {'Present' if CFG.has_cookies_file() else 'None'}\n"
            f"Cookie string: {'Set' if CFG.cookie_string else 'None'}\n"
            "Send a Netscape cookies.txt file directly, or:\n"
            "  /cookies name1=val1; name2=val2\n"
            "  /cookies clear"
        )
        return
    arg = parts[1].strip()
    if arg.lower() in ("clear", "reset", "none"):
        CFG.cookie_string = ""
        CFG.save()
        COOKIES_FILE.unlink(missing_ok=True)
        await event.respond("🧹 Cleared cookies.")
        return
    CFG.cookie_string = arg
    CFG.save()
    await event.respond("✅ Saved inline Cookie header string.")


async def cmd_res(event):
    if event.sender_id != OWNER_ID:
        return
    parts = (event.message.text or "").split(None, 1)
    if len(parts) < 2:
        await event.respond(f"Default max resolution: {CFG.default_resolution}p\nUsage: /res 1080 (or 720 / 480)")
        return
    m = RES_TOKEN.fullmatch(parts[1].strip())
    if not m:
        await event.respond("❌ Invalid resolution. Use e.g. /res 1080 or /res 720")
        return
    CFG.default_resolution = int(m.group(1))
    CFG.save()
    await event.respond(f"✅ Default max resolution set to {CFG.default_resolution}p")


async def cmd_cancel(event):
    if event.sender_id != OWNER_ID:
        return
    if current_batch and not current_batch.finished:
        current_batch.cancelled = True
        await event.respond("⛔ Cancelling current batch…")
    else:
        await event.respond("No batch is running.")


async def cmd_clean(event):
    if event.sender_id != OWNER_ID:
        return
    active = current_batch.batch_dir if (current_batch and not current_batch.finished) else None
    n = sweep_temp_dirs(skip=active)
    await event.respond(f"🧹 Swept {n} leftover temp item(s).\n💾 Disk: {disk_info()}")


async def cmd_forget(event):
    if event.sender_id != OWNER_ID:
        return
    n = len(HISTORY)
    HISTORY.clear()
    save_history()
    await event.respond(f"🧠 Cleared {n} remembered upload(s).")


async def on_document(event):
    doc = event.message.document
    if not doc:
        return
    log.info("📩 Incoming document from sender_id=%s chat_id=%s file=%r size=%s",
             event.sender_id, event.chat_id, document_name(doc), getattr(doc, "size", None))
    if event.sender_id != OWNER_ID:
        log.warning("⛔ Rejected document from sender_id=%s (expected OWNER_ID=%s)", event.sender_id, OWNER_ID)
        await event.respond(f"⛔ Unauthorized (your Telegram ID: {event.sender_id}).")
        return
    fname = document_name(doc).lower()
    mime = (doc.mime_type or "").lower()
    is_list = fname.endswith((".txt", ".text", ".list", ".csv")) or mime.startswith("text/") or (doc.size and doc.size < 2 * 1024 * 1024)
    if not is_list:
        await event.respond("Please send a .txt or .csv link list (or cookies.txt).")
        return
    if current_batch and not current_batch.finished and current_batch.cancelled:
        await event.respond("⏳ Previous batch is cancelling — wait a moment.")
        return

    tmpdir = tempfile.mkdtemp(prefix="tglist_")
    try:
        path = await event.client.download_media(event.message, file=tmpdir + os.sep)
        if not path:
            await event.respond("❌ Failed to download file from Telegram.")
            return
        raw = Path(path).read_bytes()
        text = decode_txt(raw)

        # Detect Netscape cookies.txt upload
        if "cookie" in fname or "# netscape http cookie file" in text[:300].lower():
            COOKIES_FILE.write_bytes(raw)
            await event.respond(f"🍪 Saved cookies file ({COOKIES_FILE.name}, {len(raw)} bytes).")
            return

        jobs = parse_jobs(text, Path(tempfile.gettempdir()) / "placeholder")
        valid = [j for j in jobs if j.status != "failed"]
        if not valid:
            await event.respond(
                "❌ No valid links found.\nExpected formats:\n"
                "  name,https://...\n"
                "  name,https://...,pdf\n"
                "  name,https://...,mp4,720\n"
                "  Topicid,Name,videoid,link (CSV)"
            )
            return

        if current_batch and not current_batch.finished and not current_batch.cancelled:
            await add_to_running_batch(event, jobs, document_name(doc) or "list.txt")
            return

        await run_batch(
            event.client, event.chat_id, jobs,
            document_name(doc) or "list.txt",
            upload_clients=getattr(event.client, "_upload_clients", None),
        )
    except Exception as e:
        log.exception("on_document batch error")
        await event.respond(f"❌ Batch error: {type(e).__name__}: {e}")
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


async def on_text(event):
    if event.message.document:
        return
    txt = (event.message.text or "").strip()
    if not txt or txt.startswith("/"):
        return
    log.info("📩 Incoming text from sender_id=%s chat_id=%s text=%r",
             event.sender_id, event.chat_id, txt[:80])
    if event.sender_id != OWNER_ID:
        log.warning("⛔ Rejected text from sender_id=%s (expected OWNER_ID=%s)", event.sender_id, OWNER_ID)
        await event.respond(f"⛔ Unauthorized (your Telegram ID: {event.sender_id}).")
        return
    if not looks_like_link_list(txt):
        await event.respond(HELP_TEXT)
        return
    if current_batch and not current_batch.finished and current_batch.cancelled:
        await event.respond("⏳ Previous batch is cancelling — wait a moment.")
        return

    try:
        jobs = parse_jobs(txt, Path(tempfile.gettempdir()) / "placeholder")
        if current_batch and not current_batch.finished:
            await add_to_running_batch(event, jobs, "direct-message")
            return
        await run_batch(
            event.client, event.chat_id, jobs, "direct-link",
            upload_clients=getattr(event.client, "_upload_clients", None),
        )
    except Exception as e:
        log.exception("on_text batch error")
        await event.respond(f"❌ Batch error: {type(e).__name__}: {e}")


# ----------------------------------------------------------------------
# Entry Point
# ----------------------------------------------------------------------
def _session_dir() -> Path:
    # Prefer /content/tg_bot_sessions on Colab (persists across script reruns in same runtime),
    # otherwise /tmp/tg_bot_sessions. Never touch Google Drive FUSE mounts for SQLite.
    colab_dir = Path("/content")
    base = colab_dir if colab_dir.is_dir() and os.access(colab_dir, os.W_OK) else Path(tempfile.gettempdir())
    d = base / "tg_bot_sessions"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _make_session(session_name: str):
    """Store SQLite sessions in a dedicated writable dir (never in Google Drive or temp-sweep paths),
    falling back to MemorySession if SQLite cannot write."""
    try:
        sess_path = _session_dir() / session_name
        s = SQLiteSession(str(sess_path))
        # Verify the SQLite file is actually writable before handing it to Telethon
        c = s._cursor()
        c.execute("CREATE TABLE IF NOT EXISTS _rw_check (x INTEGER)")
        c.execute("DROP TABLE _rw_check")
        s.save()
        return s
    except Exception as e:
        log.warning("SQLite session %s not writable (%s) — using MemorySession", session_name, e)
        return MemorySession()


def _apply_bot_index_from_cli():
    global BOT_NAME, BOT_HANDLE, BOT_TOKEN, HISTORY_FILE, COOKIES_FILE, HTTP_CONFIG_FILE
    idx = None
    if "--bot" in sys.argv:
        pos = sys.argv.index("--bot")
        if pos + 1 < len(sys.argv) and sys.argv[pos + 1].isdigit():
            idx = int(sys.argv[pos + 1])
    elif os.environ.get("BOT_INDEX", "").isdigit():
        idx = int(os.environ["BOT_INDEX"])

    if idx is None:
        return
    token_paths = [Path("token.txt"), Path("/content/token.txt"), Path("/home/user/uploads/token.txt")]
    tp = next((p for p in token_paths if p.exists()), None)
    if not tp:
        if idx != 1:
            log.warning("--bot %d requested, but token.txt not found in current folder or /content/", idx)
        return
    entries = []
    for raw in tp.read_text(encoding="utf-8", errors="replace").splitlines():
        line = raw.strip()
        if line and "," in line:
            h, t = [x.strip() for x in line.split(",", 1)]
            if ":" in t:
                entries.append((h, t))
    if 1 <= idx <= len(entries):
        BOT_HANDLE, BOT_TOKEN = entries[idx - 1]
        BOT_NAME = f"upbot{idx}"
        HISTORY_FILE = Path(f"uploaded_history_{BOT_NAME}.json")
        COOKIES_FILE = Path(f"{BOT_NAME}_cookies.txt")
        HTTP_CONFIG_FILE = Path(f"{BOT_NAME}_http_config.json")
        CFG.load()
        log.info("Selected Bot #%d: %s", idx, BOT_HANDLE)
    else:
        log.warning("--bot %d out of range (token.txt has %d bots)", idx, len(entries))


async def _start_bot_client(session, bot_id: str):
    """Connect & authorize TelegramClient with clear FloodWait and corrupted-session recovery."""
    for attempt in range(1, 3):
        client = TelegramClient(
            session,
            API_ID,
            API_HASH,
            flood_sleep_threshold=300,
        )
        try:
            await client.start(bot_token=BOT_TOKEN)
            client.parse_mode = None
            return client
        except FloodWaitError as e:
            try:
                await client.disconnect()
            except Exception:
                pass
            wait_s = int(getattr(e, "seconds", 0) or 0)
            if wait_s <= 300:
                log.warning("⏳ Telegram FloodWait on login (%ds) — sleeping and retrying...", wait_s)
                await asyncio.sleep(wait_s + 2)
                continue
            raise RuntimeError(
                f"Telegram rate-limited bot login (FloodWait {fmt_eta(wait_s)} / {wait_s}s) for {BOT_HANDLE}. "
                f"Wait {fmt_eta(wait_s)} before logging into this bot again, OR upload token.txt and run "
                f"with another bot (e.g. !python demobot1.py --bot 2)."
            ) from e
        except Exception as e:
            try:
                await client.disconnect()
            except Exception:
                pass
            err_txt = f"{type(e).__name__}: {e}"
            if attempt == 1 and ("sqlite" in err_txt.lower() or "readonly" in err_txt.lower() or "authkey" in err_txt.lower()):
                log.warning("Session %s failed (%s) — resetting to MemorySession and retrying...",
                            f"{BOT_NAME}_{bot_id}", err_txt)
                try:
                    (_session_dir() / f"{BOT_NAME}_{bot_id}.session").unlink(missing_ok=True)
                except OSError:
                    pass
                session = MemorySession()
                continue
            raise


async def main_async():
    _apply_bot_index_from_cli()
    try:
        import cryptg  # noqa: F401
        crypto = "⚡ cryptg ACTIVE"
    except ImportError:
        crypto = "⚠ cryptg NOT installed (slower crypto)"

    n_hist = load_history()
    n_swept = sweep_temp_dirs()

    bot_id = BOT_TOKEN.split(":")[0]
    try:
        client = await _start_bot_client(_make_session(f"{BOT_NAME}_{bot_id}"), bot_id)
    except Exception as e:
        log.error("❌ Cannot start %s (%s): %s", BOT_NAME, BOT_HANDLE, e)
        return

    me = await client.get_me()
    upload_clients = [client]
    for i in range(2, UPLOAD_SESSIONS + 1):
        try:
            # receive_updates=False is critical so Telegram delivers all user messages to `client` only!
            c = TelegramClient(
                _make_session(f"{BOT_NAME}_{bot_id}_tx{i}"),
                API_ID,
                API_HASH,
                flood_sleep_threshold=300,
                receive_updates=False,
            )
            await c.start(bot_token=BOT_TOKEN)
            c.parse_mode = None
            upload_clients.append(c)
        except Exception as e:
            log.warning("Extra session %d skipped (%s)", i, e)
            break
    client._upload_clients = upload_clients

    log.info("✔ Logged in as %s (@%s) [DC %s] • %s", me.first_name, me.username, client.session.dc_id, crypto)
    log.info("  Tools: aria2c=%s • ffmpeg=%s • Referer=%s • MaxRes=%dp",
             bool(shutil.which("aria2c")), bool(shutil.which("ffmpeg")), CFG.referer or "None", CFG.default_resolution)
    log.info("  Memory: %d uploaded remembered • Swept %d temp item(s) • Disk: %s",
             n_hist, n_swept, disk_info())
    log.info("  Owner ID: %d • Send /start to @%s in Telegram.", OWNER_ID, me.username)

    client.add_event_handler(cmd_start, events.NewMessage(pattern=r"^/(start|help)(@\w+)?$"))
    client.add_event_handler(cmd_status, events.NewMessage(pattern=r"^/(status|settings)(@\w+)?$"))
    client.add_event_handler(cmd_referer, events.NewMessage(pattern=r"^/referer(@\w+)?(\s|$)"))
    client.add_event_handler(cmd_header, events.NewMessage(pattern=r"^/header(@\w+)?(\s|$)"))
    client.add_event_handler(cmd_cookies, events.NewMessage(pattern=r"^/cookies(@\w+)?(\s|$)"))
    client.add_event_handler(cmd_res, events.NewMessage(pattern=r"^/res(@\w+)?(\s|$)"))
    client.add_event_handler(cmd_cancel, events.NewMessage(pattern=r"^/cancel(@\w+)?$"))
    client.add_event_handler(cmd_clean, events.NewMessage(pattern=r"^/(clean|storage)(@\w+)?$"))
    client.add_event_handler(cmd_forget, events.NewMessage(pattern=r"^/(forget|reset)(@\w+)?$"))
    client.add_event_handler(on_document, events.NewMessage(incoming=True))
    client.add_event_handler(on_text, events.NewMessage(incoming=True))

    try:
        await client.run_until_disconnected()
    finally:
        for c in upload_clients[1:]:
            try:
                await c.disconnect()
            except Exception:
                pass


async def _run_test_link(line: str, seconds: int = 8):
    """Test downloading a few segments of a single link locally and print file info."""
    global RATE_LIMIT_RETRIES
    RATE_LIMIT_RETRIES = 0
    tmp = Path(tempfile.mkdtemp(prefix=f"{BOT_NAME}_test_"))
    class _DummyTracker:
        cancelled = False
        batch_dir = tmp
        admitted = {}
        escape_job = None
    try:
        jobs = parse_jobs(line, tmp)
        job = jobs[0]
        if job.status == "failed":
            print(f"❌ Invalid line: {job.error}")
            return 1
        print(f"Testing download ({seconds}s segment cap for streams): {job.name} <- {job.url}")
        async with aiohttp.ClientSession() as session:
            await download_job(job, session, _DummyTracker(), test_seconds=seconds)
        sz = job.path.stat().st_size if job.path.exists() else 0
        info = await ffprobe_info(job.path) if job.ext in VIDEO_EXTS or job.ext in AUDIO_EXTS else {}
        print(f"✅ Downloaded: {job.path.name} ({fmt_bytes(sz)}) info={info}")
        return 0
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def main():
    if "--parse" in sys.argv:
        p = Path(sys.argv[sys.argv.index("--parse") + 1])
        jobs = parse_jobs(decode_txt(p.read_bytes()), Path("/tmp/parse-test"))
        print(f"Parsed {len(jobs)} job(s):")
        for j in jobs[:30]:
            print(f"  [{j.index}] name={j.name!r} ext={j.ext!r} res={j.resolution} url={j.url[:75]}"
                  + (f" ERR={j.error}" if j.error else ""))
        if len(jobs) > 30:
            print(f"  ... ({len(jobs) - 30} more)")
        return

    if "--test-link" in sys.argv:
        line = sys.argv[sys.argv.index("--test-link") + 1]
        secs = 8
        if "--seconds" in sys.argv:
            secs = int(sys.argv[sys.argv.index("--seconds") + 1])
        rc = asyncio.run(_run_test_link(line, seconds=secs))
        sys.exit(rc)

    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop and loop.is_running():
        import nest_asyncio
        nest_asyncio.apply()
        loop.run_until_complete(main_async())
    else:
        asyncio.run(main_async())


if __name__ == "__main__":
    main()
