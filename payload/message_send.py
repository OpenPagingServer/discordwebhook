import io
import json
import mimetypes
import os
import re
import subprocess
import threading
import wave
from datetime import datetime
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import pymysql
import requests
from dotenv import load_dotenv

try:
    from active_broadcast_store import fetch_active_broadcast
except Exception:
    def fetch_active_broadcast(_msg_id):
        return None

try:
    from broadcasts import legacy_type
except Exception:
    def legacy_type(value):
        token = str(value or "").strip().lower()
        if token in {"audio", "bell", "voice"}:
            return "audio"
        if token in {"liveaudio", "page"}:
            return "liveaudio"
        if token in {"liveaudio+text", "page+text"}:
            return "liveaudio+text"
        if "audio" in token and "text" in token:
            return "text+audio"
        return "text"

try:
    from endpoints import MODULE_LOG_DIR, connect_endpoint_ipc
except Exception:
    MODULE_LOG_DIR = Path(os.getenv("OPS_ENDPOINT_MODULE_LOG_DIR", "/var/log/openpagingserver/endpointmodules"))
    connect_endpoint_ipc = None

BASE_DIR = Path(__file__).resolve().parent
ENV_PATH = BASE_DIR.parent.parent / ".env"
load_dotenv(ENV_PATH)

DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_NAME = os.getenv("DB_NAME")
DEBUG = os.getenv("DEBUG", "").strip().lower() == "true"

MODULE_NAME = "discordwebhook"
DISPLAY_NAME = "DiscordWebhook"
ENDPOINT_TABLE = "endpoints-output-discord"
MODULE_SETTINGS_TABLE = "endpoints-modulesettings-discord"
LOG_FILE = MODULE_LOG_DIR / MODULE_NAME / "module.log"
REQUEST_TIMEOUT_SECONDS = 10
SAMPLE_RATE = 8000
FRAME_SIZE = 160
ASSET_PATH = os.getenv("ASSET_PATH", "/var/lib/openpagingserver/assets/")
OPS_PROJECT_ROOT_ENV = str(os.getenv("OPS_PROJECT_ROOT") or "").strip()
OPS_PROJECT_ROOT = Path(OPS_PROJECT_ROOT_ENV).resolve() if OPS_PROJECT_ROOT_ENV else None
FALLBACK_ASSET_DIRS = [
    OPS_PROJECT_ROOT / "assets" if OPS_PROJECT_ROOT is not None else None,
    OPS_PROJECT_ROOT / "sip" / "audio" if OPS_PROJECT_ROOT is not None else None,
    Path.cwd() / "assets",
    BASE_DIR / "assets",
    BASE_DIR.parent / "assets",
]
DEFAULT_SETTINGS = {
    "username": "",
    "avatar-url": "",
    "tts": "0",
    "use-embeds": "1",
}

column_cache = {}
column_cache_lock = threading.Lock()
active_streams = {}
active_streams_lock = threading.Lock()

ULAW_DECODE_TABLE = []
ULAW_PCM_LE_TABLE = []
for _ulaw in range(256):
    _value = ~_ulaw & 0xFF
    _sign = _value & 0x80
    _exponent = (_value >> 4) & 0x07
    _mantissa = _value & 0x0F
    _sample = ((_mantissa << 3) + 0x84) << _exponent
    _sample -= 0x84
    _sample = -_sample if _sign else _sample
    ULAW_DECODE_TABLE.append(_sample)
    ULAW_PCM_LE_TABLE.append(int(_sample).to_bytes(2, "little", signed=True))


def debug_log(message):
    if not DEBUG:
        return
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(LOG_FILE, "a", encoding="utf-8") as handle:
        handle.write(f"[{timestamp}] {message}\n")


def db():
    return pymysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME,
        cursorclass=pymysql.cursors.DictCursor,
    )


def truthy(value):
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def normalize_message_type(value):
    token = str(legacy_type(value) or "").strip().lower()
    if token == "audio":
        return "audio"
    if token in {"text+audio", "textaudio", "audio+text"}:
        return "text+audio"
    if token == "liveaudio":
        return "liveaudio"
    if token in {"liveaudio+text", "text+liveaudio", "page+text"}:
        return "liveaudio+text"
    return "text"


def message_has_audio(message_type):
    return normalize_message_type(message_type) in {"audio", "text+audio", "liveaudio", "liveaudio+text"}


def table_columns(table_name):
    with column_cache_lock:
        cached = column_cache.get(table_name)
    if cached is not None:
        return cached
    conn = db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COLUMN_NAME FROM information_schema.COLUMNS "
                "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s",
                (DB_NAME, table_name),
            )
            columns = {row["COLUMN_NAME"] for row in cur.fetchall()}
    finally:
        conn.close()
    with column_cache_lock:
        column_cache[table_name] = columns
    return columns


def ensure_database_schema():
    conn = db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"CREATE TABLE IF NOT EXISTS `{ENDPOINT_TABLE}` ("
                "`id` INT NOT NULL AUTO_INCREMENT, "
                "`name` VARCHAR(255) NOT NULL DEFAULT '', "
                "`webhook_url` VARCHAR(2048) NOT NULL DEFAULT '', "
                "`status` VARCHAR(32) NOT NULL DEFAULT 'Unchecked', "
                "`mention_text` VARCHAR(255) NOT NULL DEFAULT '', "
                "`username` VARCHAR(80) NOT NULL DEFAULT '', "
                "`avatar_url` VARCHAR(2048) NOT NULL DEFAULT '', "
                "`exclude_bells` TINYINT(1) NOT NULL DEFAULT 1, "
                "PRIMARY KEY (`id`), KEY `status_idx` (`status`)"
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci"
            )
            cur.execute(
                f"CREATE TABLE IF NOT EXISTS `{MODULE_SETTINGS_TABLE}` ("
                "`parameter` VARCHAR(128) NOT NULL, `value` TEXT, PRIMARY KEY (`parameter`)"
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci"
            )
            cur.execute(f"SHOW COLUMNS FROM `{ENDPOINT_TABLE}`")
            endpoint_columns = {row["Field"] for row in cur.fetchall()}
            if "exclude_bells" not in endpoint_columns:
                cur.execute(
                    f"ALTER TABLE `{ENDPOINT_TABLE}` "
                    "ADD COLUMN `exclude_bells` TINYINT(1) NOT NULL DEFAULT 1"
                )
            for key, value in DEFAULT_SETTINGS.items():
                cur.execute(
                    f"INSERT INTO `{MODULE_SETTINGS_TABLE}` (`parameter`, `value`) VALUES (%s, %s) "
                    "ON DUPLICATE KEY UPDATE `parameter` = `parameter`",
                    (key, value),
                )
        conn.commit()
    finally:
        conn.close()


def parse_targets(targets):
    target_info = {
        "all": False,
        "endpoint_ids": [],
    }
    for target in targets:
        token = str(target or "").strip()
        if not token:
            continue
        if token.lower() == "all":
            target_info["all"] = True
            continue
        if token not in target_info["endpoint_ids"]:
            target_info["endpoint_ids"].append(token)
    return target_info


def fetch_configured_endpoints(targets=None):
    ensure_database_schema()
    conn = db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT `id`, `name`, `webhook_url`, `status`, `mention_text`, `username`, `avatar_url`, `exclude_bells` "
                f"FROM `{ENDPOINT_TABLE}` WHERE `webhook_url` IS NOT NULL AND `webhook_url` <> '' "
                "ORDER BY `name` ASC, `id` ASC"
            )
            rows = cur.fetchall()
    finally:
        conn.close()
    if not targets:
        return rows
    target_info = parse_targets(targets)
    if target_info["all"]:
        return rows
    allowed = {str(item) for item in target_info["endpoint_ids"]}
    return [row for row in rows if str(row.get("id")) in allowed]


def update_endpoint_status(endpoint_id, status):
    conn = db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE `{ENDPOINT_TABLE}` SET `status`=%s WHERE `id`=%s",
                (status, endpoint_id),
            )
        conn.commit()
    finally:
        conn.close()


def load_settings():
    ensure_database_schema()
    values = dict(DEFAULT_SETTINGS)
    conn = db()
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT `parameter`, `value` FROM `{MODULE_SETTINGS_TABLE}`")
            for row in cur.fetchall():
                key = str(row.get("parameter") or "")
                if key in values:
                    values[key] = "" if row.get("value") is None else str(row.get("value"))
    finally:
        conn.close()
    return values


def load_system_product_name():
    conn = db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT `value` FROM `systemsettings` WHERE `parameter`=%s LIMIT 1",
                ("product_name",),
            )
            row = cur.fetchone()
            if row:
                return str(row.get("value") or "").strip()
    except Exception:
        return ""
    finally:
        conn.close()
    return ""


def merge_missing_message_fields(primary, fallback):
    merged = dict(fallback or {})
    for key, value in (primary or {}).items():
        if value not in (None, ""):
            merged[key] = value
        elif key not in merged:
            merged[key] = value
    return merged


def fetch_message(msg_id):
    message_columns = table_columns("messages")
    broadcast_columns = table_columns("broadcasts")
    message_select = ["name", "shortmessage", "longmessage", "type", "audio", "color", "icon", "expires"]
    broadcast_select = [
        "id",
        "name",
        "shortmessage",
        "longmessage",
        "type",
        "audio",
        "sender",
        "issued",
        "expires",
        "template_id",
        "color",
        "icon",
        "expires_rule",
    ]
    message_select = [column for column in message_select if column in message_columns]
    broadcast_select = [column for column in broadcast_select if column in broadcast_columns]

    conn = db()
    try:
        with conn.cursor() as cur:
            message = fetch_active_broadcast(msg_id)
            history_message = None
            if "id" in broadcast_columns and broadcast_select:
                cur.execute(
                    f"SELECT {', '.join(f'`{column}`' for column in broadcast_select)} "
                    "FROM `broadcasts` WHERE `id`=%s LIMIT 1",
                    (msg_id,),
                )
                history_message = cur.fetchone()
            if message:
                if history_message:
                    message = merge_missing_message_fields(message, history_message)
                if not str(message.get("id") or "").strip():
                    message["id"] = str(msg_id or "").strip()
                message["name"] = message.get("name") or "Broadcast"
                message["type"] = normalize_message_type(message.get("type"))
                return message
            if history_message:
                if not str(history_message.get("id") or "").strip():
                    history_message["id"] = str(msg_id or "").strip()
                history_message["name"] = history_message.get("name") or "Broadcast"
                history_message["type"] = normalize_message_type(history_message.get("type"))
                return history_message
            if message_select:
                cur.execute(
                    f"SELECT {', '.join(f'`{column}`' for column in message_select)} "
                    "FROM `messages` WHERE `messageid`=%s LIMIT 1",
                    (msg_id,),
                )
                message = cur.fetchone()
                if message:
                    message["id"] = str(msg_id or "").strip()
                    message["name"] = message.get("name") or "Broadcast"
                    message["type"] = normalize_message_type(message.get("type"))
                return message
            return None
    finally:
        conn.close()


def fetch_endpoints_and_message(targets, msg_id):
    endpoints = fetch_configured_endpoints(targets)
    message = fetch_message(msg_id)
    debug_log(
        f"fetch_endpoints_and_message targets={targets} "
        f"endpoint_ids={[row.get('id') for row in endpoints]} message_found={bool(message)} "
        f"message_type={'' if not message else message.get('type')!r}"
    )
    return endpoints, message


def is_bell_message(message):
    message = message or {}
    sender = str(message.get("sender") or "").strip().lower()
    template_id = str(message.get("template_id") or "").strip().lower()
    return sender == "belld" or template_id.startswith("bell-")


def eligible_endpoints(endpoints, message):
    bell_message = is_bell_message(message)
    filtered = []
    for endpoint in endpoints or []:
        if bell_message and truthy(endpoint.get("exclude_bells")):
            continue
        filtered.append(endpoint)
    return filtered


def check_webhook(webhook_url):
    url = str(webhook_url or "").strip()
    if not url:
        return "Offline"
    try:
        response = requests.get(url, timeout=REQUEST_TIMEOUT_SECONDS)
        if response.status_code in (200, 204, 429):
            return "Online"
        return "Offline"
    except requests.RequestException:
        return "Offline"


def message_text(shortmessage, longmessage):
    short_text = str(shortmessage or "").strip()
    long_text = str(longmessage or "").strip()
    if not long_text:
        return short_text
    if not short_text:
        return long_text
    if short_text == long_text or long_text.startswith(short_text):
        return long_text
    return f"{short_text}\n\n{long_text}"


def truncate(value, limit):
    text = str(value or "")
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 3)].rstrip() + "..."


def format_message_timestamp(value):
    raw = str(value or "").strip()
    if not raw:
        return ""
    iso_text = raw[:-1] + "+00:00" if raw.endswith("Z") else raw
    try:
        parsed = datetime.fromisoformat(iso_text)
        return parsed.astimezone().strftime("%Y-%m-%d %H:%M:%S")
    except ValueError:
        return raw


def parse_message_datetime(value):
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    raw = str(value).strip()
    if not raw:
        return None
    iso_text = raw[:-1] + "+00:00" if raw.endswith("Z") else raw
    try:
        return datetime.fromisoformat(iso_text)
    except ValueError:
        pass
    for pattern in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(raw, pattern)
        except ValueError:
            continue
    return None


def discord_timestamp(value, style="F"):
    dt = parse_message_datetime(value)
    if dt is None:
        return ""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=datetime.now().astimezone().tzinfo)
    try:
        return f"<t:{int(dt.timestamp())}:{style}>"
    except (OverflowError, OSError, ValueError):
        return ""


def parse_embed_color(value):
    token = str(value or "").strip().lower()
    if not token:
        return None
    if token.startswith("#"):
        token = token[1:]
    if token.startswith("0x"):
        token = token[2:]
    if len(token) == 3:
        token = "".join(ch * 2 for ch in token)
    if len(token) != 6 or any(ch not in "0123456789abcdef" for ch in token):
        return None
    return int(token, 16)


def sanitize_filename(value, fallback):
    token = re.sub(r"[^A-Za-z0-9._-]+", "-", str(value or "").strip()).strip("._-")
    return token or fallback


def local_now():
    return datetime.now().astimezone()


def localize_datetime(value=None):
    dt = parse_message_datetime(value)
    if dt is None:
        return local_now()
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=local_now().tzinfo)
    return dt.astimezone()


def timestamped_attachment_basename(delivery, fallback):
    label = sanitize_filename(
        delivery.get("attachment_label") or delivery.get("title") or delivery.get("id"),
        fallback,
    )
    timestamp = localize_datetime(delivery.get("issued")).strftime("%Y%m%d-%H%M")
    return f"{label}-{timestamp}"


def asset_search_roots():
    roots = [Path(ASSET_PATH)]
    for root in FALLBACK_ASSET_DIRS:
        if root is None:
            continue
        roots.append(Path(root))
    unique = []
    seen = set()
    for root in roots:
        key = str(root)
        if not key or key in seen:
            continue
        seen.add(key)
        unique.append(root)
    return unique


def resolve_asset_file(asset_file):
    candidate = Path(str(asset_file or "").strip())
    if candidate.is_file():
        return candidate
    for root in asset_search_roots():
        try:
            path = (root / candidate.name).resolve()
        except Exception:
            path = root / candidate.name
        if path.is_file():
            return path
        fallback = root / str(asset_file or "").strip()
        if fallback.is_file():
            return fallback
    return None


def resolve_audio_file(audio_file):
    return resolve_asset_file(audio_file)


def ffmpeg_pcm_bytes(file_path):
    try:
        process = subprocess.Popen(
            [
                "ffmpeg",
                "-v",
                "quiet",
                "-i",
                str(file_path),
                "-ar",
                str(SAMPLE_RATE),
                "-ac",
                "1",
                "-f",
                "s16le",
                "pipe:1",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
    except OSError as exc:
        debug_log(f"ffmpeg_start_failed file={file_path} error={exc.__class__.__name__}: {exc}")
        return b""
    stdout, _stderr = process.communicate()
    if process.returncode != 0:
        debug_log(f"ffmpeg_failed file={file_path} returncode={process.returncode}")
        return b""
    return stdout or b""


def silence_pcm_bytes(duration_seconds):
    try:
        duration = max(0.0, float(duration_seconds))
    except (TypeError, ValueError):
        return b""
    sample_count = int(duration * SAMPLE_RATE)
    return b"\x00\x00" * sample_count


def write_pcm_wave_bytes(pcm_bytes):
    buffer = io.BytesIO()
    with wave.open(buffer, "wb") as wav_file:
        wav_file.setnchannels(1)
        wav_file.setsampwidth(2)
        wav_file.setframerate(SAMPLE_RATE)
        wav_file.writeframes(pcm_bytes or b"")
    return buffer.getvalue()


def merge_audio_files_to_wav_bytes(audio_files_str):
    pcm_chunks = []
    for audio_file in str(audio_files_str or "").split(":"):
        token = str(audio_file or "").strip()
        if not token:
            continue
        if token.startswith("%silence(") and token.endswith(")"):
            pcm = silence_pcm_bytes(token[9:-1])
            if pcm:
                pcm_chunks.append(pcm)
            continue
        file_path = resolve_audio_file(token)
        if file_path is None:
            debug_log(f"audio_file_missing token={token!r}")
            continue
        pcm = ffmpeg_pcm_bytes(file_path)
        if pcm:
            pcm_chunks.append(pcm)
    if not pcm_chunks:
        return None
    return write_pcm_wave_bytes(b"".join(pcm_chunks))


def ulaw_bytes_to_wav_bytes(raw_audio):
    if not raw_audio:
        return None
    pcm = bytearray()
    for byte in raw_audio:
        pcm.extend(ULAW_PCM_LE_TABLE[byte])
    return write_pcm_wave_bytes(bytes(pcm))


def live_page_text(metadata):
    data = metadata if isinstance(metadata, dict) else {}
    for key in ("message", "text", "description", "body"):
        value = str(data.get(key) or "").strip()
        if value:
            return value
    return ""


def build_delivery_message(action, message, msg_id, metadata=None):
    product_name = load_system_product_name()
    if action == "prepare_livepage":
        metadata = metadata if isinstance(metadata, dict) else {}
        title = "Page" if truthy(metadata.get("_page_complete")) else "Page (in progress)"
        body = live_page_text(metadata)
        return {
            "id": str(msg_id or ""),
            "author": "",
            "title": title,
            "body": body,
            "type": "liveaudio",
            "sender": str(metadata.get("sender") or metadata.get("caller") or "").strip(),
            "issued": str(metadata.get("issued") or local_now().strftime("%Y-%m-%d %H:%M:%S")).strip(),
            "expires": str(metadata.get("expires") or "").strip(),
            "color": "",
            "audio": "",
            "icon": str(metadata.get("icon") or "").strip(),
            "attachment_label": "Page",
            "product_name": product_name,
        }

    message = message or {}
    msg_type = normalize_message_type(message.get("type"))
    bell_message = is_bell_message(message)
    name = str(message.get("name") or "").strip()
    short_text = str(message.get("shortmessage") or "").strip()
    long_text = str(message.get("longmessage") or "").strip()
    title = "Bell" if bell_message else (short_text or name or "Broadcast")
    author = ""
    if not bell_message and name and name != title:
        author = name
    body = long_text
    if not body:
        fallback_text = message_text(short_text, long_text)
        body = fallback_text if fallback_text != title else ""
    return {
        "id": str(message.get("id") or msg_id or ""),
        "author": author,
        "title": title,
        "body": body,
        "type": msg_type,
        "sender": str(message.get("sender") or "").strip(),
        "issued": str(message.get("issued") or "").strip(),
        "expires": str(message.get("expires") or "").strip(),
        "color": str(message.get("color") or "").strip(),
        "audio": str(message.get("audio") or "").strip(),
        "icon": str(message.get("icon") or "").strip(),
        "attachment_label": "Bell" if bell_message else (name or title or "Broadcast"),
        "product_name": product_name,
    }


def bottom_line_text(delivery):
    parts = []
    sender = str(delivery.get("sender") or "").strip() or "Unknown"
    parts.append(f"Sent by {sender}")
    issued = discord_timestamp(delivery.get("issued"))
    if issued:
        parts.append(f"Issued {issued}")
    expires = discord_timestamp(delivery.get("expires"))
    if expires:
        parts.append(f"Expires {expires}")
    product_name = str(delivery.get("product_name") or "").strip()
    if product_name:
        parts.append(product_name)
    return truncate(" \u2022 ".join(parts), 1024)


def build_message_content(endpoint, settings, delivery):
    use_embeds = truthy(settings.get("use-embeds"))
    mention_text = str(endpoint.get("mention_text") or "").strip()
    if use_embeds:
        return truncate(mention_text, 2000)
    lines = []
    if mention_text:
        lines.append(mention_text)
    author = str(delivery.get("author") or "").strip()
    if author:
        lines.append(author)
    if delivery.get("title"):
        lines.append(f"**{delivery['title']}**")
    if delivery.get("body"):
        lines.append(delivery["body"])
    bottom_line = bottom_line_text(delivery)
    if bottom_line:
        lines.append(bottom_line)
    return truncate("\n\n".join(line for line in lines if line), 2000)


def build_message_embeds(settings, delivery):
    if not truthy(settings.get("use-embeds")):
        return []
    embed = {
        "title": truncate(delivery.get("title") or "Broadcast", 256),
    }
    body = str(delivery.get("body") or "").strip()
    if body:
        embed["description"] = truncate(body, 4096)
    author = str(delivery.get("author") or "").strip()
    if author:
        embed["author"] = {"name": truncate(author, 256)}
    color = parse_embed_color(delivery.get("color"))
    if color is not None:
        embed["color"] = color
    bottom_line = bottom_line_text(delivery)
    if bottom_line:
        embed["fields"] = [{"name": "\u200b", "value": bottom_line, "inline": False}]
    thumbnail_url = str(delivery.get("thumbnail_url") or "").strip()
    if thumbnail_url:
        embed["thumbnail"] = {"url": thumbnail_url}
    return [embed]


def build_webhook_payload(endpoint, settings, delivery):
    payload = {
        "allowed_mentions": {"parse": ["users", "roles", "everyone"]},
    }
    content = build_message_content(endpoint, settings, delivery)
    if content:
        payload["content"] = content
    embeds = build_message_embeds(settings, delivery)
    if embeds:
        payload["embeds"] = embeds
    username = str(endpoint.get("username") or settings.get("username") or "").strip()
    avatar_url = str(endpoint.get("avatar_url") or settings.get("avatar-url") or "").strip()
    if username:
        payload["username"] = truncate(username, 80)
    if avatar_url:
        payload["avatar_url"] = avatar_url
    if truthy(settings.get("tts")):
        payload["tts"] = True
    if not payload.get("content") and not payload.get("embeds"):
        payload["content"] = truncate(str(endpoint.get("mention_text") or "").strip() or "Open Paging Server alert", 2000)
    return payload


def webhook_request_url(webhook_url):
    parsed = urlparse(str(webhook_url or "").strip())
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    query["wait"] = "true"
    return urlunparse(parsed._replace(query=urlencode(query)))


def build_binary_attachment(filename, file_bytes, content_type):
    return {
        "filename": filename,
        "bytes": file_bytes,
        "content_type": content_type,
    }


def build_audio_attachment_from_wav_bytes(delivery, wav_bytes, fallback):
    if not wav_bytes:
        return None
    filename = f"{timestamped_attachment_basename(delivery, fallback)}.wav"
    return build_binary_attachment(filename, wav_bytes, "audio/wav")


def build_audio_attachment_from_message(message, delivery):
    if not message_has_audio(delivery.get("type")):
        return None
    audio_files = str((message or {}).get("audio") or delivery.get("audio") or "").strip()
    if not audio_files:
        return None
    wav_bytes = merge_audio_files_to_wav_bytes(audio_files)
    return build_audio_attachment_from_wav_bytes(delivery, wav_bytes, "broadcast-audio")


def build_icon_attachment(delivery):
    icon = str(delivery.get("icon") or "").strip()
    if not icon:
        return None
    if re.match(r"^https?://", icon, re.IGNORECASE):
        delivery["thumbnail_url"] = icon
        return None
    file_path = resolve_asset_file(icon)
    if file_path is None:
        debug_log(f"icon_file_missing icon={icon!r}")
        return None
    try:
        file_bytes = file_path.read_bytes()
    except OSError as exc:
        debug_log(f"icon_file_read_failed file={file_path} error={exc.__class__.__name__}: {exc}")
        return None
    content_type = mimetypes.guess_type(file_path.name)[0] or "application/octet-stream"
    filename = f"{sanitize_filename(file_path.stem, 'icon')}{file_path.suffix.lower()}"
    return build_binary_attachment(filename, file_bytes, content_type)


def finalize_delivery_attachments(delivery, attachments=None):
    prepared_delivery = dict(delivery or {})
    built_attachments = list(attachments or [])
    icon_attachment = build_icon_attachment(prepared_delivery)
    if icon_attachment is not None:
        built_attachments.append(icon_attachment)
        prepared_delivery["thumbnail_url"] = f"attachment://{icon_attachment['filename']}"
    return prepared_delivery, built_attachments


def prepare_message_delivery(message, delivery):
    attachments = []
    audio_attachment = build_audio_attachment_from_message(message, delivery)
    if audio_attachment is not None:
        attachments.append(audio_attachment)
    return finalize_delivery_attachments(delivery, attachments)


def prepare_livepage_delivery(delivery, wav_bytes=None):
    attachments = []
    audio_attachment = build_audio_attachment_from_wav_bytes(delivery, wav_bytes, "livepage")
    if audio_attachment is not None:
        attachments.append(audio_attachment)
    return finalize_delivery_attachments(delivery, attachments)


def webhook_edit_url(webhook_url, message_id):
    parsed = urlparse(str(webhook_url or "").strip())
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    query["wait"] = "true"
    base_path = parsed.path.rstrip("/")
    edit_path = f"{base_path}/messages/{message_id}"
    return urlunparse(parsed._replace(path=edit_path, query=urlencode(query)))


def send_webhook(endpoint, payload, attachments=None, capture_message_id=False):
    endpoint_id = endpoint.get("id")
    url = webhook_request_url(endpoint.get("webhook_url"))
    prepared_attachments = list(attachments or [])
    try:
        if not prepared_attachments:
            response = requests.post(url, json=payload, timeout=REQUEST_TIMEOUT_SECONDS)
        else:
            multipart_payload = dict(payload or {})
            multipart_payload["attachments"] = [
                {"id": index, "filename": attachment["filename"]}
                for index, attachment in enumerate(prepared_attachments)
            ]
            response = requests.post(
                url,
                data={"payload_json": json.dumps(multipart_payload)},
                files=[
                    (
                        f"files[{index}]",
                        (
                            attachment["filename"],
                            attachment["bytes"],
                            attachment.get("content_type") or "application/octet-stream",
                        ),
                    )
                    for index, attachment in enumerate(prepared_attachments)
                ],
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
        preview = truncate((response.text or "").replace("\r", " ").replace("\n", " "), 200)
        success = response.status_code in (200, 204)
        debug_log(
            f"webhook_post endpoint={endpoint_id} status={response.status_code} "
            f"success={success} attachments={len(prepared_attachments)} body={preview!r}"
        )
        update_endpoint_status(endpoint_id, "Online" if success else "Offline")
        message_id = ""
        if success and capture_message_id:
            try:
                message_id = str((response.json() or {}).get("id") or "")
            except ValueError:
                message_id = ""
        return success, message_id
    except requests.RequestException as exc:
        debug_log(f"webhook_post endpoint={endpoint_id} failed error={exc.__class__.__name__}: {exc}")
        update_endpoint_status(endpoint_id, "Offline")
        return False, ""


def edit_webhook_message(endpoint, message_id, payload, attachments=None):
    endpoint_id = endpoint.get("id")
    url = webhook_edit_url(endpoint.get("webhook_url"), message_id)
    prepared_attachments = list(attachments or [])
    try:
        if not prepared_attachments:
            response = requests.patch(url, json=payload, timeout=REQUEST_TIMEOUT_SECONDS)
        else:
            multipart_payload = dict(payload or {})
            multipart_payload["attachments"] = [
                {"id": index, "filename": attachment["filename"]}
                for index, attachment in enumerate(prepared_attachments)
            ]
            response = requests.patch(
                url,
                data={"payload_json": json.dumps(multipart_payload)},
                files=[
                    (
                        f"files[{index}]",
                        (
                            attachment["filename"],
                            attachment["bytes"],
                            attachment.get("content_type") or "application/octet-stream",
                        ),
                    )
                    for index, attachment in enumerate(prepared_attachments)
                ],
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
        preview = truncate((response.text or "").replace("\r", " ").replace("\n", " "), 200)
        success = response.status_code in (200, 204)
        debug_log(
            f"webhook_edit endpoint={endpoint_id} message_id={message_id} status={response.status_code} "
            f"success={success} attachments={len(prepared_attachments)} body={preview!r}"
        )
        update_endpoint_status(endpoint_id, "Online" if success else "Offline")
        return success
    except requests.RequestException as exc:
        debug_log(
            f"webhook_edit endpoint={endpoint_id} message_id={message_id} "
            f"failed error={exc.__class__.__name__}: {exc}"
        )
        update_endpoint_status(endpoint_id, "Offline")
        return False


def send_ready_signal(module_name, stream_id):
    if connect_endpoint_ipc is None:
        return
    try:
        with connect_endpoint_ipc(timeout=1) as sock:
            sock.sendall(f"READY {module_name} {stream_id}\n".encode("utf-8"))
            sock.recv(16)
        debug_log(f"READY sent module={module_name} stream={stream_id}")
    except Exception:
        debug_log(f"READY failed module={module_name} stream={stream_id}")


def deliver_to_targets(endpoints, delivery, attachments=None, capture_message_ids=False):
    if not endpoints:
        debug_log(f"deliver skipped no_endpoints title={delivery.get('title')!r}")
        return {}
    settings = load_settings()
    prepared_attachments = list(attachments or [])
    debug_log(
        f"deliver start title={delivery.get('title')!r} type={delivery.get('type')!r} "
        f"endpoints={[row.get('id') for row in endpoints]} attachments={len(prepared_attachments)}"
    )
    message_ids = {}
    for endpoint in endpoints:
        payload = build_webhook_payload(endpoint, settings, delivery)
        success, message_id = send_webhook(
            endpoint,
            payload,
            attachments=prepared_attachments,
            capture_message_id=capture_message_ids,
        )
        if success and message_id:
            message_ids[str(endpoint.get("id") or "")] = message_id
    return message_ids


def edit_livepage_messages(endpoints, delivery, message_ids, attachments=None):
    if not endpoints:
        return
    settings = load_settings()
    prepared_attachments = list(attachments or [])
    for endpoint in endpoints:
        endpoint_id = str(endpoint.get("id") or "")
        payload = build_webhook_payload(endpoint, settings, delivery)
        message_id = str((message_ids or {}).get(endpoint_id) or "").strip()
        if message_id:
            if edit_webhook_message(endpoint, message_id, payload, attachments=prepared_attachments):
                continue
        send_webhook(endpoint, payload, attachments=prepared_attachments)


def async_deliver_to_targets(endpoints, delivery, attachments=None):
    threading.Thread(
        target=deliver_to_targets,
        args=(list(endpoints or []), dict(delivery or {})),
        kwargs={"attachments": list(attachments or [])},
        daemon=True,
    ).start()


def start_livepage_message_sender(stream_id):
    with active_streams_lock:
        stream = active_streams.get(stream_id)
        if stream is None:
            return None
        delivery = build_delivery_message(
            "prepare_livepage",
            None,
            stream.get("msg_id"),
            {
                **(stream.get("metadata") or {}),
                "issued": stream.get("created_at"),
                "_page_complete": False,
            },
        )
        delivery, attachments = prepare_livepage_delivery(delivery)
        endpoints = list(stream.get("endpoints") or [])

    def worker():
        message_ids = deliver_to_targets(endpoints, delivery, attachments=attachments, capture_message_ids=True)
        with active_streams_lock:
            live_stream = active_streams.get(stream_id)
            if live_stream is not None:
                live_stream.setdefault("message_ids", {}).update(message_ids)

    thread = threading.Thread(target=worker, daemon=True)
    thread.start()
    return thread


def handle_dispatch(action, stream_id, msg_id, targets, metadata=None):
    normalized_targets = []
    for target in targets:
        token = str(target or "").strip()
        if token and token not in normalized_targets:
            normalized_targets.append(token)

    if not normalized_targets:
        if action in {"prepare_audio", "prepare_livepage"}:
            send_ready_signal(MODULE_NAME, stream_id)
        return

    debug_log(
        f"handle_dispatch action={action} stream={stream_id} msg={msg_id} "
        f"targets={normalized_targets} metadata={metadata}"
    )

    if action == "prepare_livepage":
        endpoints = eligible_endpoints(fetch_configured_endpoints(normalized_targets), metadata)
        if endpoints:
            with active_streams_lock:
                active_streams[stream_id] = {
                    "endpoints": endpoints,
                    "metadata": metadata if isinstance(metadata, dict) else {},
                    "msg_id": msg_id,
                    "audio": bytearray(),
                    "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "message_ids": {},
                    "send_thread": None,
                }
        send_ready_signal(MODULE_NAME, stream_id)
        if endpoints:
            thread = start_livepage_message_sender(stream_id)
            with active_streams_lock:
                live_stream = active_streams.get(stream_id)
                if live_stream is not None:
                    live_stream["send_thread"] = thread
        return

    endpoints, message = fetch_endpoints_and_message(normalized_targets, msg_id)
    if not message:
        debug_log(f"message_not_found msg={msg_id}")
        if action == "prepare_audio":
            send_ready_signal(MODULE_NAME, stream_id)
        return

    endpoints = eligible_endpoints(endpoints, message)
    msg_type = normalize_message_type(message.get("type"))
    delivery = build_delivery_message(action, message, msg_id, metadata)
    delivery, attachments = prepare_message_delivery(message, delivery)

    if msg_type == "text":
        if action == "prepare_audio":
            send_ready_signal(MODULE_NAME, stream_id)
            async_deliver_to_targets(endpoints, delivery, attachments=attachments)
            return
        deliver_to_targets(endpoints, delivery, attachments=attachments)
        return

    if action != "prepare_audio":
        debug_log(f"skipping_non_prepare_audio action={action} msg={msg_id} type={msg_type}")
        return

    send_ready_signal(MODULE_NAME, stream_id)
    threading.Thread(
        target=lambda: deliver_to_targets(
            endpoints,
            delivery,
            attachments=attachments,
        ),
        daemon=True,
    ).start()


def handle_api(command_string):
    parts = str(command_string or "").strip().split()
    if len(parts) < 4:
        return
    handle_dispatch(parts[0], parts[2], parts[3], [parts[1]])


def receive_audio(chunk, stream_id):
    with active_streams_lock:
        stream = active_streams.get(stream_id)
    if stream is None:
        return
    stream.get("audio", bytearray()).extend(chunk or b"")


def end_stream(stream_id):
    with active_streams_lock:
        stream = active_streams.get(stream_id)
    if stream is None:
        return
    send_thread = stream.get("send_thread")
    if send_thread is not None and send_thread.is_alive():
        send_thread.join(timeout=5)
    with active_streams_lock:
        stream = active_streams.pop(stream_id, stream)
    delivery = build_delivery_message(
        "prepare_livepage",
        None,
        stream.get("msg_id"),
        {
            **(stream.get("metadata") or {}),
            "issued": stream.get("created_at"),
            "_page_complete": True,
        },
    )
    wav_bytes = ulaw_bytes_to_wav_bytes(bytes(stream.get("audio") or b""))
    delivery, attachments = prepare_livepage_delivery(delivery, wav_bytes=wav_bytes)
    edit_livepage_messages(
        stream.get("endpoints") or [],
        delivery,
        stream.get("message_ids") or {},
        attachments=attachments,
    )


def get_endpoint_status_payload():
    endpoints = []
    for row in fetch_configured_endpoints():
        endpoint_id = str(row.get("id") or "")
        name = str(row.get("name") or f"Discord Webhook {endpoint_id}")
        exclude_bells = truthy(row.get("exclude_bells"))
        endpoints.append(
            {
                "id": endpoint_id,
                "name": name,
                "address": "",
                "model": "Discord Webhook",
                "status": str(row.get("status") or "Unknown"),
                "type": "Webhook Endpoint",
                "direction": "Output",
                "output_capable": True,
                "bell_capable": not exclude_bells,
                "capabilities": ["output"],
            }
        )
    return {
        "module": MODULE_NAME,
        "display_name": DISPLAY_NAME,
        "endpoints": endpoints,
    }
