"""
Silent Hill Voice Call Bot — v4.1 BEAST MODE
═══════════════════════════════════════════════════════════════════════════════
Voice: LiveKit SFU (see call_system.py for all voice logic).
Chat: in-room sticker uploads, message deletion, swipe-to-reply, view-once
      images, host avatars, hidden-suffix admin ("Sor-").
Games: Uno (2-5 players, +2/+4 stacking), Zombie (Old-Maid variant).
Stream: watch-together video with synced playback.
Persistence: rooms + tokens survive restarts; stickers sync to GitHub.
═══════════════════════════════════════════════════════════════════════════════
"""

import asyncio, json, os, re, time, uuid, hmac, hashlib, base64, io
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
from fastapi import FastAPI, Query, WebSocket, WebSocketDisconnect, HTTPException, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from uvicorn import Config, Server

# Pillow for sticker upload pipeline (resize + recompress to WebP).
# Optional — if missing, sticker upload is disabled but the rest of the bot
# runs fine. requirements.txt should now include `Pillow>=10.0.0`.
try:
    from PIL import Image, ImageOps
    PIL_OK = True
except ImportError:
    PIL_OK = False

try:
    from kyodo import ChatMessage, EventType, AsyncClient as Client
    KYODO_OK = True
except ImportError:
    KYODO_OK = False

# Uno game module — self-contained, only touched via handle_ws /
# on_peer_leave / on_room_cleanup. All state lives in uno.games.
import uno as uno_mod
# Old-Maid-style Zombie game module. Same lifecycle pattern as uno.
import zombie as zomb_mod

# LiveKit-backed voice (replaces all P2P mesh / TURN code).
# All voice transport logic lives in call_system.py — touch that file
# to tweak voice, this file just imports and wires it up.
import call_system

# ─── CONFIG ─────────────────────────────────────────────────────────────────
# User explicitly requested credentials remain hardcoded as defaults.
# (They're not considered sensitive in this project.)
EMAIL = os.getenv("BOT_EMAIL")
PASSWORD = os.getenv("BOT_PASSWORD")
DEVICE_ID = os.getenv("BOT_DEVICE_ID", "870d649515ce700797d6a56965689f3aaa7d5e82dfdce994b239e00e37238184")
CHAT_ID = os.getenv("BOT_CHAT_ID", "cmh2gy89r01pvt33exijh1wr3")
CIRCLE_ID = os.getenv("BOT_CIRCLE_ID", "cm9bylrbn00hmux6t43mczt2o")
WEB_APP_URL = os.environ.get("WEB_APP_URL", "http://localhost:8000")
PORT = int(os.environ.get("PORT", "8000"))

# Room capacity. v3.8 hardened to 15 peers. Configurable via env var.
MAX_PEERS_PER_ROOM = int(os.environ.get("MAX_PEERS_PER_ROOM", "15"))

# memory hardening
MAX_CHAT_MESSAGES = int(os.environ.get("MAX_CHAT_MESSAGES", "200"))
IMAGE_RETAIN_COUNT = int(os.environ.get("IMAGE_RETAIN_COUNT", "30"))
MAX_IMAGE_BYTES = int(os.environ.get("MAX_IMAGE_BYTES", "400000"))  # was 600000

# stickers folder. Anything in this folder ending in .jpg/.jpeg/.png/.webp
# becomes a sticker. The server lists the directory live on each /stickers
# request, so adding files via GitHub redeploy or even a manual file drop
# makes them available immediately. Filenames must match SAFE_STICKER_NAME.
STICKERS_DIR = os.environ.get("STICKERS_DIR", "stickers")
STICKER_EXTS = {".jpg", ".jpeg", ".png", ".webp", ".gif"}
# Strict allowlist on filenames to prevent any path-traversal or weirdness:
# letters, digits, dots, dashes, underscores. Max length 64 to keep WS payloads
# tiny and to discourage anyone from stuffing data into filenames.
SAFE_STICKER_NAME = re.compile(r"^[A-Za-z0-9._-]{1,64}$")

# Sticker upload pipeline:
#   • Hard cap on raw payload, anything larger → rejected before decode
#   • Pillow decode runs inside Semaphore(1) so RAM stays bounded under load
#   • Output: WebP @ 1024px max edge, quality 85 (50–120 KB typical)
#   • If GITHUB_TOKEN+REPO set, uploaded stickers also commit to GitHub so
#     they survive Render's filesystem wipe on restart. Otherwise ephemeral.
MAX_STICKERS = int(os.environ.get("MAX_STICKERS", "30"))
MAX_STICKER_UPLOAD_BYTES = int(os.environ.get("MAX_STICKER_UPLOAD_BYTES", "5242880"))  # 5 MB raw
STICKER_OUTPUT_MAX_EDGE = int(os.environ.get("STICKER_OUTPUT_MAX_EDGE", "1024"))
STICKER_OUTPUT_QUALITY = int(os.environ.get("STICKER_OUTPUT_QUALITY", "85"))
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "")
GITHUB_REPO = os.environ.get("GITHUB_REPO", "")  # e.g. "username/silent-hill-bot"
GITHUB_BRANCH = os.environ.get("GITHUB_BRANCH", "main")
GITHUB_STICKERS_PATH = os.environ.get("GITHUB_STICKERS_PATH", "stickers")

# Admin auth via hidden-suffix trick. A user joining as "Sor-" (or "sor-")
# is recognized as admin: server strips the trailing "-" before broadcasting
# the name, and sets is_admin=True. Plain "Sor" is just a regular peer with
# no badge and no powers. This way users who try to impersonate by typing
# "Sor" will fail because they don't know about the hidden suffix.
ADMIN_NAME_BASE = os.environ.get("ADMIN_NAME_BASE", "sor").lower()
ADMIN_NAME_SUFFIX = os.environ.get("ADMIN_NAME_SUFFIX", "-")  # the hidden bit

# TURN/STUN gone — LiveKit owns the transport. See call_system.py for env vars.

tokens: Dict[str, dict] = {}
rooms: Dict[str, dict] = {}
kyodo_client = None

# per-room host avatar assignments. Maps room_id -> {peer_id: avatar_url}.
# Persists for the room's lifetime. Cleared on peer leave and on room cleanup.
host_assigned_avatars: Dict[str, Dict[str, str]] = {}

# per-room streaming state. room_id -> { streamer_pid, streamer_name,
# playlist, idx, playing, time, last_update_at, files }. Cleared on stream
# end, streamer leave, or room teardown. Files on disk also deleted.
streams: Dict[str, dict] = {}

# Room + token persistence — rooms and invite tokens survive server restarts.
# On boot, existing rooms are restored from registry files so active calls aren't
# killed by Render deployments.
ROOMS_REGISTRY = "rooms_registry.json"
TOKENS_REGISTRY = "tokens_registry.json"

def _persist_rooms():
    """Save room metadata (no peer websockets) to disk."""
    try:
        payload = {}
        for rid, rdata in rooms.items():
            payload[rid] = {
                "chat_file": rdata.get("chat_file", f"{rid}_chat.json"),
                "created": rdata.get("created"),
                "creator_uid": rdata.get("creator_uid"),
                "creator_name": rdata.get("creator_name"),
            }
        json_write(ROOMS_REGISTRY, payload)
    except Exception as e:
        print(f"[persist] rooms err: {e}")

def _persist_tokens():
    """Save token → room_id mapping to disk (no sensitive data)."""
    try:
        payload = {tok: {"room_id": v["room_id"], "creator": v.get("creator", False)}
                     for tok, v in tokens.items()}
        json_write(TOKENS_REGISTRY, payload)
    except Exception as e:
        print(f"[persist] tokens err: {e}")

def _restore_rooms():
    """On boot, recreate room entries from persisted registry."""
    global rooms
    restored = 0
    try:
        reg = json_read(ROOMS_REGISTRY, {})
        for rid, meta in reg.items():
            chat_file = meta.get("chat_file", f"{rid}_chat.json")
            # Only restore if chat file still exists (room wasn't properly cleaned up)
            if os.path.exists(chat_file):
                rooms[rid] = {
                    "peers": {},
                    "chat_file": chat_file,
                    "created": meta.get("created", datetime.now().isoformat()),
                    "creator_uid": meta.get("creator_uid"),
                    "creator_name": meta.get("creator_name"),
                }
                restored += 1
        if restored:
            print(f"[persist] restored {restored} room(s) from registry")
    except Exception as e:
        print(f"[persist] restore rooms err: {e}")

def _restore_tokens():
    """On boot, restore valid tokens from persisted registry."""
    global tokens
    restored = 0
    try:
        reg = json_read(TOKENS_REGISTRY, {})
        for tok, meta in reg.items():
            rid = meta.get("room_id", "")
            # Only restore token if the room still exists
            if rid in rooms:
                tokens[tok] = {"room_id": rid, "creator": meta.get("creator", False)}
                restored += 1
        if restored:
            print(f"[persist] restored {restored} token(s) from registry")
    except Exception as e:
        print(f"[persist] restore tokens err: {e}")

# Semaphore(1) ensures at most one Pillow decode/resize happens at a
# time across the whole process, so RAM stays bounded even if 10 people hit
# upload simultaneously. They queue.
_sticker_upload_sem = asyncio.Semaphore(1)
# Lock ensures the "count → reject if >= cap → write" sequence is atomic.
_sticker_count_lock = asyncio.Lock()


def detect_admin(raw_name: str) -> Tuple[str, bool]:
    """v3.12 hidden-suffix admin auth.

    If the user joins as "<ADMIN_NAME_BASE><ADMIN_NAME_SUFFIX>" (e.g. "Sor-"),
    we recognize them as admin and strip the suffix from the displayed name.
    Anyone joining as plain "<ADMIN_NAME_BASE>" (e.g. "Sor") is NOT admin —
    they're just a regular user. The trailing dash never appears in the UI,
    so impersonators don't know it exists.

    Returns (display_name, is_admin).
    """
    if not raw_name:
        return raw_name, False
    stripped = raw_name.strip()
    low = stripped.lower()
    target = ADMIN_NAME_BASE + ADMIN_NAME_SUFFIX
    if low == target:
        # Preserve the user's casing on the base, drop the suffix
        base_len = len(ADMIN_NAME_BASE)
        return stripped[:base_len], True
    return stripped, False


def json_write(p: str, d: Any):
    with open(p, "w", encoding="utf-8") as f:
        json.dump(d, f, ensure_ascii=False, indent=2)


def json_read(p: str, default=None):
    if default is None:
        default = []
    try:
        if not os.path.exists(p):
            return default
        with open(p, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def list_stickers() -> List[str]:
    """Walk STICKERS_DIR and return a sorted list of valid sticker filenames.
    Called live on every /stickers request — no caching. The dir is tiny
    and this lets new GitHub commits show up instantly. Robust against
    missing dir, weird filenames, hidden files."""
    try:
        if not os.path.isdir(STICKERS_DIR):
            return []
        out = []
        for fn in os.listdir(STICKERS_DIR):
            if fn.startswith("."):
                continue
            ext = os.path.splitext(fn)[1].lower()
            if ext not in STICKER_EXTS:
                continue
            if not SAFE_STICKER_NAME.match(fn):
                continue
            out.append(fn)
        # Natural-ish sort: s1, s2, s10 (not s1, s10, s2). Falls back to
        # plain string sort for any name that doesn't match s\d+\..+
        def sort_key(n: str):
            m = re.match(r"^s(\d+)\.", n, re.IGNORECASE)
            if m:
                return (0, int(m.group(1)), n)
            return (1, 0, n.lower())
        out.sort(key=sort_key)
        return out
    except Exception as e:
        print(f"[stickers] list err: {e}")
        return []


# ── image processing & GitHub persistence ────────────────────────────
def _process_sticker_image(raw: bytes) -> Optional[bytes]:
    """Decode arbitrary image bytes and re-encode as a clean WebP at the
    configured max edge / quality. Strips EXIF/metadata. Auto-orients.
    Returns the encoded bytes, or None on any failure.

    Runs synchronously — caller must wrap in run_in_executor or hold the
    upload semaphore so we don't pile up Pillow heap allocations.
    """
    if not PIL_OK:
        return None
    try:
        with Image.open(io.BytesIO(raw)) as im:
            im = ImageOps.exif_transpose(im)  # honor camera rotation
            # Force RGBA→RGB for non-PNG outputs; WebP handles RGBA fine but
            # we lose alpha if we accidentally drop it. Keep it.
            if im.mode not in ("RGB", "RGBA", "L"):
                im = im.convert("RGBA" if "A" in im.getbands() else "RGB")
            # Downscale (never upscale) to fit STICKER_OUTPUT_MAX_EDGE.
            w, h = im.size
            longest = max(w, h)
            if longest > STICKER_OUTPUT_MAX_EDGE:
                ratio = STICKER_OUTPUT_MAX_EDGE / float(longest)
                new_size = (max(1, int(w * ratio)), max(1, int(h * ratio)))
                im = im.resize(new_size, Image.LANCZOS)
            buf = io.BytesIO()
            im.save(buf, format="WEBP", quality=STICKER_OUTPUT_QUALITY, method=6)
            return buf.getvalue()
    except Exception as e:
        print(f"[stickers] process err: {e}")
        return None


async def _github_commit_sticker(filename: str, file_bytes: bytes) -> Tuple[bool, str]:
    """Best-effort commit of a new sticker to the GitHub repo. Returns
    (success, error_detail). Failure is non-fatal — the sticker is
    already saved locally and works for this session.

    error_detail is empty on success. On failure it's a human-readable
    short string that's safe to show in a toast (no token leaks).
    """
    if not GITHUB_TOKEN or not GITHUB_REPO:
        return False, "GITHUB_TOKEN/REPO not set"
    api = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{GITHUB_STICKERS_PATH}/{filename}"
    headers = {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    body = {
        "message": f"Add sticker {filename} (uploaded via room)",
        "content": base64.b64encode(file_bytes).decode("ascii"),
        "branch": GITHUB_BRANCH,
    }
    try:
        async with aiohttp.ClientSession() as s:
            async with s.put(api, json=body, headers=headers, timeout=30) as r:
                if r.status in (200, 201):
                    return True, ""
                txt = await r.text()
                print(f"[github] commit {filename} failed {r.status}: {txt[:200]}")
                # Map common HTTP status codes to user-actionable messages.
                if r.status == 401:
                    return False, "GitHub auth failed (401) — token invalid or expired"
                if r.status == 403:
                    return False, ("GitHub forbidden (403) — token lacks "
                                   "'Contents: write' permission for this repo")
                if r.status == 404:
                    # NOTE: GitHub fine-grained PATs (the "github_pat_..." ones)
                    # return 404 — NOT 403 — when they lack 'Contents: write'
                    # permission for the target repo. They literally hide the
                    # repo's existence from a token that can't write to it.
                    # So 404 here usually means: token is missing the write
                    # permission, OR the repo selector on the token doesn't
                    # include this specific repo, OR GITHUB_REPO/branch is
                    # genuinely wrong. We surface all three possibilities.
                    return False, (f"GitHub 404 — usually means the fine-grained "
                                   f"token lacks 'Contents: Read and write' for "
                                   f"{GITHUB_REPO!r}, OR the token's repo "
                                   f"selector doesn't include this repo. Verify "
                                   f"at github.com/settings/personal-access-tokens.")
                if r.status == 422:
                    return False, "GitHub rejected the file (422) — usually means it already exists"
                return False, f"GitHub returned HTTP {r.status}"
    except asyncio.TimeoutError:
        return False, "GitHub request timed out"
    except Exception as e:
        print(f"[github] commit err: {e}")
        return False, f"GitHub error: {type(e).__name__}"


async def _github_delete_sticker(filename: str) -> bool:
    """Best-effort delete of a sticker from the GitHub repo. Two-step (GET
    file SHA, then DELETE). Returns True on success."""
    if not GITHUB_TOKEN or not GITHUB_REPO:
        return False
    api = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{GITHUB_STICKERS_PATH}/{filename}"
    headers = {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    try:
        async with aiohttp.ClientSession() as s:
            # 1: find SHA
            async with s.get(api, headers=headers,
                             params={"ref": GITHUB_BRANCH}, timeout=15) as r:
                if r.status == 404:
                    return True  # already gone
                if r.status != 200:
                    return False
                meta = await r.json()
                sha = meta.get("sha")
                if not sha:
                    return False
            # 2: delete
            body = {
                "message": f"Delete sticker {filename}",
                "sha": sha,
                "branch": GITHUB_BRANCH,
            }
            async with s.delete(api, json=body, headers=headers, timeout=15) as r:
                return r.status in (200, 204)
    except Exception as e:
        print(f"[github] delete err: {e}")
        return False


def _generate_sticker_filename() -> str:
    """Auto-generated unique filename. We never trust the uploader's chosen
    name — eliminates collisions, traversal, and weird-character bugs in one
    stroke."""
    return f"up_{uuid.uuid4().hex[:10]}.webp"


async def _github_verify_write_permission() -> Tuple[bool, str]:
    """Boot-time sanity check that the configured token can WRITE
    to the repo. Returns (ok, reason).

    IMPORTANT: This used to do a real PUT+DELETE dance against the repo,
    which committed two files per boot. That was a disaster on Render
    when auto-deploy was on: every commit triggered a redeploy, which
    triggered another probe, which triggered more commits — an infinite
    deploy storm. Lesson learned: the probe MUST be read-only.

    The new approach: GET /repos/{owner}/{repo} returns a `permissions`
    object on authenticated requests, with `push: true` if the token has
    write access. This works correctly for:
      • Fine-grained PATs with 'Contents: Read and write' → push=true
      • Fine-grained PATs with 'Contents: Read' only → push=false
      • Classic PATs with `repo` scope → push=true (admin=true too)
      • Tokens that can't see the repo at all → 404, treated as failure

    A fine-grained PAT lacking ALL access returns 404 on this endpoint,
    so we can distinguish "no access" from "read-only access" cleanly.
    """
    if not GITHUB_TOKEN or not GITHUB_REPO:
        return False, "creds not set"

    api = f"https://api.github.com/repos/{GITHUB_REPO}"
    headers = {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(api, headers=headers, timeout=15) as r:
                body = await r.text()
                if r.status == 404:
                    return False, (f"Got 404 on /repos/{GITHUB_REPO} — token "
                                   f"can't see this repo. For fine-grained "
                                   f"PATs this means the repo selector "
                                   f"excludes it OR the token has no perms "
                                   f"on it at all.")
                if r.status == 401:
                    return False, "Got 401 — token invalid or expired"
                if r.status != 200:
                    return False, f"Got HTTP {r.status}: {body[:160]}"
                try:
                    data = json.loads(body)
                except Exception:
                    return False, f"Repo info JSON parse failed: {body[:160]}"
                perms = data.get("permissions") or {}
                can_write = bool(perms.get("push") or perms.get("admin")
                                 or perms.get("maintain"))
                if can_write:
                    return True, "ok"
                return False, ("Repo visible but token has READ-ONLY access. "
                               "Fine-grained PAT needs 'Contents: Read and "
                               "write' (not just Read).")
    except asyncio.TimeoutError:
        return False, "GitHub timed out during probe"
    except Exception as e:
        return False, f"probe error: {type(e).__name__}: {e}"


async def _github_sync_stickers_to_disk() -> int:
    """Pull every sticker from the GitHub repo into the local
    STICKERS_DIR on boot. This is the safety net that makes uploaded
    stickers truly persistent on Render's free tier:

    - Render's filesystem is ephemeral. On every cold start the local
      stickers/ folder is wiped (or restored only to whatever shipped
      with the deploy).
    - We commit uploaded stickers to GitHub (above) so they're durable.
    - On boot, we GET the contents of the GitHub stickers/ folder and
      write each file to disk. Now the local FS reflects the GitHub
      truth — uploaded stickers are present from the moment the bot
      starts serving traffic.

    Returns the number of files synced. Non-fatal if any step fails:
    the bot just runs with whatever is on disk already.
    """
    if not GITHUB_TOKEN or not GITHUB_REPO:
        return 0
    api = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{GITHUB_STICKERS_PATH}"
    headers = {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    try:
        os.makedirs(STICKERS_DIR, exist_ok=True)
        async with aiohttp.ClientSession() as s:
            async with s.get(api, headers=headers,
                             params={"ref": GITHUB_BRANCH}, timeout=20) as r:
                if r.status == 404:
                    print(f"[github-sync] folder /{GITHUB_STICKERS_PATH} not found in repo — nothing to sync")
                    return 0
                if r.status != 200:
                    txt = await r.text()
                    print(f"[github-sync] list failed {r.status}: {txt[:200]}")
                    return 0
                items = await r.json()
                if not isinstance(items, list):
                    return 0

            synced = 0
            for it in items:
                if it.get("type") != "file":
                    continue
                name = it.get("name", "")
                if not name or name.startswith("."):
                    continue
                ext = os.path.splitext(name)[1].lower()
                if ext not in STICKER_EXTS:
                    continue
                if not SAFE_STICKER_NAME.match(name):
                    continue
                local_path = os.path.join(STICKERS_DIR, name)
                # Skip if already on disk with the same size — avoids
                # re-downloading the bundled stickers that ship with the
                # repo on every boot.
                expected_size = it.get("size", -1)
                if (os.path.isfile(local_path)
                        and expected_size > 0
                        and os.path.getsize(local_path) == expected_size):
                    continue
                # download_url is a CDN URL with no auth required
                durl = it.get("download_url")
                if not durl:
                    continue
                try:
                    async with s.get(durl, timeout=30) as fr:
                        if fr.status != 200:
                            continue
                        data = await fr.read()
                    # safety: don't write absurdly large files
                    if len(data) > MAX_STICKER_UPLOAD_BYTES:
                        continue
                    with open(local_path, "wb") as f:
                        f.write(data)
                    synced += 1
                except Exception as e:
                    print(f"[github-sync] {name} fail: {e}")
                    continue
            return synced
    except Exception as e:
        print(f"[github-sync] err: {e}")
        return 0


# ─── KYODO BOT ──────────────────────────────────────────────────────────────
async def run_kyodo_bot():
    global kyodo_client
    if not KYODO_OK:
        while True:
            await asyncio.sleep(3600)
    backoff = 5
    while True:
        t0 = time.time()
        try:
            kyodo_client = Client(deviceId=DEVICE_ID)

            @kyodo_client.middleware(EventType.ChatMessage)
            async def _filt(m: ChatMessage):
                if m.author.userId == kyodo_client.userId:
                    return False

            @kyodo_client.event(EventType.ChatMessage)
            async def _on(m: ChatMessage):
                try:
                    c = (m.content or "").strip()
                    if not c or m.chatId != CHAT_ID:
                        return
                    if c.lower() in ("/call", "!call", "/جلسة"):
                        rid = str(uuid.uuid4())[:8]
                        rooms[rid] = {
                            "peers": {},
                            "chat_file": f"{rid}_chat.json",
                            "created": datetime.now().isoformat(),
                            "creator_uid": m.author.userId,
                            "creator_name": m.author.nickname,
                        }
                        json_write(f"{rid}_chat.json", [])
                        tok = str(uuid.uuid4())
                        tokens[tok] = {"room_id": rid, "creator": True}
                        # persist so room survives deploys
                        _persist_rooms()
                        _persist_tokens()
                        asyncio.create_task(_noshow(rid))
                        link = f"{WEB_APP_URL}/call/{rid}?t={tok}"
                        await kyodo_client.send_message(
                            m.chatId,
                            f"Silent Hill Voice Session\n[click to join|{link}]",
                            m.circleId,
                        )
                except Exception as e:
                    print(f"[Kyodo] err: {e}")

            await kyodo_client.login(EMAIL, PASSWORD)
            print("[Kyodo] Logged in!")
            await kyodo_client.socket_wait()
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception as e:
            print(f"[Kyodo] crash: {e}")
        backoff = 5 if time.time() - t0 > 300 else min(backoff * 2, 120)
        await asyncio.sleep(backoff)


# ─── FASTAPI ────────────────────────────────────────────────────────────────
app = FastAPI()


@app.get("/")
async def root():
    return {"ok": True, "rooms": len(rooms), "kyodo": KYODO_OK,
            "max_peers": MAX_PEERS_PER_ROOM, "stickers": len(list_stickers())}


@app.get("/bg.jpg")
async def bg():
    return FileResponse("bg.jpg") if os.path.exists("bg.jpg") else HTMLResponse("", 404)


@app.get("/ci.jpg")
async def ci():
    return FileResponse("ci.jpg") if os.path.exists("ci.jpg") else HTMLResponse("", 404)


# ── beacon-leave endpoint ─────────────────────────────────────────
# The browser fires `pagehide` and `beforeunload` when a tab is closed,
# but ws.send() during unload is unreliable — many browsers cancel
# pending sockets before the message is flushed. navigator.sendBeacon()
# is the only API guaranteed to deliver a payload during unload. The
# client POSTs to this endpoint with their room+peer ids; we synthesize
# the same disconnect cleanup the WS would have done, so other peers
# see "X left" instantly instead of waiting for the dead TCP socket to
# time out (which on mobile can take 30-60 seconds).
@app.post("/beacon_leave")
async def beacon_leave(req: Request):
    try:
        body = await req.json()
    except Exception:
        try:
            raw = (await req.body()).decode("utf-8", "replace")
            body = json.loads(raw) if raw else {}
        except Exception:
            return JSONResponse({"ok": False, "error": "bad body"}, status_code=400)
    room_id = str(body.get("room_id", ""))
    peer_id = str(body.get("peer_id", ""))
    token = str(body.get("token", ""))
    if not room_id or not peer_id:
        return JSONResponse({"ok": False, "error": "missing fields"}, status_code=400)
    # Validate token to prevent strangers from forging leaves for other peers
    tok = tokens.get(token)
    if not tok or tok.get("room_id") != room_id:
        return JSONResponse({"ok": False, "error": "bad token"}, status_code=403)
    if room_id not in rooms:
        return JSONResponse({"ok": True})  # already gone
    room = rooms[room_id]
    peer = room["peers"].get(peer_id)
    if not peer:
        return JSONResponse({"ok": True})  # already cleaned up
    name = peer.get("name", "Unknown")
    # Close the WS — this triggers the normal disconnect path in the WS
    # handler, which broadcasts peer_left and cleans up state. We don't
    # broadcast here directly to avoid a double-broadcast race with the
    # WS finally block. The WS close is asynchronous via fire-and-forget;
    # we don't await it because the client is unloading and won't read
    # our response anyway.
    print(f"[BEACON] {peer_id} ({name}) leaving room {room_id} via beacon")
    try:
        # Schedule the close so this HTTP request can return immediately
        # (some browsers cap sendBeacon time and we want this to finish fast).
        asyncio.create_task(peer["ws"].close(code=1000))
    except Exception:
        pass
    return JSONResponse({"ok": True})


# ── avatar endpoints ──────────────────────────────────────────────
# Hosts (Sor) can assign avatars to peers who joined without one. The
# avatars themselves live in an `avatars/` folder next to this file —
# whatever .jpg/.png files exist there are listed by GET /avatars and
# served individually by GET /avatars/{filename}.
# Adding av7.jpg to the folder later will appear automatically — no code
# change required.
AVATARS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "avatars")
os.makedirs(AVATARS_DIR, exist_ok=True)

# per-game background image folders.
#   uno-bg/   → backgrounds available to the Uno game
#   zombie-bg/ → backgrounds available to the Zombie game
# Both follow the same pattern as AVATARS_DIR: drop an image file in and
# it becomes selectable from the in-game "Change Background" menu
# (host-only). Same image-extension allowlist.
UNO_BG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "uno-bg")
ZOMBIE_BG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "zombie-bg")
os.makedirs(UNO_BG_DIR, exist_ok=True)
os.makedirs(ZOMBIE_BG_DIR, exist_ok=True)
BG_EXTS = (".jpg", ".jpeg", ".png", ".webp", ".gif")
SAFE_BG_NAME = re.compile(r"^[a-zA-Z0-9._-]{1,80}$")

# in-room video streaming. The streamer uploads one or more video
# files to /stream-upload; the server stores them in STREAM_DIR with
# random IDs and tracks per-room playback state. WS messages keep all
# viewers in sync (play/pause/seek/track-change). Only the streamer can
# control. Files are cleaned up when the stream stops, the streamer
# leaves, or the room is torn down.
STREAM_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "streams")
os.makedirs(STREAM_DIR, exist_ok=True)
STREAM_EXTS = (".mp4", ".webm", ".mov", ".m4v", ".ogv", ".mkv")
STREAM_MIMES = {
    ".mp4": "video/mp4",
    ".m4v": "video/mp4",
    ".webm": "video/webm",
    ".mov": "video/quicktime",
    ".ogv": "video/ogg",
    ".mkv": "video/x-matroska",
}
MAX_STREAM_FILE_BYTES = 250 * 1024 * 1024   # 250 MB per file
MAX_STREAM_FILES_PER_STREAM = 8             # don't let one stream queue 100 vids
SAFE_STREAM_ID = re.compile(r"^[a-f0-9]{16,40}\.[a-z0-9]{2,5}$")

# Order matters: /avatars (list) must be registered BEFORE
# /avatars/{filename} (path-parameter) so the literal path matches first.
@app.get("/avatars")
async def avatars_list():
    if not os.path.isdir(AVATARS_DIR):
        return JSONResponse({"avatars": [], "count": 0})
    files = []
    for f in sorted(os.listdir(AVATARS_DIR)):
        ext = os.path.splitext(f)[1].lower()
        if ext in (".jpg", ".jpeg", ".png", ".webp", ".gif"):
            files.append("/avatars/" + f)
    return JSONResponse({"avatars": files, "count": len(files)})


@app.get("/avatars/{filename}")
async def serve_avatar(filename: str):
    filepath = os.path.join(AVATARS_DIR, filename)
    if not os.path.exists(filepath):
        raise HTTPException(status_code=404, detail="Avatar not found")
    return FileResponse(filepath)


# ── sticker endpoints ───────────────────────────────────────────────
# /stickers : returns the live list of available sticker filenames so the
#             client can render the sticker picker. No caching — adding a
#             file to GitHub appears in all open rooms on next pick-open.
# /stickers/{name} : serves the actual file with strict filename validation.
@app.get("/stickers")
async def stickers_list():
    files = list_stickers()
    return JSONResponse({"stickers": files, "count": len(files)})


@app.get("/stickers/{name}")
async def sticker_file(name: str):
    # Defense in depth: we already strip-filter at list time, but a direct
    # request must also be sanitized in case anyone tries clever paths.
    if not SAFE_STICKER_NAME.match(name):
        return HTMLResponse("bad name", 400)
    ext = os.path.splitext(name)[1].lower()
    if ext not in STICKER_EXTS:
        return HTMLResponse("bad ext", 400)
    path = os.path.join(STICKERS_DIR, name)
    # Resolve to absolute and re-check the file lives inside STICKERS_DIR.
    # Belt-and-braces against any creative \, /, or .. that slipped past
    # SAFE_STICKER_NAME (it shouldn't, but cost is zero).
    real = os.path.realpath(path)
    base = os.path.realpath(STICKERS_DIR)
    if not real.startswith(base + os.sep) and real != base:
        return HTMLResponse("nope", 400)
    if not os.path.isfile(real):
        return HTMLResponse("not found", 404)
    # Cache aggressively — sticker bytes never change. If you replace s1.jpg
    # with new content, change the filename (s1b.jpg) or strip the ETag
    # header server-side. For now: 1 hour cache is a good balance.
    return FileResponse(real, headers={"Cache-Control": "public, max-age=3600"})


# ── game background file endpoints ────────────────────────────────
# Each game has a fixed default background image at
#   uno-bg/bg1.jpg
#   zombie-bg/bg1.jpg
# in the repo. These endpoints just serve files from those folders with
# the same strict filename validation used by avatars/stickers. The list
# endpoints + host-side picker were removed in v3.26 — backgrounds are no
# longer player-changeable; the file in the repo is the background.

def _serve_bg_file(directory: str, url_prefix: str, name: str):
    if not SAFE_BG_NAME.match(name):
        return HTMLResponse("bad name", 400)
    if os.path.splitext(name)[1].lower() not in BG_EXTS:
        return HTMLResponse("bad ext", 400)
    path = os.path.join(directory, name)
    real = os.path.realpath(path)
    base = os.path.realpath(directory)
    if not real.startswith(base + os.sep) and real != base:
        return HTMLResponse("nope", 400)
    if not os.path.isfile(real):
        return HTMLResponse("not found", 404)
    return FileResponse(real, headers={"Cache-Control": "public, max-age=3600"})


@app.get("/uno-bg/{name}")
async def uno_bg_file(name: str):
    return _serve_bg_file(UNO_BG_DIR, "/uno-bg/", name)


@app.get("/zombie-bg/{name}")
async def zombie_bg_file(name: str):
    return _serve_bg_file(ZOMBIE_BG_DIR, "/zombie-bg/", name)


# ── streaming upload + serve ──────────────────────────────────────
# POST /stream-upload?room_id=X&t=TOKEN with a multipart file field "file".
#   - Validates the token like other room endpoints.
#   - Validates extension + size.
#   - Writes to STREAM_DIR/<random>.<ext>.
#   - Returns {id, url, title} for the streamer to add to the playlist.
# Files are NOT tracked in a per-room registry yet — the streamer must
# follow up with `stream_start` over WS to actually broadcast the URLs.
# If a streamer uploads but never starts, the upload becomes orphaned;
# we sweep STREAM_DIR for stale files in a background task.

def _stream_cleanup_paths(paths: list) -> None:
    """Best-effort removal of files on disk. Never raises."""
    for p in paths:
        try:
            if os.path.isfile(p):
                os.remove(p)
        except Exception:
            pass


def _stream_public_state(room_id: str) -> dict:
    """Build the state payload broadcast to all peers. Doesn't include
    server-internal stuff like file paths."""
    st = streams.get(room_id)
    if not st:
        return None
    return {
        "streamer_pid": st["streamer_pid"],
        "streamer_name": st["streamer_name"],
        "playlist": [
            {"id": x["id"], "url": x["url"], "title": x["title"]}
            for x in st["playlist"]
        ],
        "idx": st["idx"],
        "playing": st["playing"],
        "time": st["time"],
        "last_update_at": st["last_update_at"],
        "server_now": time.time(),
    }


@app.post("/stream-upload")
async def stream_upload(request: Request, room_id: str = Query(...),
                         t: str = Query(...)):
    """Receive a single video file upload for the streaming feature.

    v3.28: simplified vs the v3.27 chunked-read version. We call
    `await upload.read()` once instead of a chunked loop, which avoids
    a class of edge cases where the chunk loop never terminates.
    Diagnostic logs at every stage so we can see exactly where things
    go wrong if a user reports a stuck upload.
    """
    print(f"[stream-upload] request room={room_id}")

    tok = tokens.get(t)
    if not tok or tok.get("room_id") != room_id or room_id not in rooms:
        print(f"[stream-upload] reject: bad room/token")
        return JSONResponse({"error": "Invalid room/token"}, 403)

    # If another peer is already streaming, reject. The streamer who
    # owns the active stream can add more files via the WS message
    # instead of going through upload (we'd need to rework for that;
    # for now uploads are only accepted when no stream is active).
    existing = streams.get(room_id)
    if existing and existing.get("playlist"):
        print(f"[stream-upload] reject: already streaming")
        return JSONResponse({"error": "A stream is already running"}, 409)

    # Parse the multipart body. This requires `python-multipart` to be
    # installed — if it isn't, FastAPI raises a clear RuntimeError that
    # we surface to the client. Without the explicit catch, the request
    # would hang or return an opaque 500.
    try:
        form = await request.form()
    except Exception as e:
        print(f"[stream-upload] form parse failed: {e!r}")
        return JSONResponse({"error": f"Server can't parse upload "
                                       f"(is python-multipart installed?): {e}"}, 500)

    upload = form.get("file")
    if upload is None:
        print(f"[stream-upload] no file field")
        return JSONResponse({"error": "No file in request"}, 400)

    original = os.path.basename(getattr(upload, "filename", "video") or "video")
    ext = os.path.splitext(original)[1].lower()
    if ext not in STREAM_EXTS:
        print(f"[stream-upload] reject: bad ext {ext!r}")
        return JSONResponse({"error": f"Unsupported extension {ext or '(none)'}. "
                                       f"Allowed: {', '.join(STREAM_EXTS)}"}, 400)

    new_id = uuid.uuid4().hex + ext
    if not SAFE_STREAM_ID.match(new_id):
        return JSONResponse({"error": "internal id error"}, 500)
    out_path = os.path.join(STREAM_DIR, new_id)

    # Read the whole file at once. FastAPI's UploadFile is backed by a
    # SpooledTemporaryFile, so this either pulls from RAM (small) or from
    # the temp file (large). Either way, it's a single bounded operation
    # rather than a possibly-stuck loop.
    try:
        data = await upload.read()
    except Exception as e:
        print(f"[stream-upload] read failed: {e!r}")
        return JSONResponse({"error": f"Read failed: {e}"}, 500)

    size = len(data)
    if size == 0:
        print(f"[stream-upload] reject: 0 bytes")
        return JSONResponse({"error": "Empty file"}, 400)
    if size > MAX_STREAM_FILE_BYTES:
        print(f"[stream-upload] reject: too large {size} > {MAX_STREAM_FILE_BYTES}")
        return JSONResponse({"error": f"File too large "
                                       f"({size // (1024*1024)} MB, "
                                       f"max {MAX_STREAM_FILE_BYTES // (1024*1024)})"}, 413)

    try:
        with open(out_path, "wb") as fh:
            fh.write(data)
    except Exception as e:
        print(f"[stream-upload] write failed: {e!r}")
        return JSONResponse({"error": f"Disk write failed: {e}"}, 500)

    title = os.path.splitext(original)[0][:80] or "Video"
    print(f"[stream-upload] OK id={new_id} size={size}")
    return JSONResponse({
        "id": new_id,
        "url": "/streams/" + new_id,
        "title": title,
        "size": size,
    })


@app.get("/streams/{name}")
async def serve_stream(name: str, request: Request):
    """Serve a stream file with Range support so video seeking works."""
    if not SAFE_STREAM_ID.match(name):
        return HTMLResponse("bad name", 400)
    ext = os.path.splitext(name)[1].lower()
    if ext not in STREAM_EXTS:
        return HTMLResponse("bad ext", 400)
    path = os.path.join(STREAM_DIR, name)
    real = os.path.realpath(path)
    base = os.path.realpath(STREAM_DIR)
    if not real.startswith(base + os.sep) and real != base:
        return HTMLResponse("nope", 400)
    if not os.path.isfile(real):
        return HTMLResponse("not found", 404)
    media_type = STREAM_MIMES.get(ext, "application/octet-stream")
    # FileResponse honors Range requests automatically (Starlette ships it).
    return FileResponse(real, media_type=media_type,
                         headers={"Accept-Ranges": "bytes",
                                  "Cache-Control": "no-store"})


# ─── LIVEKIT VOICE ────────────────────────────────────────────────────
# /livekit-token gated by the same invite token that protects /call/{room_id},
# so only people with a valid invite can mint a LiveKit access token.
def _validate_livekit_room(room_id: str, invite_token: str) -> bool:
    tok = tokens.get(invite_token)
    if not tok or tok.get("room_id") != room_id:
        return False
    return room_id in rooms


call_system.register_routes(app, validate_room=_validate_livekit_room)


@app.get("/health/github")
async def health_github():
    """Browser-accessible diagnostic for sticker persistence. Never echoes
    the token, only whether it's set and whether the write probe passed."""
    out = {
        "github_token_set": bool(GITHUB_TOKEN),
        "github_token_prefix": GITHUB_TOKEN[:11] + "..." if GITHUB_TOKEN else "",
        "github_repo": GITHUB_REPO or "(not set)",
        "github_branch": GITHUB_BRANCH,
        "stickers_path": GITHUB_STICKERS_PATH,
        "local_stickers_dir": STICKERS_DIR,
        "local_stickers_count": len(list_stickers()),
        "max_stickers": MAX_STICKERS,
    }
    if GITHUB_TOKEN and GITHUB_REPO:
        ok, reason = await _github_verify_write_permission()
        out["write_probe_ok"] = ok
        out["write_probe_reason"] = reason
        out["uploads_will_persist"] = bool(ok)
    else:
        out["write_probe_ok"] = False
        out["write_probe_reason"] = "GITHUB_TOKEN or GITHUB_REPO not set"
        out["uploads_will_persist"] = False
    return JSONResponse(out)


@app.get("/call/{room_id}")
async def call_page(room_id: str, t: str = Query(...)):
    tok = tokens.get(t)
    if not tok or tok.get("room_id") != room_id or room_id not in rooms:
        return HTMLResponse("<h1>Invalid link</h1>", 403)
    html = (CALL_HTML
            .replace("__ROOM_ID__", room_id)
            .replace("__TOKEN__", t)
            .replace("__MAX_PEERS__", str(MAX_PEERS_PER_ROOM)))
    return HTMLResponse(html)


@app.websocket("/ws/{room_id}")
async def ws_endpoint(ws: WebSocket, room_id: str, t: str = Query(...)):
    tok = tokens.get(t)
    if not tok or tok.get("room_id") != room_id or room_id not in rooms:
        await ws.close(code=4001)
        return
    await ws.accept()

    room = rooms[room_id]

    # ── ROOM CAPACITY ENFORCEMENT (server is source of truth) ──
    if len(room["peers"]) >= MAX_PEERS_PER_ROOM:
        try:
            await ws.send_json({
                "type": "room_full",
                "current": len(room["peers"]),
                "max": MAX_PEERS_PER_ROOM,
            })
        except Exception:
            pass
        await ws.close(code=4003)
        print(f"[WS] room {room_id} full ({len(room['peers'])}/{MAX_PEERS_PER_ROOM}), refused new peer")
        return

    peer_id = str(uuid.uuid4())[:8]
    name, avatar = "Unknown", ""
    is_admin = False
    try:
        init = await asyncio.wait_for(ws.receive_json(), timeout=15)
        if isinstance(init, dict) and init.get("type") == "join":
            raw_name = str(init.get("name", "Unknown"))[:30]
            # hidden-suffix admin detection. "Sor-" → ("Sor", True).
            # Plain "Sor" → ("Sor", False) — a regular user, no badge, no powers.
            name, is_admin = detect_admin(raw_name)
            if not name:
                name = "Unknown"
            avatar = str(init.get("avatar", ""))[:200000]
    except asyncio.TimeoutError:
        await ws.close(code=4002)
        return
    except Exception:
        await ws.close(code=4002)
        return

    # Re-check capacity after the join handshake (race window protection)
    if len(room["peers"]) >= MAX_PEERS_PER_ROOM:
        try:
            await ws.send_json({
                "type": "room_full",
                "current": len(room["peers"]),
                "max": MAX_PEERS_PER_ROOM,
            })
        except Exception:
            pass
        await ws.close(code=4003)
        return

    # Host is the admin user (Sor) — always, regardless of who created the
    # Kyodo room or who joined the call first. The hidden-suffix admin auth
    # (joining as "Sor-") is what proves identity here. This way:
    #   • Sor always gets the Host badge + gold frame + seat #1.
    #   • Anyone else joining first will NOT become host, even temporarily.
    #   • If Sor leaves and rejoins, host transfers back to Sor.
    # The old "creator AND first to join" rule is gone — the Kyodo room
    # creator no longer matters for in-call host status.
    is_host = is_admin
    room["peers"][peer_id] = {
        "ws": ws, "name": name, "avatar": avatar,
        "muted": False, "is_host": is_host, "is_admin": is_admin,
        "joined": time.time(),
    }
    existing = [p for p in room["peers"] if p != peer_id]
    print(f"[WS] {peer_id} ({name}) joined room={room_id} host={is_host} admin={is_admin} total={len(room['peers'])}/{MAX_PEERS_PER_ROOM}")

    # tell the client whether they're admin so they can show the
    # delete (×) buttons on stickers. The client never decides this on its
    # own — server is source of truth.
    # Also tell them whether they're host so the seat tile can render the
    # Host badge / gold frame on their own avatar.
    await ws.send_json({"type": "your_id", "id": peer_id,
                        "max_peers": MAX_PEERS_PER_ROOM,
                        "is_admin": is_admin,
                        "is_host": is_host})

    # also send the current sticker list on join, so the picker is
    # ready to open instantly without an extra round trip. Client also
    # refreshes from /stickers each open in case new files have arrived.
    await ws.send_json({"type": "stickers", "stickers": list_stickers()})

    # build history payload with bounded memory cost. Send last 100
    # messages but strip image/sticker data from all but the most recent 30.
    full_history = json_read(room["chat_file"])
    history_slice = full_history[-100:] if full_history else []
    if len(history_slice) > IMAGE_RETAIN_COUNT:
        keep_from = len(history_slice) - IMAGE_RETAIN_COUNT
        for i, m in enumerate(history_slice):
            if i < keep_from and isinstance(m, dict):
                if m.get("image"):
                    m = {**m, "image": "", "image_expired": True}
                # also expire stickers in old messages, mirrors images.
                # In practice stickers are URL strings so the RAM saving is
                # tiny; this is for behavioral consistency more than memory.
                if m.get("sticker"):
                    m = {**m, "sticker": "", "sticker_expired": True}
                history_slice[i] = m
    await ws.send_json({"type": "history", "messages": history_slice})

    peer_list = [
        {"id": p, "name": room["peers"][p]["name"], "avatar": room["peers"][p]["avatar"],
         "is_host": room["peers"][p]["is_host"],
         "is_admin": room["peers"][p].get("is_admin", False),
         "muted": room["peers"][p]["muted"]}
        for p in existing
    ]
    await ws.send_json({"type": "peers", "peers": peer_list})

    # sync any existing host-avatar assignments to the newcomer so
    # they see overridden avatars on first paint instead of the original.
    if room_id in host_assigned_avatars:
        for tpid, avurl in host_assigned_avatars[room_id].items():
            try:
                await ws.send_json({"type": "peer_avatar_set",
                                     "target_pid": tpid, "avatar": avurl})
            except Exception:
                pass

    # sync any active stream state to the newcomer so they walk
    # into the same video everyone else is watching, at roughly the
    # right time. (The client reconciles drift on first state.)
    if room_id in streams:
        try:
            await ws.send_json({"type": "stream_state",
                                 "state": _stream_public_state(room_id)})
        except Exception:
            pass


    join_msg = {
        "type": "peer_joined",
        "peer": {"id": peer_id, "name": name, "avatar": avatar,
                 "is_host": is_host, "is_admin": is_admin, "muted": False},
    }
    for p in existing:
        try:
            await room["peers"][p]["ws"].send_json(join_msg)
        except Exception:
            pass

    sys_msg = {"type": "chat", "kind": "system",
               "text": f"{name} joined the call",
               "time": datetime.now().isoformat()}
    _append_msg(room_id, sys_msg)
    for p in existing:
        try:
            await room["peers"][p]["ws"].send_json(sys_msg)
        except Exception:
            pass

    ping_task = None

    async def pinger():
        try:
            while True:
                await asyncio.sleep(20)
                try:
                    await ws.send_json({"type": "ping", "t": time.time()})
                except Exception:
                    return
        except asyncio.CancelledError:
            return

    ping_task = asyncio.create_task(pinger())

    try:
        while True:
            msg = await ws.receive_json()
            mt = msg.get("type")

            if mt == "pong":
                continue

            # explicit leave from the client. Faster than waiting for
            # TCP close because we break out of the receive loop immediately;
            # the finally block then broadcasts peer_left. Used by both the
            # in-app "Leave" button and the beforeunload handler as a
            # complement to the beacon.
            if mt == "leave":
                print(f"[WS] {peer_id} ({name}) sent explicit leave")
                break

            # ── streaming WS handlers ──
            # Only the streamer can control playback. Server is the
            # authority — it broadcasts the canonical state on every
            # change, and viewers reconcile from there.
            if mt == "stream_start":
                if room_id in streams:
                    await ws.send_json({"type": "stream_error",
                                         "text": "A stream is already running"})
                    continue
                # playlist: list of {id, url, title}
                pl = msg.get("playlist", [])
                if not isinstance(pl, list) or not pl:
                    await ws.send_json({"type": "stream_error",
                                         "text": "Empty playlist"})
                    continue
                if len(pl) > MAX_STREAM_FILES_PER_STREAM:
                    pl = pl[:MAX_STREAM_FILES_PER_STREAM]
                # Validate each entry references a file we actually have.
                clean_pl = []
                file_paths = []
                for entry in pl:
                    if not isinstance(entry, dict):
                        continue
                    eid = str(entry.get("id", ""))
                    if not SAFE_STREAM_ID.match(eid):
                        continue
                    fp = os.path.join(STREAM_DIR, eid)
                    if not os.path.isfile(fp):
                        continue
                    title = str(entry.get("title", "Video"))[:80] or "Video"
                    clean_pl.append({
                        "id": eid,
                        "url": "/streams/" + eid,
                        "title": title,
                    })
                    file_paths.append(fp)
                if not clean_pl:
                    await ws.send_json({"type": "stream_error",
                                         "text": "No valid files in playlist"})
                    continue
                streams[room_id] = {
                    "streamer_pid": peer_id,
                    "streamer_name": name,
                    "playlist": clean_pl,
                    "idx": 0,
                    "playing": True,
                    "time": 0.0,
                    "last_update_at": time.time(),
                    "files": file_paths,
                }
                # Broadcast state to everyone.
                state = _stream_public_state(room_id)
                for p in room["peers"].values():
                    try:
                        await p["ws"].send_json({"type": "stream_state", "state": state})
                    except Exception:
                        pass
                print(f"[stream] room={room_id} started by {peer_id} ({len(clean_pl)} items)")
                continue

            if mt == "stream_control":
                st = streams.get(room_id)
                if not st:
                    await ws.send_json({"type": "stream_error",
                                         "text": "No stream running"})
                    continue
                if st["streamer_pid"] != peer_id:
                    await ws.send_json({"type": "stream_error",
                                         "text": "Only the streamer can control"})
                    continue
                action = msg.get("action", "")
                now = time.time()
                # Optional fields. We carefully validate / clamp them.
                changed = False
                if action == "play":
                    if not st["playing"]:
                        st["playing"] = True
                        # Preserve time as-is; client will sync to it.
                        st["last_update_at"] = now
                        changed = True
                elif action == "pause":
                    if st["playing"]:
                        # Freeze time at the actual moment of pause: time
                        # advances while playing, so derive current position
                        # from (server_now - last_update) + time.
                        elapsed = now - st["last_update_at"]
                        st["time"] = max(0.0, st["time"] + elapsed)
                        st["playing"] = False
                        st["last_update_at"] = now
                        changed = True
                elif action == "seek":
                    try:
                        new_time = float(msg.get("time", 0))
                    except (TypeError, ValueError):
                        new_time = 0
                    st["time"] = max(0.0, new_time)
                    st["last_update_at"] = now
                    changed = True
                elif action == "next":
                    if st["idx"] + 1 < len(st["playlist"]):
                        st["idx"] += 1
                        st["time"] = 0.0
                        st["playing"] = True
                        st["last_update_at"] = now
                        changed = True
                elif action == "prev":
                    if st["idx"] > 0:
                        st["idx"] -= 1
                        st["time"] = 0.0
                        st["playing"] = True
                        st["last_update_at"] = now
                        changed = True
                elif action == "goto":
                    try:
                        new_idx = int(msg.get("idx", 0))
                    except (TypeError, ValueError):
                        new_idx = -1
                    if 0 <= new_idx < len(st["playlist"]):
                        st["idx"] = new_idx
                        st["time"] = 0.0
                        st["playing"] = True
                        st["last_update_at"] = now
                        changed = True
                elif action == "tick":
                    # Heartbeat from the streamer to keep viewers' drift
                    # bounded. We accept a fresh `time` and re-mark
                    # last_update_at without flipping playing state.
                    try:
                        new_time = float(msg.get("time", st["time"]))
                    except (TypeError, ValueError):
                        new_time = st["time"]
                    st["time"] = max(0.0, new_time)
                    st["last_update_at"] = now
                    changed = True
                elif action == "ended":
                    # Streamer's video element fired 'ended'. If there's
                    # another track queued, advance. v3.32: otherwise
                    # loop the current track from the start instead of
                    # tearing the stream down. The streamer manually
                    # ends the stream via the Stop button or by leaving
                    # the call.
                    if st["idx"] + 1 < len(st["playlist"]):
                        st["idx"] += 1
                        st["time"] = 0.0
                        st["playing"] = True
                        st["last_update_at"] = now
                        changed = True
                    else:
                        # Loop the same video back to the start.
                        st["time"] = 0.0
                        st["playing"] = True
                        st["last_update_at"] = now
                        changed = True
                if changed:
                    state = _stream_public_state(room_id)
                    for p in room["peers"].values():
                        try:
                            await p["ws"].send_json({"type": "stream_state",
                                                      "state": state})
                        except Exception:
                            pass
                continue

            if mt == "stream_stop":
                st = streams.get(room_id)
                if not st:
                    continue
                if st["streamer_pid"] != peer_id:
                    await ws.send_json({"type": "stream_error",
                                         "text": "Only the streamer can stop"})
                    continue
                _stream_cleanup_paths(st.get("files", []))
                del streams[room_id]
                for p in room["peers"].values():
                    try:
                        await p["ws"].send_json({"type": "stream_state", "state": None})
                    except Exception:
                        pass
                print(f"[stream] room={room_id} stopped by {peer_id}")
                continue

            # ── Uno game dispatcher ──
            # Any message whose type starts with "uno_" is forwarded to the
            # uno module. uno needs a way to send messages back; we wrap
            # room["peers"][pid]["ws"].send_json into a single callable.
            if isinstance(mt, str) and mt.startswith("uno_"):
                async def _uno_send(target_pid, payload):
                    if target_pid is None:
                        for _p, _pd in room["peers"].items():
                            try:
                                await _pd["ws"].send_json(payload)
                            except Exception:
                                pass
                    else:
                        _pd = room["peers"].get(target_pid)
                        if _pd:
                            try:
                                await _pd["ws"].send_json(payload)
                            except Exception:
                                pass
                try:
                    await uno_mod.handle_ws(
                        room_id, peer_id, name, avatar, room, msg, _uno_send,
                    )
                except Exception as e:
                    print(f"[uno] handler error: {e}")
                continue

            # ── Zombie game dispatcher — same plumbing as Uno ──
            if isinstance(mt, str) and mt.startswith("zomb_"):
                async def _zomb_send(target_pid, payload):
                    if target_pid is None:
                        for _p, _pd in room["peers"].items():
                            try:
                                await _pd["ws"].send_json(payload)
                            except Exception:
                                pass
                    else:
                        _pd = room["peers"].get(target_pid)
                        if _pd:
                            try:
                                await _pd["ws"].send_json(payload)
                            except Exception:
                                pass
                try:
                    await zomb_mod.handle_ws(
                        room_id, peer_id, name, avatar, room, msg, _zomb_send,
                    )
                except Exception as e:
                    print(f"[zombie] handler error: {e}")
                continue

            if mt == "chat":
                text = msg.get("text", "").strip()[:1000]
                image = msg.get("image", "") or ""
                if image and (not isinstance(image, str) or len(image) > MAX_IMAGE_BYTES
                              or not image.startswith("data:image/")):
                    image = ""

                # sticker handling. Client sends just the filename
                # (e.g. "s3.jpg"); we validate and verify it exists. Stickers
                # are URLs not base64, so bandwidth/storage cost is trivial.
                sticker_raw = msg.get("sticker", "") or ""
                sticker = ""
                if sticker_raw and isinstance(sticker_raw, str) and len(sticker_raw) <= 64:
                    if SAFE_STICKER_NAME.match(sticker_raw):
                        ext = os.path.splitext(sticker_raw)[1].lower()
                        if ext in STICKER_EXTS:
                            sp = os.path.join(STICKERS_DIR, sticker_raw)
                            if os.path.isfile(sp):
                                sticker = sticker_raw

                if not text and not image and not sticker:
                    continue

                reply_to = None
                rt = msg.get("reply_to")
                if isinstance(rt, dict):
                    reply_to = {
                        "id": str(rt.get("id", ""))[:64],
                        "name": str(rt.get("name", ""))[:30],
                        "text": str(rt.get("text", ""))[:80],
                        "has_image": bool(rt.get("has_image")),
                        # replies-to-stickers
                        "has_sticker": bool(rt.get("has_sticker")),
                    }
                # view-once image flag
                view_once = bool(msg.get("view_once")) if image else False

                cm = {"type": "chat", "kind": "user",
                      "id": str(uuid.uuid4())[:12],
                      "peer_id": peer_id,
                      "name": name, "avatar": avatar, "text": text,
                      "is_admin": is_admin,  # server-trusted badge
                      "time": datetime.now().isoformat()}
                if image:
                    cm["image"] = image
                    if view_once:
                        cm["view_once"] = True
                        cm["opened_by"] = []  # tracks peer_ids who opened it
                if sticker:
                    cm["sticker"] = sticker
                if reply_to:
                    cm["reply_to"] = reply_to
                _append_msg(room_id, cm)
                for p, pd in room["peers"].items():
                    try:
                        if p == peer_id:
                            await pd["ws"].send_json({**cm, "self": True})
                        else:
                            await pd["ws"].send_json(cm)
                    except Exception:
                        pass

            # ── view-once message opened tracking ────────────────
            # Client sends { type: "msg_opened", msg_id: "..." } when they
            # view a view-once image. Server records the opener and broadcasts
            # to all room peers so the sender sees "Opened".
            elif mt == "msg_opened":
                target_msg_id = msg.get("msg_id", "")
                if not target_msg_id:
                    continue
                chat_file = f"{room_id}_chat.json"
                all_msgs = json_read(chat_file, [])
                for mm in all_msgs:
                    if mm.get("id") == target_msg_id and mm.get("view_once"):
                        opened_by = mm.get("opened_by", [])
                        if peer_id not in opened_by:
                            opened_by.append(peer_id)
                            mm["opened_by"] = opened_by
                            json_write(chat_file, all_msgs)
                        # Broadcast to everyone in the room
                        opened_payload = {
                            "type": "msg_opened",
                            "msg_id": target_msg_id,
                        }
                        for p_other, pd_other in room["peers"].items():
                            try:
                                await pd_other["ws"].send_json(opened_payload)
                            except Exception:
                                pass
                        break

            # ── message reactions ────────────────────────────────
            # Client sends { type: "react", msg_id: "...", emoji: "❤️" }
            # Toggle logic: same emoji again = remove. Different = replace.
            # Server persists in chat file and broadcasts to all room peers.
            elif mt == "react":
                target_msg_id = msg.get("msg_id", "")
                emoji = msg.get("emoji", "")
                if not target_msg_id or not emoji:
                    continue
                chat_file = f"{room_id}_chat.json"
                all_msgs = json_read(chat_file, [])
                found = False
                for mm in all_msgs:
                    if mm.get("id") == target_msg_id:
                        reactions = mm.get("reactions", {})
                        # Toggle: if same emoji, remove. Otherwise set/replace.
                        if peer_id in reactions and reactions[peer_id] == emoji:
                            del reactions[peer_id]
                        else:
                            reactions[peer_id] = emoji
                        if reactions:
                            mm["reactions"] = reactions
                        else:
                            mm.pop("reactions", None)
                        json_write(chat_file, all_msgs)
                        # Broadcast updated reactions to all room peers
                        payload = {
                            "type": "reaction",
                            "msg_id": target_msg_id,
                            "peer_id": peer_id,
                            "emoji": emoji,
                            "reactions": reactions,
                        }
                        for p_other, pd_other in room["peers"].items():
                            try:
                                await pd_other["ws"].send_json(payload)
                            except Exception:
                                pass
                        found = True
                        break
                if not found:
                    await ws.send_json({"type": "react_result", "ok": False,
                                        "error": "Message not found"})

            # host assigns avatar to a peer. Only the host (admin) can
            # do this. Storage is per-room; on broadcast every client receives
            # the new mapping and updates their UI to show the override.
            elif mt == "set_peer_avatar":
                if not is_admin:
                    await ws.send_json({"type": "peer_avatar_result", "ok": False,
                                        "error": "Only host can assign avatars"})
                    continue
                target_pid = msg.get("target_pid", "")
                avatar_url = msg.get("avatar", "")
                if not target_pid or not avatar_url:
                    continue
                # Basic safety: only allow our own /avatars/ URLs to be
                # broadcast, no arbitrary external URLs the host could try
                # to inject (which would let a compromised host force
                # everyone to load a tracker).
                if not str(avatar_url).startswith("/avatars/"):
                    continue
                if room_id not in host_assigned_avatars:
                    host_assigned_avatars[room_id] = {}
                host_assigned_avatars[room_id][target_pid] = avatar_url
                payload = {
                    "type": "peer_avatar_set",
                    "target_pid": target_pid,
                    "avatar": avatar_url,
                }
                for p_other, pd_other in room["peers"].items():
                    try:
                        await pd_other["ws"].send_json(payload)
                    except Exception:
                        pass

            elif mt in ("webrtc_offer", "webrtc_answer", "webrtc_ice", "request_relay"):
                # P2P mesh signaling is gone. Voice now flows through
                # LiveKit's SFU — clients connect directly to LiveKit and
                # never need the server to relay SDP/ICE. We accept and
                # silently drop any stragglers from cached clients so they
                # don't error out. Remove this branch entirely once you're
                # sure no one is running the old client anymore.
                pass

            elif mt in ("mute_me", "unmute_me"):
                room["peers"][peer_id]["muted"] = (mt == "mute_me")
                cmd = "mute_cmd" if mt == "mute_me" else "unmute_cmd"
                await ws.send_json({"type": cmd})
                st = {"type": "voice_state", "peer_id": peer_id,
                      "muted": room["peers"][peer_id]["muted"]}
                for p, pd in room["peers"].items():
                    if p != peer_id:
                        try:
                            await pd["ws"].send_json(st)
                        except Exception:
                            pass

            elif mt == "speaking":
                st = {"type": "speaking", "peer_id": peer_id, "level": msg.get("level", 0)}
                for p, pd in room["peers"].items():
                    if p != peer_id:
                        try:
                            await pd["ws"].send_json(st)
                        except Exception:
                            pass

            elif mt in ("typing_start", "typing_stop"):
                st = {"type": "typing", "peer_id": peer_id, "name": name,
                      "active": (mt == "typing_start")}
                for p, pd in room["peers"].items():
                    if p != peer_id:
                        try:
                            await pd["ws"].send_json(st)
                        except Exception:
                            pass

            # ── sticker upload ─────────────────────────────────────
            # Client sends { type: "sticker_upload", data_url: "data:image/...;base64,..." }
            # Anyone in the room can upload (no admin requirement). Limits:
            #   • payload ≤ MAX_STICKER_UPLOAD_BYTES (default 5 MB)
            #   • current count < MAX_STICKERS (default 30) — atomic check
            #   • Pillow available
            # Result: server resizes/reencodes to WebP, writes locally,
            # background-pushes to GitHub if creds are set, broadcasts the
            # new sticker list to every peer in every room (so other rooms
            # see new stickers too without rejoining).
            elif mt == "sticker_upload":
                if not PIL_OK:
                    await ws.send_json({"type": "sticker_result",
                                        "ok": False,
                                        "error": "Image processing unavailable on server"})
                    continue
                data_url = msg.get("data_url", "")
                if not isinstance(data_url, str) or not data_url.startswith("data:image/"):
                    await ws.send_json({"type": "sticker_result",
                                        "ok": False,
                                        "error": "Invalid image data"})
                    continue
                try:
                    _hdr, b64 = data_url.split(",", 1)
                except ValueError:
                    await ws.send_json({"type": "sticker_result",
                                        "ok": False, "error": "Malformed data URL"})
                    continue
                # Reject payloads that exceed the limit BEFORE decoding so
                # we never allocate a huge buffer.
                approx_decoded = (len(b64) * 3) // 4
                if approx_decoded > MAX_STICKER_UPLOAD_BYTES:
                    await ws.send_json({"type": "sticker_result", "ok": False,
                                        "error": f"Image too large (max {MAX_STICKER_UPLOAD_BYTES // (1024*1024)} MB)"})
                    continue
                try:
                    raw = base64.b64decode(b64, validate=False)
                except Exception:
                    await ws.send_json({"type": "sticker_result",
                                        "ok": False, "error": "Decode failed"})
                    continue
                if len(raw) > MAX_STICKER_UPLOAD_BYTES:
                    await ws.send_json({"type": "sticker_result", "ok": False,
                                        "error": "Image too large"})
                    continue

                # Atomic count check + filename reservation
                async with _sticker_count_lock:
                    if len(list_stickers()) >= MAX_STICKERS:
                        await ws.send_json({"type": "sticker_result", "ok": False,
                                            "error": f"Sticker limit reached ({MAX_STICKERS}). Ask admin to delete some."})
                        continue
                    fn = _generate_sticker_filename()

                # Heavy work: serialize through the upload semaphore so we
                # never have two Pillow decodes running simultaneously.
                async with _sticker_upload_sem:
                    loop = asyncio.get_event_loop()
                    processed = await loop.run_in_executor(
                        None, _process_sticker_image, raw)

                if not processed:
                    await ws.send_json({"type": "sticker_result", "ok": False,
                                        "error": "Could not process image"})
                    continue

                try:
                    os.makedirs(STICKERS_DIR, exist_ok=True)
                    out_path = os.path.join(STICKERS_DIR, fn)
                    with open(out_path, "wb") as f:
                        f.write(processed)
                except Exception as e:
                    print(f"[stickers] write err: {e}")
                    await ws.send_json({"type": "sticker_result", "ok": False,
                                        "error": "Could not save sticker"})
                    continue

                # Local save done — sticker is live for everyone right now.
                # Broadcast updated list to every connected peer in every room.
                new_list = list_stickers()
                payload = {"type": "stickers", "stickers": new_list}
                for r_id, r_data in rooms.items():
                    for p, pd in r_data.get("peers", {}).items():
                        try:
                            await pd["ws"].send_json(payload)
                        except Exception:
                            pass

                # AWAIT the GitHub commit before telling the user
                # success. Previously this was fire-and-forget via
                # asyncio.create_task, but on Render's free tier the worker
                # can spin down before the background task finishes the
                # HTTPS round-trip to GitHub — silently losing the commit.
                # Then on the next cold start the local file is wiped (Render's
                # ephemeral FS) and the sticker appears to "disappear".
                # Awaiting here adds 1-2s but makes persistence reliable.
                # If GitHub is misconfigured we surface the error to the user.
                gh_result = "skipped"
                gh_warning = ""
                if GITHUB_TOKEN and GITHUB_REPO:
                    ok_gh, gh_err = await _github_commit_sticker(fn, processed)
                    gh_result = "ok" if ok_gh else "failed"
                    if not ok_gh:
                        # Roll back local save so the cap doesn't drift, and
                        # tell the user to fix their GitHub config.
                        try:
                            os.remove(out_path)
                        except Exception:
                            pass
                        # re-broadcast list (now without the failed sticker)
                        new_list = list_stickers()
                        payload = {"type": "stickers", "stickers": new_list}
                        for r_id, r_data in rooms.items():
                            for p, pd in r_data.get("peers", {}).items():
                                try:
                                    await pd["ws"].send_json(payload)
                                except Exception:
                                    pass
                        await ws.send_json({"type": "sticker_result", "ok": False,
                                            "error": gh_err or "GitHub save failed"})
                        print(f"[stickers] rolled back {fn} (GitHub commit failed: {gh_err})")
                        continue
                else:
                    # GitHub creds missing → upload still works for
                    # this session, but it WILL be lost on next deploy/restart
                    # because Render's filesystem is ephemeral. Loud warning
                    # to the user so they understand why their stickers vanish
                    # after a redeploy. The sticker IS still added & broadcast
                    # — this just tells them persistence isn't wired up.
                    gh_warning = ("Sticker added — but GITHUB_TOKEN / GITHUB_REPO "
                                  "are not set on the server, so it will be "
                                  "lost on the next restart or redeploy.")
                    print(f"[stickers] WARNING: {fn} not committed to GitHub "
                          f"(GITHUB_TOKEN={bool(GITHUB_TOKEN)}, "
                          f"GITHUB_REPO={bool(GITHUB_REPO)}) — will not survive restart")

                result_msg = {"type": "sticker_result", "ok": True, "sticker": fn}
                if gh_warning:
                    result_msg["warning"] = gh_warning
                await ws.send_json(result_msg)
                print(f"[stickers] uploaded {fn} ({len(processed)} bytes) by {name} github={gh_result}")

            # ── sticker delete (admin only) ────────────────────────
            elif mt == "sticker_delete":
                if not room["peers"][peer_id].get("is_admin"):
                    await ws.send_json({"type": "sticker_result", "ok": False,
                                        "error": "Not authorized"})
                    continue
                target = msg.get("name", "")
                if not isinstance(target, str) or not SAFE_STICKER_NAME.match(target):
                    await ws.send_json({"type": "sticker_result", "ok": False,
                                        "error": "Invalid name"})
                    continue
                ext = os.path.splitext(target)[1].lower()
                if ext not in STICKER_EXTS:
                    await ws.send_json({"type": "sticker_result", "ok": False,
                                        "error": "Bad extension"})
                    continue
                path = os.path.join(STICKERS_DIR, target)
                real = os.path.realpath(path)
                base = os.path.realpath(STICKERS_DIR)
                if not (real.startswith(base + os.sep) or real == base):
                    await ws.send_json({"type": "sticker_result", "ok": False,
                                        "error": "Path violation"})
                    continue
                try:
                    if os.path.isfile(real):
                        os.remove(real)
                except Exception as e:
                    print(f"[stickers] delete err: {e}")
                    await ws.send_json({"type": "sticker_result", "ok": False,
                                        "error": "Delete failed"})
                    continue

                new_list = list_stickers()
                payload = {"type": "stickers", "stickers": new_list}
                for r_id, r_data in rooms.items():
                    for p, pd in r_data.get("peers", {}).items():
                        try:
                            await pd["ws"].send_json(payload)
                        except Exception:
                            pass

                # await GitHub delete so we know it persisted.
                # If GitHub call fails, the local file is already gone, so
                # the sticker would re-appear on the next cold start when
                # the bot pulls from GitHub again. Tell admin to retry.
                if GITHUB_TOKEN and GITHUB_REPO:
                    ok_gh = await _github_delete_sticker(target)
                    if not ok_gh:
                        await ws.send_json({"type": "sticker_result", "ok": False,
                                            "error": "Local deleted, but GitHub delete failed — sticker may return on restart"})
                        print(f"[stickers] deleted {target} locally but GitHub delete failed")
                        continue

                await ws.send_json({"type": "sticker_result", "ok": True,
                                    "deleted": target})
                print(f"[stickers] deleted {target} by admin {name}")

            # ── message deletion (users can only delete their own) ──
            # Client sends { type: "delete_msg", msg_id: "..." }
            # Server verifies the message was sent by this peer_id, marks it
            # deleted in the chat file, and broadcasts to all room peers.
            elif mt == "delete_msg":
                target_msg_id = msg.get("msg_id", "")
                if not target_msg_id:
                    await ws.send_json({"type": "delete_result", "ok": False,
                                        "error": "No message ID"})
                    continue
                chat_file = f"{room_id}_chat.json"
                all_msgs = json_read(chat_file, [])
                found = False
                for mm in all_msgs:
                    if mm.get("id") == target_msg_id:
                        # Ownership check: only the original sender can delete
                        if mm.get("peer_id") != peer_id:
                            await ws.send_json({"type": "delete_result", "ok": False,
                                                "error": "You can only delete your own messages"})
                            found = True
                            break
                        # Mark deleted: preserve metadata (name, avatar, time)
                        # but strip all content
                        mm["deleted"] = True
                        mm["text"] = ""
                        mm["image"] = ""
                        mm["sticker"] = ""
                        json_write(chat_file, all_msgs)
                        await ws.send_json({"type": "delete_result", "ok": True,
                                            "msg_id": target_msg_id})
                        # Broadcast deletion to everyone in the room
                        deletion_payload = {
                            "type": "msg_deleted",
                            "msg_id": target_msg_id,
                            "peer_id": mm.get("peer_id"),
                            "name": mm.get("name"),
                            "avatar": mm.get("avatar"),
                            "is_admin": mm.get("is_admin"),
                            "time": mm.get("time"),
                        }
                        for p_other, pd_other in room["peers"].items():
                            try:
                                await pd_other["ws"].send_json(deletion_payload)
                            except Exception:
                                pass
                        found = True
                        print(f"[delete] {name} deleted msg {target_msg_id}")
                        break
                if not found:
                    await ws.send_json({"type": "delete_result", "ok": False,
                                        "error": "Message not found"})

    except WebSocketDisconnect:
        pass
    except Exception as e:
        print(f"[WS] {peer_id} error: {e}")
    finally:
        if ping_task is not None:
            ping_task.cancel()
            try:
                await ping_task
            except asyncio.CancelledError:
                pass
        if peer_id in room["peers"]:
            del room["peers"][peer_id]
        # clean up any host-assigned avatar for this peer
        if room_id in host_assigned_avatars and peer_id in host_assigned_avatars[room_id]:
            del host_assigned_avatars[room_id][peer_id]
        lm = {"type": "peer_left", "peer_id": peer_id, "name": name}
        sm = {"type": "chat", "kind": "system",
              "text": f"{name} left the call",
              "time": datetime.now().isoformat()}
        for p, pd in list(room["peers"].items()):
            try:
                await pd["ws"].send_json(lm)
                await pd["ws"].send_json(sm)
            except Exception:
                pass

        # notify uno module that this peer left. uno will broadcast
        # any necessary uno_state / uno_event updates via the supplied send.
        try:
            async def _uno_send_after_leave(target_pid, payload):
                if target_pid is None:
                    for _p, _pd in room["peers"].items():
                        try:
                            await _pd["ws"].send_json(payload)
                        except Exception:
                            pass
                else:
                    _pd = room["peers"].get(target_pid)
                    if _pd:
                        try:
                            await _pd["ws"].send_json(payload)
                        except Exception:
                            pass
            await uno_mod.on_peer_leave(room_id, peer_id, room, _uno_send_after_leave)
        except Exception as e:
            print(f"[uno] on_peer_leave error: {e}")

        # parallel Zombie hook — same _uno_send_after_leave callable
        # works because it just dispatches by pid to room["peers"].
        try:
            await zomb_mod.on_peer_leave(room_id, peer_id, room, _uno_send_after_leave)
        except Exception as e:
            print(f"[zombie] on_peer_leave error: {e}")

        # if the leaver was the active streamer, kill the stream and
        # tell everyone. Files are removed; viewers' UIs collapse back to
        # the normal call layout.
        st = streams.get(room_id)
        if st and st.get("streamer_pid") == peer_id:
            _stream_cleanup_paths(st.get("files", []))
            del streams[room_id]
            for p in room["peers"].values():
                try:
                    await p["ws"].send_json({"type": "stream_state", "state": None})
                except Exception:
                    pass
            print(f"[stream] room={room_id} ended — streamer {peer_id} left")

        if not room["peers"]:
            async def cleanup_later():
                await asyncio.sleep(60)
                if room_id in rooms and not rooms[room_id]["peers"]:
                    f = f"{room_id}_chat.json"
                    if os.path.exists(f):
                        try:
                            os.remove(f)
                        except Exception:
                            pass
                    for tk in [k for k, v in tokens.items() if v.get("room_id") == room_id]:
                        del tokens[tk]
                    if room_id in rooms:
                        del rooms[room_id]
                        print(f"[WS] cleaned up empty room {room_id}")
                    # also clean up host-assigned avatars for the room
                    if room_id in host_assigned_avatars:
                        del host_assigned_avatars[room_id]
                    # drop any uno game state for the room
                    try:
                        uno_mod.on_room_cleanup(room_id)
                    except Exception as e:
                        print(f"[uno] on_room_cleanup error: {e}")
                    # drop any zombie game state for the room
                    try:
                        zomb_mod.on_room_cleanup(room_id)
                    except Exception as e:
                        print(f"[zombie] on_room_cleanup error: {e}")
                    # also tear down any stream + delete its files
                    if room_id in streams:
                        _stream_cleanup_paths(streams[room_id].get("files", []))
                        del streams[room_id]
                    # update persisted registry after cleanup
                    _persist_rooms()
                    _persist_tokens()
            asyncio.create_task(cleanup_later())


async def _noshow(room_id):
    await asyncio.sleep(60)
    r = rooms.get(room_id)
    if r is None or r["peers"]:
        return
    f = f"{room_id}_chat.json"
    if os.path.exists(f):
        try:
            os.remove(f)
        except Exception:
            pass
    for tk in [k for k, v in tokens.items() if v.get("room_id") == room_id]:
        del tokens[tk]
    if room_id in rooms:
        del rooms[room_id]
    print(f"[WS] expired never-joined room {room_id}")
    # update persisted registry after cleanup
    _persist_rooms()
    _persist_tokens()


def _append_msg(rid, msg):
    """Append a message to the room's chat file. v3.9 memory hardening:
    - Caps file at MAX_CHAT_MESSAGES (200) — drops oldest beyond that.
    - Strips images from messages older than IMAGE_RETAIN_COUNT (30).
    v3.10: also strips stickers (URL strings, but kept consistent).
    """
    p = f"{rid}_chat.json"
    m = json_read(p, [])
    m.append(msg)

    # Cap total messages
    if len(m) > MAX_CHAT_MESSAGES:
        m = m[-MAX_CHAT_MESSAGES:]

    # Expire image/sticker payloads on older messages
    if len(m) > IMAGE_RETAIN_COUNT:
        cutoff = len(m) - IMAGE_RETAIN_COUNT
        for i in range(cutoff):
            if isinstance(m[i], dict):
                if m[i].get("image"):
                    m[i]["image"] = ""
                    m[i]["image_expired"] = True
                if m[i].get("sticker"):
                    m[i]["sticker"] = ""
                    m[i]["sticker_expired"] = True

    json_write(p, m)


# ─── CALL UI (HTML/CSS/JS) ──────────────────────────────────────────────────
CALL_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0, user-scalable=no, viewport-fit=cover, interactive-widget=resizes-content">
<meta name="apple-mobile-web-app-capable" content="yes">
<meta name="mobile-web-app-capable" content="yes">
<title>Silent Hill</title>
<!-- LiveKit client SDK (UMD build, exposes global LivekitClient). -->
<script src="https://cdn.jsdelivr.net/npm/livekit-client@2.5.7/dist/livekit-client.umd.min.js"></script>
<style>
*{margin:0;padding:0;box-sizing:border-box;-webkit-tap-highlight-color:transparent}
/* ════════════════════════════════════════════════════════════════════════
   VIEWPORT LOCK — keep the header + seat panel fixed when keyboard opens
   ════════════════════════════════════════════════════════════════════════
   On mobile, when an input is focused and the on-screen keyboard appears,
   the browser will lift the entire page up to keep the focused field
   visible — pushing the header and seat panel out of view at the top.

   Two-part fix:
     1. html/body are LOCKED at the layout viewport size, with all scroll
        and overscroll disabled. The browser has no page-level scroll
        container to lift, so it can't move the page up.
     2. .app is `position:fixed; inset:0` so it tracks the *visual* viewport
        directly. When the keyboard opens, the visual viewport shrinks
        from below and `inset:0` follows it (browsers anchor fixed
        elements to the visual viewport when a soft keyboard is open).
        The header stays at the top, the input bar stays at the bottom of
        what's visible, and only the chat-stack in between absorbs the
        change.
   ════════════════════════════════════════════════════════════════════════ */
html,body{height:100%;overflow:hidden;overscroll-behavior:none;position:fixed;inset:0;width:100%;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;background:#000;color:#fff;touch-action:manipulation}
.app{display:flex;flex-direction:column;position:fixed;inset:0;height:100dvh;max-height:100dvh}
.bg{position:fixed;inset:0;z-index:0;background:url('/bg.jpg') center/cover no-repeat;opacity:0.4}
.bg::after{content:'';position:absolute;inset:0;background:linear-gradient(180deg,rgba(0,0,0,0.6),rgba(0,0,0,0.3),rgba(0,0,0,0.7))}
.header{position:relative;z-index:10;background:rgba(13,13,13,0.95);backdrop-filter:blur(20px);border-bottom:1px solid rgba(255,255,255,0.06);padding:8px 12px;display:flex;align-items:center;gap:10px;flex-shrink:0}
.back-btn,.menu-btn{width:36px;height:36px;display:flex;align-items:center;justify-content:center;background:none;border:none;color:#fff;font-size:20px;cursor:pointer}
.group-icon{width:36px;height:36px;border-radius:50%;object-fit:cover;border:1px solid rgba(255,255,255,0.1)}
.group-info{flex:1;min-width:0}
.group-name{font-size:15px;font-weight:600}
.group-meta{font-size:12px;color:#8e8e93}
/* ════════════════════════════════════════════════════════════════════════
   CHAT-STACK — single relative container that holds the seat panel (as an
   overlay) and the messages list (filling the area underneath). This is
   what makes the layout keyboard-proof: the seat panel is absolute inside
   chat-stack, so when the keyboard opens and chat-stack shrinks, the
   panel stays anchored at the top of the visible area while the messages
   are squeezed but still visible. Older messages flow up behind the panel
   as you scroll; collapsing the panel reveals what was behind it.
   ════════════════════════════════════════════════════════════════════════ */
.chat-stack{flex:1;min-height:0;position:relative;z-index:5;overflow:hidden}
.messages-wrap{position:absolute;inset:0;z-index:5;overflow:hidden}

/* ════════════════════════════════════════════════════════════════════════
   v3.11 MESSAGES LIST — REVERSE FLEX (WhatsApp/Telegram/Instagram pattern)
   ════════════════════════════════════════════════════════════════════════
   The list uses `flex-direction: column-reverse`, which has three crucial
   browser-level properties:

     1. The visual bottom is the DOM first child. To put a "newer" message
        at the visual bottom, JS uses insertBefore(node, container.firstChild)
        instead of appendChild. (The render code below does exactly that.)

     2. Scroll is anchored to the bottom by default. When new content is
        added at the visual bottom, the browser keeps the bottom in view
        — there is no flicker, no manual scrollTop math needed. This is
        the magic that makes new messages always visible no matter how
        the viewport shifts (mobile keyboard opening, address bar
        collapsing, orientation change).

     3. scrollTop is 0 when scrolled to the visual bottom, and becomes
        more NEGATIVE as the user scrolls UP into older messages.
        (On Chrome/Safari/Firefox today scrollTop goes 0 → -N upward;
        a few older Chromes used 0 → +N. The JS uses Math.abs() so it
        works on either convention.)

   `justify-content: flex-end` ensures that when the list is shorter than
   the viewport (e.g. just opened, only a system message), the content
   sits at the bottom of the panel, not floating at the top.
   ════════════════════════════════════════════════════════════════════════ */
.messages{height:100%;overflow-y:auto;padding:12px 12px 16px;display:flex;flex-direction:column-reverse;justify-content:flex-start;gap:6px;scroll-behavior:auto;overflow-anchor:none}
.messages::-webkit-scrollbar{width:0}
.msg-system{text-align:center;color:#8e8e93;font-size:12px;padding:6px 0}
.msg-row{display:flex;gap:8px;max-width:85%;animation:msgIn .2s ease-out;align-items:flex-start}
.msg-row.self{align-self:flex-end;flex-direction:row-reverse}
.msg-row.other{align-self:flex-start}
@keyframes msgIn{from{opacity:0;transform:translateY(6px)}to{opacity:1;transform:translateY(0)}}
.avatar{width:36px;height:36px;border-radius:50%;flex-shrink:0;background:#2c2c2e;display:flex;align-items:center;justify-content:center;font-size:14px;font-weight:600;color:#8e8e93;overflow:hidden}
.avatar img{width:100%;height:100%;object-fit:cover}
.msg-content{display:flex;flex-direction:column;gap:2px;min-width:0;max-width:260px}
.msg-header{display:flex;align-items:center;gap:6px}
.msg-row.self .msg-header{flex-direction:row-reverse}
.msg-name{font-size:12px;font-weight:600;max-width:120px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.msg-badge{font-size:9px;font-weight:700;padding:2px 8px;border-radius:10px;text-transform:uppercase}
.msg-badge.host{background:#007aff}
.msg-badge.cohost{background:#3a3a3c}
.msg-bubble{padding:8px 14px;border-radius:18px;font-size:14px;line-height:1.4;word-break:break-word;white-space:pre-wrap}
.msg-row.other .msg-bubble{background:#2c2c2e;border-bottom-left-radius:4px}
.msg-row.self .msg-bubble{background:#007aff;border-bottom-right-radius:4px}
.msg-row{cursor:pointer;transition:opacity .15s;position:relative;touch-action:pan-y;user-select:none;-webkit-user-select:none}
.msg-row:active{opacity:0.6}
.msg-row.system{cursor:default}
.msg-row.system:active{opacity:1}
/* ─── v3.13: swipe-to-reply + long-press delete ─── */
.msg-content{transition:transform .3s cubic-bezier(.2,.7,.2,1);will-change:transform}
.msg-delete-btn{position:absolute;top:50%;right:6px;transform:translateY(-50%) scale(0.5);width:34px;height:34px;border-radius:50%;background:#ff3b30;border:none;color:#fff;display:flex;align-items:center;justify-content:center;cursor:pointer;z-index:5;box-shadow:0 2px 10px rgba(0,0,0,.5);opacity:0;padding:0;animation:binPop .28s cubic-bezier(.34,1.56,.64,1) forwards}
@keyframes binPop{from{opacity:0;transform:translateY(-50%) scale(0.4)}to{opacity:1;transform:translateY(-50%) scale(1)}}
.msg-row.self .msg-delete-btn{right:auto;left:6px}
.msg-delete-btn svg{width:16px;height:16px;pointer-events:none}
.msg-delete-btn:active{transform:translateY(-50%) scale(.85);transition:transform .1s}
.msg-deleted{color:#8e8e93;font-size:13px;font-style:italic;padding:6px 0;opacity:.65}
/* ─── v3.13: View Once image placeholders (compact pill style) ───
   Small inline pill like WhatsApp/Instagram — icon + "Photo" text.
   Each user sees "Opened" only after THEY personally open it. */
.viewonce-card{display:inline-flex;align-items:center;gap:6px;padding:8px 16px;border-radius:18px;background:#007aff;color:#fff;font-size:14px;font-weight:600;cursor:pointer;transition:transform .15s,opacity .2s;user-select:none;-webkit-user-select:none}
.viewonce-card:active{transform:scale(.95)}
.viewonce-card .vo-icon-wrap{position:relative;width:20px;height:20px;display:flex;align-items:center;justify-content:center}
.viewonce-card .vo-icon-wrap svg{width:20px;height:20px}
.viewonce-card .vo-num{position:absolute;font-size:9px;font-weight:800;color:#fff;top:50%;left:50%;transform:translate(-50%,-50%);margin-top:0.5px}
.viewonce-opened{display:inline-flex;align-items:center;gap:6px;padding:8px 16px;border-radius:18px;background:rgba(255,255,255,0.08);color:rgba(255,255,255,0.45);font-size:14px;font-weight:500;pointer-events:none;user-select:none}
.viewonce-opened svg{width:16px;height:16px;opacity:.5}
/* ─── v3.13: Image send preview overlay (WhatsApp style) ─── */
.img-send-overlay{position:fixed;inset:0;z-index:350;background:rgba(0,0,0,0.93);display:flex;flex-direction:column;animation:msgIn .2s}
.img-send-preview{flex:1;display:flex;align-items:center;justify-content:center;padding:20px;min-height:0}
.img-send-preview img{max-width:95vw;max-height:65vh;border-radius:12px;object-fit:contain}
/* Bottom bar: caption on top row, buttons on bottom row — always fits on mobile */
.img-send-bar{background:rgba(20,20,22,0.98);padding:10px 12px calc(10px + env(safe-area-inset-bottom));display:flex;flex-direction:column;gap:8px;border-top:1px solid rgba(255,255,255,0.06)}
.img-send-caption{width:100%;height:44px;border-radius:22px;border:1px solid rgba(255,255,255,0.1);background:rgba(255,255,255,0.05);color:#fff;padding:0 16px;font-size:15px;outline:none}
.img-send-caption::placeholder{color:#8e8e93}
.img-send-actions{display:flex;align-items:center;gap:8px;justify-content:flex-end}
.img-send-btn{height:40px;padding:0 20px;border-radius:20px;border:none;background:#007aff;color:#fff;font-size:14px;font-weight:600;cursor:pointer;white-space:nowrap;flex-shrink:0;display:flex;align-items:center;justify-content:center;gap:4px}
.img-send-btn.vo-btn{background:linear-gradient(135deg,#ff9500,#ff6b00);color:#fff}
.img-send-btn:active{transform:scale(.93)}
.img-send-cancel{height:40px;padding:0 16px;border-radius:20px;border:1px solid rgba(255,255,255,0.15);background:transparent;color:#8e8e93;font-size:14px;cursor:pointer;flex-shrink:0;white-space:nowrap}
.img-send-cancel:active{opacity:.7}
/* View Once button: icon + short label on all screens */
.img-send-btn.vo-btn .vo-text{font-size:13px}
/* ─── v3.14: Message reactions ─── */
/* Position is set via JS (fixed, top/left) — never clipped by viewport edges */
.react-bar{display:flex;align-items:center;gap:3px;padding:5px 8px;border-radius:22px;background:#2c2c2e;border:1px solid rgba(255,255,255,0.08);box-shadow:0 6px 24px rgba(0,0,0,0.5);z-index:500;opacity:0;animation:reactPop .22s cubic-bezier(.34,1.56,.64,1) forwards;white-space:nowrap}
@keyframes reactPop{from{opacity:0;transform:scale(0.75)}to{opacity:1;transform:scale(1)}}
.react-bar button{width:38px;height:38px;border-radius:50%;border:none;background:transparent;font-size:22px;cursor:pointer;display:flex;align-items:center;justify-content:center;transition:transform .12s,background .12s;padding:0;line-height:1;touch-action:manipulation;-webkit-tap-highlight-color:transparent}
.react-bar button:hover{background:rgba(255,255,255,0.1);transform:scale(1.18)}
.react-bar button:active{transform:scale(.88)}
.react-bar .react-more{width:34px;height:34px;background:rgba(255,255,255,0.07);border-radius:50%;display:flex;align-items:center;justify-content:center}
.react-bar .react-more svg{width:15px;height:15px;color:#8e8e93}
/* Emoji picker overlay */
.emoji-picker-overlay{position:fixed;inset:0;z-index:400;background:rgba(0,0,0,0.55);display:flex;align-items:center;justify-content:center;animation:msgIn .15s}
.emoji-picker-panel{width:min(360px,95vw);max-height:min(480px,80vh);border-radius:18px;background:#1c1c1e;border:1px solid rgba(255,255,255,0.06);display:flex;flex-direction:column;overflow:hidden;box-shadow:0 16px 48px rgba(0,0,0,0.6)}
.emoji-picker-header{display:flex;align-items:center;justify-content:space-between;padding:14px 18px;border-bottom:1px solid rgba(255,255,255,0.06)}
.emoji-picker-header span{font-size:16px;font-weight:600;color:#fff}
.emoji-picker-header button{background:transparent;border:none;color:#8e8e93;font-size:20px;cursor:pointer;padding:4px;line-height:1}
.emoji-picker-header button:active{opacity:.6}
.emoji-picker-grid{flex:1;overflow-y:auto;padding:14px;display:grid;grid-template-columns:repeat(8,1fr);gap:3px}
.emoji-picker-grid button{aspect-ratio:1;border-radius:10px;border:none;background:transparent;font-size:24px;cursor:pointer;display:flex;align-items:center;justify-content:center;transition:background .1s,transform .1s;padding:0;line-height:1}
.emoji-picker-grid button:hover{background:rgba(255,255,255,0.1);transform:scale(1.08)}
.emoji-picker-grid button:active{transform:scale(.92)}
/* Reaction badges */
.reactions-row{display:flex;flex-wrap:wrap;gap:4px;margin-top:5px;padding:0 2px;pointer-events:auto;position:relative;z-index:2}
.msg-row.self .reactions-row{justify-content:flex-end}
.msg-row.other .reactions-row{justify-content:flex-start}
.reaction-badge{display:inline-flex;align-items:center;gap:3px;padding:3px 8px 3px 6px;border-radius:10px;background:rgba(255,255,255,0.07);border:1px solid rgba(255,255,255,0.06);font-size:12px;cursor:pointer;transition:background .15s;user-select:none;line-height:1;pointer-events:auto;position:relative;z-index:3}
.reaction-badge:hover{background:rgba(255,255,255,0.12)}
.reaction-badge:active{transform:scale(.95)}
.reaction-badge .react-count{font-size:11px;font-weight:700;color:#8e8e93;min-width:10px;text-align:center;margin-left:1px;pointer-events:none}
.reaction-badge.mine{background:rgba(0,122,255,0.18);border-color:rgba(0,122,255,0.3)}
.reaction-badge.mine .react-count{color:#64b5f6}

/* ════════════════════════════════════════════════════════════════════════════
   v3.22 — MESSAGE CLICK PANEL (Copy + Reply) — opens above tapped message
   ════════════════════════════════════════════════════════════════════════════ */
.msg-click-panel{position:fixed;z-index:200;background:rgba(42,42,46,0.97);backdrop-filter:blur(16px);-webkit-backdrop-filter:blur(16px);border-radius:16px;padding:6px;border:1px solid rgba(255,255,255,0.08);box-shadow:0 12px 40px rgba(0,0,0,0.55),0 2px 8px rgba(0,0,0,0.25);min-width:150px;overflow:hidden;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Helvetica,Arial,sans-serif}
.msg-click-panel-item{display:flex;align-items:center;gap:12px;padding:11px 16px;border-radius:10px;cursor:pointer;transition:background .12s,color .12s;color:#fff;font-size:14px;font-weight:500;user-select:none;-webkit-user-select:none}
.msg-click-panel-item:hover{background:rgba(255,255,255,0.1)}
.msg-click-panel-item:active{background:rgba(255,255,255,0.14)}
.msg-click-panel-icon{width:18px;height:18px;display:flex;align-items:center;justify-content:center;color:rgba(255,255,255,0.65);flex-shrink:0}
.msg-click-panel-icon svg{width:100%;height:100%}
.msg-click-panel-sep{height:1px;background:rgba(255,255,255,0.08);margin:2px 10px}

/* ════════════════════════════════════════════════════════════════════════════
   v3.22 — HOST AVATAR PICKER — host clicks an empty avatar to assign one
   ════════════════════════════════════════════════════════════════════════════ */
.av-picker{position:fixed;z-index:9999;background:rgba(38,38,42,0.98);backdrop-filter:blur(18px);-webkit-backdrop-filter:blur(18px);border-radius:18px;padding:14px;border:1px solid rgba(255,255,255,0.1);box-shadow:0 16px 48px rgba(0,0,0,0.6),0 4px 12px rgba(0,0,0,0.3);opacity:0;transform:scale(0.88) translateY(10px);transition:opacity .16s ease,transform .2s cubic-bezier(.2,.7,.2,1);pointer-events:auto}
.av-picker.show{opacity:1;transform:scale(1) translateY(0)}
.av-picker-title{font-size:13px;font-weight:600;color:rgba(255,255,255,0.7);margin-bottom:10px;text-align:center;letter-spacing:0.3px}
.av-picker-grid{display:grid;grid-template-columns:repeat(4,56px);gap:8px;justify-content:center}
.av-picker-item{width:56px;height:56px;border-radius:50%;overflow:hidden;cursor:pointer;transition:transform .15s,box-shadow .15s;border:2px solid transparent;background:#2c2c2e;box-sizing:border-box;-webkit-tap-highlight-color:rgba(255,255,255,0.2)}
.av-picker-item:hover{transform:scale(1.12);box-shadow:0 4px 16px rgba(0,0,0,0.4)}
.av-picker-item:active{transform:scale(0.95)}
.av-picker-item img{width:100%;height:100%;object-fit:cover;display:block;pointer-events:none}
.seat-av[data-assignable="true"]{cursor:pointer}
.seat-av[data-assignable="true"]:hover{box-shadow:0 0 0 2px rgba(255,193,7,0.6) inset}

.chat-img{max-width:240px;max-height:300px;border-radius:12px;display:block;cursor:pointer;margin:2px 0}
.img-expired{display:flex;align-items:center;gap:8px;padding:10px 12px;border-radius:8px;background:rgba(255,255,255,0.06);color:rgba(255,255,255,0.55);font-size:12px;font-style:italic;margin:2px 0}
.img-expired svg{width:18px;height:18px;flex-shrink:0;opacity:0.6}
.msg-bubble.has-img{padding:4px;overflow:hidden}
.msg-bubble.has-img.has-text{padding-bottom:8px}
.msg-bubble.has-img .msg-text{padding:4px 10px 0}

/* ───── STICKERS (v3.10) ───── */
/* A sticker message renders WITHOUT a bubble — just the image inline next to
   the avatar and name header. Max 140px on any side, no chrome, not clickable
   to open a preview. Like Kyodo, like Telegram, like Discord. */
.msg-row.sticker .msg-bubble,
.msg-row.has-sticker-only .msg-bubble{background:transparent;padding:0;border-radius:0}
.sticker-img{max-width:140px;max-height:140px;width:auto;height:auto;display:block;margin:2px 0;user-select:none;-webkit-user-select:none;pointer-events:none}
.sticker-expired{display:inline-flex;align-items:center;gap:6px;padding:8px 10px;border-radius:10px;background:rgba(255,255,255,0.06);color:rgba(255,255,255,0.55);font-size:12px;font-style:italic;margin:2px 0}
.sticker-expired svg{width:16px;height:16px;flex-shrink:0;opacity:0.6}

.msg-reply{border-left:3px solid rgba(255,255,255,0.55);padding:4px 8px;margin-bottom:4px;font-size:12px;background:rgba(255,255,255,0.08);border-radius:6px;display:flex;flex-direction:column;gap:1px;max-width:240px}
.msg-bubble.has-img .msg-reply{margin:4px 4px 4px}
.msg-reply-name{font-weight:600;color:rgba(255,255,255,0.95);overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.msg-reply-text{opacity:0.75;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
/* If the replied-to message contains ONLY a sticker, the reply preview lives
   above the sticker image instead of inside a bubble (since stickers have no
   bubble). Keep the reply card width sane. */
.msg-row.has-sticker-only .msg-reply{max-width:200px;margin-bottom:6px}

.jump-latest{position:absolute;right:14px;bottom:14px;width:42px;height:42px;border-radius:50%;background:#007aff;color:#fff;border:none;display:none;align-items:center;justify-content:center;cursor:pointer;box-shadow:0 4px 14px rgba(0,0,0,0.5);z-index:6;animation:msgIn .2s}
.jump-latest.show{display:flex}
.jump-latest svg{width:20px;height:20px;pointer-events:none}
.jump-latest .badge{position:absolute;top:-4px;right:-4px;min-width:20px;height:20px;background:#ff3b30;border-radius:10px;font-size:11px;font-weight:700;display:flex;align-items:center;justify-content:center;padding:0 6px;border:2px solid #0d0d0d}
.jump-latest .badge.hidden{display:none}

.reply-bar{position:relative;z-index:10;background:rgba(28,28,30,0.95);border-top:1px solid rgba(255,255,255,0.06);padding:8px 12px;display:flex;align-items:center;gap:8px;flex-shrink:0;animation:msgIn .15s}
.reply-bar-content{flex:1;display:flex;flex-direction:column;gap:2px;min-width:0;border-left:3px solid #007aff;padding-left:8px}
.reply-bar-label{font-size:11px;color:#007aff;font-weight:600}
.reply-bar-text{font-size:13px;color:#8e8e93;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.reply-bar-close{width:28px;height:28px;border-radius:50%;border:none;background:#3a3a3c;color:#fff;font-size:18px;cursor:pointer;display:flex;align-items:center;justify-content:center;flex-shrink:0;line-height:1}
.img-preview-overlay{position:fixed;inset:0;z-index:300;background:rgba(0,0,0,0.95);display:flex;align-items:center;justify-content:center;cursor:pointer;animation:msgIn .2s;padding:20px}
.img-preview-overlay img{max-width:95vw;max-height:90vh;border-radius:8px}
.img-preview-overlay .close-hint{position:absolute;top:20px;right:20px;color:#fff;font-size:32px;opacity:0.7}

.typing-bar{position:relative;z-index:10;background:transparent;padding:2px 14px 4px;font-size:11px;color:rgba(255,255,255,0.55);font-style:italic;flex-shrink:0;min-height:18px;line-height:1.3;letter-spacing:0.2px}
.typing-bar.hidden{display:none!important}

/* ───── INPUT BAR (v3.10 — textarea + sticker icon inside input) ───── */
/* The input bar gets a wrapper around the textarea so we can absolutely
   position the sticker icon inside it on the right. The textarea grows
   from 1 line up to 3 lines (max-height: 84px), then scrolls internally. */
.input-bar{position:relative;z-index:10;background:rgba(13,13,13,0.95);border-top:1px solid rgba(255,255,255,0.06);padding:8px 12px;display:flex;align-items:flex-end;gap:8px;flex-shrink:0}
.input-attach,.input-send{width:38px;height:38px;border-radius:50%;border:none;display:flex;align-items:center;justify-content:center;cursor:pointer;flex-shrink:0}
.input-attach{background:#2c2c2e;color:#fff;font-size:18px}
.input-send{background:#007aff;color:#fff;font-size:16px}
.input-send:active{transform:scale(.92)}
/* Wrapper is what the textarea + sticker icon live in. Padding-right gives
   the sticker icon a reserved 38px gutter so the cursor never collides. */
.input-wrap{flex:1;position:relative;display:flex;align-items:flex-end;background:#1c1c1e;border-radius:19px;min-height:38px;max-height:84px;transition:none}
.input-field{flex:1;width:100%;border:none;background:transparent;color:#fff;padding:9px 44px 9px 14px;font-size:14px;outline:none;line-height:1.4;font-family:inherit;resize:none;max-height:84px;overflow-y:auto;-webkit-appearance:none;appearance:none;border-radius:19px}
.input-field::placeholder{color:#8e8e93}
.input-field::-webkit-scrollbar{width:0}
/* Sticker icon button — sits absolutely inside the input wrapper on the
   right edge, vertically anchored to the bottom row of the textarea so it
   stays in line with the cursor when the textarea grows. Hidden when the
   user has typed any text. */
.sticker-btn{position:absolute;right:4px;bottom:3px;width:32px;height:32px;border-radius:50%;border:none;background:transparent;color:#8e8e93;display:flex;align-items:center;justify-content:center;cursor:pointer;padding:0;transition:color .15s,opacity .15s,transform .15s}
.sticker-btn:active{transform:scale(.9)}
.sticker-btn:hover{color:#fff}
.sticker-btn svg{width:22px;height:22px;pointer-events:none}
.sticker-btn.hidden{opacity:0;pointer-events:none;transform:scale(.8)}
.mic-btn{width:32px;height:32px;border-radius:50%;border:none;background:#3a3a3c;color:#fff;display:flex;align-items:center;justify-content:center;cursor:pointer;flex-shrink:0;transition:.2s;padding:0;align-self:flex-end;margin-bottom:3px}
.mic-btn.muted{background:#ff3b30}
.mic-btn svg{width:16px;height:16px;pointer-events:none}
.input-attach{align-self:flex-end}
.input-send{align-self:flex-end}
.leave-header-btn{width:36px;height:36px;display:flex;align-items:center;justify-content:center;background:none;border:none;color:#ff3b30;cursor:pointer;padding:0}
.leave-header-btn svg{width:20px;height:20px}

/* ═══════════════════════════════════════════════════════════════════════
   v3.23 GAMES + UNO — header button, picker overlay, Uno game UI
   ═══════════════════════════════════════════════════════════════════════
   Design goals:
     • Slot in next to leave-header-btn without changing header height.
     • Full-viewport overlays so game UI is undisturbed by chat layout.
     • Authentic Uno card visuals built entirely in CSS (no image assets).
     • Compatible with voice chat: overlays sit ABOVE the chat-stack but
       leave WebRTC <audio> elements untouched. People keep talking.
*/
.games-header-btn{width:36px;height:36px;display:flex;align-items:center;justify-content:center;background:none;border:none;cursor:pointer;padding:0;color:#bfbfc4}
.games-header-btn svg{width:22px;height:22px}
.games-header-btn:active{transform:scale(0.9);color:#fff}

/* Games picker (the small list of available games) */
.games-picker-ovl{position:fixed;inset:0;z-index:240;background:rgba(0,0,0,0.65);backdrop-filter:blur(8px);display:none;align-items:flex-end;justify-content:center;animation:fadeIn .2s}
.games-picker-ovl.show{display:flex}
.games-picker-box{width:100%;max-width:520px;background:linear-gradient(180deg,#1c1c1e,#0d0d0d);border-top-left-radius:20px;border-top-right-radius:20px;border-top:1px solid rgba(255,255,255,0.08);padding:12px 14px 22px;padding-bottom:calc(22px + env(safe-area-inset-bottom));max-height:80vh;overflow-y:auto;animation:slideUp .25s cubic-bezier(.2,.7,.2,1)}
.games-picker-handle{width:44px;height:5px;border-radius:3px;background:rgba(255,255,255,0.18);margin:0 auto 8px}
.games-picker-title{font-size:18px;font-weight:700;margin:6px 4px 12px;color:#fff}
.games-picker-list{display:grid;grid-template-columns:1fr;gap:10px}
.games-picker-item{display:flex;align-items:center;gap:14px;background:rgba(255,255,255,0.04);border:1px solid rgba(255,255,255,0.07);border-radius:14px;padding:12px 14px;cursor:pointer;transition:background .15s}
.games-picker-item:active{background:rgba(255,255,255,0.09)}
.games-picker-thumb{width:54px;height:54px;border-radius:12px;display:flex;align-items:center;justify-content:center;flex-shrink:0;font-weight:900;font-size:18px;color:#fff;letter-spacing:.5px;background:linear-gradient(135deg,#ff2a2a 0%,#ffd60a 50%,#0d8a3d 75%,#1a7ad8 100%);box-shadow:0 4px 12px rgba(0,0,0,0.35);position:relative;overflow:hidden}
.games-picker-thumb::after{content:'';position:absolute;inset:6px;border-radius:8px;background:#fff;color:#e10600;font-size:18px;font-weight:900;display:flex;align-items:center;justify-content:center}
.games-picker-thumb span{position:absolute;z-index:1;color:#e10600;font-size:22px;font-weight:900;font-style:italic;transform:rotate(-12deg);text-shadow:0 1px 0 #fff}
.games-picker-info{flex:1;min-width:0}
.games-picker-name{font-size:15px;font-weight:600;color:#fff;margin-bottom:2px}
.games-picker-desc{font-size:12px;color:#8e8e93}
.games-picker-soon{font-size:11px;font-weight:700;color:#ffd60a;background:rgba(255,214,10,0.15);padding:3px 8px;border-radius:8px}

/* ═══════════════════════════════════════════════════════════════════════
   v3.27 HEADER DROPDOWN + STREAMING UI
   ═══════════════════════════════════════════════════════════════════════
   The 3-dot button used to toggle the debug pane directly. Now it opens
   a small dropdown with two items (Logs / Start Streaming). The dropdown
   anchors to the upper-right of the viewport.
*/
.hdr-menu{position:fixed;top:54px;right:12px;z-index:235;background:#1c1c1e;border:1px solid rgba(255,255,255,0.08);border-radius:12px;padding:6px;min-width:200px;box-shadow:0 8px 28px rgba(0,0,0,0.55);transform:translateY(-6px) scale(0.96);opacity:0;pointer-events:none;transition:transform .18s,opacity .18s;transform-origin:top right}
.hdr-menu.show{transform:translateY(0) scale(1);opacity:1;pointer-events:auto}
.hdr-menu-item{display:flex;align-items:center;gap:10px;width:100%;padding:10px 12px;background:transparent;border:none;color:#fff;font-size:14px;text-align:left;cursor:pointer;border-radius:8px;transition:background .12s}
.hdr-menu-item:hover{background:rgba(255,255,255,0.06)}
.hdr-menu-item:active{background:rgba(255,255,255,0.1)}
.hdr-menu-item svg{width:18px;height:18px;flex-shrink:0;opacity:0.85}
.hdr-menu-item.danger{color:#ff5f5a}
.hdr-menu-item.danger svg{opacity:1}

/* v3.35: streaming mode no longer SHRINKS the seat panel. Instead, the
   panel auto-collapses (height 0) when a stream starts and the user
   pulls it down to its NORMAL full size when they want to see who's in
   the call — it just overlays the top of the video temporarily because
   .seat-panel is already absolutely positioned with z-index:8. Cleaner
   than maintaining a separate shrunken mode.
   We only need the specificity-bumped collapsed rule so the auto-
   collapse wins over any conflicting future rule. */
body.streaming .seat-panel.collapsed{max-height:0;min-height:0;padding-top:0;padding-bottom:0;border-bottom-width:0;opacity:0;pointer-events:none}

/* The stream panel itself */
.stream-panel{display:none;position:relative;z-index:7;flex-shrink:0;padding:8px 8px 6px;background:transparent}
body.streaming .stream-panel{display:block}
/* v3.36: actual "screen" framing — the video sits inside a rounded
   container with a thin warm border and a soft drop shadow, separated
   from the chat below by a bit of padding. Now it reads as a discrete
   "screen mounted in the room" instead of a video laid into the chat
   flow. The border-color uses the same orange accent as the play
   button so the player has one consistent identity. */
.stream-video-wrap{position:relative;width:100%;aspect-ratio:16/9;background:#000;overflow:hidden;line-height:0;border-radius:12px;border:1.5px solid rgba(255,138,43,0.55);box-shadow:0 6px 22px rgba(0,0,0,0.55),0 0 0 1px rgba(255,138,43,0.18) inset}
.stream-video-wrap video{position:absolute;left:0;top:0;width:100%;height:100%;max-width:100%;max-height:100%;object-fit:contain;object-position:center center;background:#000;display:block}
.stream-tap-shield{position:absolute;inset:0;cursor:pointer;z-index:2}
.stream-tap-shield.streamer-mode{cursor:default}
.stream-unmute-prompt{position:absolute;left:50%;bottom:14px;transform:translateX(-50%);display:none;align-items:center;justify-content:center;gap:8px;padding:9px 16px;background:rgba(0,0,0,0.82);color:#fff;font-size:13px;font-weight:600;cursor:pointer;z-index:3;border-radius:22px;backdrop-filter:blur(10px);box-shadow:0 4px 14px rgba(0,0,0,0.5);border:1px solid rgba(255,255,255,0.12);animation:streamUnmutePulse 1.5s ease-in-out infinite}
.stream-unmute-prompt.show{display:flex}
.stream-unmute-prompt svg{width:16px;height:16px;flex-shrink:0}
@keyframes streamUnmutePulse{0%,100%{box-shadow:0 4px 14px rgba(0,0,0,0.5),0 0 0 0 rgba(255,255,255,0.15)}50%{box-shadow:0 4px 14px rgba(0,0,0,0.5),0 0 0 8px rgba(255,255,255,0)}}

.stream-controls{
  position:absolute;left:0;right:0;bottom:0;z-index:4;
  padding:16px 12px 8px;
  background:linear-gradient(180deg,transparent 0%,rgba(0,0,0,0.55) 35%,rgba(0,0,0,0.9) 100%);
  color:#fff;
  opacity:1;
  transition:opacity .25s ease-out;
  pointer-events:auto;
}
/* Hidden state — fades out but stays in the DOM so taps on the wrap
   above still hit it for re-showing. */
.stream-controls.hidden{opacity:0;pointer-events:none}
/* v3.33: title sits INSIDE the bottom controls group (was a separate
   absolutely-positioned strip at the TOP of the video, which covered
   the top portion of the actual video content). Now nothing visually
   intrudes on the video except the bottom controls bar. */
.stream-title{
  font-size:11px;font-weight:600;opacity:0.88;
  overflow:hidden;text-overflow:ellipsis;white-space:nowrap;letter-spacing:0.1px;color:#fff;
  text-shadow:0 1px 3px rgba(0,0,0,0.8);
  margin-bottom:4px;
}
.stream-progress{position:relative;height:3px;background:rgba(255,255,255,0.18);border-radius:3px;cursor:pointer;touch-action:none;transition:height .12s;margin-bottom:3px}
body.streaming.is-streamer .stream-progress{cursor:pointer}
body.streaming.is-streamer .stream-progress:hover{height:5px}
.stream-progress-fill{position:absolute;left:0;top:0;bottom:0;background:linear-gradient(90deg,#ff8a2b,#ffb066);border-radius:3px;width:0%;transition:width .15s linear}
.stream-progress-handle{position:absolute;top:50%;transform:translate(-50%,-50%);width:11px;height:11px;border-radius:50%;background:#fff;left:0%;display:none;box-shadow:0 1px 4px rgba(0,0,0,0.7),0 0 0 2px rgba(255,138,43,0.5)}
body.streaming.is-streamer .stream-progress-handle{display:block}
.stream-time-row{display:flex;justify-content:space-between;font-size:10px;color:#ddd;margin-top:1px;font-variant-numeric:tabular-nums;text-shadow:0 1px 2px rgba(0,0,0,0.6)}
.stream-buttons{display:flex;align-items:center;justify-content:center;gap:14px;padding:4px 0 0}
/* Viewers see only the live caption; no controls. */
body.streaming:not(.is-streamer) .stream-buttons{display:none}
body.streaming.is-streamer .stream-viewer-info{display:none}
.stream-viewer-info{text-align:center;font-size:11px;color:#ddd;padding:4px 0 0;font-weight:500;text-shadow:0 1px 2px rgba(0,0,0,0.6)}
.stream-viewer-info::before{content:'';display:inline-block;width:6px;height:6px;border-radius:50%;background:#ff3b30;margin-right:6px;vertical-align:middle;animation:streamLive 1.4s ease-in-out infinite}
@keyframes streamLive{0%,100%{opacity:1}50%{opacity:0.3}}
.stream-btn{width:30px;height:30px;border-radius:50%;border:none;background:rgba(255,255,255,0.14);color:#fff;display:flex;align-items:center;justify-content:center;cursor:pointer;padding:0;transition:transform .1s,background .15s;backdrop-filter:blur(8px)}
.stream-btn:hover{background:rgba(255,255,255,0.22)}
.stream-btn:active{transform:scale(0.88)}
.stream-btn:disabled{opacity:0.35;cursor:not-allowed}
.stream-btn:disabled:active{transform:none}
.stream-btn svg{width:14px;height:14px}
.stream-btn-play{width:38px;height:38px;background:#ff8a2b;box-shadow:0 3px 10px rgba(255,138,43,0.4)}
.stream-btn-play:hover{background:#ff9a45}
.stream-btn-play svg{width:18px;height:18px}
.stream-btn-stop{background:rgba(255,59,48,0.28);color:#ff8a85}
.stream-btn-stop:hover{background:rgba(255,59,48,0.45)}
.stream-btn-stop svg{width:12px;height:12px}

/* Streamer's upload sheet (bottom-sheet) */
.stream-sheet-ovl{position:fixed;inset:0;z-index:248;background:rgba(0,0,0,0.7);backdrop-filter:blur(8px);display:none;align-items:flex-end;justify-content:center;animation:fadeIn .2s}
.stream-sheet-ovl.show{display:flex}
.stream-sheet-ovl:not(.show){contain:strict;pointer-events:none}
.stream-sheet-box{width:100%;max-width:520px;background:linear-gradient(180deg,#1c1c1e,#0d0d0d);border-top-left-radius:20px;border-top-right-radius:20px;border-top:1px solid rgba(255,255,255,0.08);padding:12px 14px 22px;padding-bottom:calc(22px + env(safe-area-inset-bottom));max-height:88vh;overflow-y:auto;animation:slideUp .25s cubic-bezier(.2,.7,.2,1)}
.stream-sheet-title{font-size:18px;font-weight:700;color:#fff;margin:6px 4px 4px}
.stream-sheet-sub{font-size:12px;color:#888;margin:0 4px 14px;line-height:1.45}
.stream-sheet-queue{display:flex;flex-direction:column;gap:8px;margin-bottom:10px}
.stream-queue-item{display:flex;align-items:center;gap:10px;background:rgba(255,255,255,0.04);border:1px solid rgba(255,255,255,0.06);border-radius:12px;padding:10px 12px}
.stream-queue-item.uploading{opacity:0.85}
.stream-queue-item.error{border-color:rgba(255,59,48,0.4);background:rgba(255,59,48,0.08)}
.stream-queue-item-idx{font-size:12px;font-weight:800;color:#888;min-width:18px;text-align:center}
.stream-queue-item-info{flex:1;min-width:0}
.stream-queue-item-name{font-size:13px;color:#fff;font-weight:600;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.stream-queue-item-meta{font-size:11px;color:#888;margin-top:2px}
.stream-queue-item-progress{margin-top:4px;height:3px;background:rgba(255,255,255,0.08);border-radius:2px;overflow:hidden}
.stream-queue-item-progress-fill{height:100%;background:linear-gradient(90deg,#1a7ad8,#5eaaff);width:0%;transition:width .2s}
.stream-queue-item-remove{width:28px;height:28px;border-radius:50%;border:none;background:rgba(255,255,255,0.06);color:#ff5f5a;cursor:pointer;display:flex;align-items:center;justify-content:center;flex-shrink:0;padding:0}
.stream-queue-item-remove svg{width:14px;height:14px}
.stream-queue-empty{padding:30px 0;text-align:center;color:#666;font-size:13px;font-style:italic}
.stream-sheet-upload-row{display:flex;justify-content:center;margin-bottom:14px}
.stream-sheet-upload-btn{display:inline-flex;align-items:center;gap:8px;padding:11px 18px;background:rgba(26,122,216,0.18);color:#5eaaff;border-radius:22px;font-weight:600;font-size:14px;cursor:pointer;border:1px solid rgba(26,122,216,0.35);transition:background .15s}
.stream-sheet-upload-btn:hover{background:rgba(26,122,216,0.28)}
.stream-sheet-upload-btn svg{width:18px;height:18px}
.stream-sheet-actions{display:flex;gap:8px}
.stream-sheet-actions > *{flex:1}
.stream-sheet-error{margin-top:10px;padding:8px 12px;background:rgba(255,59,48,0.12);border:1px solid rgba(255,59,48,0.3);color:#ff8a85;font-size:12px;border-radius:8px;display:none;text-align:center}
.stream-sheet-error.show{display:block}

/* ── UNO overlay (full screen game) ── */
.uno-ovl{position:fixed;inset:0;z-index:230;background:#1a1a1c;display:none;flex-direction:column;animation:fadeIn .2s}
.uno-ovl.show{display:flex}

/* Subtle vignette + warm noise (charcoal felt). The vignette deepens at
   the corners so the bright center cards pop more. Optional uploaded
   background image (set via JS as a CSS variable on .uno-ovl) layers on
   top of this base — when present, the vignette still applies above it. */
.uno-ovl::before{content:'';position:absolute;inset:0;background:var(--uno-bg-image, none) center center/cover no-repeat;pointer-events:none;z-index:0}
.uno-ovl::after{content:'';position:absolute;inset:0;background:radial-gradient(ellipse at center, transparent 0%, rgba(0,0,0,0.4) 90%, rgba(0,0,0,0.7) 100%);pointer-events:none;z-index:0}

.uno-header{position:relative;z-index:3;padding:8px 12px;display:flex;align-items:center;gap:10px;background:rgba(0,0,0,0.4);border-bottom:1px solid rgba(255,255,255,0.06);flex-shrink:0}
.uno-back{width:36px;height:36px;display:flex;align-items:center;justify-content:center;background:none;border:none;color:#fff;font-size:24px;cursor:pointer;padding:0}
.uno-title{flex:1;font-size:16px;font-weight:700;letter-spacing:0.5px}
.uno-title .uno-brand{display:inline-block;background:linear-gradient(135deg,#ff2a2a,#ffd60a,#0d8a3d,#1a7ad8);background-clip:text;-webkit-background-clip:text;color:transparent;font-style:italic;font-weight:900;font-size:18px;letter-spacing:1px;margin-right:6px}
.uno-chat-btn{width:36px;height:36px;border-radius:50%;border:none;background:rgba(255,255,255,0.1);color:#fff;cursor:pointer;display:flex;align-items:center;justify-content:center;position:relative;padding:0}
.uno-chat-btn svg{width:18px;height:18px}
.uno-chat-btn .uno-chat-badge{position:absolute;top:-2px;right:-2px;background:#ff3b30;color:#fff;font-size:10px;font-weight:700;min-width:16px;height:16px;border-radius:8px;padding:0 4px;display:none;align-items:center;justify-content:center}
.uno-chat-btn .uno-chat-badge.show{display:flex}

/* v3.26: in-game mute button. Mirrors the main call mute button's state
   via _syncAllMuteBtns(). Tap toggles mic for the whole call — players
   don't have to back out of a game to mute themselves. */
.uno-mute-btn{width:36px;height:36px;border-radius:50%;border:none;background:rgba(255,255,255,0.1);color:#fff;cursor:pointer;display:flex;align-items:center;justify-content:center;position:relative;padding:0}
.uno-mute-btn svg{width:18px;height:18px}
.uno-mute-btn.muted{background:#ff3b30}

/* v3.25: recent-actions log under the discard pile. Three lines max,
   older ones fade out. Plato-style "what just happened" feedback. */
.game-log{position:relative;z-index:2;display:flex;flex-direction:column;align-items:center;gap:1px;padding:4px 14px 0;min-height:48px;pointer-events:none;flex-shrink:0}
.game-log-line{font-size:11px;line-height:1.25;color:#fff;text-align:center;max-width:90%;text-shadow:0 1px 2px rgba(0,0,0,0.7);transition:opacity .4s,transform .4s}
.game-log-line.l0{opacity:1;font-weight:600;font-size:13px}
.game-log-line.l1{opacity:0.65}
.game-log-line.l2{opacity:0.35}

/* ── UNO body — top opponents row, table center, your hand bottom ── */
.uno-body{position:relative;z-index:2;flex:1;min-height:0;display:flex;flex-direction:column;overflow:hidden}

/* Lobby screen */
.uno-lobby{flex:1;display:flex;flex-direction:column;align-items:center;justify-content:flex-start;padding:24px 18px;gap:18px;overflow-y:auto}
.uno-lobby-logo{margin-top:12px;font-style:italic;font-weight:900;font-size:48px;letter-spacing:2px;background:linear-gradient(135deg,#ff2a2a 0%,#ffd60a 33%,#0d8a3d 66%,#1a7ad8 100%);background-clip:text;-webkit-background-clip:text;color:transparent;text-shadow:0 2px 20px rgba(255,255,255,0.1);transform:rotate(-4deg)}
.uno-lobby-section{width:100%;max-width:380px;background:rgba(0,0,0,0.35);border:1px solid rgba(255,255,255,0.08);border-radius:16px;padding:16px;backdrop-filter:blur(10px)}
.uno-lobby-h{font-size:13px;font-weight:700;color:#fff;margin-bottom:10px;letter-spacing:0.4px;text-transform:uppercase;opacity:0.9}
.uno-player-count-row{display:flex;gap:6px}
.uno-player-count-btn{flex:1;padding:14px 0;border-radius:12px;border:2px solid rgba(255,255,255,0.1);background:rgba(255,255,255,0.04);color:#fff;font-weight:700;font-size:16px;cursor:pointer;transition:all .15s;min-width:0}
.uno-player-count-btn.sel{background:linear-gradient(135deg,#e10600,#b80500);border-color:#ff5050;box-shadow:0 4px 12px rgba(225,6,0,0.4)}
.uno-player-count-btn:active{transform:scale(0.96)}
.uno-lobby-players-list{display:flex;flex-direction:column;gap:6px;min-height:80px}
.uno-lobby-player{display:flex;align-items:center;gap:10px;background:rgba(255,255,255,0.04);border-radius:10px;padding:8px 10px}
.uno-lobby-player-av{width:32px;height:32px;border-radius:50%;object-fit:cover;background:#444;display:flex;align-items:center;justify-content:center;font-weight:700;color:#fff;font-size:14px;flex-shrink:0}
.uno-lobby-player-name{flex:1;font-size:14px;color:#fff;font-weight:500;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.uno-lobby-host-badge{font-size:10px;font-weight:800;color:#000;background:#ffd60a;padding:2px 6px;border-radius:6px;letter-spacing:0.5px}
.uno-lobby-empty-slot{display:flex;align-items:center;gap:10px;border:1px dashed rgba(255,255,255,0.12);border-radius:10px;padding:8px 10px;color:#888;font-size:13px;font-style:italic}
.uno-lobby-actions{width:100%;max-width:380px;display:flex;gap:10px;margin-top:4px}
.uno-btn-primary{flex:1;padding:14px;border-radius:12px;border:none;background:linear-gradient(135deg,#0d8a3d,#0a6b30);color:#fff;font-weight:700;font-size:15px;cursor:pointer;box-shadow:0 4px 14px rgba(13,138,61,0.35);transition:transform .1s}
.uno-btn-primary:active{transform:scale(0.97)}
.uno-btn-primary:disabled{opacity:0.45;cursor:not-allowed;box-shadow:none}
.uno-btn-secondary{flex:1;padding:14px;border-radius:12px;border:1px solid rgba(255,255,255,0.15);background:rgba(255,255,255,0.05);color:#fff;font-weight:600;font-size:15px;cursor:pointer}
.uno-btn-secondary:active{background:rgba(255,255,255,0.1)}
.uno-btn-danger{flex:1;padding:14px;border-radius:12px;border:none;background:linear-gradient(135deg,#ff3b30,#c41f17);color:#fff;font-weight:700;font-size:15px;cursor:pointer}
.uno-btn-danger:active{transform:scale(0.97)}

/* Playing screen layout */
.uno-play{flex:1;display:flex;flex-direction:column;min-height:0;position:relative}
/* The table is the center "felt" area where draw + discard sit. With
   absolutely-positioned opponents around it, the table can grow to fill
   the middle without competing for vertical space with name rows. */
.uno-table{flex:1;display:flex;align-items:center;justify-content:center;gap:22px;padding:80px 8px 8px;min-height:0;position:relative;z-index:1}
/* ── PLATO-STYLE TABLE LAYOUT ──
   Players sit around the edges of the screen instead of in a row at top.
   Bottom of the screen is reserved for YOU (your hand + own avatar nearby).
   Opponents are absolutely positioned around the table perimeter, with
   the placement varying by player count:
     2p:  opponent top-center
     3p:  opponents top-left + top-right
     4p:  one left, one top, one right
     5p:  left, top-left, top-right, right
   Each seat shows the avatar, name, a fan of card-backs, and a per-seat
   thin timer bar (drains only on that player's turn — much clearer than
   one shared bar at the top). */
.uno-opponents{position:absolute;inset:0;pointer-events:none;z-index:2}
.uno-opp{position:absolute;display:flex;flex-direction:column;align-items:center;gap:3px;width:84px;transition:transform .25s,opacity .25s;opacity:0.72;pointer-events:auto}
.uno-opp.turn{opacity:1;transform:scale(1.05)}

/* Position the seats. Anchors are the OPP element's own center via the
   transform; we offset by px so they hug the corners but don't clip the
   screen edges on narrow phones. */
.uno-opp.seat-top{top:14px;left:50%;transform:translateX(-50%)}
.uno-opp.seat-top.turn{transform:translateX(-50%) scale(1.05)}
.uno-opp.seat-top-left{top:14px;left:14px}
.uno-opp.seat-top-right{top:14px;right:14px}
.uno-opp.seat-left{top:38%;left:8px;transform:translateY(-50%)}
.uno-opp.seat-left.turn{transform:translateY(-50%) scale(1.05)}
.uno-opp.seat-right{top:38%;right:8px;transform:translateY(-50%)}
.uno-opp.seat-right.turn{transform:translateY(-50%) scale(1.05)}

.uno-opp-av-wrap{position:relative;width:54px;height:54px}
.uno-opp-av{width:54px;height:54px;border-radius:50%;object-fit:cover;background:#333;display:flex;align-items:center;justify-content:center;font-weight:700;color:#fff;font-size:20px;border:2px solid rgba(255,255,255,0.12)}
.uno-opp.turn .uno-opp-av{border-color:#ffd60a;box-shadow:0 0 16px rgba(255,214,10,0.7)}

/* Fan of card-backs near each opponent, indicating their hand size.
   Up to 6 visible, then a "+N" pill if more. */
.uno-opp-back-pile{position:absolute;top:-10px;right:-22px;display:flex;align-items:flex-end}
.uno-opp-mini{width:14px;height:21px;border-radius:2px;background:linear-gradient(135deg,#a30000,#5d0000);border:1px solid #2a0000;margin-left:-9px;position:relative;box-shadow:0 1px 2px rgba(0,0,0,0.5)}
.uno-opp-mini:first-child{margin-left:0}
.uno-opp-mini::after{content:'';position:absolute;left:2px;right:2px;top:2px;bottom:2px;border-radius:50%;background:radial-gradient(circle at center,#ffd60a 0%,#c79900 70%,#000 100%);opacity:0.85}
.uno-opp-overflow{margin-left:4px;font-size:9px;font-weight:800;color:#ffd60a;background:rgba(0,0,0,0.6);padding:1px 5px;border-radius:6px;align-self:center}

.uno-opp-name{font-size:11px;color:#fff;font-weight:600;max-width:84px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;text-align:center;text-shadow:0 1px 2px rgba(0,0,0,0.6)}
.uno-opp-count{font-size:10px;color:#ffd60a;font-weight:700;text-shadow:0 1px 2px rgba(0,0,0,0.6)}
.uno-opp.uno-flag .uno-opp-count{color:#fff;background:#e10600;padding:1px 7px;border-radius:8px;animation:unoFlash 1s ease-in-out infinite}

/* Per-seat timer bar (under each opponent's name). Only visible when it's
   their turn. This replaces the single shared bar at the top, which felt
   disconnected from whose turn it actually is. */
.uno-opp-timer{width:60px;height:3px;border-radius:2px;background:rgba(255,255,255,0.08);overflow:hidden;display:none;margin-top:1px}
.uno-opp-timer.show{display:block}
.uno-opp-timer-fill{height:100%;background:linear-gradient(90deg,#0d8a3d,#ffd60a 70%,#e10600);width:100%;transform-origin:left;will-change:transform}

@keyframes unoFlash{0%,100%{transform:scale(1)}50%{transform:scale(1.15)}}

/* Your own seat indicator (a small avatar + name above the hand) so the
   layout is symmetrical with the opponent seats. */
.uno-self-seat{display:flex;align-items:center;justify-content:center;gap:8px;padding:4px 12px 0;flex-shrink:0;position:relative;z-index:2}
.uno-self-seat .uno-opp-av{width:34px;height:34px;font-size:14px;border-width:2px}
.uno-self-seat.turn .uno-opp-av{border-color:#ffd60a;box-shadow:0 0 12px rgba(255,214,10,0.5)}
.uno-self-seat-name{font-size:12px;font-weight:700;color:#fff}
.uno-self-seat-name .you-pill{font-size:9px;background:rgba(255,255,255,0.12);padding:1px 5px;border-radius:6px;margin-left:4px;font-weight:600;color:#ffd60a}
.uno-self-timer{width:80px;height:3px;border-radius:2px;background:rgba(255,255,255,0.08);overflow:hidden;display:none}
.uno-self-timer.show{display:block}
.uno-self-timer-fill{height:100%;background:linear-gradient(90deg,#0d8a3d,#ffd60a 70%,#e10600);width:100%;transform-origin:left;will-change:transform}

/* Table center — discard + draw pile */
.uno-pile{position:relative;display:flex;flex-direction:column;align-items:center;gap:6px}
.uno-pile-label{font-size:10px;color:#fff;opacity:0.7;font-weight:700;letter-spacing:1px;text-transform:uppercase;display:flex;align-items:center;justify-content:center;gap:4px}
.uno-pile-count{font-size:11px;color:#ffd60a;font-weight:700}
.uno-draw-pile-card{cursor:pointer;transition:transform .15s}
.uno-draw-pile-card:active{transform:scale(0.95)}
.uno-table-direction{position:absolute;top:50%;left:50%;transform:translate(-50%,-50%);width:140px;height:140px;border:2px dashed rgba(255,255,255,0.08);border-radius:50%;pointer-events:none;animation:spinSlow 20s linear infinite}
.uno-table-direction.rev{animation-direction:reverse}

@keyframes spinSlow{from{transform:translate(-50%,-50%) rotate(0deg)}to{transform:translate(-50%,-50%) rotate(360deg)}}

/* Color indicator (current active color) */
.uno-color-dot{width:14px;height:14px;border-radius:50%;display:inline-block;vertical-align:middle;margin-left:4px;border:1px solid rgba(255,255,255,0.4)}
.uno-color-dot.c-r{background:#e10600}
.uno-color-dot.c-y{background:#ffd60a}
.uno-color-dot.c-g{background:#0d8a3d}
.uno-color-dot.c-b{background:#1a7ad8}

/* Pending draw-stack badge — shows the accumulated penalty when +2/+4
   are being stacked, so everyone can see "next player must draw N".
   Pulses red so it grabs attention. */
.uno-stack-badge{display:none;margin-left:6px;padding:1px 7px;border-radius:8px;background:#e10600;color:#fff;font-size:10px;font-weight:800;letter-spacing:0.5px;animation:stackPulse 1s ease-in-out infinite;vertical-align:middle}
.uno-stack-badge.show{display:inline-block}
@keyframes stackPulse{0%,100%{transform:scale(1)}50%{transform:scale(1.12);box-shadow:0 0 12px rgba(225,6,0,0.6)}}

/* Turn indicator banner. Slimmer than before because each seat has its
   own timer bar now — this banner is just a quick textual status. */
.uno-turn-bar{padding:6px 14px;text-align:center;font-size:12px;font-weight:600;color:#fff;background:rgba(0,0,0,0.45);flex-shrink:0;position:relative;z-index:3;backdrop-filter:blur(6px)}
.uno-turn-bar.your-turn{background:linear-gradient(90deg,rgba(255,214,10,0.22),rgba(255,214,10,0.05),rgba(255,214,10,0.22));color:#ffd60a}

@keyframes turnPulse{0%,100%{box-shadow:inset 0 0 0 1px rgba(255,214,10,0.3)}50%{box-shadow:inset 0 0 0 1px rgba(255,214,10,0.6)}}

@keyframes timerPulse{0%,100%{opacity:1}50%{opacity:0.45}}
.uno-opp-timer-fill.warn,.uno-self-timer-fill.warn{animation:timerPulse .6s ease-in-out infinite}

/* Your hand — bottom strip */
.uno-hand-wrap{flex-shrink:0;padding:6px 0 10px;padding-bottom:calc(10px + env(safe-area-inset-bottom));background:linear-gradient(180deg,transparent,rgba(0,0,0,0.45));overflow-x:auto;overflow-y:visible;scrollbar-width:none}
.uno-hand-wrap::-webkit-scrollbar{display:none}
.uno-hand{display:flex;justify-content:center;align-items:flex-end;gap:0;padding:0 18px;min-height:120px;min-width:min-content}
.uno-hand .uno-card{transition:transform .18s cubic-bezier(.2,.7,.2,1.2),margin-left .2s}
.uno-hand .uno-card.playable{cursor:pointer}
.uno-hand .uno-card.playable:hover{transform:translateY(-8px)}
.uno-hand .uno-card.playable:active{transform:translateY(-14px) scale(1.04)}
.uno-hand .uno-card.unplayable{opacity:0.5;filter:grayscale(0.4) brightness(0.85)}

.uno-actions-row{display:flex;gap:8px;padding:0 14px 6px;justify-content:center;flex-shrink:0}
.uno-act-btn{padding:8px 16px;border-radius:18px;border:none;background:rgba(255,255,255,0.1);color:#fff;font-weight:600;font-size:13px;cursor:pointer}
.uno-act-btn.primary{background:linear-gradient(135deg,#ffd60a,#f5b800);color:#000}
.uno-act-btn.draw{background:linear-gradient(135deg,#1a7ad8,#155cb0)}
.uno-act-btn:active{transform:scale(0.95)}
.uno-act-btn:disabled{opacity:0.4;cursor:not-allowed}

/* ═══ UNO CARD VISUAL ═══ */
/* Card sizes — base (in your hand) and "back" thumbnails. */
.uno-card{position:relative;width:64px;height:96px;border-radius:8px;background:#fff;flex-shrink:0;box-shadow:0 4px 8px rgba(0,0,0,0.45),0 0 0 2px #fff inset;overflow:hidden;-webkit-user-select:none;user-select:none}
.uno-card .uno-card-face{position:absolute;inset:4px;border-radius:6px;display:flex;flex-direction:column;align-items:center;justify-content:center;overflow:hidden}
.uno-card.c-r .uno-card-face{background:#e10600}
.uno-card.c-y .uno-card-face{background:#ffd60a}
.uno-card.c-g .uno-card-face{background:#0d8a3d}
.uno-card.c-b .uno-card-face{background:#1a7ad8}
.uno-card.c-w .uno-card-face{background:#0a0a0a}

/* Oval slash (the classic Uno card center ellipse) */
.uno-card .uno-card-oval{position:absolute;width:120%;height:60%;background:#fff;border-radius:50%;transform:rotate(-22deg);box-shadow:0 0 0 1px rgba(0,0,0,0.05)}

/* Center number/symbol (italic, white outline outside the oval but with the
   oval mask it appears as the colored number inside white) */
.uno-card .uno-card-num{position:relative;z-index:2;font-style:italic;font-weight:900;color:#fff;font-size:34px;line-height:1;text-shadow:-2px 2px 0 rgba(0,0,0,0.18);letter-spacing:-1px}
.uno-card.c-r .uno-card-num{color:#e10600}
.uno-card.c-y .uno-card-num{color:#e10600}
.uno-card.c-g .uno-card-num{color:#0d8a3d;text-shadow:-2px 2px 0 rgba(0,0,0,0.12)}
.uno-card.c-b .uno-card-num{color:#1a7ad8;text-shadow:-2px 2px 0 rgba(0,0,0,0.12)}

/* On yellow cards, classic Uno uses red numerals. Already set above. */

/* Corner numbers (tiny, opposite corners) */
.uno-card .uno-card-corner{position:absolute;font-style:italic;font-weight:900;font-size:13px;color:#fff;line-height:1;z-index:3;letter-spacing:-0.5px}
.uno-card .uno-card-corner.tl{top:6px;left:6px}
.uno-card .uno-card-corner.br{bottom:6px;right:6px;transform:rotate(180deg)}

/* Action symbols: skip ⊘, reverse ⇄ → use unicode glyphs */
.uno-card.v-skip .uno-card-num,.uno-card.v-rev .uno-card-num,.uno-card.v-p2 .uno-card-num{font-size:30px}
.uno-card.v-skip .uno-card-corner,.uno-card.v-rev .uno-card-corner,.uno-card.v-p2 .uno-card-corner{font-size:11px}

/* Wild + Wild +4 — black card with 4 colored quadrants in center oval */
.uno-card.c-w .uno-card-oval{background:conic-gradient(from 45deg,#e10600 0deg 90deg,#ffd60a 90deg 180deg,#0d8a3d 180deg 270deg,#1a7ad8 270deg 360deg);width:78%;height:54%;border-radius:50%}
.uno-card.c-w .uno-card-num{color:#fff;text-shadow:0 1px 2px rgba(0,0,0,0.45);font-size:18px;z-index:3;letter-spacing:0}
.uno-card.c-w.v-wild .uno-card-num::before{content:'WILD'}
.uno-card.c-w.v-p4 .uno-card-num::before{content:'+4'}
.uno-card.c-w .uno-card-num{font-style:italic}
.uno-card.c-w .uno-card-corner{color:#fff}

/* Card back — for opponent counts / draw pile thumbnail */
.uno-card.back{background:linear-gradient(135deg,#1a1a1a,#000)}
.uno-card.back .uno-card-face{background:radial-gradient(circle at center,#e10600 0%,#a30000 60%,#600000 100%)}
.uno-card.back .uno-card-oval{background:#000;width:78%;height:54%}
.uno-card.back .uno-card-num{color:#ffd60a;font-size:22px;letter-spacing:0;font-style:italic;text-shadow:-1px 1px 0 #fff,0 0 8px rgba(255,214,10,0.4);z-index:3}
.uno-card.back .uno-card-num::before{content:'UNO'}
.uno-card.back .uno-card-corner{display:none}

/* Size variants */
.uno-card.size-md{width:78px;height:118px}
.uno-card.size-md .uno-card-num{font-size:42px}
.uno-card.size-md .uno-card-corner{font-size:15px}
.uno-card.size-md.c-w .uno-card-num{font-size:22px}
.uno-card.size-md.back .uno-card-num{font-size:28px}

.uno-card.size-lg{width:98px;height:148px}
.uno-card.size-lg .uno-card-num{font-size:54px}
.uno-card.size-lg .uno-card-corner{font-size:18px}
.uno-card.size-lg.c-w .uno-card-num{font-size:28px}
.uno-card.size-lg.back .uno-card-num{font-size:34px}

.uno-card.size-sm{width:40px;height:60px;border-radius:5px}
.uno-card.size-sm .uno-card-face{inset:2px;border-radius:3px}
.uno-card.size-sm .uno-card-num{font-size:22px}
.uno-card.size-sm .uno-card-corner{font-size:9px}
.uno-card.size-sm.c-w .uno-card-num{font-size:11px}

/* Color-picker overlay (after playing a wild) */
.uno-color-pick{position:fixed;inset:0;z-index:260;background:rgba(0,0,0,0.7);backdrop-filter:blur(8px);display:none;align-items:center;justify-content:center;animation:fadeIn .15s}
.uno-color-pick.show{display:flex}
.uno-color-pick-box{background:#1c1c1e;border-radius:18px;padding:24px;text-align:center;max-width:320px;width:90%}
.uno-color-pick-h{font-size:16px;font-weight:700;color:#fff;margin-bottom:18px}
.uno-color-pick-grid{display:grid;grid-template-columns:1fr 1fr;gap:12px}
.uno-color-pick-btn{aspect-ratio:1;border-radius:14px;border:3px solid rgba(255,255,255,0.1);cursor:pointer;display:flex;align-items:center;justify-content:center;color:#fff;font-weight:700;font-size:15px;letter-spacing:0.5px;text-shadow:0 1px 2px rgba(0,0,0,0.3);transition:transform .1s}
.uno-color-pick-btn:active{transform:scale(0.94)}
.uno-color-pick-btn.c-r{background:#e10600}
.uno-color-pick-btn.c-y{background:#ffd60a;color:#000;text-shadow:none}
.uno-color-pick-btn.c-g{background:#0d8a3d}
.uno-color-pick-btn.c-b{background:#1a7ad8}

/* Uno event toast (top center) */
.uno-toast{position:absolute;top:60px;left:50%;transform:translateX(-50%) translateY(-20px);background:rgba(0,0,0,0.85);border:1px solid rgba(255,214,10,0.4);color:#ffd60a;padding:8px 16px;border-radius:18px;font-size:13px;font-weight:600;z-index:5;opacity:0;transition:opacity .25s,transform .25s;pointer-events:none;max-width:80%;text-align:center}
.uno-toast.show{opacity:1;transform:translateX(-50%) translateY(0)}

/* Winner overlay */
.uno-winner{position:fixed;inset:0;z-index:270;background:radial-gradient(ellipse at center,rgba(255,214,10,0.25) 0%,rgba(0,0,0,0.85) 70%);display:none;align-items:center;justify-content:center;flex-direction:column;gap:18px;animation:fadeIn .3s}
.uno-winner.show{display:flex}
.uno-winner-trophy{font-size:84px;animation:trophyBounce 1.2s ease-in-out infinite}
@keyframes trophyBounce{0%,100%{transform:translateY(0) rotate(-5deg)}50%{transform:translateY(-14px) rotate(5deg)}}
.uno-winner-title{font-size:32px;font-weight:900;color:#ffd60a;text-shadow:0 3px 14px rgba(255,214,10,0.5);letter-spacing:1px;text-align:center;padding:0 20px}
.uno-winner-name{font-size:22px;color:#fff;font-weight:700}
.uno-winner-actions{display:flex;gap:10px;margin-top:8px}

/* UNO! call button — floats above hand when at 1 card */
.uno-call-btn{position:absolute;right:14px;bottom:140px;width:64px;height:64px;border-radius:50%;border:3px solid #ffd60a;background:linear-gradient(135deg,#e10600,#a30000);color:#fff;font-style:italic;font-weight:900;font-size:18px;cursor:pointer;z-index:8;display:none;align-items:center;justify-content:center;letter-spacing:1px;box-shadow:0 6px 20px rgba(225,6,0,0.5);animation:unoPop 1.2s ease-in-out infinite}
.uno-call-btn.show{display:flex}
.uno-call-btn.called{background:linear-gradient(135deg,#0d8a3d,#0a6b30);border-color:#fff;animation:none;pointer-events:none}
@keyframes unoPop{0%,100%{transform:scale(1)}50%{transform:scale(1.08);box-shadow:0 8px 28px rgba(225,6,0,0.7)}}

/* In-game side chat panel */
.uno-chat-panel{position:absolute;right:0;top:0;bottom:0;width:80%;max-width:320px;background:rgba(15,15,15,0.96);backdrop-filter:blur(16px);border-left:1px solid rgba(255,255,255,0.08);z-index:9;transform:translateX(100%);transition:transform .25s cubic-bezier(.2,.7,.2,1);display:flex;flex-direction:column}
.uno-chat-panel.open{transform:translateX(0)}
.uno-chat-panel-header{padding:10px 14px;display:flex;align-items:center;justify-content:space-between;border-bottom:1px solid rgba(255,255,255,0.06);flex-shrink:0}
.uno-chat-panel-title{font-size:14px;font-weight:700;color:#fff}
.uno-chat-panel-close{width:28px;height:28px;border-radius:50%;border:none;background:rgba(255,255,255,0.08);color:#fff;font-size:16px;cursor:pointer}
.uno-chat-msgs{flex:1;min-height:0;overflow-y:auto;padding:10px 12px;display:flex;flex-direction:column;gap:6px}
.uno-chat-msg{display:flex;flex-direction:column;gap:1px;background:rgba(255,255,255,0.04);padding:6px 10px;border-radius:10px;max-width:100%}
.uno-chat-msg.self{background:rgba(26,122,216,0.18);align-self:flex-end;max-width:88%}
.uno-chat-msg.system{background:transparent;color:#888;font-size:11px;font-style:italic;text-align:center;padding:2px 0}
.uno-chat-msg-name{font-size:11px;font-weight:700;color:#ffd60a}
.uno-chat-msg-text{font-size:13px;color:#fff;word-break:break-word;white-space:pre-wrap}
.uno-chat-input-row{padding:8px 10px;padding-bottom:calc(8px + env(safe-area-inset-bottom));display:flex;gap:6px;border-top:1px solid rgba(255,255,255,0.06);flex-shrink:0}
.uno-chat-input{flex:1;background:rgba(255,255,255,0.06);border:1px solid rgba(255,255,255,0.08);border-radius:18px;padding:8px 14px;color:#fff;font-size:14px;outline:none}
.uno-chat-send{width:36px;height:36px;border-radius:50%;border:none;background:#1a7ad8;color:#fff;font-size:18px;cursor:pointer;display:flex;align-items:center;justify-content:center}

@keyframes slideUp{from{transform:translateY(100%)}to{transform:translateY(0)}}
@keyframes fadeIn{from{opacity:0}to{opacity:1}}

/* When a new top card lands on the discard pile, slide it in from a
   random offset. Driven by JS adding the .uno-card-fly class for one
   tick. The little wobble at the end mimics a real card landing. */
@keyframes cardFly{
  0%{transform:translateY(-120px) translateX(40px) rotate(-18deg) scale(0.7);opacity:0}
  60%{opacity:1}
  85%{transform:translateY(4px) translateX(-2px) rotate(2deg) scale(1.02)}
  100%{transform:translateY(0) translateX(0) rotate(0) scale(1);opacity:1}
}
.uno-card-fly{animation:cardFly .42s cubic-bezier(.2,.7,.2,1.2)}

/* A subtle press effect on the draw pile so it feels alive */
.uno-draw-pile-card:hover{transform:translateY(-2px)}

/* Belt-and-suspenders: when overlays aren't `.show`, force them out of
   any layout role beyond display:none. Some Android Chrome builds have
   been observed to include hidden position:fixed elements in viewport
   bounds calculations during keyboard show/hide transitions, which can
   leave a gap between the chat input and the keyboard for that user.
   contain:strict explicitly tells the browser these subtrees can't
   affect anything outside themselves. */
.games-picker-ovl:not(.show),
.uno-ovl:not(.show),
.uno-color-pick:not(.show),
.uno-winner:not(.show){contain:strict;pointer-events:none}

/* ═══════════════════════════════════════════════════════════════════════
   v3.24 ZOMBIE GAME — Old-Maid variant
   Reuses many uno-* styles (header, lobby, opponent seats, chat panel,
   self-seat) since they're general-purpose. Zombie-specific bits:
     • Purple "Plato-style" felt background
     • Cute pixel/illustrated card backs (just CSS — colored gradients +
       a small 🧟 sigil for the back, ranks 7-A on the face)
     • Target-fan: when it's your turn, the player to your right's hand
       is laid out as a fan of card-backs you can tap to pick from.
   ═══════════════════════════════════════════════════════════════════════ */
.zomb-ovl{position:fixed;inset:0;z-index:230;background:#16121c;display:none;flex-direction:column;animation:fadeIn .2s}
.zomb-ovl.show{display:flex}
.zomb-ovl:not(.show){contain:strict;pointer-events:none}
.zomb-ovl::before{content:'';position:absolute;inset:0;background:var(--zomb-bg-image, none) center center/cover no-repeat;pointer-events:none;z-index:0}
.zomb-ovl::after{content:'';position:absolute;inset:0;background:radial-gradient(ellipse at center, rgba(60,30,80,0.18) 0%, rgba(0,0,0,0.55) 90%);pointer-events:none;z-index:0}

/* The "🧟 ZOMBIE" brand pill in the header */
.zomb-brand{display:inline-block;font-weight:900;letter-spacing:1.5px;font-size:16px;margin-right:6px;background:linear-gradient(135deg,#9eff5e,#3ec700);background-clip:text;-webkit-background-clip:text;color:transparent}

/* Lobby logo — chunky friendly zombie title */
.zomb-lobby-logo{margin-top:8px;text-align:center;font-weight:900;line-height:1}
.zomb-lobby-logo > span{display:block;margin-top:4px;font-size:38px;letter-spacing:3px;background:linear-gradient(180deg,#9eff5e 0%,#2da300 100%);background-clip:text;-webkit-background-clip:text;color:transparent;text-shadow:0 2px 18px rgba(158,255,94,0.18)}
.zomb-lobby-logo{font-size:54px}

/* Picker thumbnail variant for the Zombie list entry */
.games-picker-thumb-zomb{background:linear-gradient(135deg,#5e2e7a,#2e1340)!important}
.games-picker-thumb-zomb::after{display:none}
.games-picker-thumb-zomb span{position:static;color:#fff;font-size:30px;transform:none}

/* Center area where the target's hand fan + status text live */
.zomb-center{flex:1;display:flex;flex-direction:column;align-items:center;justify-content:center;gap:14px;padding:60px 12px 8px;min-height:0;position:relative;z-index:1}
.zomb-center-text{font-size:14px;color:#fff;font-weight:600;text-align:center;padding:6px 14px;background:rgba(0,0,0,0.45);border-radius:14px;max-width:90%;line-height:1.35}
.zomb-center-text.dim{color:#aaa;font-weight:500;background:rgba(0,0,0,0.25)}

/* Fan of the target's hidden cards (card-backs you can tap to pick) */
.zomb-target-fan{display:flex;justify-content:center;align-items:flex-end;gap:0;min-height:120px;padding:0 8px;flex-wrap:nowrap}
.zomb-target-fan .zomb-card{margin-left:-22px;transition:transform .18s,margin-left .2s}
.zomb-target-fan .zomb-card:first-child{margin-left:0}
.zomb-target-fan .zomb-card.pickable{cursor:pointer}
.zomb-target-fan .zomb-card.pickable:hover{transform:translateY(-10px) scale(1.03)}
.zomb-target-fan .zomb-card.pickable:active{transform:translateY(-16px) scale(1.05)}
.zomb-target-fan .zomb-card.picked{opacity:0;transform:translateY(-50px) scale(0.7)}
.zomb-target-fan-empty{color:#888;font-size:13px;font-style:italic;padding:30px 0}

/* Your own hand (face-up). Smaller than Uno cards since 27/players is a lot */
.zomb-hand{display:flex;justify-content:center;align-items:flex-end;gap:0;padding:0 14px;min-height:100px;min-width:min-content}
.zomb-hand .zomb-card{transition:transform .18s,margin-left .2s}

/* ─── ZOMB CARD ─── */
.zomb-card{position:relative;width:54px;height:80px;border-radius:6px;flex-shrink:0;box-shadow:0 3px 6px rgba(0,0,0,0.45);background:#fff;-webkit-user-select:none;user-select:none;overflow:hidden}
.zomb-card-face{position:absolute;inset:3px;border-radius:4px;display:flex;flex-direction:column;align-items:center;justify-content:center;background:#fdfdfd;font-weight:800}
.zomb-card .zomb-card-rank{font-size:22px;line-height:1;letter-spacing:-1px;color:#1a1a1a}
.zomb-card.suit-h .zomb-card-rank,.zomb-card.suit-d .zomb-card-rank{color:#e10600}
.zomb-card .zomb-card-suit{font-size:18px;line-height:1;margin-top:2px}
.zomb-card.suit-h .zomb-card-suit,.zomb-card.suit-d .zomb-card-suit{color:#e10600}
.zomb-card.suit-s .zomb-card-suit,.zomb-card.suit-c .zomb-card-suit{color:#1a1a1a}
.zomb-card .zomb-corner{position:absolute;font-size:10px;font-weight:800;line-height:1;color:#1a1a1a}
.zomb-card.suit-h .zomb-corner,.zomb-card.suit-d .zomb-corner{color:#e10600}
.zomb-card .zomb-corner.tl{top:3px;left:4px}
.zomb-card .zomb-corner.br{bottom:3px;right:4px;transform:rotate(180deg)}

/* Zombie card (the cursed one) */
.zomb-card.is-zombie .zomb-card-face{background:radial-gradient(circle at center,#5d2380 0%,#2d0e44 100%)}
.zomb-card.is-zombie .zomb-card-rank{font-size:34px;color:#9eff5e;text-shadow:0 0 8px rgba(158,255,94,0.6);filter:none}
.zomb-card.is-zombie .zomb-card-suit{display:none}
.zomb-card.is-zombie .zomb-corner{color:#9eff5e}
.zomb-card.is-zombie .zomb-corner::before{content:'🧟'}
.zomb-card.is-zombie .zomb-card-rank::before{content:'🧟'}

/* Card back — purple felt with small green sigil */
.zomb-card.back .zomb-card-face{background:linear-gradient(135deg,#5e2e7a 0%,#2d1141 100%);align-items:center;justify-content:center}
.zomb-card.back .zomb-card-face::after{content:'🧟';font-size:24px;filter:drop-shadow(0 0 3px rgba(158,255,94,0.4))}
.zomb-card.back .zomb-card-rank,.zomb-card.back .zomb-card-suit{display:none}
.zomb-card.back .zomb-corner{display:none}

/* Bigger variants */
.zomb-card.size-md{width:64px;height:94px}
.zomb-card.size-md .zomb-card-rank{font-size:26px}
.zomb-card.size-md .zomb-card-suit{font-size:20px}

/* End-game overlay — same skeleton as Uno winner but greener felt */
.zomb-end{position:fixed;inset:0;z-index:270;background:radial-gradient(ellipse at center,rgba(158,255,94,0.18) 0%,rgba(0,0,0,0.88) 70%);display:none;align-items:center;justify-content:center;flex-direction:column;gap:14px;animation:fadeIn .3s;padding:0 20px;text-align:center}
.zomb-end.show{display:flex}
.zomb-end:not(.show){contain:strict;pointer-events:none}
.zomb-end-emoji{font-size:84px;animation:zombBob 1.4s ease-in-out infinite}
@keyframes zombBob{0%,100%{transform:translateY(0) rotate(-6deg)}50%{transform:translateY(-12px) rotate(6deg)}}
.zomb-end-title{font-size:30px;font-weight:900;color:#9eff5e;text-shadow:0 3px 14px rgba(158,255,94,0.5);letter-spacing:1px}
.zomb-end-name{font-size:22px;color:#fff;font-weight:700}
.zomb-end-sub{font-size:13px;color:#aaa;font-style:italic}

/* ═══════════════════════════════════════════════════════════════════════ */


/* ───── STICKER PICKER PANEL (v3.10 — bottom sheet, Kyodo-style) ───── */
.sticker-panel{position:fixed;left:0;right:0;bottom:0;z-index:120;background:rgba(20,20,22,0.98);backdrop-filter:blur(20px);border-top-left-radius:18px;border-top-right-radius:18px;border-top:1px solid rgba(255,255,255,0.08);max-height:55vh;display:flex;flex-direction:column;transform:translateY(100%);transition:transform .25s cubic-bezier(.2,.7,.2,1);box-shadow:0 -8px 24px rgba(0,0,0,0.4);padding-bottom:env(safe-area-inset-bottom)}
.sticker-panel.open{transform:translateY(0)}
.sticker-panel-handle{width:44px;height:5px;border-radius:3px;background:rgba(255,255,255,0.18);margin:8px auto 4px;flex-shrink:0}
.sticker-panel-header{display:flex;align-items:center;justify-content:space-between;padding:6px 16px 10px;flex-shrink:0}
.sticker-panel-title{font-size:16px;font-weight:600;color:#fff}
.sticker-panel-close{width:30px;height:30px;border-radius:50%;border:none;background:rgba(255,255,255,0.08);color:#fff;font-size:18px;cursor:pointer;display:flex;align-items:center;justify-content:center;line-height:1}
.sticker-grid{flex:1;overflow-y:auto;padding:4px 12px 16px;display:grid;grid-template-columns:repeat(4,1fr);gap:8px;align-content:start}
.sticker-grid::-webkit-scrollbar{width:0}
.sticker-cell{aspect-ratio:1;background:rgba(255,255,255,0.04);border-radius:12px;display:flex;align-items:center;justify-content:center;cursor:pointer;overflow:hidden;border:none;padding:6px;transition:transform .12s,background .12s}
.sticker-cell:active{transform:scale(.93);background:rgba(255,255,255,0.08)}
.sticker-cell img{max-width:100%;max-height:100%;width:auto;height:auto;object-fit:contain;pointer-events:none}
.sticker-empty{grid-column:1/-1;text-align:center;padding:32px 12px;color:#8e8e93;font-size:13px;line-height:1.5}
.sticker-backdrop{position:fixed;inset:0;z-index:115;background:rgba(0,0,0,0.4);opacity:0;pointer-events:none;transition:opacity .2s}
.sticker-backdrop.open{opacity:1;pointer-events:auto}

/* v3.12 sticker upload UI */
.sticker-header-actions{display:flex;align-items:center;gap:8px}
.sticker-upload-btn{width:30px;height:30px;border-radius:50%;border:none;background:rgba(0,122,255,0.18);color:#0a84ff;font-size:20px;cursor:pointer;display:flex;align-items:center;justify-content:center;line-height:1;font-weight:300;transition:background .15s}
.sticker-upload-btn:hover{background:rgba(0,122,255,0.28)}
.sticker-upload-btn:active{transform:scale(.92)}
.sticker-upload-btn.busy{opacity:.6;pointer-events:none}
.sticker-upload-input{display:none}
.sticker-cell-wrap{position:relative}
.sticker-cell-del{position:absolute;top:-4px;right:-4px;width:20px;height:20px;border-radius:50%;background:#ff3b30;color:#fff;border:2px solid #141416;font-size:12px;font-weight:700;cursor:pointer;display:flex;align-items:center;justify-content:center;line-height:1;padding:0;z-index:2;box-shadow:0 2px 6px rgba(0,0,0,0.5)}
.sticker-cell-del:active{transform:scale(.85)}
.sticker-toast{position:fixed;left:50%;top:80px;transform:translateX(-50%);z-index:400;background:#1c1c1e;color:#fff;padding:10px 18px;border-radius:14px;font-size:13px;box-shadow:0 6px 18px rgba(0,0,0,0.5);opacity:0;pointer-events:none;transition:opacity .25s,transform .25s;border:1px solid rgba(255,255,255,0.08);max-width:80vw;text-align:center}
.sticker-toast.show{opacity:1;transform:translateX(-50%) translateY(0)}
.sticker-toast.err{background:#3a1f1f;border-color:rgba(255,69,58,0.4)}
.sticker-toast.ok{background:#1f2e1f;border-color:rgba(48,209,88,0.4)}
.sticker-toast.warn{background:#3a2e1a;border-color:rgba(255,159,10,0.55);color:#ffd9a8;line-height:1.4}
.sticker-uploading{grid-column:1/-1;text-align:center;padding:14px;color:#0a84ff;font-size:13px;font-weight:500}

.overlay{position:fixed;inset:0;z-index:100;background:rgba(0,0,0,.88);display:flex;align-items:center;justify-content:center}
.o-box{background:#1c1c1e;border-radius:16px;padding:24px;width:90%;max-width:340px;text-align:center}
.o-box h2{font-size:18px;margin-bottom:8px}
.o-box p{font-size:13px;color:#8e8e93;margin-bottom:14px}
.o-box input{width:100%;height:44px;border-radius:12px;border:1px solid #3a3a3c;background:#2c2c2e;color:#fff;padding:0 14px;font-size:15px;text-align:center;outline:none;margin-bottom:10px}
.o-box button{width:100%;height:44px;border-radius:12px;border:none;background:#007aff;color:#fff;font-size:15px;font-weight:600;cursor:pointer}
.av-preview{width:80px;height:80px;border-radius:50%;margin:0 auto 10px;background:#2c2c2e;display:flex;align-items:center;justify-content:center;font-size:32px;font-weight:600;color:#8e8e93;overflow:hidden;cursor:pointer;border:3px solid #3a3a3c;position:relative}
.av-preview img{position:absolute;top:0;left:0;width:100%;height:100%;object-fit:cover;display:block;border-radius:50%}
.av-in{display:none}
.debug{position:fixed;top:52px;left:0;right:0;z-index:9;background:rgba(0,0,0,.92);color:#0f0;font:11px monospace;padding:4px;max-height:200px;overflow-y:auto;display:none;white-space:pre-wrap}
.debug.show{display:block}
/* ════════════════════════════════════════════════════════════════════════
   SEAT PANEL — professional grid of voice-call seats
   ════════════════════════════════════════════════════════════════════════
   Layout:
     • Panel sits below the header. 3-column grid of avatar tiles.
     • 6 tiles fit on screen at once (3 cols × 2 rows). 7+ scrolls vertically.
     • Each tile = circular avatar + name (with tiny status dot inline).
     • Speaking → green ring around avatar pulses & glows.
     • Muted → small mic-off badge on bottom-right of avatar.
     • Host → "Host" badge under avatar (small chip).
     • Bottom edge has a drag handle: pull DOWN to collapse, a tiny pull-tab
       remains at the top of the chat so the user can re-open it.

   POSITIONING:
     The panel is position:absolute so it overlays the chat area instead of
     pushing it down. This is critical on mobile: when the keyboard opens,
     the available viewport height shrinks (100dvh tracks this). With the
     panel in the flex flow it would either get squeezed out of view or
     steal vertical space from the messages. As an absolute overlay it
     stays anchored at the top, the messages flow underneath it (older
     messages literally pass behind the panel as you scroll up — exactly
     the WhatsApp/Telegram pattern), and collapsing the panel reveals
     whatever was underneath without any reflow shock.
   ════════════════════════════════════════════════════════════════════════ */
.seat-panel{position:absolute;left:0;right:0;top:0;z-index:8;background:linear-gradient(180deg,rgba(20,20,22,0.78) 0%,rgba(20,20,22,0.62) 100%);backdrop-filter:blur(18px);-webkit-backdrop-filter:blur(18px);border-bottom:1px solid rgba(255,255,255,0.06);transition:max-height .28s cubic-bezier(.22,.61,.36,1),opacity .2s,padding .2s;overflow:hidden;max-height:min(150px,100%);display:flex;flex-direction:column}
.seat-panel.collapsed{max-height:0;padding-top:0;padding-bottom:0;border-bottom-width:0;opacity:0;pointer-events:none}
.seat-panel.dragging{transition:none}
.seat-panel.collapsed-live{border-bottom-color:rgba(255,255,255,0.04)}
/* Single horizontal row. Each seat has a fixed width (~84px) so tiles
   never squish as the room fills; with 4+ peers the row scrolls
   horizontally instead. The panel itself is much shorter than the
   previous 2-row grid → way more room left for the chat below, which
   matters most when the on-screen keyboard is up. */
.seat-grid-wrap{flex:1 1 auto;min-height:0;overflow-x:auto;overflow-y:hidden;padding:14px 12px 6px;scrollbar-width:none;-webkit-overflow-scrolling:touch}
.seat-grid-wrap::-webkit-scrollbar{display:none}
.seat-grid{display:flex;flex-direction:row;align-items:flex-start;gap:14px;min-width:max-content}
.seat{display:flex;flex-direction:column;align-items:center;gap:6px;width:84px;flex-shrink:0;min-width:0}
/* Wrapper sits OUTSIDE the clipped avatar so the mute badge can overlap
   the bottom-right edge without being chopped off by .seat-av's
   overflow:hidden (which we need to keep — it's what clips the <img>
   to a perfect circle). */
.seat-av-wrap{position:relative;width:64px;height:64px;flex-shrink:0}
.seat-av{position:relative;width:64px;height:64px;border-radius:50%;background:#2c2c2e;display:flex;align-items:center;justify-content:center;font-size:22px;font-weight:600;color:#8e8e93;overflow:hidden;border:3px solid transparent;transition:border-color .1s linear,box-shadow .1s linear;box-sizing:border-box}
.seat-av img{width:100%;height:100%;object-fit:cover;display:block}
/* Speaking ring: a gentle, reliable keyframe pulse on the green border
   + glow, exactly like v3.12.2 had. We tried a more elaborate voice-
   reactive halo using CSS custom properties + @property in v3.12.8/9
   but the cross-browser interpolation behavior wasn't consistent — on
   some setups the halo looked frozen. The simple keyframe works
   everywhere and reads clearly as "this person is speaking". */
.seat-av.speaking{border-color:#34c759;box-shadow:0 0 0 2px rgba(52,199,89,0.18),0 0 14px rgba(52,199,89,0.55);animation:seatPulse 1.4s ease-in-out infinite}
@keyframes seatPulse{
  0%,100%{box-shadow:0 0 0 2px rgba(52,199,89,0.18),0 0 10px rgba(52,199,89,0.45)}
  50%   {box-shadow:0 0 0 3px rgba(52,199,89,0.30),0 0 18px rgba(52,199,89,0.75)}
}
.seat-av.host-frame{border-color:rgba(255,204,0,0.85)}
.seat-av.host-frame.speaking{border-color:#34c759}
/* Mute badge: lives on the WRAPPER (not the clipped .seat-av), so it can
   overlap the bottom-right corner of the avatar circle while sitting
   mostly OUTSIDE the frame — matching the chat-app convention in the
   reference screenshot. */
.seat-mute{position:absolute;right:-4px;bottom:-2px;width:22px;height:22px;border-radius:50%;background:#ff3b30;border:2px solid rgba(20,20,22,0.95);display:flex;align-items:center;justify-content:center;color:#fff;z-index:2;box-shadow:0 2px 6px rgba(0,0,0,0.45);pointer-events:none}
.seat-mute svg{width:11px;height:11px}
.seat-name-row{display:flex;align-items:center;gap:5px;max-width:100%;min-width:0}
.seat-dot{width:7px;height:7px;border-radius:50%;background:#8e8e93;flex-shrink:0;transition:background .2s,box-shadow .2s}
.seat-dot.conn{background:#34c759;box-shadow:0 0 4px rgba(52,199,89,0.6)}
.seat-dot.fail{background:#ff3b30;box-shadow:0 0 4px rgba(255,59,48,0.6)}
.seat-dot.relay{background:#ff9500;box-shadow:0 0 4px rgba(255,149,0,0.6)}
.seat-dot.warn{background:#ffcc00;box-shadow:0 0 4px rgba(255,204,0,0.6)}
.seat-dot.connecting{background:#ffcc00;animation:pulse 1.2s infinite}
.seat-name{font-size:12px;font-weight:500;color:#fff;max-width:100%;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;letter-spacing:.1px}
.seat-badge{font-size:9px;font-weight:700;padding:1px 6px;border-radius:6px;background:linear-gradient(135deg,#ffd54a,#ffb300);color:#3a2400;letter-spacing:.4px;text-transform:uppercase;line-height:1.4;margin-top:-2px}
.seat-empty{opacity:.32}
.seat-empty .seat-av{background:rgba(255,255,255,0.04);border-style:dashed;border-color:rgba(255,255,255,0.10)}
.seat-empty .seat-av svg{width:26px;height:26px;color:rgba(255,255,255,0.18)}
.seat-empty .seat-name{color:rgba(255,255,255,0.25);font-style:italic}

/* Bottom drag handle of the panel — large hit target, small visual chip.
   flex-shrink:0 ensures the handle stays visible (and draggable) even if
   the panel is squeezed by the keyboard — the grid above shrinks first. */
.seat-handle{position:relative;height:18px;flex:0 0 18px;display:flex;align-items:center;justify-content:center;cursor:grab;user-select:none;touch-action:none;background:linear-gradient(180deg,transparent,rgba(0,0,0,0.18))}
.seat-handle:active{cursor:grabbing}
.seat-handle::before{content:'';width:42px;height:4px;border-radius:2px;background:rgba(255,255,255,0.28);transition:background .18s,width .18s}
.seat-handle:hover::before{background:rgba(255,255,255,0.45);width:54px}

/* Pull-tab shown when the panel is collapsed — sits flush under header.
   Same absolute-overlay positioning as .seat-panel so it doesn't disturb
   the messages flow when shown/hidden. */
.seat-pull-tab{position:absolute;left:0;right:0;top:0;z-index:8;height:0;overflow:hidden;display:flex;align-items:flex-start;justify-content:center;background:linear-gradient(180deg,rgba(20,20,22,0.85),rgba(20,20,22,0.55) 80%,transparent);transition:height .25s cubic-bezier(.22,.61,.36,1);cursor:pointer;touch-action:none;user-select:none;pointer-events:none}
.seat-pull-tab.show{height:22px;pointer-events:auto}
.seat-pull-tab-grip{display:flex;align-items:center;gap:6px;padding:3px 14px 4px;border-radius:0 0 12px 12px;background:rgba(40,40,44,0.92);border:1px solid rgba(255,255,255,0.08);border-top:none;font-size:10px;font-weight:600;color:rgba(255,255,255,0.72);letter-spacing:.3px}
.seat-pull-tab-grip svg{width:11px;height:11px}
.seat-pull-tab-grip .dotline{width:24px;height:3px;border-radius:1.5px;background:rgba(255,255,255,0.55)}

@keyframes pulse{50%{opacity:0.4}}
.hidden{display:none!important}

.room-full{position:fixed;inset:0;z-index:400;background:#e8dcc4;color:#3a2e1f;display:flex;flex-direction:column;align-items:center;justify-content:center;padding:32px;text-align:center;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif}
.room-full-icon{width:96px;height:96px;background:#3a2e1f;border-radius:50%;display:flex;align-items:center;justify-content:center;margin-bottom:20px}
.room-full-icon svg{width:54px;height:54px;color:#e8dcc4}
.room-full-count{font-size:42px;font-weight:700;letter-spacing:-1px;margin-bottom:8px;display:flex;align-items:center;gap:10px;color:#3a2e1f}
.room-full-count svg{width:32px;height:32px}
.room-full-text{font-size:17px;color:#5a4a35;font-weight:500;margin-bottom:24px}
.room-full-sub{font-size:13px;color:#7a6a55;max-width:280px;line-height:1.5}
.room-full-retry{margin-top:24px;height:42px;padding:0 22px;border-radius:21px;border:none;background:#3a2e1f;color:#e8dcc4;font-size:14px;font-weight:600;cursor:pointer}
.room-full-retry:active{opacity:0.85}
</style>
</head>
<body>
<div class="bg"></div>
<div class="debug" id="dbg"></div>

<div class="overlay" id="joinOvl">
<div class="o-box">
<h2>Join Silent Hill</h2>
<div class="av-preview" id="avPrev" onclick="document.getElementById('avIn').click()">
<span id="avInit">?</span>
</div>
<input type="file" class="av-in" id="avIn" accept="image/*" onchange="pickAv(event)">
<p style="color:#8e8e93;font-size:12px;margin-bottom:10px">Tap circle to add photo</p>
<input type="text" id="nameIn" placeholder="Your name" maxlength="20" onkeypress="if(event.key==='Enter')doJoin()">
<button id="joinBtn" onclick="doJoin()">Join Call</button>
</div>
</div>

<div class="app hidden" id="app">
<div class="header">
<button class="back-btn" onclick="leaveCall()">&#8249;</button>
<img class="group-icon" src="/ci.jpg" onerror="this.style.display='none'">
<div class="group-info"><div class="group-name">Silent Hill</div><div class="group-meta" id="mcount">0 in call</div></div>
<button class="games-header-btn" onclick="openGamesPicker()" title="Games" aria-label="Games"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="6" y1="11" x2="10" y2="11"/><line x1="8" y1="9" x2="8" y2="13"/><line x1="15" y1="12" x2="15.01" y2="12"/><line x1="18" y1="10" x2="18.01" y2="10"/><path d="M17.32 5H6.68a4 4 0 0 0-3.978 3.59c-.006.052-.01.101-.017.152C2.604 9.416 2 14.456 2 16a3 3 0 0 0 3 3c1 0 1.5-.5 2-1l1.414-1.414A2 2 0 0 1 9.828 16h4.344a2 2 0 0 1 1.414.586L17 18c.5.5 1 1 2 1a3 3 0 0 0 3-3c0-1.545-.604-6.584-.685-7.258A4 4 0 0 0 17.32 5z"/></svg></button>
<button class="leave-header-btn" onclick="leaveCall()" title="Leave call"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M9 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h4"/><polyline points="16 17 21 12 16 7"/><line x1="21" y1="12" x2="9" y2="12"/></svg></button>
<button class="menu-btn" onclick="toggleHeaderMenu(event)" aria-label="More">&#8942;</button>
<!-- v3.27: header dropdown menu — replaces the direct debug-toggle. -->
<div class="hdr-menu" id="hdrMenu">
  <button class="hdr-menu-item" onclick="onHdrLogs()">
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/><line x1="8" y1="13" x2="16" y2="13"/><line x1="8" y1="17" x2="16" y2="17"/></svg>
    <span>Logs</span>
  </button>
  <button class="hdr-menu-item" onclick="onHdrStartStream()">
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polygon points="23 7 16 12 23 17 23 7"/><rect x="1" y="5" width="15" height="14" rx="2" ry="2"/></svg>
    <span>Start Streaming</span>
  </button>
</div>
</div>

<!-- This stack hosts the seat panel (absolute overlay), the pull-tab
     (absolute overlay shown when collapsed), and the messages list.
     The seat-panel sits on top of messages — older messages flow up
     behind it as the user scrolls. Collapsing the panel reveals what
     was behind it without any reflow shock. The wrapper is flex:1 so
     it absorbs all space between the header and the input bar; when
     the mobile keyboard opens, only this region shrinks (the seat
     panel itself stays put at the top of this region). -->
<div class="chat-stack">

<!-- Seat panel: avatar-tile grid for the call. Collapsible via the bottom handle. -->
<div class="seat-panel" id="seatPanel">
  <div class="seat-grid-wrap" id="seatGridWrap">
    <div class="seat-grid" id="seatGrid"></div>
  </div>
  <div class="seat-handle" id="seatHandle" aria-label="Drag to collapse seats" role="separator"></div>
</div>
<!-- Pull-tab shown when collapsed — tap or pull down to re-open -->
<div class="seat-pull-tab" id="seatPullTab" aria-label="Pull down to show seats">
  <div class="seat-pull-tab-grip">
    <span class="dotline"></span>
    <span id="seatPullCount">0/0</span>
    <span class="dotline"></span>
  </div>
</div>

<!-- v3.27: stream player. Hidden by default; only shown when a stream is
     active. While streaming, the seat panel above shrinks (CSS via body
     .streaming class) so this player takes the prominent space. -->
<div class="stream-panel" id="streamPanel">
  <div class="stream-video-wrap" id="streamVideoWrap">
    <video id="streamVideo" playsinline preload="auto"></video>
    <div class="stream-tap-shield" id="streamTapShield"></div>
    <!-- "Tap to unmute" overlay shown when autoplay forced muted -->
    <div class="stream-unmute-prompt" id="streamUnmutePrompt" onclick="streamUserUnmute()">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polygon points="11 5 6 9 2 9 2 15 6 15 11 19 11 5"/><path d="M15.54 8.46a5 5 0 0 1 0 7.07"/><path d="M19.07 4.93a10 10 0 0 1 0 14.14"/></svg>
      <span>Tap to unmute</span>
    </div>
    <!-- v3.31: controls overlay sits at the bottom of the video frame
         itself instead of below it. Auto-hides 2.5s after the last
         interaction, fades back in on tap. Pause forces them visible.
         This reclaims most of the bottom strip for the actual video. -->
    <div class="stream-controls" id="streamControls">
      <div class="stream-title" id="streamTitle">—</div>
      <div class="stream-progress" id="streamProgress">
        <div class="stream-progress-fill" id="streamProgressFill"></div>
        <div class="stream-progress-handle" id="streamProgressHandle"></div>
      </div>
      <div class="stream-time-row">
        <span id="streamCurTime">0:00</span>
        <span id="streamDur">0:00</span>
      </div>
      <div class="stream-buttons" id="streamButtons">
        <button class="stream-btn" id="streamBtnPrev" onclick="streamCtl('prev')" aria-label="Previous">
          <svg viewBox="0 0 24 24" fill="currentColor"><polygon points="19 20 9 12 19 4 19 20"/><line x1="5" y1="19" x2="5" y2="5" stroke="currentColor" stroke-width="2"/></svg>
        </button>
        <button class="stream-btn stream-btn-play" id="streamBtnPlay" onclick="streamCtl('toggle')" aria-label="Play/pause">
          <svg viewBox="0 0 24 24" fill="currentColor"><polygon points="5 3 19 12 5 21 5 3"/></svg>
        </button>
        <button class="stream-btn" id="streamBtnNext" onclick="streamCtl('next')" aria-label="Next">
          <svg viewBox="0 0 24 24" fill="currentColor"><polygon points="5 4 15 12 5 20 5 4"/><line x1="19" y1="5" x2="19" y2="19" stroke="currentColor" stroke-width="2"/></svg>
        </button>
        <button class="stream-btn stream-btn-stop" id="streamBtnStop" onclick="streamStop()" aria-label="Stop streaming">
          <svg viewBox="0 0 24 24" fill="currentColor"><rect x="6" y="6" width="12" height="12"/></svg>
        </button>
      </div>
      <div class="stream-viewer-info" id="streamViewerInfo"><span id="streamerName">Someone</span> is streaming</div>
    </div>
  </div>
</div>

<div class="messages-wrap">
  <div class="messages" id="msgs"></div>
  <button class="jump-latest" id="jumpLatest" onclick="scrollToLatest(true)" aria-label="Jump to latest messages">
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round">
      <line x1="12" y1="5" x2="12" y2="19"/>
      <polyline points="19 12 12 19 5 12"/>
    </svg>
    <span class="badge hidden" id="jumpBadge">0</span>
  </button>
</div>

</div><!-- /.chat-stack -->

<div class="typing-bar hidden" id="typingBar"></div>
<div class="input-bar">
<button class="input-attach" onclick="document.getElementById('imgIn').click()" title="Send image">+</button>
<button class="mic-btn" id="muteBtn" onclick="toggleMute()" title="Mute"><svg id="micIcon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 1a3 3 0 0 0-3 3v8a3 3 0 0 0 6 0V4a3 3 0 0 0-3-3z"/><path d="M19 10v2a7 7 0 0 1-14 0v-2"/><line x1="12" y1="19" x2="12" y2="23"/><line x1="8" y1="23" x2="16" y2="23"/></svg></button>
<input type="file" id="imgIn" accept="image/*" style="display:none" onchange="pickChatImage(event)">
<div class="input-wrap">
  <textarea class="input-field" id="msgIn" placeholder="Write a message..." rows="1" maxlength="1000"></textarea>
  <button class="sticker-btn" id="stickerBtn" onclick="toggleStickerPanel()" title="Stickers" aria-label="Open stickers"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M21 13.5V8a3 3 0 0 0-3-3H6a3 3 0 0 0-3 3v8a3 3 0 0 0 3 3h5.5"/><path d="M21 13.5L13.5 21a4.5 4.5 0 0 1 0-9"/><path d="M13.5 21A4.5 4.5 0 0 1 18 16.5"/></svg></button>
</div>
<button class="input-send" onclick="sendMsg()">&#10148;</button>
</div>
</div>

<!-- v3.10: sticker bottom sheet + dim backdrop -->
<div class="sticker-backdrop" id="stickerBackdrop" onclick="closeStickerPanel()"></div>
<div class="sticker-panel" id="stickerPanel" role="dialog" aria-label="Stickers">
  <div class="sticker-panel-handle"></div>
  <div class="sticker-panel-header">
    <div class="sticker-panel-title">Stickers</div>
    <div class="sticker-header-actions">
      <button class="sticker-upload-btn" id="stickerUploadBtn" onclick="onStickerUploadClick()" title="Add sticker" aria-label="Upload new sticker">+</button>
      <button class="sticker-panel-close" onclick="closeStickerPanel()" aria-label="Close stickers">&times;</button>
    </div>
  </div>
  <div class="sticker-grid" id="stickerGrid"></div>
  <input type="file" id="stickerUploadInput" class="sticker-upload-input" accept="image/png,image/jpeg,image/jpg,image/webp,image/gif" onchange="onStickerFilePicked(event)">
</div>
<div id="stickerToast" class="sticker-toast" role="status" aria-live="polite"></div>

<!-- ════════════════════════════════════════════════════════════════════
     v3.23 GAMES + UNO OVERLAYS
     These are full-viewport overlays. They live OUTSIDE the .app stack
     so they overlay the entire viewport including the header, but they
     do NOT remove WebRTC <audio> elements (which are mounted under #audios
     and keep playing). Voice continues uninterrupted while playing.
     ════════════════════════════════════════════════════════════════════ -->

<!-- v3.27: streaming upload bottom-sheet. Visible only to the person
     who initiated streaming. Lets them queue one or more video files,
     reorder via remove + re-add, then "Start streaming" sends the
     playlist to the server, which broadcasts to all peers. -->
<div class="stream-sheet-ovl" id="streamSheetOvl" onclick="if(event.target===this)closeStreamSheet()">
  <div class="stream-sheet-box">
    <div class="games-picker-handle"></div>
    <div class="stream-sheet-title">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="width:20px;height:20px;vertical-align:-3px;margin-right:6px"><polygon points="23 7 16 12 23 17 23 7"/><rect x="1" y="5" width="15" height="14" rx="2" ry="2"/></svg>
      Stream a video
    </div>
    <div class="stream-sheet-sub">Add videos in the order you want them played. Everyone in the room will see them.</div>
    <div class="stream-sheet-queue" id="streamQueue"></div>
    <div class="stream-sheet-upload-row">
      <label class="stream-sheet-upload-btn">
        <input type="file" id="streamFileInput" accept="video/*" multiple onchange="onStreamFilesPicked(event)" hidden>
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="12" y1="5" x2="12" y2="19"/><line x1="5" y1="12" x2="19" y2="12"/></svg>
        <span>Add videos</span>
      </label>
    </div>
    <div class="stream-sheet-actions">
      <button class="uno-btn-secondary" onclick="closeStreamSheet()" style="padding:12px">Cancel</button>
      <button class="uno-btn-primary" id="streamSheetStart" onclick="onStreamSheetStart()" style="padding:12px">Start streaming</button>
    </div>
    <div class="stream-sheet-error" id="streamSheetError"></div>
  </div>
</div>

<!-- Games picker -->
<div class="games-picker-ovl" id="gamesPickerOvl" onclick="if(event.target===this)closeGamesPicker()">
  <div class="games-picker-box">
    <div class="games-picker-handle"></div>
    <div class="games-picker-title"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="width:20px;height:20px;vertical-align:-3px;margin-right:6px"><line x1="6" y1="11" x2="10" y2="11"/><line x1="8" y1="9" x2="8" y2="13"/><line x1="15" y1="12" x2="15.01" y2="12"/><line x1="18" y1="10" x2="18.01" y2="10"/><path d="M17.32 5H6.68a4 4 0 0 0-3.978 3.59c-.006.052-.01.101-.017.152C2.604 9.416 2 14.456 2 16a3 3 0 0 0 3 3c1 0 1.5-.5 2-1l1.414-1.414A2 2 0 0 1 9.828 16h4.344a2 2 0 0 1 1.414.586L17 18c.5.5 1 1 2 1a3 3 0 0 0 3-3c0-1.545-.604-6.584-.685-7.258A4 4 0 0 0 17.32 5z"/></svg>Games</div>
    <div class="games-picker-list">
      <div class="games-picker-item" onclick="onPickGameUno()">
        <div class="games-picker-thumb"><span>UNO</span></div>
        <div class="games-picker-info">
          <div class="games-picker-name">Uno — Classic</div>
          <div class="games-picker-desc">2-5 players · Match colors, stack +2/+4</div>
        </div>
      </div>
      <div class="games-picker-item" onclick="onPickGameZombie()">
        <div class="games-picker-thumb games-picker-thumb-zomb"><span>🧟</span></div>
        <div class="games-picker-info">
          <div class="games-picker-name">Zombie!</div>
          <div class="games-picker-desc">2-5 players · Don't get stuck with the Zombie</div>
        </div>
      </div>
      <div class="games-picker-item" style="opacity:0.5;cursor:default" onclick="event.stopPropagation()">
        <div class="games-picker-thumb" style="background:linear-gradient(135deg,#444,#222)"><span style="color:#888">SOON</span></div>
        <div class="games-picker-info">
          <div class="games-picker-name">More games</div>
          <div class="games-picker-desc">Coming next versions</div>
        </div>
        <div class="games-picker-soon">SOON</div>
      </div>
    </div>
  </div>
</div>

<!-- UNO game overlay -->
<div class="uno-ovl" id="unoOvl">
  <div class="uno-header">
    <button class="uno-back" onclick="onUnoBack()" aria-label="Back">&#8249;</button>
    <div class="uno-title"><span class="uno-brand">UNO</span><span id="unoSubtitle">Lobby</span></div>
    <button class="uno-mute-btn" id="unoMuteBtn" onclick="toggleMute()" title="Mute" aria-label="Mute"></button>
    <button class="uno-chat-btn" onclick="toggleUnoChat()" title="Chat" aria-label="Chat">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"/></svg>
      <span class="uno-chat-badge" id="unoChatBadge">1</span>
    </button>
  </div>

  <div class="uno-body" id="unoBody">

    <!-- Lobby view -->
    <div class="uno-lobby" id="unoLobby">
      <div class="uno-lobby-logo">UNO</div>

      <div class="uno-lobby-section" id="unoCreateBox">
        <div class="uno-lobby-h">Players</div>
        <div class="uno-player-count-row" id="unoPlayerCountRow">
          <button class="uno-player-count-btn sel" data-n="2" onclick="onPickPlayerCount(2)">2</button>
          <button class="uno-player-count-btn" data-n="3" onclick="onPickPlayerCount(3)">3</button>
          <button class="uno-player-count-btn" data-n="4" onclick="onPickPlayerCount(4)">4</button>
          <button class="uno-player-count-btn" data-n="5" onclick="onPickPlayerCount(5)">5</button>
        </div>
      </div>

      <div class="uno-lobby-section" id="unoWaitingBox" style="display:none">
        <div class="uno-lobby-h" id="unoWaitingHeader">Waiting for players</div>
        <div class="uno-lobby-players-list" id="unoLobbyPlayersList"></div>
      </div>

      <div class="uno-lobby-actions" id="unoLobbyActions">
        <button class="uno-btn-primary" id="unoCreateBtn" onclick="onUnoCreate()">Create Game</button>
      </div>
    </div>

    <!-- Play view -->
    <div class="uno-play" id="unoPlay" style="display:none">
      <div class="uno-opponents" id="unoOpponents"></div>
      <div class="uno-turn-bar" id="unoTurnBar">Waiting...</div>
      <div class="uno-table">
        <div class="uno-pile">
          <div class="uno-pile-label">DRAW</div>
          <div class="uno-card back size-md" id="unoDrawPile" onclick="onUnoDraw()">
            <div class="uno-card-face"><div class="uno-card-oval"></div><div class="uno-card-num"></div></div>
          </div>
          <div class="uno-pile-count" id="unoDrawCount">— left</div>
        </div>
        <div class="uno-pile">
          <div class="uno-pile-label">DISCARD <span class="uno-color-dot" id="unoColorDot"></span><span class="uno-stack-badge" id="unoStackBadge"></span></div>
          <div id="unoDiscardSlot"></div>
        </div>
      </div>
      <!-- Recent-actions log (last 3 events, fading). Plato-style hint
           for "what just happened" — sits right under the discard pile. -->
      <div class="game-log" id="unoLog"></div>
      <div class="uno-actions-row" id="unoActionsRow">
        <button class="uno-act-btn draw" id="unoBtnDraw" onclick="onUnoDraw()">Draw</button>
        <button class="uno-act-btn" id="unoBtnPass" onclick="onUnoPass()" style="display:none">Pass</button>
      </div>
      <div class="uno-self-seat" id="unoSelfSeat">
        <div class="uno-opp-av" id="unoSelfAv">?</div>
        <div>
          <div class="uno-self-seat-name" id="unoSelfName">You <span class="you-pill">YOU</span></div>
          <div class="uno-self-timer" id="unoSelfTimer"><div class="uno-self-timer-fill" id="unoSelfTimerFill"></div></div>
        </div>
      </div>
      <div class="uno-hand-wrap"><div class="uno-hand" id="unoHand"></div></div>
    </div>

    <!-- UNO! call button (floats) -->
    <button class="uno-call-btn" id="unoCallBtn" onclick="onCallUno()">UNO!</button>

    <!-- Toast -->
    <div class="uno-toast" id="unoToast"></div>

    <!-- Side chat panel (in-game chat) -->
    <div class="uno-chat-panel" id="unoChatPanel">
      <div class="uno-chat-panel-header">
        <div class="uno-chat-panel-title">Game Chat</div>
        <button class="uno-chat-panel-close" onclick="toggleUnoChat()" aria-label="Close">&times;</button>
      </div>
      <div class="uno-chat-msgs" id="unoChatMsgs"></div>
      <div class="uno-chat-input-row">
        <input type="text" class="uno-chat-input" id="unoChatInput" placeholder="Type a message..." maxlength="500" onkeypress="if(event.key==='Enter')onUnoChatSend()">
        <button class="uno-chat-send" onclick="onUnoChatSend()" aria-label="Send">&#8250;</button>
      </div>
    </div>

  </div>
</div>

<!-- Color picker (post-wild) -->
<div class="uno-color-pick" id="unoColorPick">
  <div class="uno-color-pick-box">
    <div class="uno-color-pick-h">Choose a color</div>
    <div class="uno-color-pick-grid">
      <button class="uno-color-pick-btn c-r" onclick="onPickColor('r')">RED</button>
      <button class="uno-color-pick-btn c-y" onclick="onPickColor('y')">YELLOW</button>
      <button class="uno-color-pick-btn c-g" onclick="onPickColor('g')">GREEN</button>
      <button class="uno-color-pick-btn c-b" onclick="onPickColor('b')">BLUE</button>
    </div>
  </div>
</div>

<!-- Winner overlay -->
<div class="uno-winner" id="unoWinner">
  <div class="uno-winner-trophy">🏆</div>
  <div class="uno-winner-title" id="unoWinnerTitle">Winner!</div>
  <div class="uno-winner-name" id="unoWinnerName">—</div>
  <div class="uno-winner-actions">
    <button class="uno-btn-primary" onclick="onPlayAgain()" style="padding:12px 22px">Play Again</button>
    <button class="uno-btn-secondary" onclick="onWinnerClose()" style="padding:12px 22px">Close</button>
  </div>
</div>

<!-- ════════════════════════════════════════════════════════════════════
     v3.24 ZOMBIE OVERLAY — Old-Maid-style game with the Plato vibe
     ════════════════════════════════════════════════════════════════════ -->
<div class="zomb-ovl" id="zombOvl">
  <div class="uno-header">
    <button class="uno-back" onclick="onZombBack()" aria-label="Back">&#8249;</button>
    <div class="uno-title"><span class="zomb-brand">🧟 ZOMBIE</span><span id="zombSubtitle">Lobby</span></div>
    <button class="uno-mute-btn" id="zombMuteBtn" onclick="toggleMute()" title="Mute" aria-label="Mute"></button>
    <button class="uno-chat-btn" onclick="toggleZombChat()" title="Chat" aria-label="Chat">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"/></svg>
      <span class="uno-chat-badge" id="zombChatBadge">1</span>
    </button>
  </div>

  <div class="uno-body" id="zombBody">

    <!-- Lobby -->
    <div class="uno-lobby" id="zombLobby">
      <div class="zomb-lobby-logo">🧟<br><span>ZOMBIE</span></div>
      <div class="uno-lobby-section" id="zombCreateBox">
        <div class="uno-lobby-h">Players</div>
        <div class="uno-player-count-row" id="zombPlayerCountRow">
          <button class="uno-player-count-btn sel" data-n="2" onclick="onZombPickPlayerCount(2)">2</button>
          <button class="uno-player-count-btn" data-n="3" onclick="onZombPickPlayerCount(3)">3</button>
          <button class="uno-player-count-btn" data-n="4" onclick="onZombPickPlayerCount(4)">4</button>
          <button class="uno-player-count-btn" data-n="5" onclick="onZombPickPlayerCount(5)">5</button>
        </div>
      </div>
      <div class="uno-lobby-section" id="zombWaitingBox" style="display:none">
        <div class="uno-lobby-h" id="zombWaitingHeader">Waiting for players</div>
        <div class="uno-lobby-players-list" id="zombLobbyPlayersList"></div>
      </div>
      <div class="uno-lobby-actions" id="zombLobbyActions">
        <button class="uno-btn-primary" id="zombCreateBtn" onclick="onZombCreate()">Create Game</button>
      </div>
    </div>

    <!-- Play view -->
    <div class="uno-play" id="zombPlay" style="display:none">
      <div class="uno-opponents" id="zombOpponents"></div>
      <div class="uno-turn-bar" id="zombTurnBar">Waiting...</div>
      <!-- Center "pick from" area -->
      <div class="zomb-center" id="zombCenter">
        <div class="zomb-center-text" id="zombCenterText">—</div>
        <!-- Target's hidden cards laid out fan-style; the picker taps one -->
        <div class="zomb-target-fan" id="zombTargetFan"></div>
      </div>
      <div class="game-log" id="zombLog"></div>
      <div class="uno-self-seat" id="zombSelfSeat">
        <div class="uno-opp-av" id="zombSelfAv">?</div>
        <div>
          <div class="uno-self-seat-name" id="zombSelfName">You <span class="you-pill">YOU</span></div>
          <div class="uno-self-timer" id="zombSelfTimer"><div class="uno-self-timer-fill" id="zombSelfTimerFill"></div></div>
        </div>
      </div>
      <div class="uno-hand-wrap"><div class="zomb-hand" id="zombHand"></div></div>
    </div>

    <!-- Toast -->
    <div class="uno-toast" id="zombToast"></div>

    <!-- Side chat panel (reusing uno-chat-panel styles for consistency) -->
    <div class="uno-chat-panel" id="zombChatPanel">
      <div class="uno-chat-panel-header">
        <div class="uno-chat-panel-title">Game Chat</div>
        <button class="uno-chat-panel-close" onclick="toggleZombChat()" aria-label="Close">&times;</button>
      </div>
      <div class="uno-chat-msgs" id="zombChatMsgs"></div>
      <div class="uno-chat-input-row">
        <input type="text" class="uno-chat-input" id="zombChatInput" placeholder="Type a message..." maxlength="500" onkeypress="if(event.key==='Enter')onZombChatSend()">
        <button class="uno-chat-send" onclick="onZombChatSend()" aria-label="Send">&#8250;</button>
      </div>
    </div>
  </div>
</div>

<!-- Zombie end-game overlay: shows the Zombie's name (the LOSER) -->
<div class="zomb-end" id="zombEnd">
  <div class="zomb-end-emoji">🧟</div>
  <div class="zomb-end-title" id="zombEndTitle">The Zombie!</div>
  <div class="zomb-end-name" id="zombEndName">—</div>
  <div class="zomb-end-sub" id="zombEndSub">Everyone else is a winner</div>
  <div class="uno-winner-actions">
    <button class="uno-btn-primary" onclick="onZombPlayAgain()" style="padding:12px 22px">Play Again</button>
    <button class="uno-btn-secondary" onclick="onZombEndClose()" style="padding:12px 22px">Close</button>
  </div>
</div>

<script>
// ════════════════════════════════════════════════════════════════════════════
// LIVEKIT SFU VOICE — connects on `your_id`, publishes mic, attaches remote
// audio tracks to <audio> elements keyed by peer_id (mirrors `audios[pid]`).
// Existing UI code (speaking ring, mute states, seat tiles) works unchanged.
// Old mesh-WebRTC functions are neutralized inside lkConnect() so any
// straggler call just no-ops.
// ════════════════════════════════════════════════════════════════════════════
const LK = {
  room: null,
  connectedPid: null,
  connecting: false,
  micPub: null,
  remoteAudioByPid: {},
};

const _LKC = window.LivekitClient || null;
if (!_LKC) console.error("[LK] LivekitClient SDK failed to load from CDN.");

async function lkFetchToken(identity, displayName, isAdmin) {
  const qs = new URLSearchParams({
    room: ROOM,
    identity: identity,
    name: displayName || identity,
    t: TOKEN,
    is_admin: isAdmin ? "1" : "0",
  });
  const r = await fetch("/livekit-token?" + qs.toString(), { credentials: "same-origin" });
  if (!r.ok) {
    const txt = await r.text().catch(() => "");
    throw new Error("token HTTP " + r.status + ": " + txt.slice(0, 200));
  }
  return await r.json();
}

async function lkConnect() {
  if (!_LKC) { try { log("LK SDK missing — voice disabled"); } catch(e){} return; }
  if (LK.connecting || LK.room) return;
  if (!MY_ID) { try { log("LK connect: no MY_ID yet"); } catch(e){} return; }
  LK.connecting = true;

  // neutralize the old mesh-WebRTC functions IMMEDIATELY, before any
  // peer_joined / peers handler tries to call createOffer. This is the
  // earliest safe moment — we know MY_ID is set, which means `your_id`
  // just arrived, which means peer_joined messages are about to follow.
  try {
    const _noop = function(){};
    const _noopAsync = function(){ return Promise.resolve(); };
    window.createOffer          = _noopAsync;
    window.handleOffer          = _noopAsync;
    window.handleAnswer         = _noopAsync;
    window.handleIce            = _noopAsync;
    window.forceIceRestart      = _noopAsync;
    window.scheduleRetry        = _noopAsync;
    window.startConnectionTimer = _noop;
    window.clearConnectionTimer = _noop;
    window.setupPC              = _noop;
    window.fetchIceServers      = async function(){ return []; };
    window.applyBitrateToAll    = _noopAsync;
    window.preferOpusAndTune    = function(sdp){ return sdp; };
    try { log("v4.0 mesh neutralized"); } catch(e){}
  } catch (e) {
    try { log("v4.0 neutralize err: " + (e && e.message)); } catch(e2){}
  }

  try {
    const { url, token } = await lkFetchToken(MY_ID, (myName || "User"), !!MY_IS_ADMIN);

    const room = new _LKC.Room({
      // adaptiveStream applies to VIDEO subscribers only (we don't publish
      // video). dynacast=false because we want every subscriber to get our
      // full-quality stream — no down-spec for low-bandwidth peers.
      adaptiveStream: false,
      dynacast: false,
      // BEAST MODE audio: 96 kbps fixed Opus, RED for packet-loss resilience,
      // DTX OFF so silence is real silence rather than comfort noise (keeps
      // voice texture intact between words). Echo cancellation, noise
      // suppression, and auto-gain are enabled via captureDefaults below.
      publishDefaults: {
        audioPreset: { maxBitrate: 96000 },
        dtx: false,
        red: true,
        stopMicTrackOnMute: false,
      },
      audioCaptureDefaults: {
        echoCancellation: true,
        noiseSuppression: true,
        autoGainControl: true,
        channelCount: 1,
        sampleRate: 48000,
      },
      stopLocalTrackOnUnpublish: false,
      reconnectPolicy: { nextRetryDelayInMs: () => 1000 },
    });
    LK.room = room;
    LK.connectedPid = MY_ID;

    const RE = _LKC.RoomEvent;
    const Track = _LKC.Track;

    room.on(RE.TrackSubscribed, (track, publication, participant) => {
      if (track.kind !== Track.Kind.Audio) return;
      const pid = participant.identity;
      let el = LK.remoteAudioByPid[pid];
      if (!el) {
        el = document.createElement('audio');
        el.autoplay = true;
        el.playsInline = true;
        el.setAttribute('playsinline', '');
        document.body.appendChild(el);
        LK.remoteAudioByPid[pid] = el;
      }
      try { track.attach(el); } catch (e) {}
      // Mirror into the existing `audios` map so the rest of the code (level
      // metering, self-heal scans, etc.) can find this element by pid.
      try { audios[pid] = el; } catch (e) {}
      // mark this peer as connected so the seat-tile quality dot
      // turns green. Without this the dot stays on 'new' / 'connecting'
      // and pulses yellow forever even though audio is flowing fine.
      try {
        const pm = peerMap.get(pid);
        if (pm) {
          pm.connState = 'connected';
          pm.lastHeardAt = Date.now();
          pm.recvRate = 50;   // dummy "healthy" value — keeps old broken-audio heuristic from firing
          pm.lossPct = 0;
          try { updPeers(); } catch (e) {}
        }
      } catch (e) {}
      try { log("LK audio attached " + pid); } catch (e) {}
      try { el.play && el.play().catch(()=>{}); } catch (e) {}
    });

    room.on(RE.TrackUnsubscribed, (track, publication, participant) => {
      const pid = participant.identity;
      try { track.detach().forEach(el => el.remove()); } catch (e) {}
      if (LK.remoteAudioByPid[pid]) delete LK.remoteAudioByPid[pid];
      if (audios[pid]) { try { audios[pid].srcObject = null; } catch (e) {} }
    });

    // LiveKit's ActiveSpeakersChanged fires when the set of active speakers
    // changes (server-side detection — much cleaner than our previous client
    // mic-level threshold approach). We feed it into the same UI hook the
    // old code used.
    room.on(RE.ActiveSpeakersChanged, (speakers) => {
      const speakingNow = new Set(speakers.map(s => s.identity));
      // Build per-pid level for the green ring.
      try {
        for (const [pid, p] of peerMap) {
          const sp = speakers.find(s => s.identity === pid);
          const lvl = sp ? Math.min(1, sp.audioLevel || 0.5) : 0;
          // peerMap entries store ._level for the seat-tile ring renderer.
          p._level = lvl;
          p._speaking = sp ? true : false;
        }
      } catch (e) {}
      // Also drive self if our identity is in there.
      try {
        const me = speakers.find(s => s.identity === MY_ID);
        window._selfSpeaking = !!me && !isMuted;
      } catch (e) {}
    });

    room.on(RE.ParticipantDisconnected, (participant) => {
      const pid = participant.identity;
      if (LK.remoteAudioByPid[pid]) {
        try { LK.remoteAudioByPid[pid].srcObject = null; } catch (e) {}
        try { LK.remoteAudioByPid[pid].remove(); } catch (e) {}
        delete LK.remoteAudioByPid[pid];
      }
      if (audios[pid]) delete audios[pid];
    });

    // LiveKit emits ConnectionQualityChanged with a quality enum
    // (Excellent / Good / Poor / Lost) — feed it into the seat-tile dot
    // so it actually reflects reality. The old mesh-derived stats (lossEwma,
    // recvRate, lastHeardAt) don't apply anymore, so we drive connState
    // directly here.
    room.on(RE.ConnectionQualityChanged, (quality, participant) => {
      if (!participant) return;
      const pid = participant.identity;
      // For local participant, we drive an internal flag for the self-tile.
      // For remotes, update peerMap.
      const ConnQ = _LKC.ConnectionQuality || {};
      let dot = 'connected';
      let loss = 0;
      if (quality === ConnQ.Excellent) { dot = 'connected'; loss = 0; }
      else if (quality === ConnQ.Good)  { dot = 'connected'; loss = 3; }
      else if (quality === ConnQ.Poor)  { dot = 'connected'; loss = 18; }
      else if (quality === ConnQ.Lost)  { dot = 'failed';    loss = 100; }

      if (pid === MY_ID) {
        // Self quality — there's no peerMap entry; stash in a global so the
        // self-tile renderer can read it (the existing code may not use it,
        // which is fine — leaving the hook in place for future polish).
        window._selfConnState = dot;
      } else {
        const pm = peerMap.get(pid);
        if (pm) {
          pm.connState = dot;
          pm.lossPct = loss;
          if (dot === 'connected') pm.lastHeardAt = Date.now();
          try { updPeers(); } catch (e) {}
        }
      }
    });

    // keep lastHeardAt fresh while audio is actually flowing so the
    // "audioActuallyBroken" heuristic in _seatDotClass never fires for a
    // healthy LiveKit subscription. Lightweight 2s heartbeat.
    if (LK._heartbeatTimer) clearInterval(LK._heartbeatTimer);
    LK._heartbeatTimer = setInterval(() => {
      try {
        if (!LK.room) return;
        const remotes = LK.room.remoteParticipants;
        if (!remotes) return;
        const now = Date.now();
        remotes.forEach((rp) => {
          const pid = rp.identity;
          const pm = peerMap.get(pid);
          if (!pm) return;
          // Did we receive any audio frames in the last interval? If the
          // participant has an audio publication that's subscribed and not
          // muted on their side, treat it as live.
          let alive = false;
          try {
            rp.audioTrackPublications && rp.audioTrackPublications.forEach((pub) => {
              if (pub.isSubscribed && pub.track && !pub.isMuted) alive = true;
            });
          } catch (e) {}
          if (alive) {
            pm.lastHeardAt = now;
            pm.recvRate = 50;
            if (pm.connState !== 'connected') pm.connState = 'connected';
          }
        });
      } catch (e) {}
    }, 2000);

    room.on(RE.Disconnected, () => {
      try { log("LK disconnected"); } catch (e) {}
    });

    room.on(RE.Reconnecting, () => { try { log("LK reconnecting"); } catch (e) {} });
    room.on(RE.Reconnected,  () => { try { log("LK reconnected"); } catch (e) {} });

    await room.connect(url, token);
    try { log("LK connected as " + MY_ID); } catch (e) {}

    // Publish the existing localStream's mic track to LiveKit. We reuse the
    // exact same MediaStreamTrack the rest of the code already references,
    // which means toggleMute()'s `realTrack.enabled = !isMuted` keeps
    // working unchanged.
    if (localStream) {
      const micTrack = localStream.getAudioTracks()[0];
      if (micTrack) {
        try {
          LK.micPub = await room.localParticipant.publishTrack(micTrack, {
            name: 'microphone',
            source: Track.Source.Microphone,
          });
          // Honor existing mute state in case user toggled before LK connected.
          try { micTrack.enabled = !isMuted; } catch (e) {}
          try { log("LK mic published"); } catch (e) {}
        } catch (e) {
          try { log("LK publish err: " + e.message); } catch(e2){}
        }
      }
    }
  } catch (e) {
    try { log("LK connect err: " + (e && e.message || e)); } catch(e2){}
    LK.room = null;
    LK.connectedPid = null;
  } finally {
    LK.connecting = false;
  }
}

async function lkDisconnect() {
  try {
    if (LK._heartbeatTimer) { clearInterval(LK._heartbeatTimer); LK._heartbeatTimer = null; }
    if (LK.room) {
      try { await LK.room.disconnect(); } catch (e) {}
    }
  } finally {
    LK.room = null;
    LK.connectedPid = null;
    LK.micPub = null;
    Object.values(LK.remoteAudioByPid).forEach(el => {
      try { el.srcObject = null; el.remove(); } catch (e) {}
    });
    LK.remoteAudioByPid = {};
  }
}

// Hook: when toggleMute() flips isMuted, also tell LiveKit so its
// server-side speaker detection reflects reality. The local track.enabled
// flag does the actual silencing; this just keeps LK in sync.
function lkApplyMute() {
  if (!LK.room) return;
  try {
    LK.room.localParticipant.setMicrophoneEnabled(!isMuted);
  } catch (e) {}
}

// ════════════════════════════════════════════════════════════════════════════
// v4.0 — DEAD MESH-WEBRTC FUNCTIONS
// ════════════════════════════════════════════════════════════════════════════
// The old code had ~2000 lines of RTCPeerConnection mesh logic: createOffer,
// handleOffer, handleAnswer, handleIce, forceIceRestart, scheduleRetry,
// setupPC, etc. With LiveKit, NONE of that runs. We define each as a no-op
// further down (after the original definitions, so we override them) — see
// the "v4.0 override block" near the bottom of this script.



const ROOM = "__ROOM_ID__", TOKEN = "__TOKEN__";
const MAX_PEERS = parseInt("__MAX_PEERS__", 10) || 11;
let MY_ID = "";
// server tells us in `your_id` whether we joined as admin (i.e.
// whether our typed name had the hidden suffix). Used to show the delete
// (×) buttons on the sticker grid. Never trust the client to set this.
let MY_IS_ADMIN = false;
let serverMaxPeers = MAX_PEERS;
let ws = null, localStream = null, myName = "", myAvatar = "";
// myJoinName preserves the RAW name typed by the user (which may
// include the hidden admin suffix). Sent to the server in the join
// handshake so it can authenticate admin. myName holds the stripped
// version we display in our own UI (peer status bar, etc).
let myJoinName = "";
let isMuted = false, isHost = false;
let leaving = false;
let wsRetries = 0;
let wakeLock = null;
let wakeLockCheckTimer = null;
let audioHealTimer = null;

const peers = {};
const audios = {};
const typingUsers = new Map();
let typingTimer = null;
const peerMap = new Map();
const iceBuffer = {};
const statsTimers = {};
const inboundLevelTimers = {};
const lastOfferUfrag = {};
const peerRelay = {};
let remoteAudioCtx = null;
let audioUnlocked = false;

const lastMutedAt = {};
const renegInProgress = {};
const lastRelayAt = {};
const lastOfferAt = {};
const lastIceRestartAt = {};
const lastFullRebuildAt = {};

// host-assigned avatars by peer_id. When host assigns an avatar to a
// peer, this map gets the entry, broadcast comes back through the WS, and
// every client (including the host) updates the seat tile. Stored separately
// from peerMap so we can also look up the host-assigned avatar for OUR OWN
// pid (peerMap doesn't include self).
const hostAssignedAvatars = {};

// pre-made avatars hosts can assign. Filled at page load from the
// /avatars endpoint, which lists whatever files are actually in the
// avatars/ folder. Fallback to a conservative default if fetch fails.
let HOST_AVATARS = [
  "/avatars/av1.jpg", "/avatars/av2.jpg", "/avatars/av3.jpg",
  "/avatars/av4.jpg", "/avatars/av5.jpg", "/avatars/av6.jpg"
];

async function fetchHostAvatars() {
  try {
    const r = await fetch('/avatars');
    if (!r.ok) return;
    const j = await r.json();
    if (Array.isArray(j.avatars) && j.avatars.length > 0) {
      HOST_AVATARS = j.avatars;
      log("avatars: " + HOST_AVATARS.length + " loaded");
    }
  } catch (e) {
    log("avatars: fetch failed, using fallback");
  }
}
fetchHostAvatars();

// Picker UI state
let _avPickerOpen = false;
let _avPickerEl = null;
let _avPickerTargetPid = null;

// message click panel state (Copy / Reply popup over tapped messages)
let _msgClickPanel = null;
let _msgClickPanelTimer = null;

// track full-rebuild attempts per peer. After MAX_FULL_REBUILDS
// with no successful packets received, we give up to stop wasting TURN
// bandwidth on a peer whose mic is fundamentally broken (no permission,
// hardware fail, etc). Counter resets when packets actually arrive.
const fullRebuildAttempts = {};
const peerGivenUp = {};
const MAX_FULL_REBUILDS_BEFORE_GIVEUP = 3;
const relayConnectedAt = {};
const lossEwma = {};
const sustainedBadStart = {};
let lastMuteToggleAt = 0;

const frozenJitterCounts = {};
const frozenJitterValues = {};

// list of available stickers (filenames). Refreshed on join and
// every time the user opens the picker, so adding a file to the GitHub
// stickers/ folder appears without restart.
let stickerList = [];
let stickerPanelOpen = false;

// tracks view-once message IDs that the local user has already
// opened. Persists per session only — refreshes on rejoin. Used to show
// "Opened" placeholder after the first view.
const viewOnceOpened = new Set();

let ICE_SERVERS = [
  {urls: ['stun:stun.l.google.com:19302', 'stun:stun1.l.google.com:19302']},
  {urls: 'turn:openrelay.metered.ca:443?transport=tcp',
   username: 'openrelayproject', credential: 'openrelayproject'},
  {urls: 'turns:openrelay.metered.ca:443?transport=tcp',
   username: 'openrelayproject', credential: 'openrelayproject'},
];

const AUDIO_CONSTRAINTS = {
  audio: {
    echoCancellation: true,
    noiseSuppression: true,
    autoGainControl: true,
    sampleRate: { ideal: 48000 },
    channelCount: { ideal: 1 }
  },
  video: false
};

function getPCConfig(forceRelay) {
  return {
    iceServers: ICE_SERVERS,
    bundlePolicy: 'max-bundle',
    rtcpMuxPolicy: 'require',
    iceCandidatePoolSize: 2,
    iceTransportPolicy: forceRelay ? 'relay' : 'all',
    sdpSemantics: 'unified-plan'
  };
}

function log(m) {
  const t = new Date().toLocaleTimeString().split(' ')[0];
  const line = '[' + t + '] ' + m;
  console.log(line);
  const d = document.getElementById('dbg');
  if (d) {
    d.textContent += line + '\n';
    if (d.textContent.length > 8000) d.textContent = d.textContent.slice(-6000);
    d.scrollTop = d.scrollHeight;
  }
}

function adaptiveBitrate() {
  const n = peerMap.size;
  if (n >= 12) return 16;
  if (n >= 8) return 20;
  if (n >= 5) return 24;
  return 32;
}

function statsIntervalMs() {
  const n = peerMap.size;
  if (n >= 12) return 8000;
  if (n >= 8) return 6000;
  return 4000;
}

function connTimerMs() {
  const n = peerMap.size;
  if (n >= 12) return 18000;
  if (n >= 8) return 14000;
  return 10000;
}

async function applyBitrateToAll() {
  const kbps = adaptiveBitrate();
  for (const [pid, pc] of Object.entries(peers)) {
    if (pc.connectionState === 'connected') {
      capOutboundBitrate(pc, kbps);
    }
  }
}

function pickAv(e) {
  const f = e.target.files[0]; if (!f) return;
  const r = new FileReader();
  r.onload = ev => {
    const img = new Image();
    img.onload = () => {
      const c = document.createElement('canvas');
      const sz = 128;
      c.width = c.height = sz;
      const ctx = c.getContext('2d');
      const srcSize = Math.min(img.width, img.height);
      const srcX = (img.width - srcSize) / 2;
      const srcY = (img.height - srcSize) / 2;
      ctx.drawImage(img, srcX, srcY, srcSize, srcSize, 0, 0, sz, sz);
      myAvatar = c.toDataURL('image/jpeg', 0.7);
      document.getElementById('avPrev').innerHTML = '<img src="' + myAvatar + '">';
      log("avatar OK (" + Math.round(myAvatar.length / 1024) + 'kb)');
    };
    img.src = ev.target.result;
  };
  r.readAsDataURL(f);
}
document.getElementById('nameIn').addEventListener('input', e => {
  myName = e.target.value;
  if (!myAvatar) document.getElementById('avInit').textContent = myName ? myName[0].toUpperCase() : '?';
});

// ════════════════════════════════════════════════════════════════════════════
// INPUT WIRING — typing indicator + auto-grow + sticker icon visibility
// ════════════════════════════════════════════════════════════════════════════
const INPUT_MAX_HEIGHT = 84;  // ~3 lines at 14px / 1.4 line-height with padding
const INPUT_MIN_HEIGHT = 38;  // matches the resting height (rows=1)

function autoResizeInput() {
  const el = document.getElementById('msgIn');
  if (!el) return;
  el.style.height = 'auto';
  const h = Math.min(el.scrollHeight, INPUT_MAX_HEIGHT);
  el.style.height = Math.max(h, INPUT_MIN_HEIGHT) + 'px';
}

function updateStickerIconVisibility() {
  const inEl = document.getElementById('msgIn');
  const btn = document.getElementById('stickerBtn');
  if (!inEl || !btn) return;
  if (inEl.value.length > 0) btn.classList.add('hidden');
  else btn.classList.remove('hidden');
}

(function wireInput() {
  const inEl = document.getElementById('msgIn');
  if (!inEl) return;

  inEl.addEventListener('input', () => {
    autoResizeInput();
    updateStickerIconVisibility();
    if (!ws || ws.readyState !== 1) return;
    ws.send(JSON.stringify({ type: 'typing_start' }));
    if (typingTimer) clearTimeout(typingTimer);
    typingTimer = setTimeout(() => {
      if (ws && ws.readyState === 1) ws.send(JSON.stringify({ type: 'typing_stop' }));
      typingTimer = null;
    }, 2000);
  });

  // Enter sends, Shift+Enter inserts newline. Matches every modern chat.
  inEl.addEventListener('keydown', (e) => {
    if (e.key === 'Enter' && !e.shiftKey && !e.isComposing) {
      e.preventDefault();
      sendMsg();
    }
  });
})();

async function fetchIceServers() {
  try {
    const r = await fetch('/turn', { cache: 'no-store' });
    if (r.ok) {
      const data = await r.json();
      if (data.iceServers && data.iceServers.length) {
        ICE_SERVERS = data.iceServers;
        log("ICE: " + ICE_SERVERS.length + " server entries loaded");
      }
    }
  } catch (e) {
    log("ICE fetch failed, using fallback");
  }
  try {
    const sr = await fetch('/turn-status', { cache: 'no-store' });
    if (sr.ok) {
      const st = await sr.json();
      if (!st.premium) {
        if (st.metered_configured || st.cloudflare_configured || st.custom_configured) {
          log("!!! TURN ENV SET BUT FETCH FAILED — check /turn-debug");
        } else {
          log("!!! WARN: NO TURN CONFIGURED — Gulf/MENA peers will fail");
        }
      } else {
        log("TURN: premium provider active");
      }
    }
  } catch (e) {}
}

// ════════════════════════════════════════════════════════════════════════════
// STICKERS — fetch list, render grid, send, panel open/close
// ════════════════════════════════════════════════════════════════════════════
async function fetchStickerList() {
  try {
    const r = await fetch('/stickers', { cache: 'no-store' });
    if (r.ok) {
      const data = await r.json();
      if (Array.isArray(data.stickers)) {
        stickerList = data.stickers;
        log("stickers: " + stickerList.length + " available");
        return;
      }
    }
  } catch (e) {
    log("sticker list fetch failed: " + e.message);
  }
}

function renderStickerGrid() {
  const grid = document.getElementById('stickerGrid');
  if (!grid) return;
  // keep an in-progress uploading row at the top if active.
  const uploadingHTML = stickerUploading
    ? '<div class="sticker-uploading">Uploading sticker…</div>'
    : '';
  if (!stickerList || stickerList.length === 0) {
    grid.innerHTML = uploadingHTML +
      '<div class="sticker-empty">No stickers yet.<br>Tap the <strong>+</strong> button to add one.</div>';
    return;
  }
  const html = stickerList.map(name => {
    const safe = esc(name);
    const delBtn = MY_IS_ADMIN
      ? '<button class="sticker-cell-del" data-del="' + safe + '" aria-label="Delete sticker" title="Delete">&times;</button>'
      : '';
    return '<div class="sticker-cell-wrap">' +
             delBtn +
             '<button class="sticker-cell" type="button" data-name="' + safe + '" aria-label="' + safe + '">' +
               '<img src="/stickers/' + safe + '" alt="" loading="lazy">' +
             '</button>' +
           '</div>';
  }).join('');
  grid.innerHTML = uploadingHTML + html;
  grid.querySelectorAll('.sticker-cell').forEach(cell => {
    cell.addEventListener('click', () => {
      const name = cell.getAttribute('data-name');
      if (name) sendStickerMsg(name);
    });
  });
  grid.querySelectorAll('.sticker-cell-del').forEach(btn => {
    btn.addEventListener('click', (ev) => {
      ev.stopPropagation();
      const name = btn.getAttribute('data-del');
      if (!name) return;
      if (!confirm('Delete sticker "' + name + '"?')) return;
      requestStickerDelete(name);
    });
  });
}

// ── sticker upload (client) ────────────────────────────────────────
// Strategy: pre-resize on the client BEFORE sending, so we never push
// a 5 MB phone photo over a slow 3G uplink. Result is typically <100 KB,
// upload feels instant. Server still re-resizes/recompresses defensively.
let stickerUploading = false;
const CLIENT_RESIZE_MAX_EDGE = 1024;
const CLIENT_RESIZE_QUALITY = 0.85;
const MAX_CLIENT_UPLOAD_BYTES = 5 * 1024 * 1024;

function onStickerUploadClick() {
  if (stickerUploading) return;
  const inp = document.getElementById('stickerUploadInput');
  if (inp) {
    inp.value = ''; // allow re-picking the same file twice
    inp.click();
  }
}

async function onStickerFilePicked(ev) {
  const file = ev.target.files && ev.target.files[0];
  if (!file) return;
  if (!/^image\//.test(file.type)) {
    showStickerToast('Please pick an image file.', 'err');
    return;
  }
  if (file.size > MAX_CLIENT_UPLOAD_BYTES) {
    showStickerToast('Image too large (max 5 MB).', 'err');
    return;
  }
  setStickerUploading(true);
  try {
    const dataUrl = await resizeImageToDataURL(file);
    if (!dataUrl) {
      showStickerToast('Could not read image.', 'err');
      setStickerUploading(false);
      return;
    }
    if (!ws || ws.readyState !== 1) {
      showStickerToast('Connection lost.', 'err');
      setStickerUploading(false);
      return;
    }
    ws.send(JSON.stringify({ type: 'sticker_upload', data_url: dataUrl }));
    // setStickerUploading(false) happens on sticker_result.
    // Safety timeout in case the server never replies:
    setTimeout(() => {
      if (stickerUploading) {
        setStickerUploading(false);
        showStickerToast('Upload timed out.', 'err');
      }
    }, 30000);
  } catch (e) {
    log("sticker upload error: " + e.message);
    showStickerToast('Upload failed.', 'err');
    setStickerUploading(false);
  }
}

function setStickerUploading(v) {
  stickerUploading = v;
  const btn = document.getElementById('stickerUploadBtn');
  if (btn) btn.classList.toggle('busy', v);
  if (stickerPanelOpen) renderStickerGrid();
}

function resizeImageToDataURL(file) {
  return new Promise((resolve) => {
    const img = new Image();
    const reader = new FileReader();
    reader.onload = () => {
      img.onload = () => {
        let { width, height } = img;
        const longest = Math.max(width, height);
        if (longest > CLIENT_RESIZE_MAX_EDGE) {
          const ratio = CLIENT_RESIZE_MAX_EDGE / longest;
          width = Math.max(1, Math.round(width * ratio));
          height = Math.max(1, Math.round(height * ratio));
        }
        const canvas = document.createElement('canvas');
        canvas.width = width;
        canvas.height = height;
        const ctx = canvas.getContext('2d');
        ctx.drawImage(img, 0, 0, width, height);
        // Try WebP first (smaller), fall back to JPEG.
        let dataUrl;
        try {
          dataUrl = canvas.toDataURL('image/webp', CLIENT_RESIZE_QUALITY);
          if (!dataUrl.startsWith('data:image/webp')) {
            dataUrl = canvas.toDataURL('image/jpeg', CLIENT_RESIZE_QUALITY);
          }
        } catch (e) {
          dataUrl = canvas.toDataURL('image/jpeg', CLIENT_RESIZE_QUALITY);
        }
        resolve(dataUrl);
      };
      img.onerror = () => resolve(null);
      img.src = reader.result;
    };
    reader.onerror = () => resolve(null);
    reader.readAsDataURL(file);
  });
}

function requestStickerDelete(name) {
  if (!ws || ws.readyState !== 1) return;
  ws.send(JSON.stringify({ type: 'sticker_delete', name: name }));
}

function handleStickerResult(m) {
  setStickerUploading(false);
  if (m.ok) {
    if (m.warning) {
      // server says the upload succeeded for this session but
      // won't persist (GitHub creds aren't set). Show a longer, yellow
      // warning toast so the admin knows their stickers will vanish on
      // the next restart and can fix the env vars on Render.
      showStickerToast(m.warning, 'warn', 7000);
    } else if (m.sticker) {
      showStickerToast('Sticker added!', 'ok');
    } else if (m.deleted) {
      showStickerToast('Sticker deleted', 'ok');
    } else {
      showStickerToast('Done', 'ok');
    }
  } else {
    showStickerToast(m.error || 'Operation failed', 'err', 6000);
  }
}

let _stickerToastTimer = null;
function showStickerToast(text, kind, durationMs) {
  const t = document.getElementById('stickerToast');
  if (!t) return;
  t.textContent = text;
  const kindCls = kind === 'err' ? 'err' : kind === 'ok' ? 'ok' : kind === 'warn' ? 'warn' : '';
  t.className = 'sticker-toast show ' + kindCls;
  if (_stickerToastTimer) clearTimeout(_stickerToastTimer);
  _stickerToastTimer = setTimeout(() => {
    t.classList.remove('show');
  }, durationMs || 2400);
}

// ════════════════════════════════════════════════════════════════════════════
// — MESSAGE CLICK PANEL (Copy + Reply on others' text messages)
// ════════════════════════════════════════════════════════════════════════════
// Opens a small floating panel above the tapped message with Copy and Reply
// actions. Only shows for OTHER people's TEXT messages — not stickers,
// images, or your own messages. Auto-dismisses after 3.5s or on any click
// outside. Designed to share visual language with the avatar picker so the
// whole interaction set feels consistent.
function showMsgClickPanel(msgEl, m) {
  hideMsgClickPanel();
  // Don't show for sticker-only or image-only messages
  if (m.sticker) return;
  if (m.image) return;
  // Don't show if other interactive overlays are open
  if (document.querySelector('.react-bar')) return;
  if (document.querySelector('.msg-delete-btn')) return;

  const panel = document.createElement('div');
  panel.className = 'msg-click-panel';
  panel.innerHTML =
    '<div class="msg-click-panel-item" data-action="copy">' +
      '<span class="msg-click-panel-icon"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="9" y="9" width="13" height="13" rx="2" ry="2"/><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"/></svg></span>' +
      'Copy' +
    '</div>' +
    '<div class="msg-click-panel-sep"></div>' +
    '<div class="msg-click-panel-item" data-action="reply">' +
      '<span class="msg-click-panel-icon"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="9 17 4 12 9 7"/><path d="M20 18v-2a4 4 0 0 0-4-4H4"/></svg></span>' +
      'Reply' +
    '</div>';

  // Measure invisibly first so positioning math has the real width/height.
  panel.style.visibility = 'hidden';
  panel.style.opacity = '0';
  document.body.appendChild(panel);

  const rowRect = msgEl.getBoundingClientRect();
  const panelRect = panel.getBoundingClientRect();
  const chatEl = document.getElementById('chat');
  const chatRect = chatEl ? chatEl.getBoundingClientRect()
                          : { top: 0, left: 0, right: window.innerWidth, bottom: window.innerHeight };

  // Default: centered above the message
  let left = rowRect.left + (rowRect.width / 2) - (panelRect.width / 2);
  let top = rowRect.top - panelRect.height - 8;

  const pad = 6;
  left = Math.max(chatRect.left + pad, Math.min(left, chatRect.right - panelRect.width - pad));
  // If there isn't enough room above, flip below
  if (top < chatRect.top + pad) {
    top = rowRect.bottom + 8;
  }

  panel.style.left = left + 'px';
  panel.style.top = top + 'px';
  _msgClickPanel = panel;

  // Fade-in
  panel.style.transition = 'opacity .12s ease, transform .15s ease';
  panel.style.transform = 'scale(0.92) translateY(4px)';
  requestAnimationFrame(function() {
    panel.style.visibility = 'visible';
    panel.style.opacity = '1';
    panel.style.transform = 'scale(1) translateY(0)';
  });

  panel.querySelector('[data-action="copy"]').addEventListener('click', function(ev) {
    ev.stopPropagation();
    if (m.text) {
      const textToCopy = m.text;
      if (navigator.clipboard && navigator.clipboard.writeText) {
        navigator.clipboard.writeText(textToCopy).then(function() {
          showStickerToast('Copied', 'ok', 1200);
        }).catch(function() {
          _fallbackCopy(textToCopy);
        });
      } else {
        _fallbackCopy(textToCopy);
      }
    }
    hideMsgClickPanel();
  });

  panel.querySelector('[data-action="reply"]').addEventListener('click', function(ev) {
    ev.stopPropagation();
    startReply(m);
    hideMsgClickPanel();
  });

  // Auto-dismiss after a short window so the user doesn't get a stale panel
  _msgClickPanelTimer = setTimeout(hideMsgClickPanel, 3500);

  // Outside-click dismiss — defer attach so the click that OPENED the
  // panel doesn't immediately close it.
  setTimeout(function() {
    document.addEventListener('click', _onMsgPanelDocClick);
  }, 80);
}

function _fallbackCopy(text) {
  try {
    const ta = document.createElement('textarea');
    ta.value = text;
    ta.style.position = 'fixed';
    ta.style.left = '-9999px';
    document.body.appendChild(ta);
    ta.select();
    document.execCommand('copy');
    document.body.removeChild(ta);
    showStickerToast('Copied', 'ok', 1200);
  } catch (e) {
    showStickerToast('Copy failed', 'err', 1500);
  }
}

function _onMsgPanelDocClick(e) {
  if (_msgClickPanel && !_msgClickPanel.contains(e.target)) {
    hideMsgClickPanel();
  }
}

function hideMsgClickPanel() {
  if (_msgClickPanelTimer) { clearTimeout(_msgClickPanelTimer); _msgClickPanelTimer = null; }
  document.removeEventListener('click', _onMsgPanelDocClick);
  if (_msgClickPanel) {
    const dying = _msgClickPanel;
    dying.style.opacity = '0';
    dying.style.transform = 'scale(0.92) translateY(4px)';
    setTimeout(function() {
      if (_msgClickPanel === dying) { _msgClickPanel.remove(); _msgClickPanel = null; }
    }, 150);
  }
}

// ════════════════════════════════════════════════════════════════════════════
// — HOST AVATAR PICKER
// ════════════════════════════════════════════════════════════════════════════
// When the host clicks an empty avatar tile, this opens a small grid of
// available avatar choices. Clicking one assigns it to that peer — server
// broadcasts to all clients, including the target user themselves, who
// then sees the new avatar on their own self-tile.
function showAvatarPicker(pid, anchorEl) {
  if (!isHost) return;
  if (_avPickerOpen) hideAvatarPicker();
  const p = peerMap.get(pid);
  if (!p) return;
  if (!HOST_AVATARS || !HOST_AVATARS.length) return;

  _avPickerTargetPid = pid;
  _avPickerOpen = true;

  const picker = document.createElement('div');
  picker.className = 'av-picker';
  picker.innerHTML = '<div class="av-picker-title">Pick for ' + esc(p.name || '?') + '</div><div class="av-picker-grid"></div>';

  const grid = picker.querySelector('.av-picker-grid');
  HOST_AVATARS.forEach(function(url, idx) {
    const item = document.createElement('div');
    item.className = 'av-picker-item';
    item.innerHTML = '<img src="' + url + '" alt="" draggable="false">';

    // Bind to BOTH pointerdown and click for mobile reliability. The dedupe
    // flag ensures we only fire once even if both handlers run for the same
    // tap. pointerdown fires instantly on first touch; click is the fallback
    // for browsers/platforms where pointer events aren't supported.
    let handled = false;
    const handlePick = function(ev) {
      if (handled) return;
      handled = true;
      ev.stopPropagation();
      if (ev.preventDefault) ev.preventDefault();
      assignAvatarToPeer(pid, url);
      hideAvatarPicker();
    };
    item.addEventListener('pointerdown', handlePick);
    item.addEventListener('click', handlePick);
    grid.appendChild(item);
  });

  document.body.appendChild(picker);
  _avPickerEl = picker;

  // Position with position:fixed (set in CSS) so viewport-relative coords
  // from getBoundingClientRect map directly to left/top.
  const rect = anchorEl.getBoundingClientRect();
  const pRect = picker.getBoundingClientRect();
  const pw = pRect.width || 260;
  const ph = pRect.height || 180;

  let left = rect.left + (rect.width / 2) - (pw / 2);
  let top = rect.bottom + 8;

  left = Math.max(8, Math.min(left, window.innerWidth - pw - 8));
  if (top + ph > window.innerHeight - 8) {
    top = rect.top - ph - 8;
    if (top < 8) top = 8;
  }

  picker.style.left = left + 'px';
  picker.style.top = top + 'px';

  requestAnimationFrame(function() { picker.classList.add('show'); });

  setTimeout(function() {
    document.addEventListener('click', _onAvPickerDismiss);
    document.addEventListener('pointerdown', _onAvPickerDismiss);
  }, 200);
}

function _onAvPickerDismiss(e) {
  if (_avPickerEl && !_avPickerEl.contains(e.target)) {
    hideAvatarPicker();
  }
}

function hideAvatarPicker() {
  _avPickerOpen = false;
  _avPickerTargetPid = null;
  document.removeEventListener('click', _onAvPickerDismiss);
  document.removeEventListener('pointerdown', _onAvPickerDismiss);
  if (_avPickerEl) {
    _avPickerEl.classList.remove('show');
    setTimeout(function() {
      if (_avPickerEl) { _avPickerEl.remove(); _avPickerEl = null; }
    }, 200);
  }
}

function assignAvatarToPeer(pid, avatarUrl) {
  if (!isHost) return;
  // Optimistic local update so the host sees the change immediately, even
  // before the server's broadcast comes back.
  hostAssignedAvatars[pid] = avatarUrl;
  const p = peerMap.get(pid);
  if (p) p._hostAvatar = avatarUrl;
  updPeers();
  if (ws && ws.readyState === 1) {
    ws.send(JSON.stringify({ type: 'set_peer_avatar', target_pid: pid, avatar: avatarUrl }));
  }
  log('host avatar assigned to ' + pid);
}

async function toggleStickerPanel() {
  if (stickerPanelOpen) closeStickerPanel();
  else await openStickerPanel();
}

async function openStickerPanel() {
  await fetchStickerList();
  renderStickerGrid();
  const panel = document.getElementById('stickerPanel');
  const back = document.getElementById('stickerBackdrop');
  if (panel) panel.classList.add('open');
  if (back) back.classList.add('open');
  stickerPanelOpen = true;
  const inEl = document.getElementById('msgIn');
  if (inEl) inEl.blur();
}

function closeStickerPanel() {
  const panel = document.getElementById('stickerPanel');
  const back = document.getElementById('stickerBackdrop');
  if (panel) panel.classList.remove('open');
  if (back) back.classList.remove('open');
  stickerPanelOpen = false;
}

function sendStickerMsg(name) {
  if (!ws || ws.readyState !== 1) return;
  if (!name) return;
  if (typingTimer) { clearTimeout(typingTimer); typingTimer = null; }
  ws.send(JSON.stringify({ type: 'typing_stop' }));
  const payload = { type: 'chat', text: '', sticker: name };
  if (replyingTo) {
    payload.reply_to = replyingTo;
    cancelReply();
  }
  ws.send(JSON.stringify(payload));
  closeStickerPanel();
}

async function acquireWakeLock() {
  try {
    if ('wakeLock' in navigator) {
      wakeLock = await navigator.wakeLock.request('screen');
      log("wakeLock OK");
      wakeLock.addEventListener('release', () => {
        log("wakeLock released event");
        wakeLock = null;
      });
    }
  } catch (e) { log("wakeLock fail"); }
}

function startWakeLockWatch() {
  if (wakeLockCheckTimer) clearInterval(wakeLockCheckTimer);
  wakeLockCheckTimer = setInterval(() => {
    if (!wakeLock && document.visibilityState === 'visible') {
      log("wakeLock dropped — reacquiring");
      acquireWakeLock();
    }
  }, 30000);
}

function startAudioSelfHeal() {
  if (audioHealTimer) clearInterval(audioHealTimer);
  audioHealTimer = setInterval(() => {
    Object.entries(audios).forEach(([pid, audio]) => {
      try {
        const pc = peers[pid];
        if (!pc || pc.connectionState === 'closed' || pc.connectionState === 'failed') return;
        const stream = audio.srcObject;
        const hasLiveTracks = stream && stream.getTracks().some(t => t.readyState === 'live');
        if (!hasLiveTracks) {
          const receivers = pc.getReceivers().filter(r => r.track && r.track.readyState === 'live');
          if (receivers.length > 0) {
            const newStream = new MediaStream(receivers.map(r => r.track));
            audio.srcObject = newStream;
            audio.play().catch(() => {});
            log("audio heal: restored stream for " + pid);
          }
        }
        if (audio.paused && hasLiveTracks) {
          audio.play().catch(() => {});
          log("audio heal: resumed paused audio for " + pid);
        }
      } catch (e) {}
    });
    if (localStream) {
      const micTrack = localStream.getAudioTracks()[0];
      if (micTrack && micTrack.readyState === 'ended') {
        log("audio heal: local mic track ended, reacquiring");
        navigator.mediaDevices.getUserMedia(AUDIO_CONSTRAINTS).then(newStream => {
          const newTrack = newStream.getAudioTracks()[0];
          if (newTrack) {
            newTrack.enabled = !isMuted;
            Object.values(peers).forEach(pc => {
              pc.getSenders().forEach(s => {
                if (s.track && s.track.kind === 'audio') {
                  try { s.replaceTrack(newTrack); } catch (e) {}
                }
              });
            });
            localStream.getTracks().forEach(tr => tr.stop());
            localStream = newStream;
            watchLocalTrack();
            setupLocalLevelMonitor();
            log("audio heal: mic reacquired");
          }
        }).catch(e => log("audio heal: mic reacquire failed: " + e.message));
      }
    }
  }, 5000);
}

document.addEventListener('visibilitychange', () => {
  if (document.visibilityState === 'visible') {
    if (!wakeLock) acquireWakeLock();
    log("foreground — refreshing peer health");
    Object.entries(peers).forEach(([pid, pc]) => {
      if (pc.connectionState === 'disconnected' && MY_ID > pid) {
        log("disconnected peer on resume -> ICE restart " + pid);
        forceIceRestart(pid);
      }
    });
  }
});

window.addEventListener('online', () => {
  log("network online — refreshing connections");
  Object.entries(peers).forEach(([pid, pc]) => {
    if (MY_ID > pid && pc.connectionState !== 'closed') {
      setTimeout(() => forceIceRestart(pid), Math.random() * 500);
    }
  });
});
window.addEventListener('offline', () => {
  log("network offline");
});

function watchLocalTrack() {
  if (!localStream) return;
  const t = localStream.getAudioTracks()[0];
  if (!t || t._watched) return;
  t._watched = true;
  t.addEventListener('ended', async () => {
    log("local mic track ended — reacquiring");
    try {
      const newStream = await navigator.mediaDevices.getUserMedia(AUDIO_CONSTRAINTS);
      const newTrack = newStream.getAudioTracks()[0];
      newTrack.enabled = !isMuted;
      Object.values(peers).forEach(pc => {
        pc.getSenders().forEach(s => {
          if (s.track && s.track.kind === 'audio') {
            try { s.replaceTrack(newTrack); } catch (e) { log("reacquire replaceTrack err: " + e.message); }
          }
        });
      });
      try { localStream.getTracks().forEach(tr => tr.stop()); } catch (e) {}
      localStream = newStream;
      watchLocalTrack();
      setupLocalLevelMonitor();
      log("mic reacquired");
    } catch (e) {
      log("mic reacquire failed: " + e.message);
    }
  });
}

async function doJoin() {
  const rawName = document.getElementById('nameIn').value.trim();
  if (!rawName) { alert("Enter name"); return; }
  // if user typed the hidden admin suffix (e.g. "Sor-"), strip it
  // for local display. The raw value (with "-") is still sent to the
  // server in the join handshake so it can verify admin status. The
  // server-stripped name will be the one other peers see; we strip it
  // ourselves here so OUR own UI (peer status bar "(You)" tag, etc.)
  // also shows the clean name.
  const ADMIN_BASE = "sor";
  const ADMIN_SUFFIX = "-";
  let displayName = rawName;
  if (rawName.toLowerCase() === (ADMIN_BASE + ADMIN_SUFFIX)) {
    displayName = rawName.slice(0, ADMIN_BASE.length);  // preserve casing
  }
  myName = displayName;
  // Send the RAW name to the server so the suffix can authenticate admin.
  myJoinName = rawName;
  document.getElementById('joinBtn').disabled = true;
  document.getElementById('joinBtn').textContent = "...";

  try {
    remoteAudioCtx = getSharedAC();
    if (remoteAudioCtx.state === 'suspended') await remoteAudioCtx.resume();
    const silentBuf = remoteAudioCtx.createBuffer(1, remoteAudioCtx.sampleRate * 0.1, remoteAudioCtx.sampleRate);
    const silentSrc = remoteAudioCtx.createBufferSource();
    silentSrc.buffer = silentBuf;
    silentSrc.connect(remoteAudioCtx.destination);
    silentSrc.start();
    audioUnlocked = true;
    log("audio unlocked");
  } catch (e) { log("audio unlock fail: " + e.message); }

  await fetchIceServers();
  fetchStickerList();

  try {
    localStream = await navigator.mediaDevices.getUserMedia(AUDIO_CONSTRAINTS);
    log("mic OK");
    setupLocalLevelMonitor();
    watchLocalTrack();
  } catch (e) {
    log("mic err: " + e.message);
  }
  document.getElementById('joinOvl').classList.add('hidden');
  document.getElementById('app').classList.remove('hidden');
  acquireWakeLock();
  startWakeLockWatch();
  startAudioSelfHeal();
  setupScrollLock();
  autoResizeInput();
  updateStickerIconVisibility();

  if (_peerLevelTicker) clearInterval(_peerLevelTicker);
  // 80ms ticker (was 150ms). Drives the voice-reactive halo —
  // tighter cadence here means the green ring tracks audio amplitude
  // more fluidly, which feels meaningfully more alive. Cost is trivial
  // (a few CSS variable writes per frame).
  _peerLevelTicker = setInterval(updPeerLevels, 80);

  connectWS();
}

function showRoomFullScreen(current, max) {
  leaving = true;
  if (ws && ws.readyState === 1) { try { ws.close(); } catch (e) {} }
  cleanupRTC();

  const app = document.getElementById('app');
  const ovl = document.getElementById('joinOvl');
  if (app) app.classList.add('hidden');
  if (ovl) ovl.classList.add('hidden');

  let el = document.getElementById('roomFullScreen');
  if (el) el.remove();
  el = document.createElement('div');
  el.id = 'roomFullScreen';
  el.className = 'room-full';
  el.innerHTML =
    '<div class="room-full-icon">' +
      '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">' +
        '<path d="M20 21v-2a4 4 0 0 0-4-4H8a4 4 0 0 0-4 4v2"/>' +
        '<circle cx="12" cy="7" r="4"/>' +
      '</svg>' +
    '</div>' +
    '<div class="room-full-count">' +
      '<span>' + current + '/' + max + '</span>' +
      '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">' +
        '<path d="M20 21v-2a4 4 0 0 0-4-4H8a4 4 0 0 0-4 4v2"/>' +
        '<circle cx="12" cy="7" r="4"/>' +
      '</svg>' +
    '</div>' +
    '<div class="room-full-text">Room Is Full</div>' +
    '<div class="room-full-sub">When someone leaves, a slot opens up. Try again in a moment.</div>' +
    '<button class="room-full-retry" onclick="location.reload()">Try Again</button>';
  document.body.appendChild(el);
}

function connectWS() {
  const p = location.protocol === 'https:' ? 'wss:' : 'ws:';
  const url = p + '//' + location.host + '/ws/' + ROOM + '?t=' + TOKEN;
  log("WS connect (try " + (wsRetries + 1) + ")");

  ws = new WebSocket(url);
  let gotRoomFull = false;

  ws.onopen = () => {
    log("WS open");
    wsRetries = 0;
    ws.send(JSON.stringify({ type: 'join', name: myJoinName || myName, avatar: myAvatar }));
  };

  ws.onmessage = async (ev) => {
    let m; try { m = JSON.parse(ev.data); } catch (e) { return; }

    // forward all uno_* messages to the Uno client module before the
    // switch — keeps the existing switch clean and untouched.
    if (m && typeof m.type === 'string' && m.type.indexOf('uno_') === 0) {
      try { unoHandleServerMsg(m); } catch (e) { log('uno msg err'); }
      return;
    }

    // parallel router for the Zombie game (prefix 'zomb_')
    if (m && typeof m.type === 'string' && m.type.indexOf('zomb_') === 0) {
      try { zombHandleServerMsg(m); } catch (e) { log('zomb msg err'); }
      return;
    }

    // stream state/error from server
    if (m && m.type === 'stream_state') {
      try { streamHandleState(m.state); } catch (e) { log('stream state err: ' + e); }
      return;
    }
    if (m && m.type === 'stream_error') {
      try { _streamShowSheetError(m.text || 'Stream error'); } catch (e) {}
      return;
    }

    switch (m.type) {
      case 'ping':
        ws.send(JSON.stringify({ type: 'pong' }));
        break;

      case 'room_full':
        gotRoomFull = true;
        log("room full: " + m.current + "/" + m.max);
        showRoomFullScreen(m.current, m.max);
        break;

      case 'your_id':
        MY_ID = m.id;
        if (m.max_peers) serverMaxPeers = m.max_peers;
        MY_IS_ADMIN = !!m.is_admin;
        isHost = !!m.is_host;
        log("myId=" + MY_ID + " maxPeers=" + serverMaxPeers + " admin=" + MY_IS_ADMIN + " host=" + isHost);
        // now that we have our identity, connect to LiveKit.
        // Fire-and-forget: if it fails, the user still sees the chat etc.,
        // we log the error and they can refresh.
        try { lkConnect(); } catch (e) { log("lkConnect throw: " + e.message); }
        break;

      case 'stickers':
        if (Array.isArray(m.stickers)) {
          stickerList = m.stickers;
          log("stickers (push): " + stickerList.length);
          // if the panel is open, refresh in place so users see
          // freshly-uploaded or freshly-deleted stickers immediately.
          if (stickerPanelOpen) renderStickerGrid();
        }
        break;

      case 'sticker_result':
        // response to an upload or delete attempt. Show inline status.
        handleStickerResult(m);
        break;

      // ── message deletion handlers ──
      case 'msg_deleted':
        handleMsgDeleted(m);
        break;

      case 'delete_result':
        if (!m.ok) {
          showStickerToast(m.error || 'Delete failed', 'err', 4000);
        }
        break;

      // ── view-once opened tracking ──
      // Someone opened a view-once image. Only update the SENDER's UI.
      // The opener already updated their own UI locally in openImagePreview().
      // Other recipients keep seeing "Photo" — they can still open it.
      case 'msg_opened':
        if (m.msg_id) {
          const voRow = document.getElementById('msgs');
          if (voRow) {
            const row = voRow.querySelector('[data-msg-id="' + esc(m.msg_id) + '"]');
            // Only the sender sees "Opened" when someone else opens it.
            // Everyone else keeps their "Photo" pill until they personally open.
            if (row && row.classList.contains('self')) {
              markViewOnceOpened(m.msg_id);
            }
          }
        }
        break;

      // ── message reactions ──
      case 'reaction':
        handleReaction(m);
        break;

      case 'history':
        // history arrives oldest-first. With column-reverse, the
        // visual bottom is DOM first child, so to keep the order
        // (newest at the bottom) we render oldest LAST — i.e. iterate
        // in chronological order and let renderMsg() insertBefore()
        // the first child. That naturally puts newer messages below
        // older ones in the visual layout.
        m.messages.forEach(renderMsg);
        // After history, we are by definition at the visual bottom
        // (no scrolling has happened). Ensure the unread/jump-button
        // state reflects that.
        scrollToLatest(false, true);
        break;

      case 'chat':
        renderMsg(m);
        // also mirror into the in-game Uno chat panel so players
        // can chat while playing (they're chatting in the same room).
        try { unoMirrorChat(m); } catch (e) {}
        // same mirror for Zombie's in-game chat panel.
        try { zombMirrorChat(m); } catch (e) {}
        break;

      case 'peers': {
        log("existing peers: " + m.peers.length);
        const currentIds = new Set(m.peers.map(p => p.id));
        for (const [id, _] of peerMap) {
          if (!currentIds.has(id)) {
            nukePeer(id);
            peerMap.delete(id);
          }
        }
        let staggerIdx = 0;
        for (const p of m.peers) {
          addPeer(p);
          if (MY_ID > p.id) {
            const delay = staggerIdx * 80;
            log("I'm larger (" + MY_ID + ">" + p.id + ") -> offer in " + delay + "ms");
            setTimeout(() => createOffer(p.id), delay);
            staggerIdx++;
          } else {
            log("I'm smaller (" + MY_ID + "<" + p.id + ") -> wait");
          }
        }
        // Always render the grid after the initial peer list — otherwise the
        // user sees an empty panel when they're alone in the room until
        // someone else joins.
        updPeers();
        updCount();
        break;
      }

      case 'peer_joined':
        addPeer(m.peer);
        renderSys(m.peer.name + " joined");
        if (MY_ID && MY_ID > m.peer.id) {
          log("late: I'm larger -> offer to " + m.peer.id);
          createOffer(m.peer.id);
        } else if (MY_ID) {
          log("late: I'm smaller -> wait for offer from " + m.peer.id);
        }
        applyBitrateToAll();
        break;

      case 'peer_left':
        nukePeer(m.peer_id);
        peerMap.delete(m.peer_id);
        if (typingUsers.has(m.peer_id)) { typingUsers.delete(m.peer_id); renderTyping(); }
        // also clear any host-assigned avatar override for this peer.
        // If we don't, and a brand-new peer happens to reuse the same id
        // (very unlikely but possible), they'd inherit the old override.
        if (hostAssignedAvatars[m.peer_id]) delete hostAssignedAvatars[m.peer_id];
        renderSys(m.name + ' left');
        updCount();
        updPeers();
        applyBitrateToAll();
        break;

      // host-assigned avatar update broadcast. Comes either from a
      // peer's recent set_peer_avatar action OR as a sync packet sent to
      // a newly-joining peer for every existing override in the room.
      case 'peer_avatar_set':
        if (m.target_pid && m.avatar) {
          hostAssignedAvatars[m.target_pid] = m.avatar;
          const pm = peerMap.get(m.target_pid);
          if (pm) pm._hostAvatar = m.avatar;
          updPeers();
        }
        break;

      case 'webrtc_offer':
        await handleOffer(m.from, m.sdp);
        break;
      case 'webrtc_answer':
        await handleAnswer(m.from, m.sdp);
        break;
      case 'webrtc_ice':
        await handleIce(m.from, m.candidate);
        break;

      case 'request_relay': {
        const reason = m.reason || '';
        const isZombie = reason.startsWith('zombie');
        const isFullRebuild = reason.startsWith('full-rebuild');
        const isFreshRelay = reason === 'stuck-checking' || reason === 'retry-after-fail';
        if (!isZombie && !isFullRebuild && !isFreshRelay && MY_ID < m.from) {
          break;
        }
        log("got relay request from " + m.from + " (" + reason + ")");
        peerRelay[m.from] = true;
        if ((isZombie || isFullRebuild) && MY_ID > m.from) {
          log("counterpart " + (isZombie ? "zombie" : "full-rebuild") + " request -> rebuild " + m.from);
          destroyPeer(m.from);
          renegInProgress[m.from] = true;
          setTimeout(() => {
            createOffer(m.from);
            setTimeout(() => { renegInProgress[m.from] = false; }, 3000);
          }, 300);
        } else if (isFreshRelay) {
          log("fresh relay request: smaller side clearing PC for " + m.from);
          destroyPeer(m.from);
        } else {
          await switchPeerToRelay(m.from);
        }
        break;
      }

      case 'mute_cmd': {
        if (localStream) {
          const t = localStream.getAudioTracks()[0];
          if (t && t.readyState === 'live') t.enabled = false;
        }
        isMuted = true;
        document.getElementById('muteBtn').classList.add('muted');
        document.getElementById('muteBtn').innerHTML = '<svg id="micIcon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="1" y1="1" x2="23" y2="23"/><path d="M9 9v6a3 3 0 0 0 5.12 2.12M15 9.34V4a3 3 0 0 0-5.94-.6"/><path d="M17 16.95A7 7 0 0 1 5 12v-2m14 0v2a7 7 0 0 1-.11 1.23"/><line x1="12" y1="19" x2="12" y2="23"/><line x1="8" y1="23" x2="16" y2="23"/></svg>';
        updPeers();
        break;
      }
      case 'unmute_cmd': {
        if (localStream) {
          const t = localStream.getAudioTracks()[0];
          if (t && t.readyState === 'live') t.enabled = true;
        }
        isMuted = false;
        document.getElementById('muteBtn').classList.remove('muted');
        document.getElementById('muteBtn').innerHTML = '<svg id="micIcon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 1a3 3 0 0 0-3 3v8a3 3 0 0 0 6 0V4a3 3 0 0 0-3-3z"/><path d="M19 10v2a7 7 0 0 1-14 0v-2"/><line x1="12" y1="19" x2="12" y2="23"/><line x1="8" y1="23" x2="16" y2="23"/></svg>';
        updPeers();
        break;
      }

      case 'voice_state': {
        const p = peerMap.get(m.peer_id);
        if (p) {
          p.muted = m.muted;
          if (m.muted) {
            // when a peer mutes, clear their cached speaking flag
            // immediately. Otherwise the LAST speaking:true they sent before
            // muting stays in our peerMap forever — they appear to glow
            // green to everyone else even though they're muted, because
            // they've stopped sending speaking events. Clearing both
            // `speaking` and `actuallyHeard` ensures the green ring drops
            // instantly on every remote viewer the moment mute lands.
            p.speaking = false;
            p.actuallyHeard = false;
            lastMutedAt[m.peer_id] = Date.now();
          }
          updPeers();
        }
        break;
      }

      case 'speaking': {
        const p = peerMap.get(m.peer_id);
        if (p) {
          // ignore speaking events from a peer we know is muted.
          // This guards against race conditions where a "speaking:true"
          // packet was already in flight when the mute landed (the peer's
          // own local timer stopped, but UDP was already on the wire).
          if (p.muted) {
            p.speaking = false;
          } else {
            p.speaking = m.level > 0.05;
          }
          updPeers();
        }
        break;
      }

      case 'typing': {
        if (m.active) typingUsers.set(m.peer_id, m.name || '?');
        else typingUsers.delete(m.peer_id);
        renderTyping();
        break;
      }
    }
  };

  ws.onclose = e => {
    log("WS close " + e.code);
    if (gotRoomFull || e.code === 4003) {
      return;
    }
    if (!leaving) {
      const delay = Math.min(1000 * Math.pow(1.5, wsRetries), 15000);
      wsRetries++;
      log("WS reconnect in " + delay + "ms");
      setTimeout(connectWS, delay);
    } else {
      cleanupRTC();
    }
  };

  ws.onerror = e => { log("WS err"); };
}

function addPeer(p) {
  if (!peerMap.has(p.id)) {
    peerMap.set(p.id, {
      name: p.name, avatar: p.avatar || '',
      is_host: p.is_host, is_admin: !!p.is_admin, muted: p.muted,
      connState: 'new', retries: 0, speaking: false,
      usedRelay: false
    });
  }
  if (p.is_host) log("peer " + p.id + " HOST");
  updCount();
  updPeers();
}

function updCount() {
  const total = peerMap.size + 1;
  document.getElementById('mcount').textContent = total + '/' + serverMaxPeers + ' in call';
}

// ════════════════════════════════════════════════════════════════════════════
// SEAT GRID — replaces the old pill strip. Same data sources, new look.
// ════════════════════════════════════════════════════════════════════════════
// • Each call participant (including yourself) gets a tile with:
//     - circular avatar (or first-letter fallback)
//     - speaking ring: green border + glow when speaking
//     - mute badge (bottom-right of avatar) when muted
//     - host frame (gold border) for the host
//     - name + tiny status dot (the existing connection-quality dot logic)
//     - "Host" chip under the host's name
// • The grid is a single horizontal row. ~3-4 tiles fit on a typical
//   phone width without scrolling; with more peers, the row scrolls
//   horizontally inside .seat-grid-wrap.
// • Empty placeholder tiles are shown to communicate capacity:
//   minimum 4 visible slots, plus 1 trailing "invite" slot once the
//   room has 4+ people (until the 15-peer cap is reached).
// • The whole panel is collapsible — see the seat-handle drag logic below.

function _seatDotClass(p, id) {
  // Mirrors the old pill's dot logic exactly so connection quality stays
  // accurate. Returns one of: 'conn' | 'fail' | 'relay' | 'warn' | 'connecting'.
  let dot = '';
  if (p.connState === 'connected') {
    const smoothed = lossEwma[id] !== undefined ? lossEwma[id] : (p.lossPct || 0);
    const onRelay = peerRelay[id] || p.usedRelay;
    const muted = p.muted;
    const heardRecently = p.lastHeardAt && (Date.now() - p.lastHeardAt) < 8000;
    const noPacketsArriving = (p.recvRate !== undefined) && (p.recvRate < 1);
    const audioActuallyBroken = !muted && !heardRecently && noPacketsArriving && smoothed > 15;
    if (audioActuallyBroken) dot = 'fail';
    else if (onRelay) dot = (smoothed > 12) ? 'warn' : 'relay';
    else dot = (smoothed > 12) ? 'warn' : 'conn';
  } else if (p.connState === 'failed' || p.connState === 'closed') {
    dot = 'fail';
  } else if (p.connState === 'connecting' || p.connState === 'checking' || p.connState === 'new') {
    dot = 'connecting';
  }
  return dot;
}

function _avatarHTML(name, avatarData) {
  const initial = name && name.length ? esc(name[0].toUpperCase()) : '?';
  if (avatarData) {
    return '<img src="' + avatarData + '" alt="">';
  }
  return '<span>' + initial + '</span>';
}

const MUTE_SVG = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.4" stroke-linecap="round" stroke-linejoin="round"><line x1="1" y1="1" x2="23" y2="23"/><path d="M9 9v3a3 3 0 0 0 5.12 2.12M15 9.34V4a3 3 0 0 0-5.94-.6"/><path d="M17 16.95A7 7 0 0 1 5 12v-2m14 0v2a7 7 0 0 1-.11 1.23"/><line x1="12" y1="19" x2="12" y2="23"/></svg>';
const EMPTY_SVG = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="8" r="4"/><path d="M4 21c0-4.4 3.6-8 8-8s8 3.6 8 8"/></svg>';

function _seatTile(opts) {
  // opts: { name, avatar, dot, speaking, muted, isHost, isYou, pid }
  const speakingClass = opts.speaking ? ' speaking' : '';
  const hostFrame = opts.isHost ? ' host-frame' : '';
  const muteBadge = opts.muted
    ? '<span class="seat-mute" aria-label="Muted">' + MUTE_SVG + '</span>'
    : '';
  const hostBadge = opts.isHost ? '<span class="seat-badge">Host</span>' : '';
  const nameTxt = esc(opts.name) + (opts.isYou ? ' (You)' : '');
  const pidAttr = opts.pid ? ' data-pid="' + opts.pid + '"' : ' data-self="1"';

  // avatar resolution priority:
  //   1. Host-assigned avatar (if any) — explicit override, takes precedence
  //      so the host can replace even someone's own profile picture if
  //      they want to (matches your description: "host can change pfps").
  //   2. The peer's own avatar.
  //   3. Empty (renders the initial-letter placeholder via _avatarHTML).
  // For our own self tile (opts.isYou && !opts.pid), look up under MY_ID
  // because peerMap doesn't include self — but hostAssignedAvatars uses
  // pid as key, so we need MY_ID to find our own override.
  const lookupPid = opts.pid || (opts.isYou ? MY_ID : '');
  const hostAv = lookupPid ? hostAssignedAvatars[lookupPid] : null;
  const effectiveAvatar = hostAv || opts.avatar || '';

  return (
    '<div class="seat"' + pidAttr + '>' +
      '<div class="seat-av-wrap">' +
        '<div class="seat-av' + speakingClass + hostFrame + '">' +
          _avatarHTML(opts.name, effectiveAvatar) +
        '</div>' +
        muteBadge +
      '</div>' +
      '<div class="seat-name-row">' +
        '<span class="seat-dot ' + opts.dot + '"></span>' +
        '<span class="seat-name">' + nameTxt + '</span>' +
      '</div>' +
      hostBadge +
    '</div>'
  );
}

function _emptySeatTile() {
  return (
    '<div class="seat seat-empty" aria-hidden="true">' +
      '<div class="seat-av-wrap">' +
        '<div class="seat-av">' + EMPTY_SVG + '</div>' +
      '</div>' +
      '<div class="seat-name-row"><span class="seat-name">Empty</span></div>' +
    '</div>'
  );
}

function updPeers() {
  const grid = document.getElementById('seatGrid');
  if (!grid) return;
  let h = '';

  // ── Seat ordering rule ────────────────────────────────────────────────
  // The host (Sor) ALWAYS occupies seat #1, regardless of who joined the
  // call first. After the host comes "me" (if I'm not the host), then
  // everyone else in their natural join order.
  // This means: if Sor joins a room that already has people in it, his
  // tile shows up at the top of the grid and pushes everyone down by one.
  // If Sor is currently absent, seat #1 is just whoever else is in
  // position; as soon as Sor rejoins, he reclaims seat #1 automatically.
  // ──────────────────────────────────────────────────────────────────────

  // Step 1: find the host among the remote peers (if any).
  let hostPeerId = null;
  peerMap.forEach((p, id) => {
    if (p.is_host && hostPeerId === null) hostPeerId = id;
  });

  // Step 2: if I'M the host, render my own tile in seat #1.
  // Otherwise, render the host's tile (if there is one) in seat #1, then me.
  // For both self and remote peers, mute is a HARD override on the speaking
  // ring — a muted person never visually glows green, regardless of any
  // stale speaking flag still in our state. Belt-and-suspenders on top of
  // the speaking-event handler clearing the flag at receive time.
  const renderSelf = () => {
    h += _seatTile({
      name: myName,
      avatar: myAvatar,
      dot: isMuted ? 'fail' : 'conn',
      speaking: !isMuted && !!window._selfSpeaking,
      muted: isMuted,
      isHost: !!isHost,
      isYou: true,
      pid: ''
    });
  };
  const renderRemote = (id) => {
    const p = peerMap.get(id);
    if (!p) return;
    h += _seatTile({
      name: p.name,
      avatar: p.avatar,
      dot: _seatDotClass(p, id),
      speaking: !p.muted && !!(p.speaking || p.actuallyHeard),
      muted: !!p.muted,
      isHost: !!p.is_host,
      isYou: false,
      pid: id
    });
  };

  if (isHost) {
    // I'm the host → seat #1 is me. Then everyone else in join order.
    renderSelf();
    peerMap.forEach((p, id) => renderRemote(id));
  } else if (hostPeerId) {
    // Host is someone else (Sor) → seat #1 is them, seat #2 is me, then
    // everyone else (skipping the host since we already rendered them).
    renderRemote(hostPeerId);
    renderSelf();
    peerMap.forEach((p, id) => {
      if (id !== hostPeerId) renderRemote(id);
    });
  } else {
    // No host present in the room right now → me first, then everyone else.
    renderSelf();
    peerMap.forEach((p, id) => renderRemote(id));
  }

  // ─── Empty-seat padding rule ─────────────────────────────────────────
  // Two principles working together:
  //   (a) Minimum 4 visible slots — so a small room (1-3 people) still
  //       looks balanced with placeholder seats filling the rest.
  //   (b) After all visible seats are filled, always trail with ONE
  //       empty "invite" slot so users see there's room to grow.
  // Combined formula: visible = clamp(real + 1, 4, room_max).
  //   • 1 real  → 1 + 3 empties = 4 slots
  //   • 3 real  → 3 + 1 empty   = 4 slots
  //   • 4 real  → 4 + 1 empty   = 5 slots  (rule b kicks in)
  //   • 5 real  → 5 + 1 empty   = 6 slots
  //   • 14 real → 14 + 1 empty  = 15 slots
  //   • 15 real → 15 + 0 empty  = 15 slots (room full — no invite slot)
  // The single trailing empty makes new joins feel natural: as soon as
  // someone fills the invite slot, a fresh invite slot appears after them.
  const total = peerMap.size + 1;
  const MIN_VISIBLE_SLOTS = 4;
  const visible = Math.min(Math.max(total + 1, MIN_VISIBLE_SLOTS), serverMaxPeers);
  for (let i = total; i < visible; i++) h += _emptySeatTile();
  grid.innerHTML = h;

  // if I'm the host, attach click handlers to any seat avatar that's
  // currently empty (no own avatar AND no host-assigned override yet). These
  // are the assignable tiles — clicking opens the picker. We re-attach on
  // every render because grid.innerHTML wipes all listeners.
  if (isHost) {
    grid.querySelectorAll('.seat[data-pid]').forEach(function(seat) {
      const pid = seat.getAttribute('data-pid');
      if (!pid) return;
      const p = peerMap.get(pid);
      if (!p) return;
      // Skip if peer already has an avatar (their own OR host-assigned)
      if (p.avatar) return;
      if (hostAssignedAvatars[pid]) return;
      const avEl = seat.querySelector('.seat-av');
      if (!avEl) return;
      avEl.setAttribute('data-assignable', 'true');
      avEl.title = 'Click to assign avatar';
      avEl.addEventListener('click', function(ev) {
        ev.stopPropagation();
        showAvatarPicker(pid, avEl);
      });
    });
  }

  // Pull-tab count (shown when panel is collapsed)
  const pullCount = document.getElementById('seatPullCount');
  if (pullCount) pullCount.textContent = total + '/' + serverMaxPeers + ' in call';
}

function updPeerLevels() {
  // Per-frame update: flip .speaking on tiles whose state changed. The CSS
  // keyframe seatPulse handles the actual visual animation.
  const grid = document.getElementById('seatGrid');
  if (!grid) return;

  // self tile
  const selfTile = grid.querySelector('.seat[data-self="1"] .seat-av');
  if (selfTile) {
    const isActive = !!window._selfSpeaking && !isMuted;
    if (isActive && !selfTile.classList.contains('speaking')) selfTile.classList.add('speaking');
    else if (!isActive && selfTile.classList.contains('speaking')) selfTile.classList.remove('speaking');
  }

  // remote peer tiles — muted peers never glow even if stale speaking flags linger
  grid.querySelectorAll('.seat[data-pid]').forEach(seat => {
    const pid = seat.getAttribute('data-pid');
    const p = peerMap.get(pid);
    if (!p) return;
    const av = seat.querySelector('.seat-av');
    if (!av) return;
    const isActive = !p.muted && !!(p.speaking || p.actuallyHeard);
    if (isActive && !av.classList.contains('speaking')) av.classList.add('speaking');
    else if (!isActive && av.classList.contains('speaking')) av.classList.remove('speaking');
  });
}
let _peerLevelTicker = null;

// ════════════════════════════════════════════════════════════════════════════
// SEAT PANEL DRAG-TO-COLLAPSE
// ════════════════════════════════════════════════════════════════════════════
// The user can grab the bottom handle and drag the panel up to collapse it,
// or tap the small pull-tab below the header to expand it again. Tap on the
// handle alone (no drag) toggles collapsed state — same affordance as a
// disclosure chip. State is preserved in memory only (resets each session,
// which matches the rest of the UI's state).

let _seatCollapsed = false;
let _seatDrag = null;

function setSeatCollapsed(collapsed, animate) {
  _seatCollapsed = !!collapsed;
  const panel = document.getElementById('seatPanel');
  const tab = document.getElementById('seatPullTab');
  if (!panel || !tab) return;
  if (!animate) panel.classList.add('dragging');
  panel.style.maxHeight = '';  // clear any inline height set by the drag
  if (_seatCollapsed) {
    panel.classList.add('collapsed');
    tab.classList.add('show');
  } else {
    panel.classList.remove('collapsed');
    tab.classList.remove('show');
  }
  if (!animate) {
    // force reflow then re-enable transitions
    void panel.offsetHeight;
    panel.classList.remove('dragging');
  }
}

function _seatHandlePointerDown(ev) {
  // Only react to primary button / single touch
  if (ev.button !== undefined && ev.button !== 0) return;
  const panel = document.getElementById('seatPanel');
  if (!panel) return;
  const rect = panel.getBoundingClientRect();
  _seatDrag = {
    startY: ev.clientY,
    startH: rect.height,
    moved: false,
    pointerId: ev.pointerId
  };
  panel.classList.add('dragging');
  try { ev.target.setPointerCapture(ev.pointerId); } catch (e) {}
  ev.preventDefault();
}

function _seatHandlePointerMove(ev) {
  if (!_seatDrag) return;
  const dy = ev.clientY - _seatDrag.startY;
  if (Math.abs(dy) > 3) _seatDrag.moved = true;
  const panel = document.getElementById('seatPanel');
  if (!panel) return;
  // dy negative = dragging up (collapsing). dy positive = dragging down (expanding).
  // Max height capped at the natural panel height (150px for the single-row layout).
  let newH = _seatDrag.startH + dy;
  newH = Math.max(0, Math.min(150, newH));
  panel.style.maxHeight = newH + 'px';
  // Live-toggle the collapsed visual class so the pull-tab can appear smoothly
  if (newH < 30 && !panel.classList.contains('collapsed-live')) {
    panel.classList.add('collapsed-live');
    document.getElementById('seatPullTab').classList.add('show');
  } else if (newH >= 30 && panel.classList.contains('collapsed-live')) {
    panel.classList.remove('collapsed-live');
    document.getElementById('seatPullTab').classList.remove('show');
  }
}

function _seatHandlePointerUp(ev) {
  if (!_seatDrag) return;
  const panel = document.getElementById('seatPanel');
  const moved = _seatDrag.moved;
  const startedExpanded = !_seatCollapsed;
  const finalH = panel ? parseFloat(panel.style.maxHeight || '150') : 150;
  _seatDrag = null;
  if (panel) {
    panel.classList.remove('dragging');
    panel.classList.remove('collapsed-live');
  }
  // Decision:
  //   - If the user just tapped without moving → toggle.
  //   - If they dragged: anything below ~50% of normal height → collapse.
  //                      anything above → expand.
  if (!moved) {
    setSeatCollapsed(!_seatCollapsed, true);
  } else {
    const collapseThreshold = 50;
    if (finalH < collapseThreshold) setSeatCollapsed(true, true);
    else setSeatCollapsed(false, true);
  }
  try { ev.target.releasePointerCapture(ev.pointerId); } catch (e) {}
}

function initSeatPanel() {
  const handle = document.getElementById('seatHandle');
  const tab = document.getElementById('seatPullTab');
  if (handle) {
    handle.addEventListener('pointerdown', _seatHandlePointerDown);
    handle.addEventListener('pointermove', _seatHandlePointerMove);
    handle.addEventListener('pointerup', _seatHandlePointerUp);
    handle.addEventListener('pointercancel', _seatHandlePointerUp);
  }
  if (tab) {
    // Tap or drag-down on the pull-tab also expands.
    tab.addEventListener('click', () => {
      if (_seatCollapsed) setSeatCollapsed(false, true);
    });
  }
}
// Wire up handlers once the DOM is ready (the script tag is at the end of
// <body> already so the elements exist, but we still guard for safety).
if (document.readyState !== 'loading') initSeatPanel();
else document.addEventListener('DOMContentLoaded', initSeatPanel);

// ════════════════════════════════════════════════════════════════════════════
// VISUAL VIEWPORT TRACKER — bulletproof keyboard handling
// ════════════════════════════════════════════════════════════════════════════
// CSS alone (position:fixed; inset:0) is *almost* enough on modern browsers,
// but iOS Safari and some Android Chrome builds still occasionally lift the
// layout viewport when an input is focused and would otherwise be obscured
// by the keyboard. The visualViewport API gives us the *actual* visible
// rectangle of the page, accounting for the keyboard. We mirror its size
// onto .app via inline styles so:
//   • The header NEVER scrolls off the top.
//   • The seat panel stays anchored under the header.
//   • The chat-stack between header and input shrinks, and the messages
//     inside it remain scrollable normally.
//   • The input bar sits exactly above the keyboard, never under it.
// We also pin window scroll to (0,0) — if the browser tries to lift the
// page despite our CSS, we snap it back instantly.
// ────────────────────────────────────────────────────────────────────────────
(function setupVisualViewportLock() {
  const vv = window.visualViewport;
  const app = () => document.getElementById('app');
  function applyVV() {
    const a = app();
    if (!a) return;
    if (vv) {
      // Use the visual viewport's actual height. When the keyboard opens
      // this number drops; .app shrinks with it. offsetTop > 0 means the
      // browser has already scrolled the page — we cancel that.
      a.style.height = vv.height + 'px';
      a.style.top = vv.offsetTop + 'px';
    } else {
      // Old browsers: rely purely on CSS dvh.
      a.style.height = '';
      a.style.top = '';
    }
    // Always pin window scroll to 0 — if anything tried to lift the page,
    // undo it immediately.
    if (window.scrollY !== 0 || window.scrollX !== 0) {
      window.scrollTo(0, 0);
    }
  }
  if (vv) {
    vv.addEventListener('resize', applyVV);
    vv.addEventListener('scroll', applyVV);
  }
  window.addEventListener('resize', applyVV);
  // Also re-apply when an input gains/loses focus — this is the moment the
  // browser tries hardest to lift the page.
  document.addEventListener('focusin', () => {
    // Two passes: one immediately, one after the keyboard-open animation
    // (typically ~250-350ms on Android, ~200ms on iOS) finishes settling.
    applyVV();
    setTimeout(applyVV, 50);
    setTimeout(applyVV, 350);
  }, true);
  document.addEventListener('focusout', () => {
    applyVV();
    setTimeout(applyVV, 50);
    setTimeout(applyVV, 350);
  }, true);
  // Initial apply
  if (document.readyState !== 'loading') applyVV();
  else document.addEventListener('DOMContentLoaded', applyVV);
})();

// ════════════════════════════════════════════════════════════════════════════
// SCROLL LOGIC FOR REVERSE-FLEX MESSAGE LIST
// ════════════════════════════════════════════════════════════════════════════
// Mental model:
//   • Visual bottom (newest message)  →  scrollTop ≈ 0
//   • Visual top    (oldest message)  →  scrollTop is far from 0
//
// Different browsers historically used different signs for scrollTop with
// column-reverse. Modern Chrome/Safari/Firefox give NEGATIVE scrollTop as
// you scroll up. Some older builds gave POSITIVE. To be bulletproof we
// only ever look at Math.abs(scrollTop) and treat it as "how far from
// the visual bottom in pixels". This is the same trick used by the
// react-scroll-to-bottom library that Telegram Web and Discord both
// use under the hood.
// ════════════════════════════════════════════════════════════════════════════

let userIsAtBottom = true;
let unreadCount = 0;
const NEAR_BOTTOM_PX = 80;

function distanceFromVisualBottom(el) {
  // With column-reverse, |scrollTop| is the pixel distance from the
  // visual bottom. We use Math.abs to handle both sign conventions.
  return Math.abs(el.scrollTop);
}

function setupScrollLock() {
  const el = document.getElementById('msgs');
  if (!el) return;
  el.addEventListener('scroll', () => {
    const dist = distanceFromVisualBottom(el);
    const wasAtBottom = userIsAtBottom;
    userIsAtBottom = dist <= NEAR_BOTTOM_PX;
    if (userIsAtBottom && !wasAtBottom) {
      unreadCount = 0;
      updateJumpButton();
    } else if (userIsAtBottom) {
      if (unreadCount !== 0) { unreadCount = 0; updateJumpButton(); }
    } else {
      // Scrolled up; just refresh the button visibility (count unchanged)
      updateJumpButton();
    }
  }, { passive: true });
}

function isAtBottom() {
  const el = document.getElementById('msgs');
  if (!el) return true;
  return distanceFromVisualBottom(el) <= NEAR_BOTTOM_PX;
}

function scrollToLatest(smooth, force) {
  // Visual bottom = scrollTop 0 in column-reverse layouts.
  const el = document.getElementById('msgs');
  if (!el) return;
  if (smooth) {
    el.scrollTo({ top: 0, behavior: 'smooth' });
  } else {
    el.scrollTop = 0;
  }
  userIsAtBottom = true;
  unreadCount = 0;
  updateJumpButton();
}

function updateJumpButton() {
  const btn = document.getElementById('jumpLatest');
  const badge = document.getElementById('jumpBadge');
  if (!btn || !badge) return;
  if (userIsAtBottom || unreadCount === 0) {
    btn.classList.remove('show');
    badge.classList.add('hidden');
  } else {
    btn.classList.add('show');
    if (unreadCount > 0) {
      badge.textContent = unreadCount > 99 ? '99+' : unreadCount;
      badge.classList.remove('hidden');
    } else {
      badge.classList.add('hidden');
    }
  }
}

let _zombieCounts = {};
let _zombieCooldowns = {};

function detectFrozenJitter(pid, jitter, dRecv) {
  if (dRecv > 0) {
    frozenJitterCounts[pid] = 0;
    frozenJitterValues[pid] = jitter;
    return false;
  }
  if (jitter <= 0.05) {
    frozenJitterCounts[pid] = 0;
    frozenJitterValues[pid] = jitter;
    return false;
  }
  const mutedAgo = lastMutedAt[pid] ? Date.now() - lastMutedAt[pid] : Infinity;
  if (mutedAgo < 4000) return false;
  if (peerMap.get(pid) && peerMap.get(pid).muted) return false;
  if (_zombieCooldowns[pid] && Date.now() - _zombieCooldowns[pid] < 10000) return false;

  if (frozenJitterValues[pid] !== undefined && jitter === frozenJitterValues[pid]) {
    frozenJitterCounts[pid] = (frozenJitterCounts[pid] || 0) + 1;
  } else {
    frozenJitterCounts[pid] = 1;
  }
  frozenJitterValues[pid] = jitter;

  if (frozenJitterCounts[pid] >= 3) {
    const p = peerMap.get(pid);
    if (p && p.connState === 'connected' && !p.muted && (!p.recvLevel || p.recvLevel < 0.005)) {
      _zombieCooldowns[pid] = Date.now();
      frozenJitterCounts[pid] = 0;
      return true;
    }
  }
  return false;
}

function clearConnectionTimer(pc) {
  if (pc && pc._connTimer) {
    clearTimeout(pc._connTimer);
    pc._connTimer = null;
  }
  if (pc) pc._connTimerFires = 0;
}

// startConnectionTimer now accepts an optional customTimeout.
// The "brand new PC" grace period uses this to schedule 30s instead of 10s.
function startConnectionTimer(pid, customTimeout) {
  const pc = peers[pid];
  if (!pc || pc._connTimer) return;
  pc._connTimerFires = pc._connTimerFires || 0;
  const timeoutMs = customTimeout || connTimerMs();
  pc._connTimer = setTimeout(() => {
    pc._connTimer = null;
    if (peers[pid] !== pc || pc.connectionState === 'connected' || pc.connectionState === 'closed') {
      return;
    }
    // check LIVE TRACK first, before all other state checks.
    // On relay/TURN connections the iceConnectionState can lag behind
    // reality — the track is playing but state says "new" or
    // "have-local-offer". If audio is flowing we NEVER kill it.
    const hasLiveTrack = pc.getReceivers && pc.getReceivers().some(function(r) {
      return r.track && r.track.readyState === 'live';
    });
    // a "live" track that's actually receiving zero packets is a
    // zombie — the browser hasn't noticed the relay died. Look at the
    // most recent STATS reading; if packets-per-interval (lossPct via
    // peerInfo.recvRate) is genuinely 0, treat the track as DEAD and
    // fall through to the rebuild path.
    const piRecv = (peerMap.get(pid) || {}).recvRate;
    const trackActuallyAlive = hasLiveTrack && (piRecv === undefined || piRecv > 0);
    if (trackActuallyAlive) {
      log("CONN-TIMER " + pid + " track is LIVE, skipping all checks");
      startConnectionTimer(pid, connTimerMs() + 10000);  // recheck in 20s
      return;
    }
    if (hasLiveTrack && piRecv === 0) {
      log("CONN-TIMER " + pid + " track is LIVE but recvRate=0 — zombie track, forcing rebuild");
      // Don't return — fall through to rebuild logic below.
    }
    if (pc.iceConnectionState === 'connected' || pc.iceConnectionState === 'completed') {
      log("CONN-TIMER " + pid + " ICE healthy (" + pc.iceConnectionState + "), waiting for conn flip");
      pc._connTimerFires++;
      if (pc._connTimerFires < 3) startConnectionTimer(pid);
      return;
    }
    if (pc.signalingState === 'have-local-offer') {
      log("CONN-TIMER " + pid + " have-local-offer, waiting for answer");
      pc._connTimerFires++;
      if (pc._connTimerFires < 3) startConnectionTimer(pid);
      return;
    }
    // brand new PCs get 30s grace (10s base + 20s extra).
    // Previously startConnectionTimer(pid) was called which only gave 10s,
    // causing premature ICE restarts and broken audio.
    if (pc.iceConnectionState === 'new' && pc._connTimerFires === 0) {
      log("CONN-TIMER " + pid + " brand new PC, more time");
      pc._connTimerFires++;
      startConnectionTimer(pid, connTimerMs() + 10000);  // 20s grace (10+10), not 10s
      return;
    }
    if (pc.iceConnectionState === 'checking') {
      pc._connTimerFires++;
      if (pc._connTimerFires >= 1 && !peerRelay[pid] && MY_ID > pid) {
        log("CONN-TIMER " + pid + " stuck checking -> force RELAY (likely blocked UDP)");
        peerRelay[pid] = true;
        lastRelayAt[pid] = Date.now();
        if (ws && ws.readyState === 1) {
          ws.send(JSON.stringify({ type: 'request_relay', to: pid, reason: 'stuck-checking' }));
        }
        destroyPeer(pid);
        renegInProgress[pid] = true;
        setTimeout(() => {
          createOffer(pid);
          setTimeout(() => { renegInProgress[pid] = false; }, 3000);
        }, 200);
        return;
      }
      log("CONN-TIMER " + pid + " still checking, waiting");
      if (pc._connTimerFires < 3) startConnectionTimer(pid);
      return;
    }
    pc._connTimerFires++;
    if (pc._connTimerFires > 2) {
      log("CONN-TIMER " + pid + " max fires, full retry");
      scheduleRetry(pid);
      return;
    }
    log("CONN-TIMER " + pid + " stuck=" + pc.connectionState + "/" + pc.iceConnectionState + " -> ICE restart");
    forceIceRestart(pid);
  }, timeoutMs);
}

async function forceIceRestart(pid) {
  if (MY_ID <= pid) {
    log("ICE restart ignored (smaller side) " + pid);
    return;
  }
  if (lastIceRestartAt[pid] && Date.now() - lastIceRestartAt[pid] < 8000) {
    log("ICE restart cooldown " + pid);
    return;
  }
  const pc = peers[pid];
  if (!pc || pc.connectionState === 'closed') return;
  const p = peerMap.get(pid);
  if (!p || p._iceRestarting || p._retrying) return;
  if (pc.iceConnectionState === 'checking' || pc.iceConnectionState === 'connecting') return;
  if (pc.signalingState === 'have-remote-offer') {
    log("ICE restart skipped (have-remote-offer) " + pid);
    return;
  }
  p._iceRestarting = true;
  lastIceRestartAt[pid] = Date.now();
  log("FAST ICE restart " + pid);
  try {
    const o = await pc.createOffer({ iceRestart: true });
    o.sdp = preferOpusAndTune(o.sdp);
    await pc.setLocalDescription(o);
    ws.send(JSON.stringify({ type: 'webrtc_offer', to: pid, sdp: pc.localDescription.sdp }));
  } catch (e) {
    log("fast iceRestart fail: " + e.message);
    scheduleRetry(pid);
  } finally {
    setTimeout(() => { if (p) p._iceRestarting = false; }, 3000);
  }
}

function destroyPeer(pid, keepAudio) {
  if (peers[pid]) {
    clearConnectionTimer(peers[pid]);
    const dying = peers[pid];
    dying.onicecandidate = null;
    dying.onicecandidateerror = null;
    dying.ontrack = null;
    dying.onconnectionstatechange = null;
    dying.oniceconnectionstatechange = null;
    try { dying.close(); } catch (e) {}
    delete peers[pid];
  }
  if (audios[pid] && !keepAudio) {
    try { audios[pid].srcObject = null; } catch (e) {}
  }
  if (statsTimers[pid]) {
    clearInterval(statsTimers[pid]);
    delete statsTimers[pid];
  }
  if (inboundLevelTimers[pid]) {
    clearInterval(inboundLevelTimers[pid]);
    delete inboundLevelTimers[pid];
  }
  delete iceBuffer[pid];
  delete renegInProgress[pid];
  delete frozenJitterCounts[pid];
  delete frozenJitterValues[pid];
  delete lossEwma[pid];
  delete sustainedBadStart[pid];
  delete relayConnectedAt[pid];
}

function nukePeer(pid) {
  destroyPeer(pid);
  if (audios[pid]) {
    try { audios[pid].pause(); audios[pid].srcObject = null; audios[pid].remove(); } catch (e) {}
    delete audios[pid];
  }
  delete peerRelay[pid];
  delete lastOfferUfrag[pid];
  delete lastMutedAt[pid];
  delete renegInProgress[pid];
  delete lastRelayAt[pid];
  delete lastOfferAt[pid];
  delete lastIceRestartAt[pid];
  delete lastFullRebuildAt[pid];
  delete fullRebuildAttempts[pid];
  delete peerGivenUp[pid];
  delete _zombieCounts[pid];
  delete _zombieCooldowns[pid];
  delete frozenJitterCounts[pid];
  delete frozenJitterValues[pid];
  delete lossEwma[pid];
  delete sustainedBadStart[pid];
  delete relayConnectedAt[pid];
}

function shouldForceRelay(pid) {
  if (peerRelay[pid]) return true;
  const p = peerMap.get(pid);
  return p && p.retries >= 1;
}

async function createOffer(pid) {
  if (lastOfferAt[pid] && Date.now() - lastOfferAt[pid] < 2000) {
    log("offer DEDUP " + pid);
    return;
  }
  lastOfferAt[pid] = Date.now();
  renegInProgress[pid] = true;

  log("offer->" + pid + (shouldForceRelay(pid) ? " (RELAY-ONLY)" : ""));
  destroyPeer(pid);
  renegInProgress[pid] = true;

  const p = peerMap.get(pid);
  if (!p) { renegInProgress[pid] = false; return; }

  try {
    const pc = new RTCPeerConnection(getPCConfig(shouldForceRelay(pid)));
    setupPC(pc, pid);
    peers[pid] = pc;
    if (localStream) localStream.getTracks().forEach(t => pc.addTrack(t, localStream));

    const offer = await pc.createOffer();
    offer.sdp = preferOpusAndTune(offer.sdp);
    await pc.setLocalDescription(offer);
    ws.send(JSON.stringify({ type: 'webrtc_offer', to: pid, sdp: pc.localDescription.sdp }));
    log("offer SENT " + pid);
    startConnectionTimer(pid);
  } catch (e) {
    log("offer FAIL " + pid + ": " + e.message);
  } finally {
    setTimeout(() => { renegInProgress[pid] = false; }, 3000);
  }
}

async function handleOffer(from, sdp) {
  log("offer<-" + from);

  const ufragMatch = (sdp || '').match(/a=ice-ufrag:(\S+)/);
  const ufrag = ufragMatch ? ufragMatch[1] : null;
  if (ufrag && lastOfferUfrag[from] === ufrag) {
    log("duplicate offer ignored " + from + " (ufrag=" + ufrag + ")");
    return;
  }
  if (ufrag) lastOfferUfrag[from] = ufrag;

  const existing = peers[from];

  if (existing && existing.signalingState !== 'closed' && existing.connectionState !== 'failed') {
    renegInProgress[from] = true;
    try {
      clearConnectionTimer(existing);
      if (existing.signalingState === 'have-local-offer') {
        if (MY_ID > from) {
          log("collision: I'm impolite, ignoring offer from " + from);
          renegInProgress[from] = false;
          return;
        }
        log("collision: polite rollback for " + from);
        try {
          await existing.setLocalDescription({ type: 'rollback' });
        } catch (rollbackErr) {
          log("rollback FAILED " + from + ": " + rollbackErr.message + " -> rebuild");
          throw rollbackErr;
        }
      }

      await existing.setRemoteDescription(new RTCSessionDescription({ type: 'offer', sdp }));

      if (iceBuffer[from]) {
        for (const c of iceBuffer[from]) {
          try { await existing.addIceCandidate(new RTCIceCandidate(c)); } catch (e) {}
        }
        delete iceBuffer[from];
      }

      const ans = await existing.createAnswer();
      ans.sdp = preferOpusAndTune(ans.sdp);
      await existing.setLocalDescription(ans);
      ws.send(JSON.stringify({ type: 'webrtc_answer', to: from, sdp: existing.localDescription.sdp }));
      log("in-place answer SENT " + from);
      return;
    } catch (e) {
      log("in-place reneg FAIL " + from + ": " + e.message + " -> fresh PC");
    } finally {
      setTimeout(() => { renegInProgress[from] = false; }, 3000);
    }
  }

  destroyPeer(from);
  try {
    const pc = new RTCPeerConnection(getPCConfig(shouldForceRelay(from)));
    setupPC(pc, from);
    peers[from] = pc;

    await pc.setRemoteDescription(new RTCSessionDescription({ type: 'offer', sdp }));

    if (iceBuffer[from]) {
      for (const c of iceBuffer[from]) {
        try { await pc.addIceCandidate(new RTCIceCandidate(c)); } catch (e) {}
      }
      delete iceBuffer[from];
    }

    if (localStream) localStream.getTracks().forEach(t => pc.addTrack(t, localStream));

    const ans = await pc.createAnswer();
    ans.sdp = preferOpusAndTune(ans.sdp);
    await pc.setLocalDescription(ans);
    ws.send(JSON.stringify({ type: 'webrtc_answer', to: from, sdp: pc.localDescription.sdp }));
    log("answer SENT " + from);
  } catch (e) {
    log("answer FAIL " + from + ": " + e.message);
  }
}

async function handleAnswer(from, sdp) {
  log("ans<-" + from);
  try {
    const pc = peers[from];
    if (!pc) { log("no PC for ans " + from); return; }
    if (pc.signalingState !== 'have-local-offer') {
      log("ans skipped (state=" + pc.signalingState + ")");
      return;
    }
    await pc.setRemoteDescription(new RTCSessionDescription({ type: 'answer', sdp }));
    log("ans applied " + from);
  } catch (e) {
    log("ansErr " + from + ": " + e.message);
    scheduleRetry(from);
  }
}

async function handleIce(from, cand) {
  const pc = peers[from];
  if (pc && pc.remoteDescription && cand) {
    try { await pc.addIceCandidate(new RTCIceCandidate(cand)); } catch (e) {}
  } else if (cand) {
    if (!iceBuffer[from]) iceBuffer[from] = [];
    iceBuffer[from].push(cand);
  }
}

function preferOpusAndTune(sdp) {
  if (!sdp) return sdp;
  const lines = sdp.split('\r\n');
  let opusPt = null;
  for (const l of lines) {
    const m = l.match(/^a=rtpmap:(\d+) opus\/48000/i);
    if (m) { opusPt = m[1]; break; }
  }
  if (!opusPt) return sdp;

  let found = false;
  const out = lines.map(l => {
    const m = l.match(new RegExp('^a=fmtp:' + opusPt + ' (.*)$'));
    if (m) {
      found = true;
      let p = m[1];
      const merge = (key, val) => {
        if (new RegExp('(^|;)' + key + '=').test(p)) {
          p = p.replace(new RegExp(key + '=[^;]*'), key + '=' + val);
        } else {
          p += ';' + key + '=' + val;
        }
      };
      merge('useinbandfec', '1');
      merge('usedtx', '1');
      merge('stereo', '0');
      merge('maxaveragebitrate', '32000');
      merge('cbr', '0');
      return 'a=fmtp:' + opusPt + ' ' + p;
    }
    return l;
  });
  if (!found) {
    for (let i = 0; i < out.length; i++) {
      if (new RegExp('^a=rtpmap:' + opusPt + ' opus').test(out[i])) {
        out.splice(i + 1, 0, 'a=fmtp:' + opusPt + ' minptime=10;useinbandfec=1;usedtx=1;stereo=0;maxaveragebitrate=32000');
        break;
      }
    }
  }

  let audioStart = -1;
  for (let i = 0; i < out.length; i++) {
    if (out[i].startsWith('m=audio')) { audioStart = i; break; }
  }
  if (audioStart !== -1) {
    for (let i = audioStart + 1; i < out.length; i++) {
      if (out[i].startsWith('m=')) break;
      if (out[i].startsWith('a=sendonly') || out[i].startsWith('a=recvonly')) {
        out[i] = 'a=sendrecv';
        break;
      }
    }
    let hasAS = false;
    for (let i = audioStart + 1; i < out.length; i++) {
      if (out[i].startsWith('m=')) break;
      if (out[i].startsWith('b=AS:')) { hasAS = true; break; }
    }
    if (!hasAS) out.splice(audioStart + 1, 0, 'b=AS:40');
  }

  return out.join('\r\n');
}

let _sharedAC = null;
function getSharedAC() {
  if (!_sharedAC) {
    _sharedAC = new (window.AudioContext || window.webkitAudioContext)();
  }
  if (_sharedAC.state === 'suspended') {
    _sharedAC.resume().catch(() => {});
  }
  return _sharedAC;
}

function startInboundLevel(stream, pid) {
  if (inboundLevelTimers[pid]) {
    clearInterval(inboundLevelTimers[pid]);
    delete inboundLevelTimers[pid];
  }
  // guard against null/empty streams. Happens when remote peer
  // didn't grant mic permission — TRACK event fires with streams=[] and
  // we'd previously call createMediaStreamSource(undefined) which throws
  // and spams the log on every rebuild attempt.
  if (!stream || !(stream instanceof MediaStream) ||
      stream.getAudioTracks().length === 0) {
    return;
  }
  try {
    const ac = getSharedAC();
    if (!remoteAudioCtx) remoteAudioCtx = ac;
    const src = ac.createMediaStreamSource(stream);
    const analyser = ac.createAnalyser();
    analyser.fftSize = 256;
    src.connect(analyser);
    const data = new Uint8Array(analyser.frequencyBinCount);
    // ── Per-peer hysteresis state for actuallyHeard ────────────────────
    // Same pattern as the local-mic monitor (asymmetric thresholds +
    // release window) so the remote peer's green ring doesn't flicker
    // between words but drops promptly when they really go quiet.
    let heardState = false;
    let recvSilenceStartedAt = 0;
    const HEARD_START = 0.04;
    const HEARD_STOP = 0.018;
    const HEARD_RELEASE_MS = 300;  // a tad longer than local-side to
                                   // absorb jitter from the network
    inboundLevelTimers[pid] = setInterval(() => {
      analyser.getByteFrequencyData(data);
      let sum = 0;
      for (let i = 0; i < data.length; i++) sum += data[i];
      const level = sum / data.length / 255;
      const p = peerMap.get(pid);
      if (!p) return;
      p.recvLevel = level;
      // Hysteresis state machine — mirrors the local one.
      const now = Date.now();
      if (!heardState) {
        if (level > HEARD_START) {
          heardState = true;
          recvSilenceStartedAt = 0;
        }
      } else {
        if (level < HEARD_STOP) {
          if (recvSilenceStartedAt === 0) recvSilenceStartedAt = now;
          else if (now - recvSilenceStartedAt >= HEARD_RELEASE_MS) {
            heardState = false;
            recvSilenceStartedAt = 0;
          }
        } else {
          recvSilenceStartedAt = 0;
        }
      }
      p.actuallyHeard = heardState;
      // lastHeardAt remains driven by any audible packet (low bar) — it's
      // used elsewhere as a "have we received audio at all recently" signal
      // for connection-quality dots, separate from the speaking-ring logic.
      if (level > 0.02) p.lastHeardAt = now;
    }, 100);
  } catch (e) {
    log("inboundLevel fail " + pid + ": " + e.message);
  }
}

function showAudioUnlockUI() {
  let el = document.getElementById('audioUnlock');
  if (el) return;
  el = document.createElement('div');
  el.id = 'audioUnlock';
  el.style.cssText = 'position:fixed;top:50px;left:10px;right:10px;z-index:150;background:#ff9500;color:#000;padding:14px;border-radius:12px;text-align:center;font-weight:600;font-size:14px;cursor:pointer;animation:msgIn .3s';
  el.textContent = 'Tap here to enable sound';
  el.onclick = () => {
    log("audio unlock tapped");
    if (remoteAudioCtx && remoteAudioCtx.state === 'suspended') {
      remoteAudioCtx.resume().catch(() => {});
    }
    Object.values(audios).forEach(a => {
      try {
        a.muted = false;
        a.volume = 1.0;
        a.play().catch(() => {});
      } catch (e) {}
    });
    audioUnlocked = true;
    hideAudioUnlockUI();
  };
  document.body.appendChild(el);
}

function hideAudioUnlockUI() {
  const el = document.getElementById('audioUnlock');
  if (el) el.remove();
}

function setupPC(pc, pid) {
  pc.onicecandidate = e => {
    if (e.candidate && ws && ws.readyState === 1) {
      ws.send(JSON.stringify({ type: 'webrtc_ice', to: pid, candidate: e.candidate }));
    }
  };

  pc.onicecandidateerror = e => {
    if (e.errorCode >= 300 && e.errorCode <= 699) {
      log("ICE-ERR " + pid + " code=" + e.errorCode + " host=" + (e.address || e.host || '') + ":" + (e.port || ''));
    }
  };

  pc.ontrack = e => {
    log("TRACK " + pid + " streams=" + e.streams.length);
    let a = audios[pid];
    if (!a) {
      a = document.createElement('audio');
      a.autoplay = true;
      a.playsInline = true;
      a.volume = 1.0;
      a.muted = false;
      a.style.display = 'none';
      document.body.appendChild(a);
      audios[pid] = a;
    }
    a.srcObject = e.streams[0];
    a.muted = false;
    a.volume = 1.0;

    a.play().then(() => {
      log("PLAYING " + pid);
      audioUnlocked = true;
      hideAudioUnlockUI();
    }).catch(err => {
      log("playBlock " + pid + ": " + err.name);
      showAudioUnlockUI();
    });

    startInboundLevel(e.streams[0], pid);
  };

  pc.onconnectionstatechange = () => {
    log("PC " + pid + " conn=" + pc.connectionState);
    const p = peerMap.get(pid);
    if (p) p.connState = pc.connectionState;
    updPeers();

    if (pc.connectionState === 'connected') {
      clearConnectionTimer(pc);
      if (p) p.retries = 0;
      detectRelay(pc, pid);
      capOutboundBitrate(pc, adaptiveBitrate());
      startStats(pc, pid);
      delete frozenJitterCounts[pid];
      delete frozenJitterValues[pid];
      delete _lastJitters[pid];
      lossEwma[pid] = 0;
      delete sustainedBadStart[pid];
      if (peerRelay[pid]) relayConnectedAt[pid] = Date.now();
    }

    if (pc.connectionState === 'failed') {
      log("FAILED " + pid);
      clearConnectionTimer(pc);
      scheduleRetry(pid);
    }

    if (pc.connectionState === 'disconnected') {
      log("DISCONNECTED " + pid + " — waiting briefly");
      setTimeout(() => {
        if (peers[pid] && peers[pid].connectionState === 'disconnected') {
          log("still disconnected " + pid + " -> retry");
          scheduleRetry(pid);
        }
      }, 4000);
    }
  };

  pc.oniceconnectionstatechange = () => {
    log("PC " + pid + " ICE=" + pc.iceConnectionState);
  };
}

async function capOutboundBitrate(pc, kbps) {
  try {
    const sender = pc.getSenders().find(s => s.track && s.track.kind === 'audio');
    if (!sender) return;
    const params = sender.getParameters();
    if (!params.encodings || !params.encodings.length) {
      params.encodings = [{}];
    }
    params.encodings[0].maxBitrate = kbps * 1000;
    try { params.encodings[0].priority = 'high'; } catch (e) {}
    try { params.encodings[0].networkPriority = 'high'; } catch (e) {}
    await sender.setParameters(params);
  } catch (e) {}
}

async function detectRelay(pc, pid) {
  try {
    const stats = await pc.getStats();
    let isRelay = false;
    let candidatePair = null;
    stats.forEach(r => {
      if (r.type === 'candidate-pair' && r.state === 'succeeded' && r.nominated) {
        candidatePair = r;
      }
    });
    if (candidatePair) {
      stats.forEach(r => {
        if (r.id === candidatePair.localCandidateId && r.candidateType === 'relay') {
          isRelay = true;
        }
      });
    }
    const p = peerMap.get(pid);
    if (p) { p.usedRelay = isRelay; updPeers(); }
    log("PC " + pid + " path=" + (isRelay ? "RELAY" : "DIRECT"));
  } catch (e) {}
}

let _lastJitters = {};

function startStats(pc, pid) {
  if (statsTimers[pid]) clearInterval(statsTimers[pid]);
  let lastRecv = 0, lastLost = 0;
  let lastSent = 0;
  let consecutiveStalled = 0;
  let outboundStall = 0;
  let logTick = 0;

  lossEwma[pid] = 0;
  delete sustainedBadStart[pid];

  const intervalMs = statsIntervalMs();

  statsTimers[pid] = setInterval(async () => {
    if (!peers[pid] || peers[pid]?.connectionState === 'closed') {
      clearInterval(statsTimers[pid]);
      delete statsTimers[pid];
      return;
    }
    if (peers[pid]?.connectionState !== 'connected') return;

    try {
      const stats = await peers[pid].getStats();
      let recv = 0, lost = 0, jitter = 0, sent = 0;
      stats.forEach(r => {
        if (r.type === 'inbound-rtp' && r.kind === 'audio') {
          recv = r.packetsReceived || 0;
          lost = r.packetsLost || 0;
          jitter = r.jitter || 0;
        }
        if (r.type === 'outbound-rtp' && r.kind === 'audio') {
          sent = r.packetsSent || 0;
        }
      });
      const dRecv = recv - lastRecv;
      const dLost = lost - lastLost;
      const total = dRecv + dLost;
      const lossPct = total > 0 ? (dLost / total) * 100 : 0;
      const dSent = sent - lastSent;
      lastRecv = recv; lastLost = lost; lastSent = sent;

      const peerInfo = peerMap.get(pid);
      if (peerInfo) {
        peerInfo.lossPct = lossPct;
        peerInfo.recvRate = dRecv / (intervalMs / 1000);
      }

      // if packets are actually flowing, this peer is healthy
      // again. Reset the give-up state so we'll try rebuilds again later
      // if the connection drops a second time.
      if (dRecv > 0) {
        if (fullRebuildAttempts[pid]) fullRebuildAttempts[pid] = 0;
        if (peerGivenUp[pid]) {
          delete peerGivenUp[pid];
          log("recovered " + pid + " — rebuild counter reset");
        }
      }

      const peerMuted = peerInfo && peerInfo.muted;
      const mutedAgo = lastMutedAt[pid] ? Date.now() - lastMutedAt[pid] : Infinity;
      const inMuteGrace = mutedAgo < 4000;

      if (total > 10) {
        const alpha = 0.33;
        lossEwma[pid] = alpha * lossPct + (1 - alpha) * (lossEwma[pid] || 0);
      }
      if (peerInfo) peerInfo.lossEwma = lossEwma[pid];

      logTick++;
      const isAnomaly = lossPct > 5
        || (dRecv === 0 && !peerMuted && !inMuteGrace)
        || (dSent === 0 && !isMuted)
        || jitter > 0.1;
      const isHeartbeat = logTick <= 2 || logTick % 8 === 0;
      if (isAnomaly || isHeartbeat) {
        const tag = isAnomaly ? "STATS!" : "STATS ";
        log(tag + pid + " dR=" + dRecv + " dL=" + dLost + " (" + lossPct.toFixed(1) + "%) ewma=" + (lossEwma[pid] || 0).toFixed(1) + "% jit=" + jitter.toFixed(3) + " dS=" + dSent + (peerMuted ? " MUTED" : ""));
      }

      const relayJustConnected = relayConnectedAt[pid] && (Date.now() - relayConnectedAt[pid] < 6000);

      if (!peerMuted && !inMuteGrace && !relayJustConnected) {
        if ((lossEwma[pid] || 0) > 10) {
          if (!sustainedBadStart[pid]) sustainedBadStart[pid] = Date.now();
          const sustainedMs = Date.now() - sustainedBadStart[pid];

          if (!peerRelay[pid] && sustainedMs >= 8000) {
            requestRelaySwitch(pid, "sustained loss ewma=" + lossEwma[pid].toFixed(1) + "%");
            sustainedBadStart[pid] = null;
          }
          else if (peerRelay[pid] && (lossEwma[pid] || 0) > 15 && sustainedMs >= 16000) {
            await fullRebuild(pid, "relay also failing ewma=" + lossEwma[pid].toFixed(1) + "%");
            sustainedBadStart[pid] = null;
          }
        } else {
          sustainedBadStart[pid] = null;
        }
      }

      if (!peerMuted && !inMuteGrace && dRecv === 0) {
        consecutiveStalled++;
        if (consecutiveStalled >= 2 && !peerRelay[pid] && !relayJustConnected) {
          requestRelaySwitch(pid, "stalled (0 pkts)");
          consecutiveStalled = 0;
        } else if (consecutiveStalled >= 3 && peerRelay[pid] && !relayJustConnected) {
          await fullRebuild(pid, "stalled on relay");
          consecutiveStalled = 0;
        }
        // zombie-relay recovery. If the peer is on RELAY, has had
        // at least one full rebuild, has been at dR=0 for 5+ samples (≈20s),
        // and the rebuild cooldown is blocking us from trying again, then
        // the relay session is dead but the browser still thinks it's
        // alive (track LIVE but no packets). Force a clean teardown +
        // rebuild from scratch, ignoring the cooldown — and also nudge
        // the other side via `request_relay` so they tear down too.
        if (consecutiveStalled >= 5
            && peerRelay[pid]
            && (fullRebuildAttempts[pid] || 0) >= 1
            && !relayJustConnected) {
          log("=> ZOMBIE RELAY " + pid + " (dR=0 for ~20s after rebuild) -> force reset");
          // Clear the cooldown so fullRebuild() doesn't refuse.
          lastFullRebuildAt[pid] = 0;
          // Tear down our local PC so the rebuild starts clean.
          destroyPeer(pid);
          // Ask the other side to also tear down and accept a fresh offer
          // from us (or kick off their own offer if they're the larger ID).
          if (ws && ws.readyState === 1) {
            try {
              ws.send(JSON.stringify({ type: 'request_relay', to: pid,
                                       reason: 'zombie-relay-reset' }));
            } catch (e) {}
          }
          consecutiveStalled = 0;
          // If we're the larger ID, we drive the new offer ourselves.
          if (MY_ID > pid) {
            setTimeout(() => { try { createOffer(pid); } catch (e) {} }, 400);
          }
        }
      } else {
        consecutiveStalled = 0;
      }

      if (dSent === 0 && !isMuted) {
        outboundStall++;
        if (outboundStall >= 2 && !peerRelay[pid] && !relayJustConnected) {
          requestRelaySwitch(pid, "outbound stalled");
          outboundStall = 0;
        }
      } else {
        outboundStall = 0;
      }

      if (detectFrozenJitter(pid, jitter, dRecv)) {
        log("ZOMBIE jitter frozen on " + pid + " -> full renegotiate");
        if (MY_ID > pid) {
          destroyPeer(pid);
          setTimeout(() => createOffer(pid), 300);
        } else {
          destroyPeer(pid);
          renegInProgress[pid] = true;
          if (ws && ws.readyState === 1) {
            ws.send(JSON.stringify({ type: 'request_relay', to: pid, reason: 'zombie-reneg' }));
          }
          setTimeout(() => { renegInProgress[pid] = false; }, 3000);
        }
      }
    } catch (e) {}
  }, intervalMs);
}

async function fullRebuild(pid, reason) {
  // stop infinite rebuild loops on permanently-broken peers.
  // If a peer has no audio track at all (e.g. didn't grant mic permission),
  // every rebuild produces the exact same dead connection. After N tries
  // we give up to stop wasting TURN bandwidth.
  if (peerGivenUp[pid]) {
    return;
  }
  fullRebuildAttempts[pid] = (fullRebuildAttempts[pid] || 0) + 1;
  if (fullRebuildAttempts[pid] > MAX_FULL_REBUILDS_BEFORE_GIVEUP) {
    peerGivenUp[pid] = true;
    log("=> GIVING UP on " + pid + " after " + MAX_FULL_REBUILDS_BEFORE_GIVEUP +
        " rebuild attempts. They likely never granted mic permission. " +
        "Counter will reset if packets ever start arriving.");
    return;
  }

  if (lastFullRebuildAt[pid] && Date.now() - lastFullRebuildAt[pid] < 30000) {
    log("full rebuild cooldown " + pid);
    return;
  }
  if (renegInProgress[pid]) {
    log("full rebuild deferred (reneg in progress) " + pid);
    return;
  }
  lastFullRebuildAt[pid] = Date.now();
  log("=> FULL REBUILD " + pid + " (" + reason + ") [attempt " +
      fullRebuildAttempts[pid] + "/" + MAX_FULL_REBUILDS_BEFORE_GIVEUP + "]");

  try { await fetchIceServers(); } catch (e) {}

  if (MY_ID > pid) {
    destroyPeer(pid);
    setTimeout(() => createOffer(pid), 300);
  } else {
    destroyPeer(pid);
    renegInProgress[pid] = true;
    if (ws && ws.readyState === 1) {
      ws.send(JSON.stringify({ type: 'request_relay', to: pid, reason: 'full-rebuild:' + reason }));
    }
    setTimeout(() => { renegInProgress[pid] = false; }, 3000);
  }
}

async function requestRelaySwitch(pid, reason) {
  const p = peerMap.get(pid);
  if (!p) return;
  if (peerRelay[pid]) return;

  if (lastRelayAt[pid] && Date.now() - lastRelayAt[pid] < 10000) {
    log("relay cooldown " + pid);
    return;
  }
  if (renegInProgress[pid]) {
    log("relay deferred (reneg in progress) " + pid);
    return;
  }

  peerRelay[pid] = true;
  lastRelayAt[pid] = Date.now();
  log("-> AUTO-RELAY for " + pid + " (" + reason + ")");
  if (MY_ID > pid) {
    await switchPeerToRelay(pid);
    if (ws && ws.readyState === 1) {
      ws.send(JSON.stringify({ type: 'request_relay', to: pid, reason: 'mutual-relay:' + reason }));
    }
  } else {
    if (ws && ws.readyState === 1) {
      ws.send(JSON.stringify({ type: 'request_relay', to: pid, reason: reason }));
    }
  }
}

async function switchPeerToRelay(pid) {
  if (renegInProgress[pid]) {
    log("switchPeerToRelay: reneg in progress, deferring " + pid);
    return;
  }
  renegInProgress[pid] = true;

  if (MY_ID < pid) {
    log("relay: smaller side destroying PC for " + pid);
    destroyPeer(pid);
    renegInProgress[pid] = false;
    return;
  }

  const pc = peers[pid];
  if (!pc) {
    try { await createOffer(pid); } finally {}
    return;
  }
  try {
    pc.setConfiguration(getPCConfig(true));
    const offer = await pc.createOffer({ iceRestart: true });
    offer.sdp = preferOpusAndTune(offer.sdp);
    await pc.setLocalDescription(offer);
    ws.send(JSON.stringify({ type: 'webrtc_offer', to: pid, sdp: pc.localDescription.sdp }));
    log("relay-mode offer SENT " + pid);
    startConnectionTimer(pid);
  } catch (e) {
    log("setConfig fail " + pid + ": " + e.message + " -> full rebuild");
    destroyPeer(pid);
    await createOffer(pid);
  } finally {
    setTimeout(() => { renegInProgress[pid] = false; }, 3000);
  }
}

async function scheduleRetry(pid) {
  const p = peerMap.get(pid);
  if (!p) return;
  if (p._retrying) return;
  p._retrying = true;
  p.retries = (p.retries || 0) + 1;

  const maxRetries = peerMap.size >= 12 ? 5 : 4;
  if (p.retries > maxRetries) {
    log("GIVE UP on " + pid + " (after " + maxRetries + " retries)");
    p._retrying = false;
    destroyPeer(pid);
    return;
  }

  if (!peerRelay[pid]) {
    log("retry forcing RELAY for " + pid);
    peerRelay[pid] = true;
    lastRelayAt[pid] = Date.now();
    if (ws && ws.readyState === 1 && MY_ID > pid) {
      ws.send(JSON.stringify({ type: 'request_relay', to: pid, reason: 'retry-after-fail' }));
    }
  }

  const delay = Math.min(1000 * p.retries, 4000);
  log("retry #" + p.retries + " in " + delay + "ms -> " + pid + " (RELAY)");
  setTimeout(async () => {
    if (!peerMap.has(pid)) { p._retrying = false; return; }
    if (!ws || ws.readyState !== 1) { p._retrying = false; return; }

    if (MY_ID > pid) {
      await createOffer(pid);
    } else {
      destroyPeer(pid);
      log("smaller side: cleared stale PC for " + pid + ", waiting for offer");
    }
    p._retrying = false;
  }, delay);
}

let localAnalyser = null, localLevelTimer = null;
function setupLocalLevelMonitor() {
  if (!localStream) return;
  try {
    const ac = getSharedAC();
    const src = ac.createMediaStreamSource(localStream);
    localAnalyser = ac.createAnalyser();
    localAnalyser.fftSize = 256;
    src.connect(localAnalyser);
    const data = new Uint8Array(localAnalyser.frequencyBinCount);
    let lastSent = 0, lastLevel = 0;
    // ── Hysteresis state ──────────────────────────────────────────────
    // Two thresholds, two transitions:
    //   • START_THRESH = 0.05 — has to clear this to flip ON. High enough
    //     that desk fans / fridge hum don't trigger.
    //   • STOP_THRESH = 0.025 — must drop below this to even START
    //     considering "stopped." Lower than START so brief dips between
    //     words ("hello, my-name is...") don't break the speaking state.
    //   • RELEASE_MS = 250 — silence must persist this long below
    //     STOP_THRESH before we actually flip OFF.
    // This is the standard pattern used by Discord, Zoom, FaceTime —
    // makes the ring feel snappy on real stops but glued during natural
    // micro-pauses in speech.
    const START_THRESH = 0.05;
    const STOP_THRESH = 0.025;
    const RELEASE_MS = 250;
    let speakingState = false;        // the "official" on/off we broadcast
    let silenceStartedAt = 0;         // when level first dropped below STOP_THRESH
    const MIN_BROADCAST_INTERVAL = 500;
    localLevelTimer = setInterval(() => {
      if (isMuted || !localStream) return;
      localAnalyser.getByteFrequencyData(data);
      let sum = 0;
      for (let i = 0; i < data.length; i++) sum += data[i];
      const level = sum / data.length / 255;
      const now = Date.now();

      // ── Hysteresis state machine ────────────────────────────────────
      let newSpeaking = speakingState;
      if (!speakingState) {
        // Not currently speaking. Need to clear the HIGH threshold to start.
        if (level > START_THRESH) {
          newSpeaking = true;
          silenceStartedAt = 0;
        }
      } else {
        // Currently speaking. Stay on unless level drops below LOW threshold
        // and stays there for RELEASE_MS.
        if (level < STOP_THRESH) {
          if (silenceStartedAt === 0) silenceStartedAt = now;
          else if (now - silenceStartedAt >= RELEASE_MS) {
            newSpeaking = false;
            silenceStartedAt = 0;
          }
        } else {
          // Brief dip didn't reach silence threshold OR silence didn't
          // last long enough — reset the silence timer, keep glowing.
          silenceStartedAt = 0;
        }
      }

      const stateChanged = newSpeaking !== speakingState;
      const enoughTimePassed = now - lastSent >= MIN_BROADCAST_INTERVAL;
      if (stateChanged || (newSpeaking && enoughTimePassed)) {
        if (ws && ws.readyState === 1) {
          ws.send(JSON.stringify({ type: 'speaking', level: newSpeaking ? level : 0 }));
          lastSent = now;
        }
      }
      speakingState = newSpeaking;
      // Drive my own seat-tile green ring locally (no need to wait for
      // a server roundtrip — feels instant and avoids the 500ms throttle).
      window._selfSpeaking = speakingState;
      lastLevel = level;
    }, 100);
  } catch (e) { log("levelMon fail"); }
}

function cleanupRTC() {
  Object.keys({...peers, ...audios}).forEach(pid => nukePeer(pid));
  if (localStream) {
    localStream.getTracks().forEach(t => t.stop());
    localStream = null;
  }
  if (localLevelTimer) { clearInterval(localLevelTimer); localLevelTimer = null; }
  if (wakeLockCheckTimer) { clearInterval(wakeLockCheckTimer); wakeLockCheckTimer = null; }
  if (audioHealTimer) { clearInterval(audioHealTimer); audioHealTimer = null; }
  if (wakeLock) { try { wakeLock.release(); } catch (e) {} wakeLock = null; }
  // also tear down LiveKit so we don't leak the room connection.
  try { lkDisconnect(); } catch (e) {}
}

let _muteDebounceTimer = null;

// returns the SVG <svg>...</svg> string for the mic icon in
// either muted or live state. Used by every mute button on screen
// (main call, in-Uno-game, in-Zombie-game) so they all look identical.
function _micIconHtml(muted) {
  return muted
    ? '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="1" y1="1" x2="23" y2="23"/><path d="M9 9v6a3 3 0 0 0 5.12 2.12M15 9.34V4a3 3 0 0 0-5.94-.6"/><path d="M17 16.95A7 7 0 0 1 5 12v-2m14 0v2a7 7 0 0 1-.11 1.23"/><line x1="12" y1="19" x2="12" y2="23"/><line x1="8" y1="23" x2="16" y2="23"/></svg>'
    : '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 1a3 3 0 0 0-3 3v8a3 3 0 0 0 6 0V4a3 3 0 0 0-3-3z"/><path d="M19 10v2a7 7 0 0 1-14 0v-2"/><line x1="12" y1="19" x2="12" y2="23"/><line x1="8" y1="23" x2="16" y2="23"/></svg>';
}

// Walks every mute button on the page and aligns its appearance with
// the current isMuted state. Called after toggleMute() and whenever a
// new mute button enters the DOM (e.g. when an in-game overlay opens).
function syncAllMuteBtns() {
  const ids = ['muteBtn', 'unoMuteBtn', 'zombMuteBtn'];
  const html = _micIconHtml(isMuted);
  ids.forEach(id => {
    const b = document.getElementById(id);
    if (!b) return;
    b.classList.toggle('muted', isMuted);
    b.innerHTML = html;
  });
}

function toggleMute() {
  if (!localStream) return;

  const now = Date.now();
  if (now - lastMuteToggleAt < 300) {
    log("mute debounce — ignored");
    return;
  }
  lastMuteToggleAt = now;

  isMuted = !isMuted;
  const realTrack = localStream.getAudioTracks()[0];
  if (!realTrack) return;

  if (realTrack.readyState === 'live') realTrack.enabled = !isMuted;

  // keep LiveKit in sync so server-side speaker detection reflects
  // our state. The .enabled flag above does the actual silencing; this
  // just tells LK we want our publication marked muted/live.
  try { lkApplyMute(); } catch (e) {}

  // When muting, instantly zero out the speaking flag so the green
  // ring drops on the next animation frame without waiting for the
  // next mic-level tick.
  if (isMuted) {
    window._selfSpeaking = false;
  }

  // sync ALL mute buttons (main call + in-game) in one pass.
  syncAllMuteBtns();

  if (_muteDebounceTimer) clearTimeout(_muteDebounceTimer);
  _muteDebounceTimer = setTimeout(() => {
    if (ws && ws.readyState === 1) {
      ws.send(JSON.stringify({ type: isMuted ? 'mute_me' : 'unmute_me' }));
    }
  }, 150);

  updPeers();
}

let replyingTo = null;

// ════════════════════════════════════════════════════════════════════════════
// IMAGE SENDING — WhatsApp-style preview + View Once
// ════════════════════════════════════════════════════════════════════════════
// Pick image → show preview overlay with caption input → choose "Send"
// or "View Once" → only then transmit over WS. View Once images show
// an Instagram-style placeholder card; recipients tap to open once,
// then see an "Opened" ghost. The sender also sees "Opened" after
// the first person views it.

let _imgPreviewDataUrl = '';

function pickChatImage(e) {
  const f = e.target.files && e.target.files[0];
  e.target.value = '';
  if (!f) return;
  if (!f.type.startsWith('image/')) { log("not an image: " + f.type); return; }
  const r = new FileReader();
  r.onload = ev => {
    const img = new Image();
    img.onload = () => {
      let w = img.width, h = img.height;
      const MAX = 800;
      if (w > MAX || h > MAX) {
        if (w > h) { h = Math.round(h * (MAX / w)); w = MAX; }
        else       { w = Math.round(w * (MAX / h)); h = MAX; }
      }
      const c = document.createElement('canvas');
      c.width = w; c.height = h;
      c.getContext('2d').drawImage(img, 0, 0, w, h);
      const data = c.toDataURL('image/jpeg', 0.6);
      log("img " + Math.round(data.length / 1024) + "kb");
      if (data.length > 600000) {
        const data2 = c.toDataURL('image/jpeg', 0.4);
        if (data2.length > 600000) {
          showStickerToast('Image too large after compression', 'err', 4000);
          return;
        }
        showImagePreviewOverlay(data2);
      } else {
        showImagePreviewOverlay(data);
      }
    };
    img.onerror = () => log("img decode fail");
    img.src = ev.target.result;
  };
  r.readAsDataURL(f);
}

function showImagePreviewOverlay(dataUrl) {
  // Remove any existing overlay first (this clears _imgPreviewDataUrl)
  hideImagePreviewOverlay();
  // NOW set the data URL — after hide is done clearing
  _imgPreviewDataUrl = dataUrl;
  const overlay = document.createElement('div');
  overlay.id = 'imgSendOverlay';
  overlay.className = 'img-send-overlay';
  // two-row layout — caption on top, buttons below. Always fits on mobile.
  // Uses addEventListener (not inline onclick) for 100% cross-browser reliability.
  overlay.innerHTML =
    '<div class="img-send-preview">' +
      '<img src="' + esc(dataUrl) + '" alt="Preview">' +
    '</div>' +
    '<div class="img-send-bar">' +
      '<input type="text" class="img-send-caption" id="imgCaption" placeholder="Add a caption..." maxlength="200">' +
      '<div class="img-send-actions">' +
        '<button class="img-send-cancel" id="imgCancelBtn">Cancel</button>' +
        '<button class="img-send-btn vo-btn" id="imgVoBtn" title="View Once">' +
          '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="width:15px;height:15px;vertical-align:middle;pointer-events:none"><path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z"/><circle cx="12" cy="12" r="3"/></svg>' +
          '<span class="vo-text">Once</span>' +
        '</button>' +
        '<button class="img-send-btn" id="imgSendBtn">Send</button>' +
      '</div>' +
    '</div>';
  document.body.appendChild(overlay);

  // Wire up buttons with addEventListener (never inline onclick)
  const cancelBtn = document.getElementById('imgCancelBtn');
  const voBtn     = document.getElementById('imgVoBtn');
  const sendBtn   = document.getElementById('imgSendBtn');
  const capIn     = document.getElementById('imgCaption');

  if (cancelBtn) cancelBtn.addEventListener('click', hideImagePreviewOverlay);
  if (voBtn)     voBtn.addEventListener('click',     function() { sendImageFromPreview(true);  });
  if (sendBtn)   sendBtn.addEventListener('click',   function() { sendImageFromPreview(false); });

  // Enter key in caption sends normally (not view-once)
  if (capIn) {
    capIn.addEventListener('keydown', function(e) {
      if (e.key === 'Enter' && !e.shiftKey) {
        e.preventDefault();
        sendImageFromPreview(false);
      }
    });
    // Focus caption on desktop only — on mobile we don't auto-focus to avoid keyboard popping
    if (!('ontouchstart' in window)) {
      setTimeout(function() { capIn.focus(); }, 50);
    }
  }
}

function hideImagePreviewOverlay() {
  _imgPreviewDataUrl = '';
  const el = document.getElementById('imgSendOverlay');
  if (el) el.remove();
}

function sendImageFromPreview(viewOnce) {
  const captionIn = document.getElementById('imgCaption');
  const caption = captionIn ? captionIn.value.trim() : '';
  if (!_imgPreviewDataUrl) return;
  sendImageMsg(_imgPreviewDataUrl, caption, viewOnce);
  hideImagePreviewOverlay();
}

function sendImageMsg(dataUrl, caption, viewOnce) {
  if (!ws || ws.readyState !== 1) return;
  if (typingTimer) { clearTimeout(typingTimer); typingTimer = null; }
  ws.send(JSON.stringify({ type: 'typing_stop' }));
  const payload = { type: 'chat', text: caption || '', image: dataUrl };
  if (viewOnce) payload.view_once = true;
  if (replyingTo) {
    payload.reply_to = replyingTo;
    cancelReply();
  }
  ws.send(JSON.stringify(payload));
}

function startReply(m) {
  if (!m || m.kind === 'system') return;
  const txt = (m.text || '').trim();
  let snippet;
  if (txt) snippet = txt.slice(0, 80);
  else if (m.image || m.image_expired) snippet = 'Image';
  else if (m.sticker || m.sticker_expired) snippet = 'Sticker';
  else snippet = '';
  replyingTo = {
    id: m.id || '',
    name: m.name || '?',
    text: snippet,
    has_image: !!(m.image || m.image_expired),
    has_sticker: !!(m.sticker || m.sticker_expired)
  };
  showReplyBar();
  const inEl = document.getElementById('msgIn');
  if (inEl) inEl.focus();
}

function showReplyBar() {
  let el = document.getElementById('replyBar');
  if (!el) {
    el = document.createElement('div');
    el.id = 'replyBar';
    el.className = 'reply-bar';
    const inputBar = document.querySelector('.input-bar');
    if (inputBar) inputBar.parentNode.insertBefore(el, inputBar);
  }
  let previewText = replyingTo.text || '';
  if (!previewText) {
    if (replyingTo.has_sticker) previewText = 'Sticker';
    else if (replyingTo.has_image) previewText = 'Image';
  }
  el.innerHTML =
    '<div class="reply-bar-content">' +
      '<span class="reply-bar-label">Replying to ' + esc(replyingTo.name) + '</span>' +
      '<span class="reply-bar-text">' + esc(previewText) + '</span>' +
    '</div>' +
    '<button class="reply-bar-close" onclick="cancelReply()" aria-label="Cancel reply">&times;</button>';
}

function cancelReply() {
  replyingTo = null;
  const el = document.getElementById('replyBar');
  if (el) el.remove();
}

// ── image preview (normal + view-once tracking) ─────────────────────
// For normal images: simple tap-to-view full screen.
// For view-once images: first tap shows placeholder, second tap opens
// full preview. On close, the image is marked "opened" locally and the
// server is notified so the sender sees "Opened".
function openImagePreview(src, msgId, isViewOnce) {
  if (isViewOnce && msgId && !viewOnceOpened.has(msgId)) {
    // First time opening this view-once image
    viewOnceOpened.add(msgId);
    markViewOnceOpened(msgId);
    // Notify server so sender can see "Opened"
    if (ws && ws.readyState === 1) {
      ws.send(JSON.stringify({ type: 'msg_opened', msg_id: msgId }));
    }
  }
  const overlay = document.createElement('div');
  overlay.className = 'img-preview-overlay';
  overlay.innerHTML = '<span class="close-hint">&times;</span><img src="' + esc(src) + '">';
  overlay.onclick = function() { overlay.remove(); };
  document.body.appendChild(overlay);
}

// swap a view-once "Photo" pill for the "Opened" pill.
// Called locally when this user opens an image, or for the sender
// when they receive the msg_opened broadcast.
function markViewOnceOpened(msgId) {
  const c = document.getElementById('msgs');
  if (!c) return;
  const row = c.querySelector('[data-msg-id="' + esc(msgId) + '"]');
  if (!row) return;
  const card = row.querySelector('.viewonce-card');
  if (!card) return;  // already "Opened" or not a view-once message
  // Replace the clickable "Photo" pill with the faded "Opened" pill
  const openedHTML = '<div class="viewonce-opened">' +
    '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z"/><circle cx="12" cy="12" r="3"/></svg>' +
    'Opened</div>';
  card.outerHTML = openedHTML;
}

// ════════════════════════════════════════════════════════════════════════════
// MESSAGE REACTIONS — Instagram/WhatsApp/Telegram style
// ════════════════════════════════════════════════════════════════════════════
// Long-press on OTHER's message → reaction bar with ❤️ 🔥 😭 🦦 +
// Double-click/tap on OTHER's message → quick 🤍 heart reaction
// Reactions shown as compact badges bottom-right of message bubble.

const EMOJI_PICKER_LIST = [
  '🤍','👍','👎','❤️','🔥','😂','😭','😡','😍',
  '🤩','😮','😢','😅','😆','🤔','👏','🙏',
  '💯','🚀','💪','🎉','😊','😘','🥰','😋',
  '😜','😎','🤓','😏','😒','😔','😤','😠',
  '🤬','😱','😨','😰','😥','😪','😴','😷',
  '🥵','🥶','😵','🤯','🥳','🤠','💀','👻',
  '👽','🤖','💩','🦋','🌸','🌈','✨','⭐',
  '💫','💥','💎','🍀','🌺','🌻','🌹','🥀'
];
const QUICK_REACTIONS = ['🤍','❤️','🔥','😭','🦦'];

// track active reaction bar timer + listener to prevent leaks
var _reactBarTimer = null;
var _reactBarOutsideFn = null;

function _clearReactBarTimer() {
  if (_reactBarTimer) { clearTimeout(_reactBarTimer); _reactBarTimer = null; }
  if (_reactBarOutsideFn) {
    document.removeEventListener('click', _reactBarOutsideFn);
    _reactBarOutsideFn = null;
  }
}

// Show floating reaction bar above a message
// uses addEventListener (not onclick) for 100% mobile reliability
// position:fixed with viewport clamping — never clipped by screen edges
// The bar is placed above the message row, centered, but forced to stay within
// the viewport (with 8px padding on each side).
function showReactionBar(row, msgId) {
  // Kill any old timer/listener before creating new bar
  _clearReactBarTimer();
  // Remove any existing reaction bars from DOM
  document.querySelectorAll('.react-bar').forEach(function(b) { b.remove(); });
  const bar = document.createElement('div');
  bar.className = 'react-bar';
  // Build quick-reaction buttons with addEventListener
  QUICK_REACTIONS.forEach(function(emoji) {
    var btn = document.createElement('button');
    btn.textContent = emoji;
    btn.title = emoji;
    btn.addEventListener('click', function(ev) {
      ev.stopPropagation();
      sendReaction(msgId, emoji);
      hideReactionBar();
    });
    bar.appendChild(btn);
  });
  // More button (+)
  var moreBtn = document.createElement('button');
  moreBtn.className = 'react-more';
  moreBtn.title = 'More reactions';
  moreBtn.innerHTML = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><line x1="12" y1="5" x2="12" y2="19"/><line x1="5" y1="12" x2="19" y2="12"/></svg>';
  moreBtn.addEventListener('click', function(ev) {
    ev.stopPropagation();
    showEmojiPicker(msgId);
    hideReactionBar();
  });
  bar.appendChild(moreBtn);
  document.body.appendChild(bar);  // append to body for fixed positioning
  // Compute position: center above the message row, clamped to viewport
  var rowRect = row.getBoundingClientRect();
  var barRect = bar.getBoundingClientRect();
  var pad = 8;
  var top = rowRect.top - barRect.height - 6;
  if (top < pad) top = rowRect.bottom + 6;  // if no room above, show below
  var left = rowRect.left + rowRect.width / 2 - barRect.width / 2;
  left = Math.max(pad, Math.min(left, window.innerWidth - barRect.width - pad));
  bar.style.position = 'fixed';
  bar.style.top = top + 'px';
  bar.style.left = left + 'px';
  bar.style.bottom = 'auto';
  bar.style.zIndex = '500';
  // Auto-hide after 4 seconds — track globally so next bar clears it
  _reactBarTimer = setTimeout(function() {
    _reactBarTimer = null;
    _reactBarOutsideFn = null;
    hideReactionBar();
  }, 4000);
  // Hide on outside click — track globally so next bar removes it
  _reactBarOutsideFn = function(e) {
    if (!bar.contains(e.target)) {
      _clearReactBarTimer();
      hideReactionBar();
    }
  };
  setTimeout(function() {
    if (_reactBarOutsideFn) document.addEventListener('click', _reactBarOutsideFn);
  }, 50);
}
function hideReactionBar() {
  document.querySelectorAll('.react-bar').forEach(function(b) { b.remove(); });
}

// Show full emoji picker overlay
// all buttons use addEventListener, no inline onclick
function showEmojiPicker(msgId) {
  hideEmojiPicker();
  const overlay = document.createElement('div');
  overlay.id = 'emojiPickerOverlay';
  overlay.className = 'emoji-picker-overlay';
  overlay.innerHTML =
    '<div class="emoji-picker-panel">' +
      '<div class="emoji-picker-header">' +
        '<span>React</span>' +
        '<button id="emojiPickerClose">&times;</button>' +
      '</div>' +
      '<div class="emoji-picker-grid" id="emojiPickerGrid"></div>' +
    '</div>';
  document.body.appendChild(overlay);
  // Wire up close button
  var closeBtn = document.getElementById('emojiPickerClose');
  if (closeBtn) closeBtn.addEventListener('click', hideEmojiPicker);
  // Build emoji grid
  const grid = document.getElementById('emojiPickerGrid');
  EMOJI_PICKER_LIST.forEach(function(emoji) {
    var btn = document.createElement('button');
    btn.textContent = emoji;
    btn.addEventListener('click', function() {
      sendReaction(msgId, emoji);
      hideEmojiPicker();
    });
    grid.appendChild(btn);
  });
  // Close on overlay background click
  overlay.addEventListener('click', function(e) { if (e.target === overlay) hideEmojiPicker(); });
}
function hideEmojiPicker() {
  const el = document.getElementById('emojiPickerOverlay');
  if (el) el.remove();
}

// Send reaction to server
function sendReaction(msgId, emoji) {
  if (!msgId || !emoji || !ws || ws.readyState !== 1) return;
  ws.send(JSON.stringify({ type: 'react', msg_id: msgId, emoji: emoji }));
}

// Handle incoming reaction broadcast
// strict null-check before touching DOM
function handleReaction(m) {
  if (m && m.msg_id && m.reactions && typeof m.reactions === 'object') {
    updateMessageReactions(m.msg_id, m.reactions);
  }
}

// Update reaction badges on an existing message
function updateMessageReactions(msgId, reactions) {
  const c = document.getElementById('msgs');
  if (!c) return;
  const row = c.querySelector('[data-msg-id="' + esc(msgId) + '"]');
  if (!row) return;
  let rRow = row.querySelector('.reactions-row');
  if (!rRow) {
    rRow = document.createElement('div');
    rRow.className = 'reactions-row';
    const msgContent = row.querySelector('.msg-content');
    if (msgContent) msgContent.appendChild(rRow);
    else row.appendChild(rRow);
  }
  renderReactions(rRow, reactions, MY_ID || '');
}

// Render reaction badges into a container
function renderReactions(container, reactions, myPeerId) {
  container.innerHTML = '';
  if (!reactions || typeof reactions !== 'object') return;
  // Group by emoji: { emoji: count, ... }
  const counts = {};
  const mine = {};
  for (var pid in reactions) {
    var emoji = reactions[pid];
    counts[emoji] = (counts[emoji] || 0) + 1;
    if (pid === myPeerId) mine[emoji] = true;
  }
  // Sort by count desc
  var entries = [];
  for (var e in counts) entries.push([e, counts[e]]);
  entries.sort(function(a, b) { return b[1] - a[1]; });
  entries.forEach(function(entry) {
    var emoji = entry[0], count = entry[1];
    var badge = document.createElement('div');
    badge.className = 'reaction-badge' + (mine[emoji] ? ' mine' : '');
    badge.innerHTML = emoji + '<span class="react-count">' + count + '</span>';
    badge.addEventListener('click', function(ev) {
      ev.stopPropagation();
      var msgRow = badge.closest('.msg-row');
      var mid = msgRow ? msgRow.getAttribute('data-msg-id') : '';
      if (mid) sendReaction(mid, emoji);
    });
    container.appendChild(badge);
  });
}

// ════════════════════════════════════════════════════════════════════════════
// MESSAGE INSERTION — REVERSE FLEX PRIMITIVE
// ════════════════════════════════════════════════════════════════════════════
// In a column-reverse list, the visual bottom is DOM index 0. To make a
// new message appear at the visual bottom, we insertBefore(node, firstChild)
// instead of appendChild. This single helper centralizes that rule so any
// future code that adds a message gets it right by default.
// ════════════════════════════════════════════════════════════════════════════
function appendToVisualBottom(container, node) {
  if (!container) return;
  // insertBefore(node, null) === appendChild, so this is safe even when
  // the container is empty.
  container.insertBefore(node, container.firstChild);
}

// ── handle incoming message deletion broadcast ──────────────────────
function handleMsgDeleted(m) {
  const c = document.getElementById('msgs');
  if (!c || !m.msg_id) return;
  const row = c.querySelector('[data-msg-id="' + esc(m.msg_id) + '"]');
  if (!row) return;
  const wasAtBottom = isAtBottom();
  const isSelf = row.classList.contains('self');
  const pi = peerMap.get(m.peer_id) || {};
  const name = m.name || pi.name || '?';
  const avSrc = m.avatar || pi.avatar || '';
  const showBadge = !!(m.is_admin || pi.is_admin);
  let avHTML;
  if (avSrc) avHTML = '<div class="avatar"><img src="' + esc(avSrc) + '"></div>';
  else avHTML = '<div class="avatar"><span>' + esc(name[0].toUpperCase()) + '</span></div>';
  const header = '<div class="msg-header"><span class="msg-name">' + esc(name) + '</span>' + (showBadge ? '<span class="msg-badge host">Host</span>' : '') + '</div>';
  row.innerHTML = avHTML + '<div class="msg-content">' + header + '<div class="msg-deleted">This message was deleted</div></div>';
  row.style.pointerEvents = 'none';
  if (wasAtBottom) scrollToLatest(false);
}

// ── swipe-to-reply + long-press-delete gestures ─────────────────────
// Swipe OTHER people's messages RIGHT to reply.
// Swipe YOUR own messages LEFT to reply.
// Long-press YOUR own message to reveal a red trash bin.
function attachMessageGestures(row, m) {
  if (m.deleted || m.kind === 'system' || !m.id) return;
  const content = row.querySelector('.msg-content');
  if (!content) return;
  let startX = 0, startY = 0, startTime = 0;
  let isDragging = false;
  let longPressTimer = null;
  let didSwipe = false;
  let didLongPress = false;
  let deleteBtn = null;
  const isOwn = !!m.self;
  const SWIPE_THRESHOLD = 55;
  const LONG_PRESS_MS = 500;

  function removeDeleteBtn() {
    if (deleteBtn) { deleteBtn.remove(); deleteBtn = null; }
  }
  function showDeleteBtn() {
    removeDeleteBtn();
    document.querySelectorAll('.msg-delete-btn').forEach(function(b) { b.remove(); });
    deleteBtn = document.createElement('button');
    deleteBtn.className = 'msg-delete-btn';
    deleteBtn.innerHTML = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="3 6 5 6 21 6"/><path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"/><line x1="10" y1="11" x2="10" y2="17"/><line x1="14" y1="11" x2="14" y2="17"/></svg>';
    deleteBtn.title = 'Delete message';
    deleteBtn.addEventListener('click', function(e) {
      e.stopPropagation();
      if (ws && ws.readyState === 1) {
        ws.send(JSON.stringify({ type: 'delete_msg', msg_id: m.id }));
      }
      removeDeleteBtn();
    });
    row.appendChild(deleteBtn);
    setTimeout(removeDeleteBtn, 3000);
  }
  document.addEventListener('click', function onDocClick(e) {
    if (deleteBtn && !row.contains(e.target)) removeDeleteBtn();
  });

  // branch long-press — own message → delete bin, other's → reaction bar
  function onStart(x, y) {
    startX = x; startY = y; startTime = Date.now();
    isDragging = false; didSwipe = false; didLongPress = false;
    longPressTimer = setTimeout(function() {
      if (!isDragging && isOwn) { didLongPress = true; showDeleteBtn(); }
      else if (!isDragging && !isOwn) { didLongPress = true; showReactionBar(row, m.id); }
    }, LONG_PRESS_MS);
  }
  function onMove(x, y) {
    if (!startTime) return;
    var dx = x - startX, dy = y - startY;
    if (Math.abs(dy) > Math.abs(dx) && Math.abs(dy) > 10) {
      if (longPressTimer) { clearTimeout(longPressTimer); longPressTimer = null; }
      startTime = 0; return;
    }
    if (!isDragging && (Math.abs(dx) > 8 || Math.abs(dy) > 8)) {
      isDragging = true;
      if (longPressTimer) { clearTimeout(longPressTimer); longPressTimer = null; }
    }
    if (!isDragging) return;
    var validDir = isOwn ? (dx < 0) : (dx > 0);
    if (!validDir) return;
    var cap = isOwn ? Math.max(dx, -110) : Math.min(dx, 110);
    content.style.transition = 'none';
    content.style.transform = 'translateX(' + (cap * 0.35) + 'px)';
    didSwipe = true;
  }
  function onEnd(x, y) {
    if (longPressTimer) { clearTimeout(longPressTimer); longPressTimer = null; }
    content.style.transition = 'transform .3s cubic-bezier(.2,.7,.2,1)';
    content.style.transform = '';
    if (didSwipe && startTime) {
      var dx = x - startX;
      var triggered = isOwn ? (dx < -SWIPE_THRESHOLD) : (dx > SWIPE_THRESHOLD);
      if (triggered) startReply(m);
    }
    startTime = 0; isDragging = false;
  }

  row.addEventListener('touchstart', function(e) {
    onStart(e.touches[0].clientX, e.touches[0].clientY);
  }, { passive: true });
  row.addEventListener('touchmove', function(e) {
    onMove(e.touches[0].clientX, e.touches[0].clientY);
  }, { passive: true });
  row.addEventListener('touchend', function(e) {
    onEnd(e.changedTouches[0].clientX, e.changedTouches[0].clientY);
  }, { passive: true });
  row.addEventListener('mousedown', function(e) {
    if (e.button !== 0) return;
    onStart(e.clientX, e.clientY);
  });
  row.addEventListener('mousemove', function(e) {
    if (e.buttons & 1) onMove(e.clientX, e.clientY);
  });
  row.addEventListener('mouseup', function(e) {
    if (e.button !== 0) return;
    onEnd(e.clientX, e.clientY);
  });

  // click no longer triggers reply — swipe only. Image preview still works.
  // tap on someone else's text message opens the Copy/Reply panel.
  row.addEventListener('click', function(ev) {
    if (didSwipe) { didSwipe = false; return; }
    if (didLongPress) { didLongPress = false; return; }
    if (deleteBtn) { removeDeleteBtn(); return; }
    // dismiss reaction bar on click elsewhere
    if (document.querySelector('.react-bar')) { hideReactionBar(); return; }
    // dismiss any open click-panel on a fresh tap before deciding
    // what to do — prevents overlap from rapid successive taps.
    hideMsgClickPanel();
    var t = ev.target;
    // Normal image preview still works on tap
    if (t.classList && t.classList.contains('chat-img')) {
      ev.stopPropagation(); openImagePreview(t.src); return;
    }
    // View-once placeholder tap is handled by the placeholder's own click
    if (t.closest && t.closest('.viewonce-card')) return;
    // Reaction badge clicks are handled by their own listener
    if (t.closest && t.closest('.reaction-badge')) return;
    // Avatar clicks do nothing
    if (t.tagName === 'IMG' && t.closest('.avatar')) return;
    // show Copy/Reply panel for OTHER people's text messages.
    // Skipped for images, stickers, view-once, and own messages.
    if (!isOwn && m.text && !m.sticker && !m.image && !m.view_once) {
      showMsgClickPanel(row, m);
    }
  });
}

function renderMsg(m) {
  const c = document.getElementById('msgs'); if (!c) return;
  const wasAtBottom = isAtBottom();

  if (m.kind === 'system') {
    const d = document.createElement('div');
    d.className = 'msg-system';
    d.textContent = m.text;
    appendToVisualBottom(c, d);
  } else {
    const isSelf = !!m.self;
    const pi = peerMap.get(m.peer_id) || {};
    const name = m.name || pi.name || '?';
    const showBadge = !!(m.is_admin || pi.is_admin);
    const avSrc = m.avatar || hostAssignedAvatars[m.peer_id] || pi.avatar || '';

    const hasSticker = !!(m.sticker || m.sticker_expired);
    const hasImage = !!(m.image || m.image_expired);
    const hasText = !!(m.text && m.text.length > 0);
    const stickerOnly = hasSticker && !hasText && !hasImage;
    const isDeleted = !!m.deleted;

    const row = document.createElement('div');
    row.className = 'msg-row ' + (isSelf ? 'self' : 'other');
    if (stickerOnly && !isDeleted) row.classList.add('has-sticker-only');
    if (m.id) row.setAttribute('data-msg-id', m.id);

    let avHTML;
    if (avSrc) avHTML = '<div class="avatar"><img src="' + esc(avSrc) + '"></div>';
    else avHTML = '<div class="avatar"><span>' + esc(name[0].toUpperCase()) + '</span></div>';
    const header = '<div class="msg-header"><span class="msg-name">' + esc(name) + '</span>' + (showBadge ? '<span class="msg-badge host">Host</span>' : '') + '</div>';

    // deleted messages show a placeholder with avatar + name
    if (isDeleted) {
      row.innerHTML = avHTML + '<div class="msg-content">' + header + '<div class="msg-deleted">This message was deleted</div></div>';
      row.style.pointerEvents = 'none';
      appendToVisualBottom(c, row);
      if (m.self || wasAtBottom) scrollToLatest(false);
      else { unreadCount++; updateJumpButton(); }
      return;
    }

    let replyHTML = '';
    if (m.reply_to) {
      const r = m.reply_to;
      let previewText = r.text || '';
      if (!previewText) {
        if (r.has_sticker) previewText = 'Sticker';
        else if (r.has_image) previewText = 'Image';
      }
      replyHTML = '<div class="msg-reply">' +
                    '<span class="msg-reply-name">' + esc(r.name || '?') + '</span>' +
                    '<span class="msg-reply-text">' + esc(previewText) + '</span>' +
                  '</div>';
    }

    let contentHTML;
    if (stickerOnly) {
      let stickerHTML;
      if (m.sticker_expired) {
        stickerHTML = '<div class="sticker-expired"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="3" width="18" height="18" rx="2" ry="2"/><circle cx="8.5" cy="8.5" r="1.5"/><polyline points="21 15 16 10 5 21"/></svg><span>Sticker no longer available</span></div>';
      } else {
        stickerHTML = '<img class="sticker-img" src="/stickers/' + esc(m.sticker) + '" alt="sticker" draggable="false">';
      }
      contentHTML = header + replyHTML + stickerHTML;
    } else {
      // view-once images — compact pill, per-user "Opened" state.
      // Each user sees "Opened" only after THEY personally open it.
      // The sender sees "Opened" when anyone opens it (tracked server-side).
      const isViewOnce = !!m.view_once;
      const iOpenedIt = isViewOnce && viewOnceOpened.has(m.id);
      const senderSeesOpened = isViewOnce && !!m.self && Array.isArray(m.opened_by) && m.opened_by.length > 0;
      const showAsOpened = iOpenedIt || senderSeesOpened;

      if (isViewOnce && !showAsOpened) {
        // Unopened: compact blue pill with circle-"1" icon + "Photo"
        const voIcon = '<span class="vo-icon-wrap">' +
                         '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="9"/></svg>' +
                         '<span class="vo-num">1</span>' +
                       '</span>';
        imgHTML = '<div class="viewonce-card" data-vo-src="' + esc(m.image) + '" data-vo-id="' + esc(m.id) + '">' +
                    voIcon + 'Photo' +
                  '</div>';
        let textHTML = '';
        if (m.text) textHTML = '<div class="msg-text">' + esc(m.text) + '</div>';
        const bubbleClass = 'msg-bubble has-img' + (m.text ? ' has-text' : '');
        contentHTML = header + '<div class="' + bubbleClass + '">' + replyHTML + imgHTML + textHTML + '</div>';
      } else if (isViewOnce && showAsOpened) {
        // Opened by this user: faded "Opened" pill (non-interactive)
        imgHTML = '<div class="viewonce-opened">' +
                    '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z"/><circle cx="12" cy="12" r="3"/></svg>' +
                    'Opened</div>';
        let textHTML = '';
        if (m.text) textHTML = '<div class="msg-text">' + esc(m.text) + '</div>';
        const bubbleClass = 'msg-bubble' + (m.text ? ' has-text' : '');
        contentHTML = header + '<div class="' + bubbleClass + '">' + replyHTML + imgHTML + textHTML + '</div>';
      } else {
        // Normal image (non-view-once)
        let imgHTML = '';
        if (m.image) {
          imgHTML = '<img class="chat-img" src="' + esc(m.image) + '" alt="image">';
        } else if (m.image_expired) {
          imgHTML = '<div class="img-expired"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="3" width="18" height="18" rx="2" ry="2"/><circle cx="8.5" cy="8.5" r="1.5"/><polyline points="21 15 16 10 5 21"/></svg><span>Image no longer available</span></div>';
        }
        let textHTML = '';
        if (m.text) textHTML = '<div class="msg-text">' + esc(m.text) + '</div>';
        let bubbleClass = 'msg-bubble';
        if (m.image) bubbleClass += ' has-img';
        if (m.image && m.text) bubbleClass += ' has-text';
        const bubbleInner = replyHTML + imgHTML + (textHTML || (m.image ? '' : esc(m.text)));
        contentHTML = header + '<div class="' + bubbleClass + '">' + bubbleInner + '</div>';
      }
    }

    row.innerHTML = avHTML + '<div class="msg-content">' + contentHTML + '</div>';

    // wire up view-once card click handler
    if (m.view_once && m.image) {
      const voCard = row.querySelector('.viewonce-card');
      if (voCard) {
        voCard.addEventListener('click', function(ev) {
          ev.stopPropagation();
          const src = voCard.getAttribute('data-vo-src');
          const mid = voCard.getAttribute('data-vo-id');
          if (src && mid) openImagePreview(src, mid, true);
        });
      }
    }

    // render reactions row (for history + new messages)
    if (m.id && m.reactions && Object.keys(m.reactions).length > 0) {
      const msgContent = row.querySelector('.msg-content');
      if (msgContent) {
        const rRow = document.createElement('div');
        rRow.className = 'reactions-row';
        renderReactions(rRow, m.reactions, MY_ID || '');
        msgContent.appendChild(rRow);
      }
    }

    // attach swipe-to-reply and long-press-delete gestures
    // long-press on other's messages now shows reaction bar
    attachMessageGestures(row, m);

    appendToVisualBottom(c, row);
  }

  if (m.self || wasAtBottom) {
    scrollToLatest(false);
  } else {
    if (m.kind !== 'system') {
      unreadCount++;
      updateJumpButton();
    }
  }
}

function renderSys(t) {
  const c = document.getElementById('msgs'); if (!c) return;
  const wasAtBottom = isAtBottom();
  const d = document.createElement('div');
  d.className = 'msg-system';
  d.textContent = t;
  appendToVisualBottom(c, d);
  if (wasAtBottom) scrollToLatest(false);
}

function esc(t) {
  const d = document.createElement('div');
  d.textContent = t || '';
  return d.innerHTML;
}

function renderTyping() {
  const el = document.getElementById('typingBar');
  if (!el) return;
  const names = Array.from(typingUsers.values());
  if (names.length === 0) {
    el.classList.add('hidden');
    el.textContent = '';
    return;
  }
  el.classList.remove('hidden');
  if (names.length === 1) {
    el.textContent = names[0] + ' is typing...';
  } else if (names.length === 2) {
    el.textContent = names[0] + ' and ' + names[1] + ' are typing...';
  } else {
    el.textContent = names.length + ' people are typing...';
  }
}

function sendMsg() {
  const inEl = document.getElementById('msgIn');
  const text = inEl.value.trim();
  if (!text || !ws || ws.readyState !== 1) return;
  if (typingTimer) { clearTimeout(typingTimer); typingTimer = null; }
  ws.send(JSON.stringify({ type: 'typing_stop' }));
  const payload = { type: 'chat', text: text };
  if (replyingTo) {
    payload.reply_to = replyingTo;
    cancelReply();
  }
  ws.send(JSON.stringify(payload));
  inEl.value = '';
  autoResizeInput();
  updateStickerIconVisibility();
  // keep the mobile keyboard up after sending. Without this, the
  // keyboard collapses on every send because some mobile browsers blur the
  // textarea when the value is reset programmatically.
  inEl.focus();
}

function leaveCall() {
  log("leave");
  leaving = true;
  // belt-and-suspenders — fire the beacon AND close the WS. The
  // beacon is the reliable path (works during unload); the leave message
  // over WS is the fast path (broadcasts peer_left immediately if WS still
  // alive). ws.close at the end ensures we tear down cleanly even if
  // the server processed our leave message.
  if (ws && ws.readyState === 1) {
    try { ws.send(JSON.stringify({ type: 'leave' })); } catch (e) {}
  }
  sendLeaveBeacon();
  if (ws && ws.readyState === 1) ws.close();
  cleanupRTC();
  try { window.close(); } catch (e) {}
  document.body.innerHTML = '<div style="display:flex;align-items:center;justify-content:center;height:100vh;background:#000;color:#fff;font-family:sans-serif"><h2>Left the call</h2></div>';
}

// navigator.sendBeacon is the ONLY reliable way to send a payload
// during unload (beforeunload / pagehide). Plain fetch and ws.send get
// cancelled by the browser. The server has a /beacon_leave endpoint that
// receives this and immediately removes us from the room, so other peers
// see the leave instantly instead of waiting for TCP timeout (which on
// mobile can stretch 30-60 seconds and is the root cause of zombie tiles).
let _beaconSent = false;
function sendLeaveBeacon() {
  if (_beaconSent) return;  // only once per session
  if (!MY_ID) return;       // never joined, nothing to leave
  _beaconSent = true;
  const payload = JSON.stringify({ room_id: ROOM, peer_id: MY_ID, token: TOKEN });
  try {
    if (navigator.sendBeacon) {
      // Browsers want a Blob with JSON type for proper Content-Type
      const blob = new Blob([payload], { type: 'application/json' });
      const ok = navigator.sendBeacon('/beacon_leave', blob);
      log('leave beacon: ' + (ok ? 'queued' : 'rejected'));
    } else {
      // Older browsers — best-effort synchronous XHR
      const xhr = new XMLHttpRequest();
      xhr.open('POST', '/beacon_leave', false);  // synchronous
      xhr.setRequestHeader('Content-Type', 'application/json');
      xhr.send(payload);
    }
  } catch (e) {
    log('leave beacon failed: ' + (e && e.message ? e.message : e));
  }
}

// fire the beacon on every plausible unload signal. Different
// browsers/platforms favor different ones — pagehide is most reliable on
// iOS Safari, beforeunload on desktop Chrome, visibilitychange (hidden)
// catches Android Chrome backgrounding the tab. Firing on all three is
// redundant but the beacon dedup flag prevents double-sends.
window.addEventListener('beforeunload', function() {
  leaving = true;
  // Try fast path first (WS leave) — broadcasts peer_left without waiting
  // for TCP close. Beacon is the reliable backup if the WS send is
  // cancelled by the browser during unload.
  if (ws && ws.readyState === 1) {
    try { ws.send(JSON.stringify({ type: 'leave' })); } catch (e) {}
  }
  sendLeaveBeacon();
  cleanupRTC();
});
window.addEventListener('pagehide', function() {
  // pagehide fires when the user navigates away on iOS, where beforeunload
  // doesn't fire reliably. Don't set leaving=true here — the page might be
  // restored from bfcache (pageshow.persisted = true) and we want to be
  // able to reconnect cleanly if that happens.
  sendLeaveBeacon();
});
// visibilitychange catches tab-switch / app-background on mobile, but we
// only treat HIDDEN as a leave signal when combined with a real close. We
// can't tell the difference between "user backgrounded the app for 2
// seconds" and "user is leaving forever", so we DON'T fire the beacon
// here — that would log false leaves every time someone checks another
// app. The other handlers cover the actual leave cases.

// ═══════════════════════════════════════════════════════════════════════
// GAMES + UNO CLIENT MODULE — self-contained
// ═══════════════════════════════════════════════════════════════════════
// All Uno state lives in the unoCli namespace to avoid colliding with the
// rest of the client (which has many globals). The module:
//   • Renders the games picker and the Uno overlay
//   • Holds the local mirror of server state (received as uno_state)
//   • Holds your private hand (received as uno_hand)
//   • Renders cards in CSS (no images)
//   • Posts uno_* actions back over the existing ws
//   • Mirrors call chat into a side panel so people can talk while playing
//
// Voice continues uninterrupted while the overlay is open — WebRTC <audio>
// elements live outside this overlay.

const unoCli = {
  open: false,                // is the Uno overlay open?
  state: null,                // latest server state object
  hand: [],                   // your private hand
  selectedPlayerCount: 2,     // lobby pick
  awaitingColorForCard: null, // when wild needs color, the card id
  chatPanelOpen: false,
  chatBadge: 0,
  chatMirrorBuf: [],          // small ring of recent chat msgs (for re-open)
};

function escHtml(s) {
  return String(s == null ? '' : s)
    .replace(/&/g, '&amp;').replace(/</g, '&lt;')
    .replace(/>/g, '&gt;').replace(/"/g, '&quot;').replace(/'/g, '&#39;');
}

// ═══════════════════════════════════════════════════════════════════════
// HEADER DROPDOWN + STREAMING CLIENT
// ═══════════════════════════════════════════════════════════════════════
// The header 3-dot button now opens a small dropdown menu instead of
// toggling the debug pane directly. The menu has:
//   - Logs (toggle the existing debug pane)
//   - Start Streaming (open the upload sheet; disabled if a stream is
//     already running)
//
// Streaming model:
//   1. Streamer opens the upload sheet, picks 1+ video files.
//   2. Each file uploads to /stream-upload, returns {id, url, title}.
//   3. Streamer hits "Start streaming" → ws.send({type:'stream_start',
//      playlist:[{id,title}, ...]}).
//   4. Server validates + broadcasts {type:'stream_state', state}.
//   5. Every client (including streamer) reacts to stream_state by
//      showing the stream-panel, loading the current url, syncing
//      currentTime / playing.
//   6. Only the streamer's UI shows controls (.is-streamer class on
//      body). Viewers see read-only progress + title.
//   7. Streamer's video element emits timeupdate every few seconds —
//      we send {type:'stream_control', action:'tick', time} so viewers
//      stay in sync. Server stores the time; new joiners get it.

// ── header dropdown ──
function toggleHeaderMenu(ev) {
  if (ev) ev.stopPropagation();
  // if the debug pane is currently visible, a single tap on the
  // 3-dot button closes it directly (one-tap dismiss). Otherwise, the
  // tap opens the dropdown menu as usual. This matches user
  // expectation: the same button that opened the logs should also
  // close them.
  const dbg = document.getElementById('dbg');
  if (dbg && dbg.classList.contains('show')) {
    dbg.classList.remove('show');
    // Also collapse the menu if it happened to be open.
    document.getElementById('hdrMenu').classList.remove('show');
    return;
  }
  const m = document.getElementById('hdrMenu');
  const showing = m.classList.toggle('show');
  if (showing) {
    // Click-outside-to-close
    setTimeout(() => {
      const handler = (e) => {
        if (!m.contains(e.target)) {
          m.classList.remove('show');
          document.removeEventListener('click', handler, true);
        }
      };
      document.addEventListener('click', handler, true);
    }, 0);
  }
}
function onHdrLogs() {
  document.getElementById('hdrMenu').classList.remove('show');
  document.getElementById('dbg').classList.toggle('show');
}
function onHdrStartStream() {
  document.getElementById('hdrMenu').classList.remove('show');
  if (streamCli.state) {
    // Someone is already streaming — refuse politely.
    if (streamCli.state.streamer_pid === MY_ID) {
      // It's me; jump to the player (no upload sheet — that's only for
      // starting a new stream).
      return;
    }
    alert(streamCli.state.streamer_name + ' is already streaming. Wait for them to stop.');
    return;
  }
  openStreamSheet();
}

// ── upload sheet state ──
const streamCli = {
  // null when no stream is active. Otherwise the latest server state.
  state: null,
  // Local queue while the streamer is BUILDING a playlist before tapping
  // "Start streaming". Each item: {tmpId, name, size, file, blobUrl,
  // status:'pending'|'uploading'|'done'|'error', progress:0-1,
  // serverId, error}
  queue: [],
  // per-server-id mapping back to the local blob URL. The
  // STREAMER's <video> element prefers the blob URL so playback starts
  // immediately without waiting for any network. Viewers use the server
  // URL like before. Cleared on stop / cleanup so we don't leak blobs.
  localBlobUrls: {},  // serverId -> blob:URL
  // Are we the streamer? Mirrors body.is-streamer.
  isStreamer: false,
  // default to UNmuted. We try playing with sound first; the
  // syncVideoToState catch handler flips this to true and shows the
  // tap-to-unmute prompt only if the browser blocks autoplay.
  videoMuted: false,
  // Clock skew for sync (sample once from first stream_state)
  serverDelta: null,
  // Throttle for the streamer's "tick" heartbeat
  lastTickAt: 0,
  // Throttle to avoid micro-seek thrash on viewers
  lastSeekedTo: -1,
};

function openStreamSheet() {
  streamCli.queue = [];
  renderStreamQueue();
  document.getElementById('streamSheetError').classList.remove('show');
  document.getElementById('streamSheetOvl').classList.add('show');
}
function closeStreamSheet() {
  document.getElementById('streamSheetOvl').classList.remove('show');
  // If user closed without starting, abandon the queue (uploads stay
  // on server briefly; will be cleaned up when room ends).
  streamCli.queue = [];
}

function _streamShowSheetError(text) {
  const el = document.getElementById('streamSheetError');
  el.textContent = text;
  el.classList.add('show');
  setTimeout(() => el.classList.remove('show'), 4500);
}

// User picked one or more files from the file input.
function onStreamFilesPicked(ev) {
  const files = Array.from(ev.target.files || []);
  if (!files.length) return;
  // We allow up to 8 items total in the queue.
  const MAX_FILES = 8;
  for (const f of files) {
    if (streamCli.queue.length >= MAX_FILES) {
      _streamShowSheetError(`Max ${MAX_FILES} videos per stream`);
      break;
    }
    if (!f.type.startsWith('video/') &&
        !/\.(mp4|webm|mov|m4v|ogv|mkv)$/i.test(f.name)) {
      _streamShowSheetError(`${f.name}: not a video file`);
      continue;
    }
    if (f.size > 250 * 1024 * 1024) {
      _streamShowSheetError(`${f.name}: over 250 MB`);
      continue;
    }
    const item = {
      tmpId: 'q' + Date.now() + Math.random().toString(36).slice(2, 6),
      name: f.name,
      size: f.size,
      file: f,
      // blob: URL for instant local playback by the streamer.
      // No network involved — the browser plays directly from the file
      // they just picked. Viewers still need the upload to finish.
      blobUrl: URL.createObjectURL(f),
      status: 'pending',
      progress: 0,
      serverId: null,
      title: f.name.replace(/\.[^.]+$/, '').slice(0, 80) || 'Video',
    };
    streamCli.queue.push(item);
    uploadStreamItem(item);
  }
  renderStreamQueue();
  // Reset the input so the same file can be picked again later.
  ev.target.value = '';
}

function uploadStreamItem(item) {
  item.status = 'uploading';
  item.progress = 0;
  renderStreamQueue();
  const xhr = new XMLHttpRequest();
  const url = '/stream-upload?room_id=' + encodeURIComponent(ROOM)
              + '&t=' + encodeURIComponent(TOKEN);
  xhr.open('POST', url);
  xhr.upload.onprogress = (e) => {
    if (e.lengthComputable) {
      item.progress = e.loaded / e.total;
      renderStreamQueue();
    }
  };
  xhr.onload = () => {
    try {
      const j = JSON.parse(xhr.responseText || '{}');
      if (xhr.status >= 200 && xhr.status < 300 && j.id) {
        item.status = 'done';
        item.progress = 1;
        item.serverId = j.id;
        item.title = j.title || item.title;
        // remember the blob URL so the streamer's local <video>
        // can prefer it over the server URL. Viewers don't have access
        // to this mapping — they only see what the server broadcasts.
        if (item.blobUrl) {
          streamCli.localBlobUrls[j.id] = item.blobUrl;
        }
        // If we already started streaming and were waiting for THIS
        // file's upload, the server-broadcast state now has its URL, but
        // we may have been showing an "uploading..." placeholder. The
        // next stream_state from the server (or a refresh) will pick it
        // up automatically.
      } else {
        item.status = 'error';
        item.error = j.error || `HTTP ${xhr.status}`;
        console.warn('[stream-upload] failed:', xhr.status, xhr.responseText);
      }
    } catch (e) {
      item.status = 'error';
      item.error = 'Bad response from server';
      console.warn('[stream-upload] parse error:', e, xhr.responseText);
    }
    renderStreamQueue();
  };
  xhr.onerror = () => {
    item.status = 'error';
    item.error = 'Network error (check Render is up)';
    console.warn('[stream-upload] network error');
    renderStreamQueue();
  };
  // Some browsers fire ontimeout instead. 5 minute cap for large videos.
  xhr.timeout = 5 * 60 * 1000;
  xhr.ontimeout = () => {
    item.status = 'error';
    item.error = 'Upload timed out';
    renderStreamQueue();
  };
  const form = new FormData();
  form.append('file', item.file, item.name);
  xhr.send(form);
}

function removeStreamItem(tmpId) {
  // revoke the blob URL so the browser can reclaim memory.
  // Also clear it from localBlobUrls if it was already uploaded.
  const it = streamCli.queue.find(x => x.tmpId === tmpId);
  if (it && it.blobUrl) {
    try { URL.revokeObjectURL(it.blobUrl); } catch (e) {}
    if (it.serverId) delete streamCli.localBlobUrls[it.serverId];
  }
  streamCli.queue = streamCli.queue.filter(it => it.tmpId !== tmpId);
  renderStreamQueue();
}

function renderStreamQueue() {
  const list = document.getElementById('streamQueue');
  if (!streamCli.queue.length) {
    list.innerHTML = '<div class="stream-queue-empty">Add videos below to start streaming</div>';
  } else {
    list.innerHTML = '';
    streamCli.queue.forEach((item, idx) => {
      const div = document.createElement('div');
      div.className = 'stream-queue-item';
      if (item.status === 'uploading') div.classList.add('uploading');
      if (item.status === 'error') div.classList.add('error');
      let metaText = '';
      const sizeMb = (item.size / (1024 * 1024)).toFixed(1) + ' MB';
      if (item.status === 'pending')   metaText = sizeMb + ' · queued';
      else if (item.status === 'uploading') metaText = sizeMb + ' · uploading ' + Math.round(item.progress * 100) + '%';
      else if (item.status === 'done') metaText = sizeMb + ' · ready';
      else if (item.status === 'error') metaText = sizeMb + ' · ' + (item.error || 'error');
      const showBar = item.status === 'uploading';
      div.innerHTML =
        `<div class="stream-queue-item-idx">${idx + 1}</div>
         <div class="stream-queue-item-info">
           <div class="stream-queue-item-name">${escHtml(item.title)}</div>
           <div class="stream-queue-item-meta">${escHtml(metaText)}</div>
           ${showBar ? `<div class="stream-queue-item-progress"><div class="stream-queue-item-progress-fill" style="width:${(item.progress * 100).toFixed(0)}%"></div></div>` : ''}
         </div>
         <button class="stream-queue-item-remove" aria-label="Remove" data-tmpid="${item.tmpId}">
           <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg>
         </button>`;
      list.appendChild(div);
    });
    list.querySelectorAll('.stream-queue-item-remove').forEach(btn => {
      btn.onclick = () => removeStreamItem(btn.getAttribute('data-tmpid'));
    });
  }
  // Enable Start only if there's at least one done item and no uploads pending.
  const ready = streamCli.queue.some(it => it.status === 'done');
  const inFlight = streamCli.queue.some(it => it.status === 'uploading' || it.status === 'pending');
  const btn = document.getElementById('streamSheetStart');
  btn.disabled = !ready || inFlight;
  btn.style.opacity = btn.disabled ? '0.5' : '1';
  btn.textContent = inFlight ? 'Uploading...' : 'Start streaming';
}

function onStreamSheetStart() {
  if (!ws || ws.readyState !== 1) return;
  const items = streamCli.queue.filter(it => it.status === 'done' && it.serverId);
  if (!items.length) {
    _streamShowSheetError('Add at least one video');
    return;
  }
  ws.send(JSON.stringify({
    type: 'stream_start',
    playlist: items.map(it => ({ id: it.serverId, title: it.title })),
  }));
  // The server will send stream_state back; the sheet closes on success.
  closeStreamSheet();
}

// ── stream_state handler — wires the video player ──
function streamHandleState(state) {
  if (!state) {
    // Stream ended. Tear down player + reclaim local blob memory.
    streamCli.state = null;
    streamCli.isStreamer = false;
    streamCli.serverDelta = null;
    streamCli.lastSeekedTo = -1;
    // revoke any blob URLs we held so the browser frees the
    // file bytes. Without this, large videos can sit in memory until
    // the tab is closed.
    Object.values(streamCli.localBlobUrls).forEach(u => {
      try { URL.revokeObjectURL(u); } catch (e) {}
    });
    streamCli.localBlobUrls = {};
    streamCli.queue.forEach(it => {
      if (it.blobUrl) {
        try { URL.revokeObjectURL(it.blobUrl); } catch (e) {}
      }
    });
    streamCli.queue = [];
    document.body.classList.remove('streaming', 'is-streamer');
    // restore the seat panel to its normal expanded state.
    if (typeof setSeatCollapsed === 'function') {
      setSeatCollapsed(false, true);
    }
    const v = document.getElementById('streamVideo');
    try { v.pause(); } catch (e) {}
    try { v.removeAttribute('src'); v.load(); } catch (e) {}
    document.getElementById('streamUnmutePrompt').classList.remove('show');
    return;
  }
  const prevState = streamCli.state;
  streamCli.state = state;
  // Sample clock skew once
  if (streamCli.serverDelta == null && state.server_now) {
    streamCli.serverDelta = state.server_now - Date.now() / 1000;
  }
  streamCli.isStreamer = state.streamer_pid === MY_ID;
  const wasStreaming = document.body.classList.contains('streaming');
  document.body.classList.add('streaming');
  document.body.classList.toggle('is-streamer', streamCli.isStreamer);

  // when a stream first starts, auto-collapse the seat panel so
  // the video gets the most vertical room possible. The user can still
  // pull it back down manually via the pull-tab if they want to see
  // everyone's avatars. We only auto-collapse on the FIRST state
  // arrival (wasStreaming was false) — subsequent state updates don't
  // re-collapse so we respect a manual expansion.
  if (!wasStreaming && typeof setSeatCollapsed === 'function') {
    setSeatCollapsed(true, true);
  }

  // Title + viewer info
  const cur = state.playlist[state.idx];
  if (cur) {
    let t = cur.title || 'Video';
    if (state.playlist.length > 1) t += `  (${state.idx + 1}/${state.playlist.length})`;
    document.getElementById('streamTitle').textContent = t;
  }
  document.getElementById('streamerName').textContent = state.streamer_name || 'Someone';

  // Prev/next button enable
  document.getElementById('streamBtnPrev').disabled = state.idx <= 0;
  document.getElementById('streamBtnNext').disabled = state.idx + 1 >= state.playlist.length;

  // swap the play-button icon between ▶ and ⏸ based on state.
  // Previously the icon stayed as ▶ forever — the action worked but
  // visually it looked broken.
  const playBtn = document.getElementById('streamBtnPlay');
  if (playBtn) {
    playBtn.innerHTML = state.playing
      // Pause icon — two vertical bars
      ? '<svg viewBox="0 0 24 24" fill="currentColor"><rect x="6" y="4" width="4" height="16"/><rect x="14" y="4" width="4" height="16"/></svg>'
      // Play icon — triangle
      : '<svg viewBox="0 0 24 24" fill="currentColor"><polygon points="5 3 19 12 5 21 5 3"/></svg>';
    playBtn.setAttribute('aria-label', state.playing ? 'Pause' : 'Play');
  }

  // Sync the <video> element.
  syncVideoToState(state, prevState);
  // Tap shield is only "active" for viewers (they tap to acknowledge);
  // streamer's shield is non-blocking so their native controls work.
  document.getElementById('streamTapShield').classList.toggle('streamer-mode',
                                                                streamCli.isStreamer);
  // only show controls on specific transitions, not every state
  // change. The streamer's "tick" heartbeat fires every ~4.5s and was
  // making the bar flash in and out constantly. Now:
  //   - First time we see this stream (no prevState) → show briefly so
  //     users notice the controls exist
  //   - Transition from playing → paused → keep controls visible (the
  //     pause indicator should be obvious; streamHideControls also
  //     refuses to hide while paused)
  //   - Transition from paused → playing → kick off the auto-hide so
  //     the bar fades out shortly after a manual resume
  // Otherwise: don't touch the bar. The user controls visibility by
  // tapping the video.
  const wasPlaying = prevState ? prevState.playing : null;
  if (!prevState) {
    streamShowControls(true);
  } else if (wasPlaying && !state.playing) {
    // Paused — show & keep visible (streamHideControls refuses to hide
    // while paused, so no autohide timer needed).
    streamShowControls(false);
  } else if (!wasPlaying && state.playing) {
    // Resumed — show briefly, then auto-hide.
    streamShowControls(true);
  }
}

function syncVideoToState(state, prevState) {
  const v = document.getElementById('streamVideo');
  if (!v) return;
  const cur = state.playlist[state.idx];
  if (!cur) return;

  // streamer plays from their local blob URL whenever possible —
  // instant start, no network. Viewers always use the server URL.
  let newSrc = cur.url;
  if (streamCli.isStreamer && streamCli.localBlobUrls[cur.id]) {
    newSrc = streamCli.localBlobUrls[cur.id];
  }
  const idxChanged = !prevState || prevState.idx !== state.idx;
  // Note: currentSrc on blob URLs is the resolved blob URL; we compare
  // the full string match. For server URLs we tolerate the cache-bust
  // query that might be appended.
  const srcChanged = !v.currentSrc
    || (!v.currentSrc.endsWith(newSrc) && v.currentSrc !== newSrc);
  // try with sound on first. Users have almost always interacted
  // with the page by the time someone starts a stream (joined the call,
  // sent a chat, tapped buttons) so the browser will let us autoplay with
  // sound. Only fall back to muted + prompt if play() actually rejects.
  if (idxChanged || srcChanged) {
    streamCli.lastSeekedTo = -1;
    try {
      v.src = newSrc;
      v.muted = streamCli.videoMuted;
      v.load();
    } catch (e) { return; }
  }

  // Reconcile playing vs paused.
  if (state.playing) {
    // First, try the user's actual mute preference (default false).
    v.muted = streamCli.videoMuted;
    v.play().then(() => {
      // Success — hide the prompt if shown.
      document.getElementById('streamUnmutePrompt').classList.remove('show');
    }).catch(() => {
      // Browser blocked autoplay-with-sound. Switch to muted + prompt.
      // This is the standard Chrome/Safari fallback path.
      streamCli.videoMuted = true;
      v.muted = true;
      v.play().catch(() => {});
      document.getElementById('streamUnmutePrompt').classList.add('show');
    });
  } else {
    try { v.pause(); } catch (e) {}
    document.getElementById('streamUnmutePrompt').classList.remove('show');
  }

  // Compute the "expected time" given server timing + playback state.
  // Only seek if drift > 1.0 seconds — avoids jitter from frequent ticks.
  const expectedTime = computeExpectedTime(state);
  if (Number.isFinite(expectedTime)
      && Math.abs(v.currentTime - expectedTime) > 1.0
      && Math.abs(streamCli.lastSeekedTo - expectedTime) > 0.3) {
    try {
      v.currentTime = expectedTime;
      streamCli.lastSeekedTo = expectedTime;
    } catch (e) {}
  }
}

function computeExpectedTime(state) {
  if (!state) return NaN;
  if (!state.playing) return state.time;
  // Live position = stored time + (now - last_update_at) in server clock.
  const localNow = Date.now() / 1000;
  const serverNow = localNow + (streamCli.serverDelta || 0);
  return state.time + Math.max(0, serverNow - state.last_update_at);
}

// User taps the video → unmute it.
function streamUserUnmute() {
  streamCli.videoMuted = false;
  const v = document.getElementById('streamVideo');
  v.muted = false;
  // Some browsers need a fresh play() after the unmute.
  v.play().catch(() => {});
  document.getElementById('streamUnmutePrompt').classList.remove('show');
}

// Streamer-only control buttons
function streamCtl(action) {
  if (!ws || ws.readyState !== 1) return;
  if (!streamCli.isStreamer) return;
  const v = document.getElementById('streamVideo');
  if (action === 'toggle') {
    action = (streamCli.state && streamCli.state.playing) ? 'pause' : 'play';
    if (action === 'play') {
      // If we're at the end of the current video, restart it.
      if (v && v.duration && v.currentTime >= v.duration - 0.5) {
        try { v.currentTime = 0; } catch (e) {}
        ws.send(JSON.stringify({type:'stream_control', action:'seek', time: 0}));
      }
    }
  }
  if (action === 'seek') {
    // Caller passes the seek time as a second arg via the array form,
    // but we don't use that here — see streamSeekTo for the slider.
    return;
  }
  ws.send(JSON.stringify({type:'stream_control', action: action}));
}

function streamSeekTo(t) {
  if (!ws || ws.readyState !== 1) return;
  if (!streamCli.isStreamer) return;
  ws.send(JSON.stringify({type:'stream_control', action:'seek', time: t}));
}

function streamStop() {
  if (!ws || ws.readyState !== 1) return;
  if (!streamCli.isStreamer) return;
  ws.send(JSON.stringify({type:'stream_stop'}));
}

// Streamer's <video> events → outbound ticks/transitions.
function streamWireVideoElement() {
  const v = document.getElementById('streamVideo');
  if (!v || v._streamWired) return;
  v._streamWired = true;

  // Throttled time-update heartbeat (only from streamer)
  v.addEventListener('timeupdate', () => {
    if (!streamCli.isStreamer) return;
    if (!streamCli.state) return;
    const now = Date.now();
    if (now - streamCli.lastTickAt < 4500) return;
    streamCli.lastTickAt = now;
    try {
      ws.send(JSON.stringify({type:'stream_control', action:'tick',
                               time: v.currentTime}));
    } catch (e) {}
    // Also locally update progress UI
    streamRenderProgressFromVideo();
  });
  v.addEventListener('ended', () => {
    if (!streamCli.isStreamer) return;
    try {
      ws.send(JSON.stringify({type:'stream_control', action:'ended'}));
    } catch (e) {}
  });
  // Local progress UI for viewers too (driven by video element)
  v.addEventListener('timeupdate', streamRenderProgressFromVideo);
  v.addEventListener('loadedmetadata', streamRenderProgressFromVideo);
}

function streamRenderProgressFromVideo() {
  const v = document.getElementById('streamVideo');
  const fill = document.getElementById('streamProgressFill');
  const handle = document.getElementById('streamProgressHandle');
  const cur = document.getElementById('streamCurTime');
  const dur = document.getElementById('streamDur');
  if (!v) return;
  const t = v.currentTime || 0;
  const d = v.duration || 0;
  const pct = d > 0 ? (t / d) * 100 : 0;
  fill.style.width = pct + '%';
  handle.style.left = pct + '%';
  cur.textContent = formatTime(t);
  dur.textContent = formatTime(d);
}

function formatTime(s) {
  if (!Number.isFinite(s) || s < 0) return '0:00';
  const m = Math.floor(s / 60);
  const sec = Math.floor(s % 60);
  return m + ':' + (sec < 10 ? '0' : '') + sec;
}

// Make the progress bar draggable (streamer only).
function streamWireProgressDrag() {
  const bar = document.getElementById('streamProgress');
  if (!bar || bar._wired) return;
  bar._wired = true;
  let dragging = false;

  function posToTime(clientX) {
    const v = document.getElementById('streamVideo');
    const rect = bar.getBoundingClientRect();
    const frac = Math.max(0, Math.min(1, (clientX - rect.left) / rect.width));
    return frac * (v.duration || 0);
  }

  bar.addEventListener('pointerdown', (e) => {
    if (!streamCli.isStreamer) return;
    dragging = true;
    bar.setPointerCapture(e.pointerId);
    const t = posToTime(e.clientX);
    document.getElementById('streamVideo').currentTime = t;
    streamSeekTo(t);
  });
  bar.addEventListener('pointermove', (e) => {
    if (!dragging) return;
    const t = posToTime(e.clientX);
    document.getElementById('streamVideo').currentTime = t;
  });
  bar.addEventListener('pointerup', (e) => {
    if (!dragging) return;
    dragging = false;
    try { bar.releasePointerCapture(e.pointerId); } catch (err) {}
    const t = posToTime(e.clientX);
    streamSeekTo(t);
  });
}

// Wire on first load
streamWireProgressDrag();
streamWireVideoElement();

// ── auto-hiding controls overlay ──
// The controls fade out 2.5s after the last user interaction with the
// video frame. Tap the video → controls fade back in + timer restarts.
// While paused, controls stay visible (so the user knows it's paused).
// Hover/tap inside the controls themselves also keeps them visible.
let streamHideTimer = null;
const STREAM_HIDE_MS = 2500;

function streamShowControls(autohide) {
  const c = document.getElementById('streamControls');
  if (!c) return;
  c.classList.remove('hidden');
  if (streamHideTimer) { clearTimeout(streamHideTimer); streamHideTimer = null; }
  if (autohide !== false) streamScheduleHide();
}

function streamHideControls() {
  const c = document.getElementById('streamControls');
  if (!c) return;
  // Never hide while paused — user should see that it's paused.
  if (streamCli.state && !streamCli.state.playing) return;
  // Never hide while the unmute prompt is showing — that needs visibility.
  if (document.getElementById('streamUnmutePrompt').classList.contains('show')) return;
  c.classList.add('hidden');
}

function streamScheduleHide() {
  if (streamHideTimer) clearTimeout(streamHideTimer);
  streamHideTimer = setTimeout(streamHideControls, STREAM_HIDE_MS);
}

// Any tap on the video frame (whether the shield, the video itself, or
// the controls region) re-shows the controls and restarts the timer.
(function wireStreamControlsAutohide() {
  const wrap = document.getElementById('streamVideoWrap');
  if (!wrap) return;
  wrap.addEventListener('click', (e) => {
    // If the click was on a control button, the button's own handler
    // runs — we still reset the timer so it doesn't fade mid-interaction.
    streamShowControls(true);
    // Tap on bare video (or shield) for viewers: also unmute if muted.
    const onShield = e.target.id === 'streamTapShield'
                     || e.target.id === 'streamVideo';
    if (onShield && streamCli.videoMuted) {
      streamUserUnmute();
    }
  });
  // Hovering the controls keeps them visible — but on mobile this
  // doesn't really fire, hence the tap handler above.
  const c = document.getElementById('streamControls');
  c.addEventListener('pointerenter', () => {
    if (streamHideTimer) { clearTimeout(streamHideTimer); streamHideTimer = null; }
  });
  c.addEventListener('pointerleave', () => streamScheduleHide());
})();

function closeGamesPicker() {
  document.getElementById('gamesPickerOvl').classList.remove('show');
}
// restored function (was eaten by an earlier refactor). Opens
// the games picker bottom sheet from the gamepad icon in the header.
function openGamesPicker() {
  document.getElementById('gamesPickerOvl').classList.add('show');
}
function onPickGameUno() {
  closeGamesPicker();
  openUno();
}

// ── Uno overlay open/close ──
function openUno() {
  unoCli.open = true;
  const ovl = document.getElementById('unoOvl');
  ovl.classList.add('show');
  // every player gets the same fixed background image from
  // /uno-bg/bg1.jpg in the repo. If the file isn't present, the layer
  // is empty and the dark default shows through.
  applyFixedGameBg('uno');
  // Mirror the call's mute state into the in-game mute button.
  syncAllMuteBtns();
  if (!unoCli.state) {
    unoShowLobbyCreate();
  } else {
    unoRender();
  }
  unoRenderChat();
}
function onUnoBack() {
  // Just hide the overlay. We don't auto-leave the game — the server keeps
  // your seat until you explicitly leave or quit the call. This lets you
  // peek at the room chat or check audio without losing your turn.
  document.getElementById('unoOvl').classList.remove('show');
  unoCli.open = false;
}

// ── Lobby create view ──
function unoShowLobbyCreate() {
  document.getElementById('unoLobby').style.display = '';
  document.getElementById('unoPlay').style.display = 'none';
  document.getElementById('unoCreateBox').style.display = '';
  document.getElementById('unoWaitingBox').style.display = 'none';
  document.getElementById('unoSubtitle').textContent = 'Lobby';
  const actions = document.getElementById('unoLobbyActions');
  actions.innerHTML =
    '<button class="uno-btn-primary" id="unoCreateBtn" onclick="onUnoCreate()">Create Game</button>';
}
function onPickPlayerCount(n) {
  unoCli.selectedPlayerCount = n;
  document.querySelectorAll('#unoPlayerCountRow .uno-player-count-btn').forEach(b => {
    b.classList.toggle('sel', Number(b.getAttribute('data-n')) === n);
  });
}
function onUnoCreate() {
  if (!ws || ws.readyState !== 1) return;
  ws.send(JSON.stringify({ type: 'uno_create',
                           players: unoCli.selectedPlayerCount }));
}
function onUnoJoin() {
  if (!ws || ws.readyState !== 1) return;
  ws.send(JSON.stringify({ type: 'uno_join' }));
}
function onUnoStart() {
  if (!ws || ws.readyState !== 1) return;
  ws.send(JSON.stringify({ type: 'uno_start' }));
}
function onUnoLeave() {
  if (!ws || ws.readyState !== 1) return;
  ws.send(JSON.stringify({ type: 'uno_leave' }));
}
function onUnoDraw() {
  if (!ws || ws.readyState !== 1) return;
  if (!unoIsMyTurn()) { unoToast("Not your turn"); return; }
  ws.send(JSON.stringify({ type: 'uno_draw' }));
}
function onUnoPass() {
  if (!ws || ws.readyState !== 1) return;
  ws.send(JSON.stringify({ type: 'uno_pass' }));
}
function onCallUno() {
  if (!ws || ws.readyState !== 1) return;
  ws.send(JSON.stringify({ type: 'uno_call_uno' }));
  const btn = document.getElementById('unoCallBtn');
  btn.classList.add('called');
  btn.textContent = '✓';
}

// ── Card play ──
function onUnoCardClick(cardId) {
  if (!unoCli.state || unoCli.state.phase !== 'playing') return;
  if (!unoIsMyTurn()) { unoToast("Not your turn"); return; }
  const card = unoCli.hand.find(c => c.id === cardId);
  if (!card) return;
  if (!unoCardIsPlayable(card)) {
    unoToast("Can't play that card");
    return;
  }
  if (card.color === 'w') {
    // Open color picker
    unoCli.awaitingColorForCard = cardId;
    document.getElementById('unoColorPick').classList.add('show');
    return;
  }
  ws.send(JSON.stringify({ type: 'uno_play', card_id: cardId }));
}
function onPickColor(c) {
  document.getElementById('unoColorPick').classList.remove('show');
  if (!unoCli.awaitingColorForCard) return;
  ws.send(JSON.stringify({ type: 'uno_play',
                           card_id: unoCli.awaitingColorForCard,
                           chosen_color: c }));
  unoCli.awaitingColorForCard = null;
}

// ── Server → client message dispatcher ──
function unoHandleServerMsg(m) {
  switch (m.type) {
    case 'uno_state':
      // If the new state has no winner (e.g. fresh lobby after Play Again),
      // any leftover winner overlay must be dismissed so the friend who
      // didn't tap "Play Again" themselves still sees the new lobby
      // instead of being stuck behind the trophy modal.
      if (m.state && !m.state.winner) {
        const w = document.getElementById('unoWinner');
        if (w && w.classList.contains('show')) {
          w.classList.remove('show');
          // Also reset clock-skew sample since this is effectively a new
          // game from this client's perspective.
          unoCli._serverDelta = null;
        }
      }
      unoCli.state = m.state || null;
      unoRender();
      // Make sure the timer animation is alive whenever we're playing.
      // unoRenderPlay() also kicks it, but in case the lobby view rendered
      // we still want the loop ticking so it activates the moment play
      // starts.
      if (unoCli.state && unoCli.state.phase === 'playing') {
        unoStartTimerLoop();
      }
      break;
    case 'uno_hand':
      unoCli.hand = Array.isArray(m.hand) ? m.hand : [];
      unoRenderHand();
      unoRenderCallBtn();
      break;
    case 'uno_event':
      unoOnEvent(m);
      break;
    case 'uno_error':
      unoToast(m.text || 'Error');
      break;
    case 'uno_closed':
      unoCli.state = null;
      unoCli.hand = [];
      unoCli._serverDelta = null;  // fresh clock-skew sample next game
      // If the winner overlay is up (player hadn't dismissed it yet), hide
      // it so the lobby/create screen is visible.
      try { document.getElementById('unoWinner').classList.remove('show'); } catch(e){}
      unoToast(m.text || 'Game closed');
      if (unoCli.open) unoShowLobbyCreate();
      break;
  }
}

function unoOnEvent(m) {
  const t = m.text || '';
  if (m.kind === 'win') {
    // Use the server's authoritative state (which includes winner pid) to
    // build the overlay text — safer than parsing the event text.
    const s = unoCli.state || {};
    const winnerPid = s.winner || m.peer_id;
    const isMe = winnerPid === MY_ID;
    let wname = '—';
    if (s.players) {
      const wp = s.players.find(p => p.pid === winnerPid);
      if (wp) wname = wp.name || '—';
    }
    document.getElementById('unoWinnerTitle').textContent =
      isMe ? 'You Won!' : 'Winner!';
    document.getElementById('unoWinnerName').textContent = wname;
    document.getElementById('unoWinner').classList.add('show');
  } else if (t) {
    unoToast(t);
  }
}

function onWinnerClose() {
  document.getElementById('unoWinner').classList.remove('show');
  // Tell the server to drop the finished game. Once it's gone, anyone can
  // open a fresh lobby normally.
  if (ws && ws.readyState === 1) {
    try { ws.send(JSON.stringify({ type: 'uno_close' })); } catch (e) {}
  }
  // Reset local state immediately so the lobby creation screen shows
  // without waiting for the server roundtrip.
  unoCli.state = null;
  unoCli.hand = [];
  unoShowLobbyCreate();
}

function onPlayAgain() {
  // Hide winner overlay, request server to spin up a fresh lobby with the
  // same max-player count. The server auto-joins the requester. Other
  // players will see the new lobby state via uno_state broadcast and can
  // tap "Join Game" to rejoin.
  document.getElementById('unoWinner').classList.remove('show');
  // Reset clock skew estimate — a fresh game means a fresh sample is fine.
  unoCli._serverDelta = null;
  if (ws && ws.readyState === 1) {
    try { ws.send(JSON.stringify({ type: 'uno_play_again' })); } catch (e) {}
  }
  // Don't reset local state — wait for the uno_state broadcast that will
  // arrive with the new lobby. This keeps the transition smooth.
}

// ── Toast ──
let unoToastTimer = null;
function unoToast(text) {
  const el = document.getElementById('unoToast');
  if (!el) return;
  el.textContent = text;
  el.classList.add('show');
  if (unoToastTimer) clearTimeout(unoToastTimer);
  unoToastTimer = setTimeout(() => el.classList.remove('show'), 2200);
}

// ── Render: top-level ──
function unoRender() {
  if (!unoCli.state) {
    unoShowLobbyCreate();
    return;
  }
  const s = unoCli.state;
  if (s.phase === 'lobby') {
    unoRenderLobbyWaiting();
  } else if (s.phase === 'playing') {
    unoRenderPlay();
  } else if (s.phase === 'finished') {
    // Final state already triggered the winner overlay; stay on play screen
    // until user dismisses winner.
    unoRenderPlay();
  }
}

// ── Lobby (after create or after others created) ──
function unoRenderLobbyWaiting() {
  const s = unoCli.state;
  document.getElementById('unoLobby').style.display = '';
  document.getElementById('unoPlay').style.display = 'none';
  document.getElementById('unoCreateBox').style.display = 'none';
  document.getElementById('unoWaitingBox').style.display = '';
  document.getElementById('unoSubtitle').textContent =
    `Lobby · ${s.players.length}/${s.max_players}`;

  const wh = document.getElementById('unoWaitingHeader');
  wh.textContent = `Waiting for players (${s.players.length}/${s.max_players})`;

  const list = document.getElementById('unoLobbyPlayersList');
  list.innerHTML = '';
  s.players.forEach(p => {
    const row = document.createElement('div');
    row.className = 'uno-lobby-player';
    const isHost = p.pid === s.host;
    const initial = escHtml((p.name || '?').slice(0, 1).toUpperCase());
    const avInner = p.avatar
      ? `<img src="${escHtml(p.avatar)}" style="width:100%;height:100%;border-radius:50%;object-fit:cover" onerror="this.style.display='none';this.parentNode.textContent='${initial}'">`
      : initial;
    row.innerHTML =
      `<div class="uno-lobby-player-av">${avInner}</div>
       <div class="uno-lobby-player-name">${escHtml(p.name || '?')}${p.pid === MY_ID ? ' <span style="opacity:0.5">(you)</span>' : ''}</div>
       ${isHost ? '<div class="uno-lobby-host-badge">HOST</div>' : ''}`;
    list.appendChild(row);
  });
  for (let i = s.players.length; i < s.max_players; i++) {
    const slot = document.createElement('div');
    slot.className = 'uno-lobby-empty-slot';
    slot.innerHTML = `<div class="uno-lobby-player-av" style="background:rgba(255,255,255,0.04)">+</div><div>Waiting for player ${i + 1}...</div>`;
    list.appendChild(slot);
  }

  // Actions: show "Join" if not in players, "Start" if host and >=2, otherwise nothing.
  const inGame = s.players.some(p => p.pid === MY_ID);
  const isHost = s.host === MY_ID;
  const actions = document.getElementById('unoLobbyActions');
  let html = '';
  if (!inGame) {
    html += `<button class="uno-btn-primary" onclick="onUnoJoin()">Join Game</button>`;
  } else {
    if (isHost && s.players.length >= 2) {
      html += `<button class="uno-btn-primary" onclick="onUnoStart()">Start Now</button>`;
    }
    html += `<button class="uno-btn-secondary" onclick="onUnoLeave()">Leave</button>`;
  }
  actions.innerHTML = html;
}

// ── Play view ──
function unoRenderPlay() {
  document.getElementById('unoLobby').style.display = 'none';
  document.getElementById('unoPlay').style.display = '';

  const s = unoCli.state;
  document.getElementById('unoSubtitle').textContent = `Game · ${s.players.length} players`;

  // Opponents — positioned around the table edges (top, sides) per
  // player count. The list of opponents is rotated so the player to my
  // LEFT (next in clockwise order) is on the right side of the table —
  // matching the way a real round table looks when you sit at it.
  const oppEl = document.getElementById('unoOpponents');
  oppEl.innerHTML = '';
  const myIdx = s.players.findIndex(p => p.pid === MY_ID);
  // Build the visual ordering: start AFTER me, go in turn-direction.
  // direction = 1 means clockwise (next index after mine), -1 means
  // counter-clockwise (previous). The seat for "next player" goes to
  // the visual right.
  const oppsInOrder = [];
  if (myIdx !== -1) {
    const n = s.players.length;
    for (let k = 1; k < n; k++) {
      const idx = (myIdx + k + n) % n;
      oppsInOrder.push(s.players[idx]);
    }
  } else {
    // Spectator (shouldn't really happen) — just show everyone
    s.players.forEach(p => oppsInOrder.push(p));
  }
  // Pick seat positions based on opponent count. The keys are seat-class
  // names — each layout reads "next player first, then around".
  const seatLayouts = {
    1: ['seat-top'],
    2: ['seat-top-right', 'seat-top-left'],
    3: ['seat-right', 'seat-top', 'seat-left'],
    4: ['seat-right', 'seat-top-right', 'seat-top-left', 'seat-left'],
  };
  const seats = seatLayouts[oppsInOrder.length] || [];

  oppsInOrder.forEach((p, i) => {
    const seatCls = seats[i] || 'seat-top';
    const count = (s.hand_counts && s.hand_counts[p.pid]) || 0;
    const isTurn = s.turn === p.pid;
    const isUno = s.uno_called && s.uno_called.indexOf(p.pid) !== -1;
    const div = document.createElement('div');
    div.className = 'uno-opp ' + seatCls
                    + (isTurn ? ' turn' : '')
                    + (count === 1 ? ' uno-flag' : '');
    div.setAttribute('data-pid', p.pid);
    const initial = escHtml((p.name || '?').slice(0, 1).toUpperCase());
    const av = p.avatar
      ? `<img class="uno-opp-av" src="${escHtml(p.avatar)}" onerror="this.style.display='none'">`
      : `<div class="uno-opp-av">${initial}</div>`;
    // Mini card-back fan. Cap at 6 visible to avoid cluttering; if more,
    // show a small overflow pill ("+N").
    const visible = Math.min(count, 6);
    const overflow = count > 6 ? `<div class="uno-opp-overflow">+${count - 6}</div>` : '';
    const miniHtml = '<div class="uno-opp-mini"></div>'.repeat(visible) + overflow;
    div.innerHTML =
      `<div class="uno-opp-av-wrap">${av}<div class="uno-opp-back-pile">${miniHtml}</div></div>
       <div class="uno-opp-name">${escHtml(p.name || '?')}</div>
       <div class="uno-opp-count">${count} ${count === 1 ? (isUno ? 'UNO!' : 'card') : 'cards'}</div>
       <div class="uno-opp-timer${isTurn ? ' show' : ''}"><div class="uno-opp-timer-fill" data-pid="${escHtml(p.pid)}"></div></div>`;
    oppEl.appendChild(div);
  });

  // Self seat — show your name + avatar above the hand, with your own
  // timer bar that drains while it's your turn. Mirrors the opponents
  // visually so the layout feels symmetrical.
  const me = (myIdx !== -1) ? s.players[myIdx] : null;
  const myCount = (me && s.hand_counts) ? (s.hand_counts[MY_ID] || 0) : 0;
  const myIsTurn = s.turn === MY_ID;
  const selfSeat = document.getElementById('unoSelfSeat');
  selfSeat.className = 'uno-self-seat' + (myIsTurn ? ' turn' : '');
  const selfAv = document.getElementById('unoSelfAv');
  if (me && me.avatar) {
    selfAv.outerHTML = `<img class="uno-opp-av" id="unoSelfAv" src="${escHtml(me.avatar)}" onerror="this.outerHTML='<div class=\\'uno-opp-av\\' id=\\'unoSelfAv\\'>${escHtml((me.name||'?').slice(0,1).toUpperCase())}</div>'">`;
  } else {
    selfAv.outerHTML = `<div class="uno-opp-av" id="unoSelfAv">${escHtml(((me && me.name) || '?').slice(0,1).toUpperCase())}</div>`;
  }
  const selfNameEl = document.getElementById('unoSelfName');
  const myNameTxt = (me && me.name) ? me.name : 'You';
  selfNameEl.innerHTML = escHtml(myNameTxt) + ' <span class="you-pill">YOU' + (myCount === 1 ? ' · UNO!' : '') + '</span>';
  const selfTimer = document.getElementById('unoSelfTimer');
  selfTimer.classList.toggle('show', myIsTurn);

  // Turn bar
  const turnBar = document.getElementById('unoTurnBar');
  const myTurn = unoIsMyTurn();
  // Trigger a soft haptic pulse the moment it BECOMES our turn (not on
  // every render). Some browsers ignore this silently — that's fine.
  if (myTurn && !unoCli._lastWasMyTurn) {
    try { if (navigator.vibrate) navigator.vibrate(30); } catch (e) {}
  }
  unoCli._lastWasMyTurn = myTurn;
  if (s.winner) {
    turnBar.textContent = '🏆 Game finished';
    turnBar.className = 'uno-turn-bar';
  } else if (myTurn) {
    if (s.draw_pending > 0) {
      // Stacking is allowed — tell the player they can counter with +2/+4
      // (any color) or draw the whole stack.
      turnBar.textContent = `Draw ${s.draw_pending} or stack a +2/+4`;
    } else if (s.must_pass) {
      turnBar.textContent = 'Play your drawn card or pass';
    } else {
      turnBar.textContent = 'Your turn';
    }
    turnBar.className = 'uno-turn-bar your-turn';
  } else {
    const curName = (s.players.find(p => p.pid === s.turn) || {}).name || '?';
    if (s.draw_pending > 0) {
      turnBar.textContent = `${curName} must draw ${s.draw_pending} (or counter)`;
    } else {
      turnBar.textContent = `${curName}'s turn`;
    }
    turnBar.className = 'uno-turn-bar';
  }

  // Draw pile count
  document.getElementById('unoDrawCount').textContent = `${s.draw_pile_count} left`;

  // Discard top card. We track the previous top card id so we can give
  // newly-played cards a subtle slide-in animation. (Skipped on initial
  // render to avoid animating the opener.)
  const ds = document.getElementById('unoDiscardSlot');
  const newTopId = s.top_card ? s.top_card.id : null;
  const prevTopId = unoCli._lastDiscardId;
  ds.innerHTML = '';
  if (s.top_card) {
    const node = unoBuildCard(s.top_card, { size: 'md' });
    if (prevTopId && newTopId && newTopId !== prevTopId) {
      node.classList.add('uno-card-fly');
    }
    ds.appendChild(node);
  }
  unoCli._lastDiscardId = newTopId;
  // Color dot
  const dot = document.getElementById('unoColorDot');
  dot.className = 'uno-color-dot c-' + (s.color || '');

  // Stack badge — shows the pending draw count when a +2/+4 chain is
  // building. Visible to every player so the table sees the stakes grow.
  const stackBadge = document.getElementById('unoStackBadge');
  if (s.draw_pending > 0) {
    stackBadge.textContent = '+' + s.draw_pending;
    stackBadge.classList.add('show');
  } else {
    stackBadge.classList.remove('show');
  }

  // Direction indicator — we don't have a discrete element for it but we
  // could rotate the table-direction ring (omitted for simplicity).

  // Pass button visibility
  const passBtn = document.getElementById('unoBtnPass');
  passBtn.style.display = (myTurn && s.must_pass) ? '' : 'none';

  // Draw button enable
  const drawBtn = document.getElementById('unoBtnDraw');
  drawBtn.disabled = !myTurn || s.must_pass;

  unoRenderHand();
  unoRenderCallBtn();
  // Kick the timer animation loop. unoStartTimerLoop() is RAF-driven and
  // will self-stop when there's no active turn or the overlay is closed.
  unoStartTimerLoop();
}

// Drives the per-seat turn-timer bars. requestAnimationFrame loop that
// reads turn_started_at + turn_timeout from server state and updates the
// fill width of the bar belonging to the *current* player. Self-stops
// when the game isn't playing or the overlay is closed. Server clock
// skew is corrected from a one-shot delta sample captured at the first
// state arrival (preserves monotonicity vs re-deriving every frame).
let unoTimerRaf = null;
function unoStartTimerLoop() {
  if (unoTimerRaf) return;  // already running — RAF will continue
  const tick = () => {
    unoTimerRaf = null;
    const s = unoCli.state;
    if (!s || s.phase !== 'playing' || !unoCli.open) {
      // Hide everything timer-related.
      document.querySelectorAll('.uno-opp-timer-fill,.uno-self-timer-fill').forEach(el => {
        el.style.transform = 'scaleX(0)';
      });
      return;
    }
    // Clock-skew correction (one-shot).
    const localNow = Date.now() / 1000;
    if (unoCli._serverDelta == null && s.server_now) {
      unoCli._serverDelta = s.server_now - localNow;
    }
    const serverNow = localNow + (unoCli._serverDelta || 0);
    const elapsed = serverNow - (s.turn_started_at || serverNow);
    const total = s.turn_timeout || 30;
    const remaining = Math.max(0, total - elapsed);
    const frac = Math.max(0, Math.min(1, remaining / total));
    const warn = remaining < 5 && remaining > 0;

    // Only the active player's bar fills; everyone else's is invisible
    // (their .uno-opp-timer doesn't have .show). We update transform
    // rather than width so layout doesn't reflow every frame.
    const activeFill = s.turn === MY_ID
      ? document.getElementById('unoSelfTimerFill')
      : document.querySelector(`.uno-opp-timer-fill[data-pid="${CSS.escape(s.turn || '')}"]`);

    // Reset all fills first (cheap — short lists)
    document.querySelectorAll('.uno-opp-timer-fill,.uno-self-timer-fill').forEach(el => {
      if (el !== activeFill) el.style.transform = 'scaleX(0)';
    });

    if (activeFill) {
      activeFill.style.transition = 'none';
      activeFill.style.transform = 'scaleX(' + frac.toFixed(3) + ')';
      activeFill.classList.toggle('warn', warn);
    }
    unoTimerRaf = requestAnimationFrame(tick);
  };
  unoTimerRaf = requestAnimationFrame(tick);
}

function unoIsMyTurn() {
  return unoCli.state && unoCli.state.phase === 'playing'
         && unoCli.state.turn === MY_ID;
}

function unoCardIsPlayable(card) {
  if (!unoCli.state || unoCli.state.phase !== 'playing') return false;
  if (!unoIsMyTurn()) return false;
  // Stacking rule: when a draw_stack is pending, only +2 and +4 are legal
  // (any color — color match is ignored on a counter). Everything else
  // forces them to draw the stack.
  if (unoCli.state.draw_pending > 0) {
    return card.value === '+2' || card.value === '+4';
  }
  if (unoCli.state.must_pass) {
    // Only the last-drawn card can be played; the server enforces, but
    // we hint visually.
    const last = unoCli.hand[unoCli.hand.length - 1];
    if (!last || last.id !== card.id) return false;
  }
  const top = unoCli.state.top_card;
  if (!top) return false;
  if (card.color === 'w') return true;
  if (card.color === unoCli.state.color) return true;
  if (card.value === top.value && top.color !== 'w') return true;
  return false;
}

// ── Hand rendering ──
function unoRenderHand() {
  const h = document.getElementById('unoHand');
  if (!h) return;
  h.innerHTML = '';
  // Adaptive overlap: when you have many cards, cards must overlap more
  // tightly so they all fit on screen. We compute a per-card margin so
  // the whole fan fits within ~92vw, with a sensible floor (you can
  // still see each card's rank). Card width is 64px, base overlap -26.
  const n = unoCli.hand.length;
  const cardW = 64;
  const viewportW = Math.max(280, Math.min(window.innerWidth - 30, 560));
  // Total width with default overlap: cardW + (n-1)*(cardW + margin)
  // We want: cardW + (n-1)*(cardW + margin) <= viewportW
  // → margin >= (viewportW - cardW)/(n-1) - cardW   (margin is NEGATIVE)
  let overlap = -26;  // default looks best with 7-10 cards
  if (n > 1) {
    const needed = (viewportW - cardW) / (n - 1) - cardW;
    // Cap at -54 so cards don't disappear entirely; floor at -26.
    overlap = Math.max(-54, Math.min(-26, needed));
  }
  unoCli.hand.forEach((card, idx) => {
    const node = unoBuildCard(card, { size: '' });
    if (idx > 0) node.style.marginLeft = overlap + 'px';
    if (unoCardIsPlayable(card)) {
      node.classList.add('playable');
    } else {
      node.classList.add('unplayable');
    }
    node.addEventListener('click', () => onUnoCardClick(card.id));
    h.appendChild(node);
  });
}

function unoRenderCallBtn() {
  const btn = document.getElementById('unoCallBtn');
  if (!btn) return;
  const s = unoCli.state;
  if (!s || s.phase !== 'playing') {
    btn.classList.remove('show');
    return;
  }
  if (unoCli.hand.length === 1) {
    btn.classList.add('show');
    if (s.uno_called && s.uno_called.indexOf(MY_ID) !== -1) {
      btn.classList.add('called');
      btn.textContent = '✓';
    } else {
      btn.classList.remove('called');
      btn.textContent = 'UNO!';
    }
  } else {
    btn.classList.remove('show', 'called');
    btn.textContent = 'UNO!';
  }
}

// ── Card builder ──
// Builds a CSS-only Uno card DOM node from a card object.
//   color: r|y|g|b|w  ·  value: 0..9|skip|rev|+2|wild|+4
function unoBuildCard(card, opts) {
  opts = opts || {};
  const div = document.createElement('div');
  let cls = 'uno-card c-' + (card.color || 'w');
  if (opts.size === 'md') cls += ' size-md';
  else if (opts.size === 'lg') cls += ' size-lg';
  else if (opts.size === 'sm') cls += ' size-sm';

  // value class
  if (card.value === 'skip') cls += ' v-skip';
  else if (card.value === 'rev') cls += ' v-rev';
  else if (card.value === '+2') cls += ' v-p2';
  else if (card.value === 'wild') cls += ' v-wild';
  else if (card.value === '+4') cls += ' v-p4';

  div.className = cls;

  let centerSym = card.value;
  if (card.value === 'skip') centerSym = '⦸';
  else if (card.value === 'rev') centerSym = '⇄';
  else if (card.value === '+2') centerSym = '+2';
  else if (card.value === 'wild') centerSym = ''; // ::before puts "WILD"
  else if (card.value === '+4') centerSym = '';   // ::before puts "+4"

  // Corner glyphs use the short form
  let cornerSym = card.value;
  if (card.value === 'skip') cornerSym = 'Ø';
  else if (card.value === 'rev') cornerSym = '⇄';
  else if (card.value === '+2') cornerSym = '+2';
  else if (card.value === 'wild') cornerSym = '';
  else if (card.value === '+4') cornerSym = '+4';

  div.innerHTML =
    (cornerSym ? `<div class="uno-card-corner tl">${cornerSym}</div>` : '') +
    `<div class="uno-card-face">
       <div class="uno-card-oval"></div>
       <div class="uno-card-num">${centerSym}</div>
     </div>` +
    (cornerSym ? `<div class="uno-card-corner br">${cornerSym}</div>` : '');
  return div;
}

// ── In-game side chat ──
function toggleUnoChat() {
  const p = document.getElementById('unoChatPanel');
  unoCli.chatPanelOpen = !p.classList.contains('open');
  if (unoCli.chatPanelOpen) {
    p.classList.add('open');
    unoCli.chatBadge = 0;
    document.getElementById('unoChatBadge').classList.remove('show');
    // Scroll to bottom. We intentionally do NOT auto-focus the input —
    // doing so triggers the on-screen keyboard on Android and felt jarring
    // for users who just want to read recent messages. The keyboard comes
    // up only when the user taps the input themselves.
    const list = document.getElementById('unoChatMsgs');
    list.scrollTop = list.scrollHeight;
  } else {
    p.classList.remove('open');
    // If the keyboard was open inside the panel, blur the input so it
    // dismisses on close.
    try { document.getElementById('unoChatInput').blur(); } catch (e) {}
  }
}

function onUnoChatSend() {
  const input = document.getElementById('unoChatInput');
  const t = (input.value || '').trim();
  if (!t) return;
  if (!ws || ws.readyState !== 1) return;
  // Reuse the existing chat plumbing — it broadcasts to everyone in the
  // room. The mirror function will pick the message up via the chat case.
  ws.send(JSON.stringify({ type: 'chat', text: t }));
  input.value = '';
}

function unoMirrorChat(m) {
  // Skip our own already-confirmed self echo to avoid double-rendering if
  // sometimes the server returns m.self for both copies.
  if (!m) return;
  if (m.kind && m.kind !== 'user' && m.kind !== 'system') return;
  // Push into ring buffer
  unoCli.chatMirrorBuf.push(m);
  if (unoCli.chatMirrorBuf.length > 80) unoCli.chatMirrorBuf.shift();
  if (!unoCli.open) return;  // not visible — no need to render
  unoAppendChatMsg(m);
  if (!unoCli.chatPanelOpen && m.kind === 'user') {
    unoCli.chatBadge++;
    const b = document.getElementById('unoChatBadge');
    b.textContent = unoCli.chatBadge > 9 ? '9+' : String(unoCli.chatBadge);
    b.classList.add('show');
  }
}

function unoAppendChatMsg(m) {
  const list = document.getElementById('unoChatMsgs');
  if (!list) return;
  const div = document.createElement('div');
  if (m.kind === 'system') {
    div.className = 'uno-chat-msg system';
    div.textContent = m.text || '';
  } else {
    const isSelf = (m.peer_id && m.peer_id === MY_ID) || m.self;
    div.className = 'uno-chat-msg' + (isSelf ? ' self' : '');
    const name = escHtml(m.name || '?');
    const text = escHtml(m.text || (m.image ? '📷 Photo' : (m.sticker ? '✨ Sticker' : '')));
    div.innerHTML = isSelf
      ? `<div class="uno-chat-msg-text">${text}</div>`
      : `<div class="uno-chat-msg-name">${name}</div><div class="uno-chat-msg-text">${text}</div>`;
  }
  list.appendChild(div);
  // Trim
  while (list.children.length > 80) list.removeChild(list.firstChild);
  // Auto-scroll if at bottom or panel was just opened
  const nearBottom = (list.scrollHeight - list.scrollTop - list.clientHeight) < 100;
  if (nearBottom || unoCli.chatPanelOpen) {
    list.scrollTop = list.scrollHeight;
  }
}

function unoRenderChat() {
  const list = document.getElementById('unoChatMsgs');
  if (!list) return;
  list.innerHTML = '';
  unoCli.chatMirrorBuf.forEach(unoAppendChatMsg);
}

// ═══════════════════════════════════════════════════════════════════════
// End Uno client module
// ═══════════════════════════════════════════════════════════════════════


// ═══════════════════════════════════════════════════════════════════════
// ZOMBIE CLIENT MODULE
// ═══════════════════════════════════════════════════════════════════════
// Mirrors unoCli's pattern. Renders the table the same way (positional
// opponents, self-seat at bottom, per-seat timer bars). Center area
// shows the target's hand as a fan of card-backs when it's your turn —
// tap one to pick it. Otherwise it shows whose turn it is and a
// readout of who's still in the game.

const zombCli = {
  open: false,
  state: null,
  hand: [],
  selectedPlayerCount: 2,
  chatPanelOpen: false,
  chatBadge: 0,
  chatMirrorBuf: [],
  _serverDelta: null,
  _lastWasMyTurn: false,
};

function onPickGameZombie() {
  closeGamesPicker();
  openZombGame();
}

function openZombGame() {
  zombCli.open = true;
  document.getElementById('zombOvl').classList.add('show');
  applyFixedGameBg('zombie');
  syncAllMuteBtns();
  if (!zombCli.state) {
    zombShowLobbyCreate();
  } else {
    zombRender();
  }
  zombRenderChat();
}

function onZombBack() {
  document.getElementById('zombOvl').classList.remove('show');
  zombCli.open = false;
}

function zombShowLobbyCreate() {
  document.getElementById('zombLobby').style.display = '';
  document.getElementById('zombPlay').style.display = 'none';
  document.getElementById('zombCreateBox').style.display = '';
  document.getElementById('zombWaitingBox').style.display = 'none';
  document.getElementById('zombSubtitle').textContent = 'Lobby';
  document.getElementById('zombLobbyActions').innerHTML =
    '<button class="uno-btn-primary" id="zombCreateBtn" onclick="onZombCreate()">Create Game</button>';
}

function onZombPickPlayerCount(n) {
  zombCli.selectedPlayerCount = n;
  document.querySelectorAll('#zombPlayerCountRow .uno-player-count-btn').forEach(b => {
    b.classList.toggle('sel', Number(b.getAttribute('data-n')) === n);
  });
}

function onZombCreate() {
  if (!ws || ws.readyState !== 1) return;
  ws.send(JSON.stringify({ type: 'zomb_create', players: zombCli.selectedPlayerCount }));
}
function onZombJoin() {
  if (ws && ws.readyState === 1) ws.send(JSON.stringify({ type: 'zomb_join' }));
}
function onZombStart() {
  if (ws && ws.readyState === 1) ws.send(JSON.stringify({ type: 'zomb_start' }));
}
function onZombLeave() {
  if (ws && ws.readyState === 1) ws.send(JSON.stringify({ type: 'zomb_leave' }));
}
function onZombPlayAgain() {
  document.getElementById('zombEnd').classList.remove('show');
  zombCli._serverDelta = null;
  if (ws && ws.readyState === 1) ws.send(JSON.stringify({ type: 'zomb_play_again' }));
}
function onZombEndClose() {
  document.getElementById('zombEnd').classList.remove('show');
  if (ws && ws.readyState === 1) ws.send(JSON.stringify({ type: 'zomb_close' }));
  zombCli.state = null;
  zombCli.hand = [];
  zombShowLobbyCreate();
}

// Pick a card at index `idx` from the target's hidden hand.
function onZombPickCard(idx) {
  const s = zombCli.state;
  if (!s || s.phase !== 'playing') return;
  if (s.turn !== MY_ID) { zombToast('Not your turn'); return; }
  if (!s.target) return;
  if (ws && ws.readyState === 1) {
    ws.send(JSON.stringify({
      type: 'zomb_pick',
      target_pid: s.target,
      card_idx: idx,
    }));
  }
}

function zombHandleServerMsg(m) {
  switch (m.type) {
    case 'zomb_state':
      // Hide stale end-overlay if a fresh game is starting
      if (m.state && !m.state.zombie_pid && m.state.phase !== 'finished') {
        const e = document.getElementById('zombEnd');
        if (e && e.classList.contains('show')) {
          e.classList.remove('show');
          zombCli._serverDelta = null;
        }
      }
      zombCli.state = m.state || null;
      zombRender();
      if (zombCli.state && zombCli.state.phase === 'playing') {
        zombStartTimerLoop();
      }
      break;
    case 'zomb_hand':
      zombCli.hand = Array.isArray(m.hand) ? m.hand : [];
      zombRenderHand();
      break;
    case 'zomb_event':
      zombOnEvent(m);
      break;
    case 'zomb_error':
      zombToast(m.text || 'Error');
      break;
    case 'zomb_closed':
      zombCli.state = null;
      zombCli.hand = [];
      zombCli._serverDelta = null;
      try { document.getElementById('zombEnd').classList.remove('show'); } catch(e){}
      zombToast(m.text || 'Game closed');
      if (zombCli.open) zombShowLobbyCreate();
      break;
  }
}

function zombOnEvent(m) {
  const t = m.text || '';
  if (m.kind === 'zombie') {
    // End screen — the named player is the loser.
    const s = zombCli.state || {};
    const pid = s.zombie_pid || m.peer_id;
    const isMe = pid === MY_ID;
    let wname = '?';
    if (s.players) {
      const wp = s.players.find(p => p.pid === pid);
      if (wp) wname = wp.name || '?';
    }
    document.getElementById('zombEndTitle').textContent = isMe ? "You're the Zombie!" : 'The Zombie!';
    document.getElementById('zombEndName').textContent = wname;
    document.getElementById('zombEndSub').textContent = isMe ? '🧟 Better luck next round' : 'Everyone else wins';
    document.getElementById('zombEnd').classList.add('show');
  } else if (m.kind === 'win_all') {
    document.getElementById('zombEndTitle').textContent = 'Everyone wins!';
    document.getElementById('zombEndName').textContent = '🎉';
    document.getElementById('zombEndSub').textContent = t;
    document.getElementById('zombEnd').classList.add('show');
  } else if (t) {
    zombToast(t);
  }
}

let zombToastTimer = null;
function zombToast(text) {
  const el = document.getElementById('zombToast');
  if (!el) return;
  el.textContent = text;
  el.classList.add('show');
  if (zombToastTimer) clearTimeout(zombToastTimer);
  zombToastTimer = setTimeout(() => el.classList.remove('show'), 2200);
}

function zombRender() {
  if (!zombCli.state) { zombShowLobbyCreate(); return; }
  const s = zombCli.state;
  if (s.phase === 'lobby') {
    zombRenderLobbyWaiting();
  } else {
    zombRenderPlay();
  }
}

function zombRenderLobbyWaiting() {
  const s = zombCli.state;
  document.getElementById('zombLobby').style.display = '';
  document.getElementById('zombPlay').style.display = 'none';
  document.getElementById('zombCreateBox').style.display = 'none';
  document.getElementById('zombWaitingBox').style.display = '';
  document.getElementById('zombSubtitle').textContent =
    `Lobby · ${s.players.length}/${s.max_players}`;
  const wh = document.getElementById('zombWaitingHeader');
  wh.textContent = `Waiting for players (${s.players.length}/${s.max_players})`;
  const list = document.getElementById('zombLobbyPlayersList');
  list.innerHTML = '';
  s.players.forEach(p => {
    const row = document.createElement('div');
    row.className = 'uno-lobby-player';
    const isHost = p.pid === s.host;
    const initial = escHtml((p.name || '?').slice(0, 1).toUpperCase());
    const avInner = p.avatar
      ? `<img src="${escHtml(p.avatar)}" style="width:100%;height:100%;border-radius:50%;object-fit:cover" onerror="this.style.display='none';this.parentNode.textContent='${initial}'">`
      : initial;
    row.innerHTML =
      `<div class="uno-lobby-player-av">${avInner}</div>
       <div class="uno-lobby-player-name">${escHtml(p.name || '?')}${p.pid === MY_ID ? ' <span style="opacity:0.5">(you)</span>' : ''}</div>
       ${isHost ? '<div class="uno-lobby-host-badge">HOST</div>' : ''}`;
    list.appendChild(row);
  });
  for (let i = s.players.length; i < s.max_players; i++) {
    const slot = document.createElement('div');
    slot.className = 'uno-lobby-empty-slot';
    slot.innerHTML = `<div class="uno-lobby-player-av" style="background:rgba(255,255,255,0.04)">+</div><div>Waiting for player ${i + 1}...</div>`;
    list.appendChild(slot);
  }
  const inGame = s.players.some(p => p.pid === MY_ID);
  const isHost = s.host === MY_ID;
  const actions = document.getElementById('zombLobbyActions');
  let html = '';
  if (!inGame) {
    html += `<button class="uno-btn-primary" onclick="onZombJoin()">Join Game</button>`;
  } else {
    if (isHost && s.players.length >= 2) {
      html += `<button class="uno-btn-primary" onclick="onZombStart()">Start Now</button>`;
    }
    html += `<button class="uno-btn-secondary" onclick="onZombLeave()">Leave</button>`;
  }
  actions.innerHTML = html;
}

function zombRenderPlay() {
  document.getElementById('zombLobby').style.display = 'none';
  document.getElementById('zombPlay').style.display = '';

  const s = zombCli.state;
  document.getElementById('zombSubtitle').textContent =
    `Game · ${s.players.length - (s.finished || []).length} active`;

  // Positional opponents — exact same logic as Uno's renderer.
  const oppEl = document.getElementById('zombOpponents');
  oppEl.innerHTML = '';
  const myIdx = s.players.findIndex(p => p.pid === MY_ID);
  const oppsInOrder = [];
  if (myIdx !== -1) {
    const n = s.players.length;
    for (let k = 1; k < n; k++) {
      const idx = (myIdx + k + n) % n;
      oppsInOrder.push(s.players[idx]);
    }
  }
  const seatLayouts = {
    1: ['seat-top'],
    2: ['seat-top-right', 'seat-top-left'],
    3: ['seat-right', 'seat-top', 'seat-left'],
    4: ['seat-right', 'seat-top-right', 'seat-top-left', 'seat-left'],
  };
  const seats = seatLayouts[oppsInOrder.length] || [];
  oppsInOrder.forEach((p, i) => {
    const seatCls = seats[i] || 'seat-top';
    const count = (s.hand_counts && s.hand_counts[p.pid]) || 0;
    const isTurn = s.turn === p.pid;
    const isTarget = s.target === p.pid;
    const isOut = (s.finished || []).indexOf(p.pid) !== -1;
    const div = document.createElement('div');
    div.className = 'uno-opp ' + seatCls + (isTurn ? ' turn' : '');
    if (isOut) div.style.opacity = '0.3';
    if (isTarget) div.style.filter = 'drop-shadow(0 0 14px rgba(255,90,90,0.7))';
    const initial = escHtml((p.name || '?').slice(0, 1).toUpperCase());
    const av = p.avatar
      ? `<img class="uno-opp-av" src="${escHtml(p.avatar)}" onerror="this.style.display='none'">`
      : `<div class="uno-opp-av">${initial}</div>`;
    const visible = Math.min(count, 6);
    const overflow = count > 6 ? `<div class="uno-opp-overflow">+${count - 6}</div>` : '';
    const miniHtml = '<div class="uno-opp-mini"></div>'.repeat(visible) + overflow;
    const status = isOut ? 'OUT 🎉'
                  : (count === 0 ? 'no cards'
                     : (count + (count === 1 ? ' card' : ' cards')));
    div.innerHTML =
      `<div class="uno-opp-av-wrap">${av}<div class="uno-opp-back-pile">${miniHtml}</div></div>
       <div class="uno-opp-name">${escHtml(p.name || '?')}</div>
       <div class="uno-opp-count">${status}</div>
       <div class="uno-opp-timer${isTurn ? ' show' : ''}"><div class="uno-opp-timer-fill" data-pid="${escHtml(p.pid)}"></div></div>`;
    oppEl.appendChild(div);
  });

  // Self seat
  const me = (myIdx !== -1) ? s.players[myIdx] : null;
  const myCount = (me && s.hand_counts) ? (s.hand_counts[MY_ID] || 0) : 0;
  const myIsTurn = s.turn === MY_ID;
  const isOutMe = (s.finished || []).indexOf(MY_ID) !== -1;
  if (myIsTurn && !zombCli._lastWasMyTurn) {
    try { if (navigator.vibrate) navigator.vibrate(30); } catch (e) {}
  }
  zombCli._lastWasMyTurn = myIsTurn;
  const selfSeat = document.getElementById('zombSelfSeat');
  selfSeat.className = 'uno-self-seat' + (myIsTurn ? ' turn' : '');
  const selfAv = document.getElementById('zombSelfAv');
  if (me && me.avatar) {
    selfAv.outerHTML = `<img class="uno-opp-av" id="zombSelfAv" src="${escHtml(me.avatar)}" onerror="this.outerHTML='<div class=\\'uno-opp-av\\' id=\\'zombSelfAv\\'>${escHtml((me.name||'?').slice(0,1).toUpperCase())}</div>'">`;
  } else {
    selfAv.outerHTML = `<div class="uno-opp-av" id="zombSelfAv">${escHtml(((me && me.name) || '?').slice(0,1).toUpperCase())}</div>`;
  }
  const myName = (me && me.name) ? me.name : 'You';
  document.getElementById('zombSelfName').innerHTML =
    escHtml(myName) + ' <span class="you-pill">' + (isOutMe ? 'OUT 🎉' : 'YOU') + '</span>';
  document.getElementById('zombSelfTimer').classList.toggle('show', myIsTurn);

  // Turn bar
  const turnBar = document.getElementById('zombTurnBar');
  if (s.phase === 'finished') {
    turnBar.textContent = '🧟 Game finished';
    turnBar.className = 'uno-turn-bar';
  } else if (myIsTurn) {
    const tgt = s.players.find(p => p.pid === s.target);
    turnBar.textContent = tgt ? `Your turn — pick from ${tgt.name}` : 'Your turn';
    turnBar.className = 'uno-turn-bar your-turn';
  } else {
    const cur = s.players.find(p => p.pid === s.turn);
    const tgt = s.players.find(p => p.pid === s.target);
    turnBar.textContent = (cur && tgt)
      ? `${cur.name} picks from ${tgt.name}`
      : 'Waiting...';
    turnBar.className = 'uno-turn-bar';
  }

  // Center: target hand fan (if it's my turn, show clickable card-backs)
  const center = document.getElementById('zombCenter');
  const centerText = document.getElementById('zombCenterText');
  const fan = document.getElementById('zombTargetFan');
  fan.innerHTML = '';
  if (s.phase === 'finished') {
    centerText.textContent = 'Game over';
    centerText.className = 'zomb-center-text';
  } else if (myIsTurn) {
    const tgt = s.players.find(p => p.pid === s.target);
    const tgtCount = tgt ? (s.hand_counts[tgt.pid] || 0) : 0;
    centerText.textContent = tgt
      ? `Pick a card from ${tgt.name}'s hand`
      : 'No target';
    centerText.className = 'zomb-center-text';
    // Build N face-down cards
    for (let i = 0; i < tgtCount; i++) {
      const c = zombBuildCard(null, { back: true });
      c.classList.add('pickable');
      c.addEventListener('click', () => onZombPickCard(i));
      fan.appendChild(c);
    }
    if (tgtCount === 0) {
      fan.innerHTML = '<div class="zomb-target-fan-empty">No cards to pick</div>';
    }
  } else {
    // Spectator-ish — show whose turn + their target's card count
    const cur = s.players.find(p => p.pid === s.turn);
    const tgt = s.players.find(p => p.pid === s.target);
    centerText.textContent = (cur && tgt)
      ? `${cur.name} is picking from ${tgt.name}...`
      : '';
    centerText.className = 'zomb-center-text dim';
    const tgtCount = tgt ? (s.hand_counts[tgt.pid] || 0) : 0;
    for (let i = 0; i < tgtCount; i++) {
      fan.appendChild(zombBuildCard(null, { back: true }));
    }
  }

  zombRenderHand();
  zombStartTimerLoop();
}

function zombRenderHand() {
  const el = document.getElementById('zombHand');
  if (!el) return;
  el.innerHTML = '';
  // Same adaptive-overlap logic as Uno hand. Card width is 54px here.
  const n = zombCli.hand.length;
  const cardW = 54;
  const viewportW = Math.max(280, Math.min(window.innerWidth - 30, 560));
  let overlap = -26;
  if (n > 1) {
    const needed = (viewportW - cardW) / (n - 1) - cardW;
    overlap = Math.max(-44, Math.min(-26, needed));
  }
  zombCli.hand.forEach((card, idx) => {
    const node = zombBuildCard(card, { back: false });
    if (idx > 0) node.style.marginLeft = overlap + 'px';
    el.appendChild(node);
  });
}

// Builds a Zombie card node. `card` may be null for face-down backs.
function zombBuildCard(card, opts) {
  opts = opts || {};
  const div = document.createElement('div');
  if (opts.back || !card) {
    div.className = 'zomb-card back';
    div.innerHTML = '<div class="zomb-card-face"></div>';
    return div;
  }
  const isZombie = card.rank === 'Z';
  let cls = 'zomb-card suit-' + (card.suit || 'z');
  if (isZombie) cls += ' is-zombie';
  div.className = cls;
  const rankDisp = isZombie ? '' : card.rank;
  const suitChar = {h:'♥',d:'♦',s:'♠',c:'♣'}[card.suit] || '';
  div.innerHTML =
    `<div class="zomb-corner tl">${escHtml(rankDisp)}</div>
     <div class="zomb-card-face">
       <div class="zomb-card-rank">${escHtml(rankDisp)}</div>
       <div class="zomb-card-suit">${suitChar}</div>
     </div>
     <div class="zomb-corner br">${escHtml(rankDisp)}</div>`;
  return div;
}

// Per-seat timer loop for Zombie (mirrors Uno's, hits zomb-prefixed els)
let zombTimerRaf = null;
function zombStartTimerLoop() {
  if (zombTimerRaf) return;
  const tick = () => {
    zombTimerRaf = null;
    const s = zombCli.state;
    if (!s || s.phase !== 'playing' || !zombCli.open) {
      document.querySelectorAll('#zombOvl .uno-opp-timer-fill, #zombOvl .uno-self-timer-fill').forEach(el => {
        el.style.transform = 'scaleX(0)';
      });
      return;
    }
    const localNow = Date.now() / 1000;
    if (zombCli._serverDelta == null && s.server_now) {
      zombCli._serverDelta = s.server_now - localNow;
    }
    const serverNow = localNow + (zombCli._serverDelta || 0);
    const elapsed = serverNow - (s.turn_started_at || serverNow);
    const total = s.turn_timeout || 25;
    const remaining = Math.max(0, total - elapsed);
    const frac = Math.max(0, Math.min(1, remaining / total));
    const warn = remaining < 5 && remaining > 0;
    const activeFill = s.turn === MY_ID
      ? document.getElementById('zombSelfTimerFill')
      : document.querySelector(`#zombOvl .uno-opp-timer-fill[data-pid="${CSS.escape(s.turn || '')}"]`);
    document.querySelectorAll('#zombOvl .uno-opp-timer-fill, #zombOvl .uno-self-timer-fill').forEach(el => {
      if (el !== activeFill) el.style.transform = 'scaleX(0)';
    });
    if (activeFill) {
      activeFill.style.transition = 'none';
      activeFill.style.transform = 'scaleX(' + frac.toFixed(3) + ')';
      activeFill.classList.toggle('warn', warn);
    }
    zombTimerRaf = requestAnimationFrame(tick);
  };
  zombTimerRaf = requestAnimationFrame(tick);
}

// Chat panel — reuses unoMirrorChat pattern but writes into the zomb list
function toggleZombChat() {
  const p = document.getElementById('zombChatPanel');
  zombCli.chatPanelOpen = !p.classList.contains('open');
  if (zombCli.chatPanelOpen) {
    p.classList.add('open');
    zombCli.chatBadge = 0;
    document.getElementById('zombChatBadge').classList.remove('show');
    const list = document.getElementById('zombChatMsgs');
    list.scrollTop = list.scrollHeight;
  } else {
    p.classList.remove('open');
    try { document.getElementById('zombChatInput').blur(); } catch (e) {}
  }
}
function onZombChatSend() {
  const input = document.getElementById('zombChatInput');
  const t = (input.value || '').trim();
  if (!t || !ws || ws.readyState !== 1) return;
  ws.send(JSON.stringify({ type: 'chat', text: t }));
  input.value = '';
}
function zombMirrorChat(m) {
  if (!m) return;
  if (m.kind && m.kind !== 'user' && m.kind !== 'system') return;
  zombCli.chatMirrorBuf.push(m);
  if (zombCli.chatMirrorBuf.length > 80) zombCli.chatMirrorBuf.shift();
  if (!zombCli.open) return;
  zombAppendChatMsg(m);
  if (!zombCli.chatPanelOpen && m.kind === 'user') {
    zombCli.chatBadge++;
    const b = document.getElementById('zombChatBadge');
    b.textContent = zombCli.chatBadge > 9 ? '9+' : String(zombCli.chatBadge);
    b.classList.add('show');
  }
}
function zombAppendChatMsg(m) {
  const list = document.getElementById('zombChatMsgs');
  if (!list) return;
  const div = document.createElement('div');
  if (m.kind === 'system') {
    div.className = 'uno-chat-msg system';
    div.textContent = m.text || '';
  } else {
    const isSelf = (m.peer_id && m.peer_id === MY_ID) || m.self;
    div.className = 'uno-chat-msg' + (isSelf ? ' self' : '');
    const name = escHtml(m.name || '?');
    const text = escHtml(m.text || (m.image ? '📷 Photo' : (m.sticker ? '✨ Sticker' : '')));
    div.innerHTML = isSelf
      ? `<div class="uno-chat-msg-text">${text}</div>`
      : `<div class="uno-chat-msg-name">${name}</div><div class="uno-chat-msg-text">${text}</div>`;
  }
  list.appendChild(div);
  while (list.children.length > 80) list.removeChild(list.firstChild);
  const nearBottom = (list.scrollHeight - list.scrollTop - list.clientHeight) < 100;
  if (nearBottom || zombCli.chatPanelOpen) list.scrollTop = list.scrollHeight;
}
function zombRenderChat() {
  const list = document.getElementById('zombChatMsgs');
  if (!list) return;
  list.innerHTML = '';
  zombCli.chatMirrorBuf.forEach(zombAppendChatMsg);
}

// ═══════════════════════════════════════════════════════════════════════
// End Zombie client module
// ═══════════════════════════════════════════════════════════════════════


// ═══════════════════════════════════════════════════════════════════════
// SHARED: Fixed game backgrounds + sound effects
// ═══════════════════════════════════════════════════════════════════════
// As of v3.26 each game has ONE fixed background image at:
//   /uno-bg/bg1.jpg    (served from the repo's uno-bg/ folder)
//   /zombie-bg/bg1.jpg (served from the repo's zombie-bg/ folder)
// We set the matching CSS variable on the overlay element so the
// already-in-place ::before layer paints it. If the image isn't present
// in the repo (404), the layer is just empty and the dark default
// background shows through — no error, no broken state.
// The previous v3.25 host-side picker + per-room broadcast were removed
// because the user requested fixed defaults that don't reset per room.

function applyFixedGameBg(game) {
  const ovl = document.getElementById(game === 'uno' ? 'unoOvl' : 'zombOvl');
  if (!ovl) return;
  const url = (game === 'uno') ? '/uno-bg/bg1.jpg' : '/zombie-bg/bg1.jpg';
  // Cache-bust once per session so freshly-pushed images appear on the
  // first reload, but avoid hammering the server on every overlay open.
  if (!applyFixedGameBg._bust) applyFixedGameBg._bust = Date.now();
  ovl.style.setProperty(
    (game === 'uno') ? '--uno-bg-image' : '--zomb-bg-image',
    `url("${url}?v=${applyFixedGameBg._bust}")`
  );
}

// ═══════════════════════════════════════════════════════════════════════
// SOUND EFFECTS — procedural Web Audio (no asset files needed)
// ═══════════════════════════════════════════════════════════════════════
// Short clicks/whooshes/zaps generated with oscillators + filters. They
// follow user gestures (the click that triggers the action) so browsers
// don't block them. The zombie-loss sound is a slow detuned wobble.
// All gated behind sfxEnabled (default true; persisted in memory only
// within this session).

const sfx = {
  ctx: null,
  enabled: true,
  ensureCtx() {
    if (this.ctx) return this.ctx;
    try {
      this.ctx = new (window.AudioContext || window.webkitAudioContext)();
    } catch (e) {
      this.enabled = false;
    }
    return this.ctx;
  },
  // real card-flick sound. The old version was a square-wave
  // pitch-down which sounded like a UI click, not a card. A real card
  // flick is mostly noise: the paper itself snapping, ~70ms total,
  // with a quick attack and a bright but contained spectrum (bandpass
  // around 2kHz). We add a faint sine "thunk" underneath for body so
  // it doesn't feel too thin.
  play() {
    if (!this.enabled) return;
    const ctx = this.ensureCtx();
    if (!ctx) return;
    const t = ctx.currentTime;
    // Short noise burst — the "paper" layer
    const dur = 0.07;
    const buf = ctx.createBuffer(1, Math.floor(ctx.sampleRate * dur), ctx.sampleRate);
    const d = buf.getChannelData(0);
    for (let i = 0; i < d.length; i++) {
      // Slightly biased noise so we get some low-mid content
      d[i] = (Math.random() * 2 - 1) * (1 - i / d.length);
    }
    const n = ctx.createBufferSource(); n.buffer = buf;
    const bp = ctx.createBiquadFilter();
    bp.type = 'bandpass';
    bp.frequency.setValueAtTime(2200, t);
    bp.frequency.exponentialRampToValueAtTime(900, t + dur);
    bp.Q.value = 2.5;
    const ng = ctx.createGain();
    ng.gain.setValueAtTime(0.0001, t);
    ng.gain.exponentialRampToValueAtTime(0.22, t + 0.004);
    ng.gain.exponentialRampToValueAtTime(0.0001, t + dur);
    n.connect(bp); bp.connect(ng); ng.connect(ctx.destination);
    n.start(t); n.stop(t + dur);

    // Tiny low-mid "body" thunk — fills out the bottom so it sounds
    // like cardstock, not just hiss.
    const o = ctx.createOscillator();
    const og = ctx.createGain();
    o.type = 'sine';
    o.frequency.setValueAtTime(160, t);
    o.frequency.exponentialRampToValueAtTime(90, t + 0.05);
    og.gain.setValueAtTime(0.0001, t);
    og.gain.exponentialRampToValueAtTime(0.09, t + 0.005);
    og.gain.exponentialRampToValueAtTime(0.0001, t + 0.06);
    o.connect(og); og.connect(ctx.destination);
    o.start(t); o.stop(t + 0.06);
  },
  // Soft swoosh — card drawn from pile
  draw() {
    if (!this.enabled) return;
    const ctx = this.ensureCtx();
    if (!ctx) return;
    const t = ctx.currentTime;
    // White noise burst through a bandpass that sweeps up
    const buf = ctx.createBuffer(1, ctx.sampleRate * 0.18, ctx.sampleRate);
    const d = buf.getChannelData(0);
    for (let i = 0; i < d.length; i++) d[i] = (Math.random() * 2 - 1);
    const n = ctx.createBufferSource(); n.buffer = buf;
    const f = ctx.createBiquadFilter(); f.type = 'bandpass';
    f.frequency.setValueAtTime(600, t);
    f.frequency.exponentialRampToValueAtTime(2400, t + 0.16);
    f.Q.value = 4;
    const g = ctx.createGain();
    g.gain.setValueAtTime(0.0001, t);
    g.gain.exponentialRampToValueAtTime(0.14, t + 0.02);
    g.gain.exponentialRampToValueAtTime(0.0001, t + 0.18);
    n.connect(f); f.connect(g); g.connect(ctx.destination);
    n.start(t); n.stop(t + 0.2);
  },
  // Triumphant little 3-note rise — winner
  win() {
    if (!this.enabled) return;
    const ctx = this.ensureCtx();
    if (!ctx) return;
    const t = ctx.currentTime;
    const notes = [523.25, 659.25, 783.99];  // C5 E5 G5
    notes.forEach((freq, i) => {
      const o = ctx.createOscillator();
      const g = ctx.createGain();
      o.type = 'triangle';
      o.frequency.value = freq;
      const start = t + i * 0.12;
      g.gain.setValueAtTime(0.0001, start);
      g.gain.exponentialRampToValueAtTime(0.18, start + 0.02);
      g.gain.exponentialRampToValueAtTime(0.001, start + 0.32);
      o.connect(g); g.connect(ctx.destination);
      o.start(start); o.stop(start + 0.35);
    });
  },
  // much more zombie-y groan. The previous version was a single
  // detuned sawtooth pair — sounded more like a synth pad than a
  // creature. This rebuild layers:
  //   1) 3 detuned sawtooth voices (chorus thickness) with vibrato
  //   2) A throat-rattle: filtered noise bandpass-swept through the
  //      ~600-1800 Hz "vocal tract" range
  //   3) A second growl an octave below for menace
  //   4) The original throat-click at the start
  // Total ~1.5s. Volume is gentle so it doesn't startle.
  zombie() {
    if (!this.enabled) return;
    const ctx = this.ensureCtx();
    if (!ctx) return;
    const t = ctx.currentTime;
    const dur = 1.5;

    // Master gain so we can shape the overall envelope as one curve.
    const master = ctx.createGain();
    master.gain.setValueAtTime(0.0001, t);
    master.gain.exponentialRampToValueAtTime(0.14, t + 0.18);
    master.gain.setValueAtTime(0.14, t + dur - 0.35);
    master.gain.exponentialRampToValueAtTime(0.0001, t + dur);
    master.connect(ctx.destination);

    // Body lowpass — keeps the whole thing dark/throaty.
    const body = ctx.createBiquadFilter();
    body.type = 'lowpass';
    body.frequency.setValueAtTime(900, t);
    body.frequency.exponentialRampToValueAtTime(500, t + dur);
    body.Q.value = 5;
    body.connect(master);

    // Vibrato LFO — modulates pitch of the saw voices so it "wobbles"
    // like a vocal cord rather than a steady drone.
    const lfo = ctx.createOscillator();
    lfo.type = 'sine';
    lfo.frequency.value = 5.5;  // ~5.5 Hz vibrato
    const lfoGain = ctx.createGain();
    lfoGain.gain.value = 6;     // ±6 cents
    lfo.connect(lfoGain);
    lfo.start(t); lfo.stop(t + dur + 0.05);

    // 3 detuned saw voices for the main groan
    const baseFreq = 95;
    const detunes = [0, -14, 12];
    detunes.forEach((det, i) => {
      const o = ctx.createOscillator();
      o.type = 'sawtooth';
      o.frequency.setValueAtTime(baseFreq, t);
      // The slow "uuughhh" descent — falls then rises slightly at the end
      o.frequency.exponentialRampToValueAtTime(baseFreq * 0.62, t + dur * 0.55);
      o.frequency.exponentialRampToValueAtTime(baseFreq * 0.75, t + dur);
      o.detune.value = det;
      lfoGain.connect(o.detune);  // apply vibrato
      const g = ctx.createGain();
      g.gain.value = 0.55 - i * 0.12;
      o.connect(g); g.connect(body);
      o.start(t); o.stop(t + dur + 0.05);
    });

    // Sub-octave growl for extra menace
    const sub = ctx.createOscillator();
    sub.type = 'sawtooth';
    sub.frequency.setValueAtTime(baseFreq * 0.5, t);
    sub.frequency.exponentialRampToValueAtTime(baseFreq * 0.32, t + dur * 0.55);
    const subG = ctx.createGain();
    subG.gain.value = 0.45;
    sub.connect(subG); subG.connect(body);
    sub.start(t); sub.stop(t + dur + 0.05);

    // Throat rattle — bandpass-filtered noise sweeping through the
    // formant range, so it sounds like vocal-cord buzz rather than hiss.
    const noiseBuf = ctx.createBuffer(1, Math.floor(ctx.sampleRate * dur),
                                       ctx.sampleRate);
    const nd = noiseBuf.getChannelData(0);
    for (let i = 0; i < nd.length; i++) nd[i] = (Math.random() * 2 - 1);
    const ns = ctx.createBufferSource(); ns.buffer = noiseBuf;
    const bp = ctx.createBiquadFilter();
    bp.type = 'bandpass';
    bp.frequency.setValueAtTime(1400, t);
    bp.frequency.exponentialRampToValueAtTime(700, t + dur * 0.5);
    bp.frequency.exponentialRampToValueAtTime(1100, t + dur);
    bp.Q.value = 6;
    const ng = ctx.createGain();
    ng.gain.setValueAtTime(0.0001, t);
    ng.gain.exponentialRampToValueAtTime(0.5, t + 0.2);
    ng.gain.setValueAtTime(0.5, t + dur - 0.3);
    ng.gain.exponentialRampToValueAtTime(0.0001, t + dur);
    ns.connect(bp); bp.connect(ng); ng.connect(body);
    ns.start(t); ns.stop(t + dur);

    // Sharp throat-click at the start (the "uuh" attack)
    const c = ctx.createOscillator();
    const cg = ctx.createGain();
    c.type = 'square'; c.frequency.value = 75;
    cg.gain.setValueAtTime(0.16, t);
    cg.gain.exponentialRampToValueAtTime(0.0001, t + 0.07);
    c.connect(cg); cg.connect(master);
    c.start(t); c.stop(t + 0.08);
  },
  // Generic short "pop" — used for the +2/+4 stack growing
  pop() {
    if (!this.enabled) return;
    const ctx = this.ensureCtx();
    if (!ctx) return;
    const t = ctx.currentTime;
    const o = ctx.createOscillator();
    const g = ctx.createGain();
    o.type = 'sine'; o.frequency.setValueAtTime(900, t);
    o.frequency.exponentialRampToValueAtTime(220, t + 0.08);
    g.gain.setValueAtTime(0.0001, t);
    g.gain.exponentialRampToValueAtTime(0.16, t + 0.01);
    g.gain.exponentialRampToValueAtTime(0.0001, t + 0.1);
    o.connect(g); g.connect(ctx.destination);
    o.start(t); o.stop(t + 0.12);
  },
};

function pushGameLog(targetId, text) {
  const el = document.getElementById(targetId);
  if (!el || !text) return;
  // Push the new line on top, demote older entries, drop anything past
  // the third slot.
  const old = Array.from(el.querySelectorAll('.game-log-line')).map(n => n.textContent);
  const lines = [text, ...old].slice(0, 3);
  el.innerHTML = '';
  lines.forEach((t, i) => {
    const d = document.createElement('div');
    d.className = 'game-log-line l' + i;
    d.textContent = t;
    el.appendChild(d);
  });
}

// Hook into Uno events to fire sounds at the right moments.
const _origUnoOnEvent = (typeof unoOnEvent === 'function') ? unoOnEvent : null;
unoOnEvent = function(m) {
  try {
    if (m.kind === 'play')         sfx.play();
    else if (m.kind === 'draw')    sfx.draw();
    else if (m.kind === 'penalty') sfx.draw();
    else if (m.kind === 'draw_pending') sfx.pop();
    else if (m.kind === 'win')     sfx.win();
    // Mirror the event text into the on-table log so people glance
    // down and see "Bob played Red 7" / "Skipped!" without reading
    // chat.
    if (m && m.text && m.kind !== 'almost_uno') pushGameLog('unoLog', m.text);
  } catch (e) {}
  if (_origUnoOnEvent) _origUnoOnEvent(m);
};

const _origZombOnEvent = (typeof zombOnEvent === 'function') ? zombOnEvent : null;
zombOnEvent = function(m) {
  try {
    if (m.kind === 'pick')    sfx.play();
    else if (m.kind === 'pair') sfx.win();
    else if (m.kind === 'out')  sfx.win();
    else if (m.kind === 'zombie') sfx.zombie();
    else if (m.kind === 'win_all') sfx.win();
    if (m && m.text && m.kind !== 'initial_discard') pushGameLog('zombLog', m.text);
  } catch (e) {}
  if (_origZombOnEvent) _origZombOnEvent(m);
};

// ═══════════════════════════════════════════════════════════════════════
// End background + sound module
// ═══════════════════════════════════════════════════════════════════════

// Tear down LiveKit on page unload so other peers see us drop immediately
// instead of waiting for LiveKit's keepalive timeout.
window.addEventListener('beforeunload', () => { try { lkDisconnect(); } catch (e) {} });
window.addEventListener('pagehide',     () => { try { lkDisconnect(); } catch (e) {} });

log("page loaded v4.1 BEAST (LiveKit voice, max " + MAX_PEERS + " peers)");
</script>
</body>
</html>"""


# ─── BACKGROUND TASKS ───────────────────────────────────────────────────────
async def keepalive():
    await asyncio.sleep(30)
    url = WEB_APP_URL if "onrender.com" in WEB_APP_URL else None
    while True:
        await asyncio.sleep(600)
        if url:
            try:
                async with aiohttp.ClientSession() as s:
                    async with s.get(url, timeout=15) as r:
                        print(f"[keepalive] {r.status}")
            except Exception as e:
                print(f"[keepalive] err: {e}")


async def memory_groomer():
    """periodically walks active chat files and applies the memory caps.
    v3.10: also strips stickers from old messages (mirrors the image expiry
    in _append_msg)."""
    await asyncio.sleep(120)
    while True:
        try:
            for rid in list(rooms.keys()):
                f = f"{rid}_chat.json"
                if not os.path.exists(f):
                    continue
                try:
                    sz = os.path.getsize(f)
                    if sz < 200_000:
                        continue
                    msgs = json_read(f, [])
                    before = len(msgs)
                    if len(msgs) > MAX_CHAT_MESSAGES:
                        msgs = msgs[-MAX_CHAT_MESSAGES:]
                    if len(msgs) > IMAGE_RETAIN_COUNT:
                        cutoff = len(msgs) - IMAGE_RETAIN_COUNT
                        for i in range(cutoff):
                            if isinstance(msgs[i], dict):
                                if msgs[i].get("image"):
                                    msgs[i]["image"] = ""
                                    msgs[i]["image_expired"] = True
                                if msgs[i].get("sticker"):
                                    msgs[i]["sticker"] = ""
                                    msgs[i]["sticker_expired"] = True
                    json_write(f, msgs)
                    new_sz = os.path.getsize(f)
                    if new_sz < sz - 50_000:
                        print(f"[groomer] trimmed {rid}: {sz//1024}KB -> {new_sz//1024}KB ({before} msgs)")
                except Exception as e:
                    print(f"[groomer] {rid} err: {e}")
        except Exception as e:
            print(f"[groomer] outer err: {e}")
        await asyncio.sleep(600)


async def main():
    print("=" * 60)
    print(f"Silent Hill Bot v4.1 BEAST MODE | {WEB_APP_URL} | Port {PORT}")
    print(f"Kyodo: {KYODO_OK} | Max peers per room: {MAX_PEERS_PER_ROOM}")
    print(f"Memory caps: {MAX_CHAT_MESSAGES} msgs/room, {IMAGE_RETAIN_COUNT} recent images, {MAX_IMAGE_BYTES//1000}KB per img")

    # restore rooms and tokens from previous run so active calls
    # survive Render deployments (rooms are otherwise purely in RAM).
    _restore_rooms()
    _restore_tokens()

    # pull stickers from GitHub before serving any traffic.
    # Render's free tier wipes the filesystem on cold start, so without
    # this, every uploaded sticker would disappear on the next restart.
    if GITHUB_TOKEN and GITHUB_REPO:
        print(f"[github-sync] pulling stickers from {GITHUB_REPO}/{GITHUB_STICKERS_PATH}@{GITHUB_BRANCH} ...")
        synced = await _github_sync_stickers_to_disk()
        print(f"[github-sync] {synced} sticker(s) downloaded fresh from GitHub")
        # write+delete probe — proves the token can actually push,
        # which is the ONLY way to be sure with fine-grained PATs (they 404
        # on writes they can't do, so a read-only token will appear "fine"
        # to a permissions GET).
        ok_write, reason = await _github_verify_write_permission()
        if ok_write:
            if reason == "ok":
                print(f"[github-sync] write probe PASSED — uploads will persist")
            else:
                # Partial success (write OK, delete not OK)
                print(f"[github-sync] write probe partial: {reason}")
        else:
            print("!" * 70)
            print("! [github-sync] WRITE PROBE FAILED                                  !")
            print(f"! Reason: {reason[:60]:<60}!" if len(reason) <= 60 else f"! Reason: {reason[:58]}..!")
            if len(reason) > 60:
                # Print the full reason on its own line, unwrapped
                print(f"! Full reason:")
                # Wrap to 66-char lines to fit our banner
                rest = reason
                while rest:
                    chunk, rest = rest[:66], rest[66:]
                    print(f"!   {chunk}")
            print("! Stickers will appear in-room but will VANISH on every restart.   !")
            print("! Existing s1.jpg..s5.jpg keep working (already in repo).          !")
            print("! Fix: github.com/settings/personal-access-tokens →                 !")
            print("!      this token → 'Contents: Read and write' on this repo.       !")
            print("!" * 70)
    else:
        print("!" * 70)
        print("! [github-sync] DISABLED — STICKER UPLOADS WILL NOT PERSIST            !")
        print(f"! GITHUB_TOKEN set: {bool(GITHUB_TOKEN)!s:<6}  GITHUB_REPO set: {bool(GITHUB_REPO)!s}                   !")
        print("! Set both env vars on Render to make uploaded stickers survive       !")
        print("! restarts. Without them, only s1.jpg..s5.jpg from the deploy stick.  !")
        print("!" * 70)

    print(f"Stickers folder: {STICKERS_DIR!r} | available now: {len(list_stickers())}")
    call_system.boot_log()
    print("Silent Hill BEAST MODE v4.1 — LiveKit SFU voice, top-quality fixed-bitrate")
    print(f"Voice: 96kbps Opus | RED+DTX | Max peers per room: {MAX_PEERS_PER_ROOM}")
    print("=" * 60)
    await asyncio.gather(
        Server(Config(app=app, host="0.0.0.0", port=PORT, log_level="warning")).serve(),
        run_kyodo_bot(),
        keepalive(),
        memory_groomer(),
    )


if __name__ == "__main__":
    asyncio.run(main())
