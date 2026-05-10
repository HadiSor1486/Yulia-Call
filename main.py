"""
Silent Hill Voice Call Bot — v3.12 BEAST MODE (IN-ROOM STICKER UPLOADS)
═══════════════════════════════════════════════════════════════════════════════
v3.12 — STICKERS CAN BE UPLOADED FROM INSIDE THE ROOM, ADMIN CAN DELETE,
        ALL WITH RAM SAFETY AND GITHUB PERSISTENCE.

  WHAT'S NEW SINCE v3.11:
    • [+] button in the sticker panel header lets ANY user upload a new
          sticker. The image is resized client-side first (max 1024px,
          WebP @ 0.85) so a 4 MB phone photo becomes ~80 KB before it
          ever leaves the device. The server then defensively re-resizes
          and re-encodes (Pillow → WebP @ q=85), strips EXIF, and writes
          to STICKERS_DIR. The new sticker is broadcast live to every
          peer in every room — it appears in the picker instantly, no
          rejoin needed.
    • Hard cap of 30 stickers (MAX_STICKERS env var). Upload at the cap
          is rejected with a clear toast — admin must delete first.
    • RAM guard: an asyncio.Semaphore(1) wraps the Pillow decode so we
          never have two heavy resize jobs in memory at once. Concurrent
          uploads queue. Plus a 5 MB hard limit on the raw upload bytes
          so a malicious payload can't OOM the bot.
    • Atomic count check via asyncio.Lock so two simultaneous uploads
          can't sneak past MAX_STICKERS.
    • Persistence on Render: if GITHUB_TOKEN + GITHUB_REPO env vars are
          set, every uploaded/deleted sticker is committed to your repo
          in the background via the GitHub Contents API. Survives
          restarts and redeploys. If not set, uploads still work for the
          current process lifetime.
    • Hidden-suffix admin auth: joining as "Sor-" (or any
          ADMIN_NAME_BASE+ADMIN_NAME_SUFFIX) marks the user admin
          server-side and strips the "-" before display. Plain "Sor" is
          treated as a regular peer with no badge and no powers. The
          Host badge and the delete (×) buttons on stickers now key off
          the server-trusted is_admin flag, never off the displayed
          name. Impersonation is structurally impossible.
    • Auto-generated unique filenames (up_<10hex>.webp) — no name
          collisions, no traversal surface.

  EVERYTHING FROM v3.11 IS UNTOUCHED:
    • Reverse-flex bottom-anchored messages (WhatsApp/Telegram pattern)
    • All scroll lock / unread / jump-button logic
    • All v3.10 stickers, replies, image preview, typing
    • All memory hardening, all WebRTC reliability
    • All TURN failover, all Kyodo bot integration

═══════════════════════════════════════════════════════════════════════════════
"""

import asyncio, json, os, re, time, uuid, hmac, hashlib, base64, io
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
from fastapi import FastAPI, Query, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from uvicorn import Config, Server

# v3.12: Pillow for sticker upload pipeline (resize + recompress to WebP).
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

# ─── CONFIG ─────────────────────────────────────────────────────────────────
# User explicitly requested credentials remain hardcoded as defaults.
# (They're not considered sensitive in this project.)
EMAIL = os.getenv("BOT_EMAIL", "hadidaoud.ha@gmail.com")
PASSWORD = os.getenv("BOT_PASSWORD", "yulia123")
DEVICE_ID = os.getenv("BOT_DEVICE_ID", "870d649515ce700797d6a56965689f3aaa7d5e82dfdce994b239e00e37238184")
CHAT_ID = os.getenv("BOT_CHAT_ID", "cmoxe9k5y1nqw0jdmeky1hya7")
CIRCLE_ID = os.getenv("BOT_CIRCLE_ID", "cm9bylrbn00hmux6t43mczt2o")
WEB_APP_URL = os.environ.get("WEB_APP_URL", "http://localhost:8000")
PORT = int(os.environ.get("PORT", "8000"))

# Room capacity. v3.8 hardened to 15 peers. Configurable via env var.
MAX_PEERS_PER_ROOM = int(os.environ.get("MAX_PEERS_PER_ROOM", "15"))

# v3.9 memory hardening
MAX_CHAT_MESSAGES = int(os.environ.get("MAX_CHAT_MESSAGES", "200"))
IMAGE_RETAIN_COUNT = int(os.environ.get("IMAGE_RETAIN_COUNT", "30"))
MAX_IMAGE_BYTES = int(os.environ.get("MAX_IMAGE_BYTES", "400000"))  # was 600000

# v3.10 stickers folder. Anything in this folder ending in .jpg/.jpeg/.png/.webp
# becomes a sticker. The server lists the directory live on each /stickers
# request, so adding files via GitHub redeploy or even a manual file drop
# makes them available immediately. Filenames must match SAFE_STICKER_NAME.
STICKERS_DIR = os.environ.get("STICKERS_DIR", "stickers")
STICKER_EXTS = {".jpg", ".jpeg", ".png", ".webp", ".gif"}
# Strict allowlist on filenames to prevent any path-traversal or weirdness:
# letters, digits, dots, dashes, underscores. Max length 64 to keep WS payloads
# tiny and to discourage anyone from stuffing data into filenames.
SAFE_STICKER_NAME = re.compile(r"^[A-Za-z0-9._-]{1,64}$")

# ── v3.12 sticker upload pipeline ───────────────────────────────────────────
# Goals: keep RAM bounded, keep stickers persistent across Render restarts.
#
# RAM strategy:
#   • Hard cap on upload payload (default 5 MB). Anything larger → reject
#     immediately, never decoded.
#   • Pillow processing happens inside an asyncio Semaphore(1) so at most one
#     image is in memory being decoded/resized at a time, regardless of how
#     many users hit upload simultaneously. Others queue.
#   • Output is always WebP @ 1024px max edge, quality 85. Typical result:
#     50–120 KB on disk. So 30 stickers ≈ 3 MB total disk footprint.
#
# Persistence strategy (Render's free tier wipes the FS on restart/redeploy):
#   • Always write to local STICKERS_DIR first → instant availability.
#   • If GITHUB_TOKEN + GITHUB_REPO env vars set, also commit to GitHub in
#     the background. On next Render deploy/restart the file is back. This
#     means uploaded stickers behave exactly like manually-committed ones.
#   • If GitHub creds aren't set, uploads still work but are ephemeral.
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

# TURN provider env vars
METERED_API_KEY = os.environ.get("METERED_API_KEY", "")
METERED_DOMAIN = os.environ.get("METERED_DOMAIN", "")
CF_TURN_TOKEN_ID = os.environ.get("CF_TURN_TOKEN_ID", "")
CF_TURN_API_TOKEN = os.environ.get("CF_TURN_API_TOKEN", "")
CUSTOM_TURN_URL = os.environ.get("CUSTOM_TURN_URL", "")
CUSTOM_TURN_USER = os.environ.get("CUSTOM_TURN_USER", "")
CUSTOM_TURN_PASS = os.environ.get("CUSTOM_TURN_PASS", "")

tokens: Dict[str, dict] = {}
rooms: Dict[str, dict] = {}
kyodo_client = None
_turn_cache = {"servers": None, "expires": 0}
_turn_lock = asyncio.Lock()  # dedupes concurrent cold-cache fetches

# v3.12: Semaphore(1) ensures at most one Pillow decode/resize happens at a
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


# ── v3.12 image processing & GitHub persistence ────────────────────────────
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
    """v3.12.5: Boot-time sanity check that the configured token can WRITE
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
    """v3.12.1: Pull every sticker from the GitHub repo into the local
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


# ─── TURN CREDENTIAL FETCHING ───────────────────────────────────────────────
async def fetch_metered_creds() -> List[dict]:
    """Fetch TURN creds from Metered. Tries SECRET KEY first (POST), then
    API KEY (GET) — whichever value the user pasted into METERED_API_KEY."""
    if not METERED_API_KEY or not METERED_DOMAIN:
        return []

    # ATTEMPT 1: SECRET KEY → POST to mint a fresh credential
    try:
        create_url = f"https://{METERED_DOMAIN}/api/v1/turn/credential?secretKey={METERED_API_KEY}"
        async with aiohttp.ClientSession() as s:
            async with s.post(
                create_url,
                json={"expiryInSeconds": 3600, "label": "silenthill-bot"},
                timeout=10,
            ) as r:
                body = await r.text()
                if r.status == 200:
                    try:
                        cred = json.loads(body)
                    except Exception:
                        cred = None
                    if isinstance(cred, dict) and cred.get("apiKey"):
                        list_url = f"https://{METERED_DOMAIN}/api/v1/turn/credentials?apiKey={cred['apiKey']}"
                        async with s.get(list_url, timeout=10) as r2:
                            list_body = await r2.text()
                            if r2.status == 200:
                                try:
                                    data = json.loads(list_body)
                                except Exception as e:
                                    print(f"[turn] METERED list JSON err: {e}")
                                    data = None
                                if isinstance(data, list) and data:
                                    print(f"[turn] METERED OK via SECRET KEY ({len(data)} ICE entries)")
                                    return data
                                print(f"[turn] METERED list shape unexpected: {list_body[:200]}")
                            else:
                                print(f"[turn] METERED list HTTP {r2.status}: {list_body[:200]}")
                    else:
                        print(f"[turn] METERED secret-key POST returned no apiKey; body={body[:200]}")
    except asyncio.TimeoutError:
        print(f"[turn] METERED secret-key POST TIMEOUT")
    except Exception as e:
        print(f"[turn] METERED secret-key POST EXCEPTION: {type(e).__name__}: {e}")

    # ATTEMPT 2: API KEY → GET
    try:
        url = f"https://{METERED_DOMAIN}/api/v1/turn/credentials?apiKey={METERED_API_KEY}"
        async with aiohttp.ClientSession() as s:
            async with s.get(url, timeout=10) as r:
                body_text = await r.text()
                if r.status == 200:
                    try:
                        data = json.loads(body_text)
                    except Exception as e:
                        print(f"[turn] METERED apikey JSON err: {e}; body={body_text[:200]}")
                        return []
                    if isinstance(data, list) and len(data) > 0:
                        print(f"[turn] METERED OK via API KEY ({len(data)} ICE entries)")
                        return data
                    print(f"[turn] METERED apikey shape unexpected: {body_text[:200]}")
                    return []
                print(f"[turn] METERED apikey HTTP {r.status}: {body_text[:300]}")
                if r.status == 401:
                    print(f"[turn] !!! Both SECRET KEY and API KEY rejected with 401")
                return []
    except asyncio.TimeoutError:
        print(f"[turn] METERED apikey TIMEOUT")
        return []
    except Exception as e:
        print(f"[turn] METERED apikey EXCEPTION: {type(e).__name__}: {e}")
    return []


async def fetch_cloudflare_creds() -> List[dict]:
    if not CF_TURN_TOKEN_ID or not CF_TURN_API_TOKEN:
        return []
    try:
        url = f"https://rtc.live.cloudflare.com/v1/turn/keys/{CF_TURN_TOKEN_ID}/credentials/generate-ice-servers"
        headers = {"Authorization": f"Bearer {CF_TURN_API_TOKEN}", "Content-Type": "application/json"}
        async with aiohttp.ClientSession() as s:
            async with s.post(url, headers=headers, json={"ttl": 3600}, timeout=10) as r:
                if r.status in (200, 201):
                    data = await r.json()
                    return data.get("iceServers", [])
    except Exception as e:
        print(f"[turn] cf err: {e}")
    return []


async def get_ice_servers() -> List[dict]:
    # Lock prevents thundering-herd on cold cache
    async with _turn_lock:
        if _turn_cache["servers"] and time.time() < _turn_cache["expires"]:
            return _turn_cache["servers"]

        servers: List[dict] = [
            {"urls": ["stun:stun.l.google.com:19302",
                      "stun:stun1.l.google.com:19302",
                      "stun:stun2.l.google.com:19302"]},
            {"urls": "stun:stun.cloudflare.com:3478"},
            {"urls": "stun:global.stun.twilio.com:3478"},
        ]

        metered = await fetch_metered_creds()
        metered_ok = bool(metered)
        if metered:
            servers.extend(metered)
            print(f"[turn] using Metered.ca ({len(metered)} URLs)")

        cf = await fetch_cloudflare_creds()
        cf_ok = bool(cf)
        if cf:
            servers.extend(cf)
            print(f"[turn] using Cloudflare ({len(cf)} URLs)")

        if CUSTOM_TURN_URL and CUSTOM_TURN_USER:
            servers.append({"urls": CUSTOM_TURN_URL,
                            "username": CUSTOM_TURN_USER,
                            "credential": CUSTOM_TURN_PASS})
            print(f"[turn] using custom TURN")

        servers.extend([
            {"urls": "turn:openrelay.metered.ca:80",
             "username": "openrelayproject", "credential": "openrelayproject"},
            {"urls": "turn:openrelay.metered.ca:443",
             "username": "openrelayproject", "credential": "openrelayproject"},
            {"urls": "turn:openrelay.metered.ca:443?transport=tcp",
             "username": "openrelayproject", "credential": "openrelayproject"},
            {"urls": "turns:openrelay.metered.ca:443?transport=tcp",
             "username": "openrelayproject", "credential": "openrelayproject"},
        ])

        _turn_cache["servers"] = servers
        metered_configured = bool(METERED_API_KEY and METERED_DOMAIN)
        cf_configured = bool(CF_TURN_TOKEN_ID and CF_TURN_API_TOKEN)
        expected_premium = metered_configured or cf_configured
        got_premium = metered_ok or cf_ok or bool(CUSTOM_TURN_URL)
        if expected_premium and not got_premium:
            print("[turn] !! PREMIUM CONFIGURED BUT FETCH FAILED — short cache (60s)")
            _turn_cache["expires"] = time.time() + 60
        else:
            _turn_cache["expires"] = time.time() + 1800
        return servers


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
                        asyncio.create_task(_noshow(rid))
                        link = f"{WEB_APP_URL}/call/{rid}?t={tok}"
                        await kyodo_client.send_message(
                            m.chatId,
                            f"Silent Hill Voice Session\n{link}\nTap to join the call.",
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


# ── v3.10 sticker endpoints ───────────────────────────────────────────────
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


@app.get("/turn")
async def turn_endpoint():
    servers = await get_ice_servers()
    return JSONResponse({"iceServers": servers})


@app.get("/health/github")
async def health_github():
    """v3.12.4: Browser-accessible diagnostic for sticker persistence.
    Hit https://<your-app>.onrender.com/health/github to see whether the
    GitHub creds work and uploads will survive restarts. Safe to share —
    we never echo the token, only whether it's set and whether the probe
    passed.
    """
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


@app.get("/turn-status")
async def turn_status():
    servers = await get_ice_servers()
    has_real_turn = False
    for entry in servers:
        urls = entry.get("urls", [])
        if isinstance(urls, str):
            urls = [urls]
        username = entry.get("username", "")
        for u in urls:
            if "turn" in u and "openrelayproject" not in username:
                has_real_turn = True
                break
        if has_real_turn:
            break
    return JSONResponse({
        "premium": has_real_turn,
        "metered_configured": bool(METERED_API_KEY),
        "cloudflare_configured": bool(CF_TURN_TOKEN_ID),
        "custom_configured": bool(CUSTOM_TURN_URL),
    })


@app.get("/turn-debug")
async def turn_debug():
    servers = await get_ice_servers()
    sanitized = []
    for entry in servers:
        urls = entry.get("urls", [])
        if isinstance(urls, str):
            urls = [urls]
        username = entry.get("username", "")
        cred = entry.get("credential", "")
        sanitized.append({
            "urls": urls,
            "username": username[:4] + "..." if username else "",
            "has_credential": bool(cred),
        })
    room_sizes = {rid: len(r["peers"]) for rid, r in rooms.items()}
    return JSONResponse({
        "total_entries": len(servers),
        "metered_env_set": bool(METERED_API_KEY and METERED_DOMAIN),
        "metered_domain": METERED_DOMAIN if METERED_DOMAIN else "(unset)",
        "cloudflare_env_set": bool(CF_TURN_TOKEN_ID and CF_TURN_API_TOKEN),
        "custom_env_set": bool(CUSTOM_TURN_URL),
        "cache_expires_in": int(_turn_cache.get("expires", 0) - time.time()),
        "max_peers_per_room": MAX_PEERS_PER_ROOM,
        "active_rooms": len(rooms),
        "room_sizes": room_sizes,
        "sticker_count": len(list_stickers()),
        "entries": sanitized,
    })


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
            # v3.12 hidden-suffix admin detection. "Sor-" → ("Sor", True).
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

    # v3.12: tell the client whether they're admin so they can show the
    # delete (×) buttons on stickers. The client never decides this on its
    # own — server is source of truth.
    # Also tell them whether they're host so the seat tile can render the
    # Host badge / gold frame on their own avatar.
    await ws.send_json({"type": "your_id", "id": peer_id,
                        "max_peers": MAX_PEERS_PER_ROOM,
                        "is_admin": is_admin,
                        "is_host": is_host})

    # v3.10: also send the current sticker list on join, so the picker is
    # ready to open instantly without an extra round trip. Client also
    # refreshes from /stickers each open in case new files have arrived.
    await ws.send_json({"type": "stickers", "stickers": list_stickers()})

    # v3.9: build history payload with bounded memory cost. Send last 100
    # messages but strip image/sticker data from all but the most recent 30.
    full_history = json_read(room["chat_file"])
    history_slice = full_history[-100:] if full_history else []
    if len(history_slice) > IMAGE_RETAIN_COUNT:
        keep_from = len(history_slice) - IMAGE_RETAIN_COUNT
        for i, m in enumerate(history_slice):
            if i < keep_from and isinstance(m, dict):
                if m.get("image"):
                    m = {**m, "image": "", "image_expired": True}
                # v3.10: also expire stickers in old messages, mirrors images.
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

            if mt == "chat":
                text = msg.get("text", "").strip()[:1000]
                image = msg.get("image", "") or ""
                if image and (not isinstance(image, str) or len(image) > MAX_IMAGE_BYTES
                              or not image.startswith("data:image/")):
                    image = ""

                # v3.10 sticker handling. Client sends just the filename
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
                        # v3.10: replies-to-stickers
                        "has_sticker": bool(rt.get("has_sticker")),
                    }
                cm = {"type": "chat", "kind": "user",
                      "id": str(uuid.uuid4())[:12],
                      "peer_id": peer_id,
                      "name": name, "avatar": avatar, "text": text,
                      "is_admin": is_admin,  # v3.12: server-trusted badge
                      "time": datetime.now().isoformat()}
                if image:
                    cm["image"] = image
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

            elif mt in ("webrtc_offer", "webrtc_answer", "webrtc_ice", "request_relay"):
                target = msg.get("to")
                msg["from"] = peer_id
                if target and target in room["peers"]:
                    try:
                        await room["peers"][target]["ws"].send_json(msg)
                    except Exception:
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

            # ── v3.12 sticker upload ─────────────────────────────────────
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

                # v3.12.1: AWAIT the GitHub commit before telling the user
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
                    # v3.12.3: GitHub creds missing → upload still works for
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

            # ── v3.12 sticker delete (admin only) ────────────────────────
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

                # v3.12.1: await GitHub delete so we know it persisted.
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
.msg-row{cursor:pointer;transition:opacity .15s}
.msg-row:active{opacity:0.6}
.msg-row.system{cursor:default}
.msg-row.system:active{opacity:1}
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
.seat-av{--lvl:0;position:relative;width:64px;height:64px;border-radius:50%;background:#2c2c2e;display:flex;align-items:center;justify-content:center;font-size:22px;font-weight:600;color:#8e8e93;overflow:hidden;border:3px solid transparent;transition:border-color .14s;box-sizing:border-box}
.seat-av img{width:100%;height:100%;object-fit:cover;display:block}
.seat-av.speaking{border-color:#34c759}
/* Voice-reactive halo: a pseudo-element behind the avatar.
   v3.12.9 fix: CSS custom properties inside calc() don't interpolate
   by default — they snap. So in v3.12.8 the halo looked frozen. The
   fix is two-fold:
     1. Register --lvl with @property so the browser knows it's a
        <number> and CAN interpolate it across transitions. Then
        anything using calc(... * var(--lvl)) animates smoothly.
     2. Layer a visible scale-pulse on top, driven by the level too —
        gives a real "breathing" feel even when level is steady.
   Browsers without @property support (older Safari) still get the
   transitions on opacity + transform, so the ring still moves. */
@property --lvl {
  syntax: '<number>';
  inherits: true;
  initial-value: 0;
}
/* The halo: position, base styles, and the level-driven box-shadow.
   transform/opacity are set in .speaking so a continuous breath
   animation can play on top without fighting the static rule. */
.seat-av::after{content:'';position:absolute;inset:-3px;border-radius:50%;pointer-events:none;box-shadow:0 0 calc(6px + 26px * var(--lvl)) calc(1px + 6px * var(--lvl)) rgba(52,199,89,calc(0.18 + 0.78 * var(--lvl)));opacity:0;transform:scale(1);transition:opacity .18s linear,box-shadow .14s ease-out;will-change:box-shadow,opacity,transform}
/* When speaking: show the halo (opacity:1) AND run a continuous breath
   keyframe so the ring visibly pulses even when audio level is steady.
   The breath adds a ±6% scale + ±15% opacity ripple on a 1.4s cycle —
   subtle enough to feel organic, not robotic. The level-driven
   box-shadow runs in parallel, so loud speech still produces a bigger
   bloom on top of the breath. */
.seat-av.speaking::after{opacity:1;animation:seatHaloBreath 1.4s ease-in-out infinite}
@keyframes seatHaloBreath{
  0%,100%{transform:scale(calc(1 + 0.04 * var(--lvl)));opacity:0.85}
  50%   {transform:scale(calc(1.06 + 0.08 * var(--lvl)));opacity:1}
}
/* Subtle continuous heartbeat for the ring border so it never looks
   completely static when someone speaks at a steady volume. This is
   independent of the level-driven halo above — it's a slow, gentle
   border-color pulse that just adds life. */
.seat-av.speaking{animation:seatBorderPulse 1.6s ease-in-out infinite}
@keyframes seatBorderPulse{0%,100%{border-color:rgba(52,199,89,0.85)}50%{border-color:rgba(52,199,89,1)}}
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
<button class="leave-header-btn" onclick="leaveCall()" title="Leave call"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M9 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h4"/><polyline points="16 17 21 12 16 7"/><line x1="21" y1="12" x2="9" y2="12"/></svg></button>
<button class="menu-btn" onclick="document.getElementById('dbg').classList.toggle('show')">&#8942;</button>
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

<script>
// ════════════════════════════════════════════════════════════════════════════
// SILENT HILL CLIENT — v3.12 BEAST MODE (IN-ROOM STICKER UPLOADS)
// ════════════════════════════════════════════════════════════════════════════

const ROOM = "__ROOM_ID__", TOKEN = "__TOKEN__";
const MAX_PEERS = parseInt("__MAX_PEERS__", 10) || 11;
let MY_ID = "";
// v3.12: server tells us in `your_id` whether we joined as admin (i.e.
// whether our typed name had the hidden suffix). Used to show the delete
// (×) buttons on the sticker grid. Never trust the client to set this.
let MY_IS_ADMIN = false;
let serverMaxPeers = MAX_PEERS;
let ws = null, localStream = null, myName = "", myAvatar = "";
// v3.12: myJoinName preserves the RAW name typed by the user (which may
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
// v3.12.2: track full-rebuild attempts per peer. After MAX_FULL_REBUILDS
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

// v3.10: list of available stickers (filenames). Refreshed on join and
// every time the user opens the picker, so adding a file to the GitHub
// stickers/ folder appears without restart.
let stickerList = [];
let stickerPanelOpen = false;

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
// v3.10 INPUT WIRING — typing indicator + auto-grow + sticker icon visibility
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
// v3.10 STICKERS — fetch list, render grid, send, panel open/close
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
  // v3.12: keep an in-progress uploading row at the top if active.
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

// ── v3.12 sticker upload (client) ────────────────────────────────────────
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
      // v3.12.3: server says the upload succeeded for this session but
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
  // v3.12: if user typed the hidden admin suffix (e.g. "Sor-"), strip it
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
  // v3.12.8: 80ms ticker (was 150ms). Drives the voice-reactive halo —
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
        break;

      case 'stickers':
        if (Array.isArray(m.stickers)) {
          stickerList = m.stickers;
          log("stickers (push): " + stickerList.length);
          // v3.12: if the panel is open, refresh in place so users see
          // freshly-uploaded or freshly-deleted stickers immediately.
          if (stickerPanelOpen) renderStickerGrid();
        }
        break;

      case 'sticker_result':
        // v3.12: response to an upload or delete attempt. Show inline status.
        handleStickerResult(m);
        break;

      case 'history':
        // v3.11: history arrives oldest-first. With column-reverse, the
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
        renderSys(m.name + ' left');
        updCount();
        updPeers();
        applyBitrateToAll();
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
            // v3.12.7: when a peer mutes, clear their cached speaking flag
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
          // v3.12.7: ignore speaking events from a peer we know is muted.
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
// • The grid is 3 columns wide. Six tiles fit on screen at once. With more
//   than 6 participants, the grid scrolls vertically inside .seat-grid-wrap.
// • Empty placeholder tiles are shown when the room isn't full (up to 6
//   visible) so the layout stays balanced — purely cosmetic.
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
  // The mute badge sits on .seat-av-wrap (NOT .seat-av) so it isn't clipped
  // by the avatar's overflow:hidden. This lets it overlap the bottom-right
  // corner from outside the circle, matching the screenshot reference.
  return (
    '<div class="seat"' + pidAttr + '>' +
      '<div class="seat-av-wrap">' +
        '<div class="seat-av' + speakingClass + hostFrame + '">' +
          _avatarHTML(opts.name, opts.avatar) +
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

  // Pull-tab count (shown when panel is collapsed)
  const pullCount = document.getElementById('seatPullCount');
  if (pullCount) pullCount.textContent = total + '/' + serverMaxPeers + ' in call';
}

function updPeerLevels() {
  // Cheap per-frame update: flips .speaking class AND writes a per-tile
  // CSS variable --lvl (0..1) that the .seat-av::after halo reads from
  // via calc(). This is what gives the green ring its real, organic
  // voice-reactive pulse — the halo's blur, spread, and opacity all
  // scale with --lvl. Updating a CSS custom property is cheap (no
  // layout/paint thrash if used in already-composited properties like
  // box-shadow and opacity).
  const grid = document.getElementById('seatGrid');
  if (!grid) return;

  // Tiny helper: map a raw mic level (often 0..0.3 for normal speech)
  // to a 0..1 visual range with a soft floor and ceiling. The ^0.7
  // gamma curve makes quiet talking visible without making loud talking
  // overdrive into pure white. Clamp to [0, 1].
  const shape = (raw) => {
    if (!raw || raw < 0.04) return 0;          // below "speaking" threshold
    const x = Math.min(1, (raw - 0.04) / 0.35); // 0..1 normalized
    return Math.pow(x, 0.7);
  };

  // ── self tile ───────────────────────────────────────────────────────
  const selfTile = grid.querySelector('.seat[data-self="1"] .seat-av');
  if (selfTile) {
    const isActive = !!window._selfSpeaking && !isMuted;
    if (isActive && !selfTile.classList.contains('speaking')) selfTile.classList.add('speaking');
    else if (!isActive && selfTile.classList.contains('speaking')) selfTile.classList.remove('speaking');
    const lvl = isActive ? shape(window._selfLevel || 0) : 0;
    selfTile.style.setProperty('--lvl', lvl.toFixed(3));
  }

  // ── remote peer tiles ───────────────────────────────────────────────
  grid.querySelectorAll('.seat[data-pid]').forEach(seat => {
    const pid = seat.getAttribute('data-pid');
    const p = peerMap.get(pid);
    if (!p) return;
    const av = seat.querySelector('.seat-av');
    if (!av) return;
    // v3.12.7: muted peers never glow green, even if stale speaking flags
    // are still set. The mute override matches what updPeers() does on
    // full re-render; this keeps the per-frame ticker consistent.
    const isActive = !p.muted && !!(p.speaking || p.actuallyHeard);
    if (isActive && !av.classList.contains('speaking')) av.classList.add('speaking');
    else if (!isActive && av.classList.contains('speaking')) av.classList.remove('speaking');
    // v3.12.8: drive the halo intensity from the real-time inbound audio
    // level (p.recvLevel is sampled at 200ms from the peer's MediaStream).
    const lvl = isActive ? shape(p.recvLevel || 0) : 0;
    av.style.setProperty('--lvl', lvl.toFixed(3));
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
// v3.11 SCROLL LOGIC FOR REVERSE-FLEX MESSAGE LIST
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

function startConnectionTimer(pid) {
  const pc = peers[pid];
  if (!pc || pc._connTimer) return;
  pc._connTimerFires = pc._connTimerFires || 0;
  const timeoutMs = connTimerMs();
  pc._connTimer = setTimeout(() => {
    pc._connTimer = null;
    if (peers[pid] !== pc || pc.connectionState === 'connected' || pc.connectionState === 'closed') {
      return;
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
    if (pc.iceConnectionState === 'new' && pc._connTimerFires === 0) {
      log("CONN-TIMER " + pid + " brand new PC, more time");
      pc._connTimerFires++;
      startConnectionTimer(pid);
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
  // v3.12.2: guard against null/empty streams. Happens when remote peer
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
    inboundLevelTimers[pid] = setInterval(() => {
      analyser.getByteFrequencyData(data);
      let sum = 0;
      for (let i = 0; i < data.length; i++) sum += data[i];
      const level = sum / data.length / 255;
      const p = peerMap.get(pid);
      if (p) {
        p.recvLevel = level;
        p.actuallyHeard = level > 0.02;
        if (level > 0.02) p.lastHeardAt = Date.now();
      }
    }, 200);
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

      // v3.12.2: if packets are actually flowing, this peer is healthy
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
  // v3.12.2: stop infinite rebuild loops on permanently-broken peers.
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
    const MIN_BROADCAST_INTERVAL = 500;
    localLevelTimer = setInterval(() => {
      if (isMuted || !localStream) return;
      localAnalyser.getByteFrequencyData(data);
      let sum = 0;
      for (let i = 0; i < data.length; i++) sum += data[i];
      const level = sum / data.length / 255;
      const now = Date.now();
      const speaking = level > 0.05;
      const wasSpeaking = lastLevel > 0.05;
      const stateChanged = speaking !== wasSpeaking;
      const enoughTimePassed = now - lastSent >= MIN_BROADCAST_INTERVAL;
      if (stateChanged || (speaking && enoughTimePassed)) {
        if (ws && ws.readyState === 1) {
          ws.send(JSON.stringify({ type: 'speaking', level: speaking ? level : 0 }));
          lastSent = now;
        }
      }
      // Also drive my own seat-tile green ring locally (no need to wait for
      // a server roundtrip — feels instant and avoids the 500ms throttle).
      window._selfSpeaking = speaking;
      // v3.12.8: expose the raw level too, so the seat tile's voice-reactive
      // halo can scale with real audio amplitude (not just on/off).
      window._selfLevel = level;
      lastLevel = level;
    }, 200);
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
}

let _muteDebounceTimer = null;

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

  // v3.12.8: when muting, instantly zero out the level/speaking flags so
  // the voice-reactive ring drops to dark on the next animation frame
  // without waiting for the next mic-level tick.
  if (isMuted) {
    window._selfSpeaking = false;
    window._selfLevel = 0;
  }

  const b = document.getElementById('muteBtn');
  if (isMuted) {
    b.classList.add('muted');
    b.innerHTML = '<svg id="micIcon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="1" y1="1" x2="23" y2="23"/><path d="M9 9v6a3 3 0 0 0 5.12 2.12M15 9.34V4a3 3 0 0 0-5.94-.6"/><path d="M17 16.95A7 7 0 0 1 5 12v-2m14 0v2a7 7 0 0 1-.11 1.23"/><line x1="12" y1="19" x2="12" y2="23"/><line x1="8" y1="23" x2="16" y2="23"/></svg>';
  } else {
    b.classList.remove('muted');
    b.innerHTML = '<svg id="micIcon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 1a3 3 0 0 0-3 3v8a3 3 0 0 0 6 0V4a3 3 0 0 0-3-3z"/><path d="M19 10v2a7 7 0 0 1-14 0v-2"/><line x1="12" y1="19" x2="12" y2="23"/><line x1="8" y1="23" x2="16" y2="23"/></svg>';
  }

  if (_muteDebounceTimer) clearTimeout(_muteDebounceTimer);
  _muteDebounceTimer = setTimeout(() => {
    if (ws && ws.readyState === 1) {
      ws.send(JSON.stringify({ type: isMuted ? 'mute_me' : 'unmute_me' }));
    }
  }, 150);

  updPeers();
}

let replyingTo = null;

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
          alert("Image too large after compression");
          return;
        }
        sendImageMsg(data2);
      } else {
        sendImageMsg(data);
      }
    };
    img.onerror = () => log("img decode fail");
    img.src = ev.target.result;
  };
  r.readAsDataURL(f);
}

function sendImageMsg(dataUrl) {
  if (!ws || ws.readyState !== 1) return;
  if (typingTimer) { clearTimeout(typingTimer); typingTimer = null; }
  ws.send(JSON.stringify({ type: 'typing_stop' }));
  const payload = { type: 'chat', text: '', image: dataUrl };
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

function openImagePreview(src) {
  const overlay = document.createElement('div');
  overlay.className = 'img-preview-overlay';
  overlay.innerHTML = '<span class="close-hint">&times;</span><img src="' + esc(src) + '">';
  overlay.onclick = () => overlay.remove();
  document.body.appendChild(overlay);
}

// ════════════════════════════════════════════════════════════════════════════
// v3.11 MESSAGE INSERTION — REVERSE FLEX PRIMITIVE
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

function renderMsg(m) {
  const c = document.getElementById('msgs'); if (!c) return;

  // v3.11: detect "was at bottom" BEFORE we add the new node. With
  // column-reverse, distanceFromVisualBottom is |scrollTop|, which is
  // unaffected by the insertion (the browser anchors scroll naturally).
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
    // v3.12: badge is based on server-trusted is_admin flag, not name.
    // The flag flows from: name ends with hidden suffix → server detects →
    // sets is_admin=True on chat message envelope. Plain name match is
    // gone, so impersonators can't sneak a badge.
    const showBadge = !!(m.is_admin || pi.is_admin);
    const avSrc = m.avatar || pi.avatar || '';

    const hasSticker = !!(m.sticker || m.sticker_expired);
    const hasImage = !!(m.image || m.image_expired);
    const hasText = !!(m.text && m.text.length > 0);
    const stickerOnly = hasSticker && !hasText && !hasImage;

    const row = document.createElement('div');
    row.className = 'msg-row ' + (isSelf ? 'self' : 'other');
    if (stickerOnly) row.classList.add('has-sticker-only');

    let avHTML;
    if (avSrc) avHTML = '<div class="avatar"><img src="' + esc(avSrc) + '"></div>';
    else avHTML = '<div class="avatar"><span>' + esc(name[0].toUpperCase()) + '</span></div>';
    const header = '<div class="msg-header"><span class="msg-name">' + esc(name) + '</span>' + (showBadge ? '<span class="msg-badge host">Host</span>' : '') + '</div>';

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

    row.innerHTML = avHTML + '<div class="msg-content">' + contentHTML + '</div>';

    row.addEventListener('click', (ev) => {
      const t = ev.target;
      if (t.classList && t.classList.contains('chat-img')) {
        ev.stopPropagation();
        openImagePreview(t.src);
        return;
      }
      if (t.tagName === 'IMG' && t.closest('.avatar')) return;
      startReply(m);
    });

    appendToVisualBottom(c, row);
  }

  // v3.11: with column-reverse, the browser already keeps the bottom
  // pinned automatically when content is added at the visual bottom and
  // the user is at the bottom. We still nudge scrollTop to 0 for self
  // messages (so we ALWAYS see what we just sent) and for the
  // wasAtBottom case (defensive: keep us pinned even if some browser
  // edge case nudged the scroll). When the user is scrolled UP reading
  // history, we don't move them — we just bump the unread badge.
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
  // v3.10.1: keep the mobile keyboard up after sending. Without this, the
  // keyboard collapses on every send because some mobile browsers blur the
  // textarea when the value is reset programmatically.
  inEl.focus();
}

function leaveCall() {
  log("leave");
  leaving = true;
  if (ws && ws.readyState === 1) ws.close();
  cleanupRTC();
  try { window.close(); } catch (e) {}
  document.body.innerHTML = '<div style="display:flex;align-items:center;justify-content:center;height:100vh;background:#000;color:#fff;font-family:sans-serif"><h2>Left the call</h2></div>';
}

window.addEventListener('beforeunload', () => {
  leaving = true;
  cleanupRTC();
});

log("page loaded v3.12 (max " + MAX_PEERS + " peers, in-room sticker uploads, seat-grid UI)");
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
    """v3.9: periodically walks active chat files and applies the memory caps.
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
    print(f"Silent Hill Bot v3.12.1 BEAST MODE | {WEB_APP_URL} | Port {PORT}")
    print(f"Kyodo: {KYODO_OK} | Max peers per room: {MAX_PEERS_PER_ROOM}")
    print(f"Memory caps: {MAX_CHAT_MESSAGES} msgs/room, {IMAGE_RETAIN_COUNT} recent images, {MAX_IMAGE_BYTES//1000}KB per img")

    # v3.12.1: pull stickers from GitHub before serving any traffic.
    # Render's free tier wipes the filesystem on cold start, so without
    # this, every uploaded sticker would disappear on the next restart.
    if GITHUB_TOKEN and GITHUB_REPO:
        print(f"[github-sync] pulling stickers from {GITHUB_REPO}/{GITHUB_STICKERS_PATH}@{GITHUB_BRANCH} ...")
        synced = await _github_sync_stickers_to_disk()
        print(f"[github-sync] {synced} sticker(s) downloaded fresh from GitHub")
        # v3.12.4: write+delete probe — proves the token can actually push,
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
    servers = await get_ice_servers()
    print(f"ICE servers configured: {len(servers)} entries")
    has_premium = bool(METERED_API_KEY or CF_TURN_TOKEN_ID or CUSTOM_TURN_URL)
    if METERED_API_KEY:
        print(f"Metered.ca configured: domain={METERED_DOMAIN}")
    if CF_TURN_TOKEN_ID:
        print("Cloudflare TURN configured")
    if CUSTOM_TURN_URL:
        print("Custom TURN configured")
    if not has_premium:
        print("!" * 60)
        print("! NO PREMIUM TURN — Gulf/MENA peers WILL fail without it !")
        print("!" * 60)
    print("v3.8: 15-peer cap, shared AudioContext, 4-tier bitrate,")
    print("v3.8: speaking throttle, scaled timers, signaling-state recovery")
    print("v3.9: memory caps + groomer (fits Render free tier)")
    print("v3.10: stickers panel + auto-growing textarea")
    print("v3.11: messages bottom-anchored via column-reverse flex")
    print("v3.12: in-room sticker uploads + admin via hidden suffix")
    print("v3.12.1: sync GitHub stickers on boot, await commits for reliability")
    print("v3.12.2: pro seat-grid UI (avatar tiles, speaking ring, drag-collapse)")
    print("v3.12.3: keyboard-proof seat panel overlay + GitHub perm self-check")
    print("v3.12.4: viewport-locked layout (header never lifts) + real write probe + /health/github")
    print("v3.12.5: read-only perm probe (no more boot-time commits → no more deploy storms)")
    print("v3.12.6: removed hardcoded credentials — now env-only (security)")
    print("v3.12.7: mute is a hard override on the speaking ring (no more stuck green)")
    print("v3.12.8: single horizontal seat row + voice-reactive halo (real audio-driven)")
    print("v3.12.9: halo actually animates now (@property for var interp + continuous breath)")
    print("v3.12.10: trailing invite-slot — always 1 empty after real seats, capped at room max")
    print("=" * 60)
    await asyncio.gather(
        Server(Config(app=app, host="0.0.0.0", port=PORT, log_level="warning")).serve(),
        run_kyodo_bot(),
        keepalive(),
        memory_groomer(),
    )


if __name__ == "__main__":
    asyncio.run(main())
