"""
Silent Hill Voice Call Bot — v3.10 BEAST MODE (STICKERS + GROW-INPUT)
═══════════════════════════════════════════════════════════════════════════════
v3.10 — STICKERS & GROWING TEXT INPUT:

  NEW IN v3.10:
    1. STICKERS PANEL — like Kyodo. A sticker icon sits on the right side
       INSIDE the message input. Tap → bottom sheet opens with all stickers
       from the `stickers/` folder (s1.jpg, s2.jpg, ...). Tap a sticker to
       send. Sheet closes automatically. The icon hides when the user is
       typing and reappears when the input is empty.
    2. STICKERS ARE LIVE — server reads the `stickers/` directory on every
       /stickers request, so adding s12.jpg to GitHub instantly appears in
       all rooms (no restart needed). No build step.
    3. STICKERS RENDER WITHOUT A BUBBLE — they appear inline at small size
       (max 140px), not clickable, no chrome around them. Just like Kyodo.
    4. REPLY-TO-STICKER works — replies show "Sticker" as the preview.
    5. STICKERS RESPECT MEMORY CAPS — when a message ages out beyond
       IMAGE_RETAIN_COUNT, its sticker reference is stripped just like
       images. (Stickers are URLs not base64 so RAM impact is tiny, but we
       keep the same expiry semantics for consistency.)
    6. AUTO-GROWING TEXT INPUT — was a single-line input that hid text
       when it overflowed. Now a textarea that grows up to 3 lines, then
       scrolls internally. Enter sends, Shift+Enter inserts a newline.
       Identical look at rest.

  PRESERVED FROM v3.9:
    • Memory hardening: MAX_CHAT_MESSAGES, IMAGE_RETAIN_COUNT, MAX_IMAGE_BYTES
    • memory_groomer() background task
    • image_expired placeholder rendering
    • All v3.8 WebRTC reliability and big-room scaling

═══════════════════════════════════════════════════════════════════════════════
"""

import asyncio, json, os, re, time, uuid, hmac, hashlib, base64
from datetime import datetime
from typing import Any, Dict, List

import aiohttp
from fastapi import FastAPI, Query, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from uvicorn import Config, Server

try:
    from kyodo import ChatMessage, EventType, AsyncClient as Client
    KYODO_OK = True
except ImportError:
    KYODO_OK = False

# ─── CONFIG ─────────────────────────────────────────────────────────────────
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
    try:
        init = await asyncio.wait_for(ws.receive_json(), timeout=15)
        if isinstance(init, dict) and init.get("type") == "join":
            name = str(init.get("name", "Unknown"))[:30]
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

    is_host = tok.get("creator", False) and len(room["peers"]) == 0
    room["peers"][peer_id] = {
        "ws": ws, "name": name, "avatar": avatar,
        "muted": False, "is_host": is_host,
        "joined": time.time(),
    }
    existing = [p for p in room["peers"] if p != peer_id]
    print(f"[WS] {peer_id} ({name}) joined room={room_id} host={is_host} total={len(room['peers'])}/{MAX_PEERS_PER_ROOM}")

    await ws.send_json({"type": "your_id", "id": peer_id, "max_peers": MAX_PEERS_PER_ROOM})

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
         "is_host": room["peers"][p]["is_host"], "muted": room["peers"][p]["muted"]}
        for p in existing
    ]
    await ws.send_json({"type": "peers", "peers": peer_list})

    join_msg = {
        "type": "peer_joined",
        "peer": {"id": peer_id, "name": name, "avatar": avatar,
                 "is_host": is_host, "muted": False},
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
<meta name="viewport" content="width=device-width, initial-scale=1.0, user-scalable=no, viewport-fit=cover">
<meta name="apple-mobile-web-app-capable" content="yes">
<meta name="mobile-web-app-capable" content="yes">
<title>Silent Hill</title>
<style>
*{margin:0;padding:0;box-sizing:border-box;-webkit-tap-highlight-color:transparent}
html,body{height:100%;overflow:hidden;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;background:#000;color:#fff}
.app{display:flex;flex-direction:column;height:100vh;height:100dvh;position:relative}
.bg{position:fixed;inset:0;z-index:0;background:url('/bg.jpg') center/cover no-repeat;opacity:0.4}
.bg::after{content:'';position:absolute;inset:0;background:linear-gradient(180deg,rgba(0,0,0,0.6),rgba(0,0,0,0.3),rgba(0,0,0,0.7))}
.header{position:relative;z-index:10;background:rgba(13,13,13,0.95);backdrop-filter:blur(20px);border-bottom:1px solid rgba(255,255,255,0.06);padding:8px 12px;display:flex;align-items:center;gap:10px;flex-shrink:0}
.back-btn,.menu-btn{width:36px;height:36px;display:flex;align-items:center;justify-content:center;background:none;border:none;color:#fff;font-size:20px;cursor:pointer}
.group-icon{width:36px;height:36px;border-radius:50%;object-fit:cover;border:1px solid rgba(255,255,255,0.1)}
.group-info{flex:1;min-width:0}
.group-name{font-size:15px;font-weight:600}
.group-meta{font-size:12px;color:#8e8e93}
.messages-wrap{flex:1;position:relative;z-index:5;overflow:hidden}
/* v3.10.3: anchor messages to the BOTTOM of the container, WhatsApp-style.
   When the room only has a few messages, they sit just above the input bar
   instead of pinned to the top of an empty scroll area — so when the
   keyboard opens the layout shrinks from the top and messages stay visible.
   `margin-top:auto` on the first child is the clean cross-browser way to do
   this in a scrollable flex column (justify-content:flex-end alone breaks
   scroll-up-to-read-history in some browsers when the list overflows). */
.messages{height:100%;overflow-y:auto;padding:12px 12px 16px;display:flex;flex-direction:column;gap:6px;scroll-behavior:auto}
.messages > *:first-child{margin-top:auto}
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

.overlay{position:fixed;inset:0;z-index:100;background:rgba(0,0,0,.88);display:flex;align-items:center;justify-content:center}
.o-box{background:#1c1c1e;border-radius:16px;padding:24px;width:90%;max-width:340px;text-align:center}
.o-box h2{font-size:18px;margin-bottom:8px}
.o-box p{font-size:13px;color:#8e8e93;margin-bottom:14px}
.o-box input{width:100%;height:44px;border-radius:12px;border:1px solid #3a3a3c;background:#2c2c2e;color:#fff;padding:0 14px;font-size:15px;text-align:center;outline:none;margin-bottom:10px}
.o-box button{width:100%;height:44px;border-radius:12px;border:none;background:#007aff;color:#fff;font-size:15px;font-weight:600;cursor:pointer}
.av-preview{width:80px;height:80px;border-radius:50%;margin:0 auto 10px;background:#2c2c2e;display:flex;align-items:center;justify-content:center;font-size:32px;font-weight:600;color:#8e8e93;overflow:hidden;cursor:pointer;border:3px solid #3a3a3c;position:relative}
.av-preview img{position:absolute;top:0;left:0;width:100%;height:100%;object-fit:cover;display:block;border-radius:50%}
.av-in{display:none}
.debug{position:fixed;top:0;left:0;right:0;z-index:200;background:rgba(0,0,0,.92);color:#0f0;font:11px monospace;padding:4px;max-height:200px;overflow-y:auto;display:none;white-space:pre-wrap}
.debug.show{display:block}
.peer-status{display:flex;gap:6px;align-items:center;overflow-x:auto;padding:4px 12px;flex-shrink:0;position:relative;z-index:8}
.peer-status::-webkit-scrollbar{display:none}
.p-s{flex-shrink:0;display:flex;align-items:center;gap:4px;background:rgba(255,255,255,0.08);padding:4px 10px;border-radius:12px;font-size:11px;transition:.2s}
.p-s.speaking{background:rgba(52,199,89,0.3);box-shadow:0 0 8px rgba(52,199,89,0.5)}
.p-s .dot{width:8px;height:8px;border-radius:50%;background:#8e8e93;transition:.2s}
.p-s .dot.conn{background:#34c759}
.p-s .dot.fail{background:#ff3b30}
.p-s .dot.relay{background:#ff9500}
.p-s .dot.warn{background:#ffcc00}
.p-s .dot.connecting{background:#ffcc00;animation:pulse 1.2s infinite}
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

<div class="peer-status" id="pstat"></div>

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
    <button class="sticker-panel-close" onclick="closeStickerPanel()" aria-label="Close stickers">&times;</button>
  </div>
  <div class="sticker-grid" id="stickerGrid"></div>
</div>

<script>
// ════════════════════════════════════════════════════════════════════════════
// SILENT HILL CLIENT — v3.10 BEAST MODE (STICKERS + GROW-INPUT)
// ════════════════════════════════════════════════════════════════════════════

const ROOM = "__ROOM_ID__", TOKEN = "__TOKEN__";
const MAX_PEERS = parseInt("__MAX_PEERS__", 10) || 11;
let MY_ID = "";
let serverMaxPeers = MAX_PEERS;
let ws = null, localStream = null, myName = "", myAvatar = "";
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
// The input is now a textarea (was <input>). Three things happen on input:
//   1. Typing indicator broadcast (unchanged behavior, just moved into the
//      same listener so we don't bind twice).
//   2. Auto-grow: height scales with content from 1 line up to 3 lines, then
//      internal scroll kicks in. Recomputed every keystroke.
//   3. Sticker icon visibility: hidden the moment any character is present,
//      shown again when the input goes empty (matches what Kyodo does).
// ════════════════════════════════════════════════════════════════════════════

const INPUT_MAX_HEIGHT = 84;  // ~3 lines at 14px / 1.4 line-height with padding
const INPUT_MIN_HEIGHT = 38;  // matches the resting height (rows=1)

function autoResizeInput() {
  const el = document.getElementById('msgIn');
  if (!el) return;
  // Reset to minimum so shrink-on-delete works, then grow to scrollHeight,
  // capped at MAX. After cap, native overflow:auto handles the scrolling.
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
  if (!stickerList || stickerList.length === 0) {
    grid.innerHTML = '<div class="sticker-empty">No stickers yet.<br>Drop images into the <code>stickers/</code> folder.</div>';
    return;
  }
  // Build buttons. We use buttons (not divs) for keyboard a11y.
  const html = stickerList.map(name => {
    const safe = esc(name);
    return '<button class="sticker-cell" type="button" data-name="' + safe + '" aria-label="' + safe + '"><img src="/stickers/' + safe + '" alt="" loading="lazy"></button>';
  }).join('');
  grid.innerHTML = html;
  // Wire up click handlers (delegation would also work but explicit is clearer)
  grid.querySelectorAll('.sticker-cell').forEach(cell => {
    cell.addEventListener('click', () => {
      const name = cell.getAttribute('data-name');
      if (name) sendStickerMsg(name);
    });
  });
}

async function toggleStickerPanel() {
  if (stickerPanelOpen) closeStickerPanel();
  else await openStickerPanel();
}

async function openStickerPanel() {
  // Refresh list each open in case new stickers landed in the folder
  await fetchStickerList();
  renderStickerGrid();
  const panel = document.getElementById('stickerPanel');
  const back = document.getElementById('stickerBackdrop');
  if (panel) panel.classList.add('open');
  if (back) back.classList.add('open');
  stickerPanelOpen = true;
  // Blur the textarea so the soft keyboard doesn't fight the bottom sheet
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
  // Close the panel right after — matches "the list will disappear" requirement
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
  const n = document.getElementById('nameIn').value.trim();
  if (!n) { alert("Enter name"); return; }
  myName = n;
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
  // v3.10: prefetch sticker list so the picker opens instantly first time
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
  // v3.10: ensure correct initial sizing for the textarea
  autoResizeInput();
  updateStickerIconVisibility();

  if (_peerLevelTicker) clearInterval(_peerLevelTicker);
  _peerLevelTicker = setInterval(updPeerLevels, 150);

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
    ws.send(JSON.stringify({ type: 'join', name: myName, avatar: myAvatar }));
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
        log("myId=" + MY_ID + " maxPeers=" + serverMaxPeers);
        break;

      // v3.10: server pushes the current sticker list on join. Client
      // also re-fetches via /stickers each time the picker opens to catch
      // brand-new files that landed mid-session.
      case 'stickers':
        if (Array.isArray(m.stickers)) {
          stickerList = m.stickers;
          log("stickers (push): " + stickerList.length);
        }
        break;

      case 'history':
        m.messages.forEach(renderMsg);
        scrollToLatest(false, true);
        // v3.10.2: history may include multiple images that haven't loaded
        // yet. Re-pin to the bottom each time one finishes loading, so the
        // user lands on the freshest message regardless of network speed.
        {
          const msgsEl = document.getElementById('msgs');
          if (msgsEl) {
            const allImgs = msgsEl.querySelectorAll('img.chat-img, img.sticker-img');
            allImgs.forEach(img => {
              if (img.complete && img.naturalWidth > 0) return;
              const repin = () => {
                if (isAtBottom()) {
                  msgsEl.scrollTop = msgsEl.scrollHeight;
                }
              };
              img.addEventListener('load',  () => requestAnimationFrame(repin), { once: true });
              img.addEventListener('error', () => requestAnimationFrame(repin), { once: true });
            });
          }
        }
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
          if (m.muted) lastMutedAt[m.peer_id] = Date.now();
          updPeers();
        }
        break;
      }

      case 'speaking': {
        const p = peerMap.get(m.peer_id);
        if (p) {
          p.speaking = m.level > 0.05;
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
      is_host: p.is_host, muted: p.muted,
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

function updPeers() {
  const el = document.getElementById('pstat');
  let h = '';
  const selfDot = isMuted ? 'fail' : 'conn';
  h += '<div class="p-s"><div class="dot ' + selfDot + '"></div>' + esc(myName) + ' (You)</div>';
  peerMap.forEach((p, id) => {
    let dot = '';
    if (p.connState === 'connected') {
      const smoothed = lossEwma[id] !== undefined ? lossEwma[id] : (p.lossPct || 0);
      const onRelay = peerRelay[id] || p.usedRelay;
      const muted = p.muted;
      const heardRecently = p.lastHeardAt && (Date.now() - p.lastHeardAt) < 8000;
      const noPacketsArriving = (p.recvRate !== undefined) && (p.recvRate < 1);

      const audioActuallyBroken = !muted && !heardRecently && noPacketsArriving && smoothed > 15;

      if (audioActuallyBroken) {
        dot = 'fail';
      } else if (onRelay) {
        dot = (smoothed > 12) ? 'warn' : 'relay';
      } else {
        dot = (smoothed > 12) ? 'warn' : 'conn';
      }
    } else if (p.connState === 'failed' || p.connState === 'closed') dot = 'fail';
    else if (p.connState === 'connecting' || p.connState === 'checking' || p.connState === 'new') dot = 'connecting';
    const speakClass = (p.speaking || p.actuallyHeard) ? ' speaking' : '';
    const muteIcon = p.muted ? ' &#128263;' : '';
    const levelPct = Math.min(100, Math.round((p.recvLevel || 0) * 200));
    const levelBar = p.connState === 'connected'
      ? '<span style="display:inline-block;width:24px;height:4px;background:rgba(255,255,255,0.15);border-radius:2px;margin-left:4px;vertical-align:middle;overflow:hidden"><span style="display:block;width:' + levelPct + '%;height:100%;background:#34c759;transition:width .1s"></span></span>'
      : '';
    h += '<div class="p-s' + speakClass + '" data-pid="' + id + '"><div class="dot ' + dot + '"></div>' + esc(p.name) + muteIcon + levelBar + '</div>';
  });
  el.innerHTML = h;
}

function updPeerLevels() {
  const el = document.getElementById('pstat');
  if (!el) return;
  el.querySelectorAll('[data-pid]').forEach(div => {
    const pid = div.getAttribute('data-pid');
    const p = peerMap.get(pid);
    if (!p) return;
    const innerBar = div.querySelector('span > span');
    if (innerBar) {
      const lvl = Math.min(100, Math.round((p.recvLevel || 0) * 200));
      innerBar.style.width = lvl + '%';
    }
    const isActive = p.speaking || p.actuallyHeard;
    if (isActive && !div.classList.contains('speaking')) div.classList.add('speaking');
    else if (!isActive && div.classList.contains('speaking')) div.classList.remove('speaking');
  });
}
let _peerLevelTicker = null;

let userIsAtBottom = true;
let unreadCount = 0;
const NEAR_BOTTOM_PX = 80;

function setupScrollLock() {
  const el = document.getElementById('msgs');
  if (!el) return;
  el.addEventListener('scroll', () => {
    const distFromBottom = el.scrollHeight - el.scrollTop - el.clientHeight;
    const wasAtBottom = userIsAtBottom;
    userIsAtBottom = distFromBottom <= NEAR_BOTTOM_PX;
    if (userIsAtBottom && !wasAtBottom) {
      unreadCount = 0;
      updateJumpButton();
    } else if (userIsAtBottom) {
      if (unreadCount !== 0) { unreadCount = 0; updateJumpButton(); }
    }
  }, { passive: true });

  // v3.10.2: when the on-screen keyboard opens/closes (or the window is
  // resized / orientation flips), the chat container's clientHeight changes.
  // If the user was parked at the bottom, keep them there — otherwise the
  // last message can suddenly sit half-hidden behind the keyboard.
  const onViewportChange = () => {
    if (userIsAtBottom) {
      requestAnimationFrame(() => {
        el.scrollTop = el.scrollHeight;
      });
    }
  };
  if (window.visualViewport) {
    window.visualViewport.addEventListener('resize', onViewportChange);
  }
  window.addEventListener('resize', onViewportChange);
  window.addEventListener('orientationchange', onViewportChange);
}

function isAtBottom() {
  const el = document.getElementById('msgs');
  if (!el) return true;
  const distFromBottom = el.scrollHeight - el.scrollTop - el.clientHeight;
  return distFromBottom <= NEAR_BOTTOM_PX;
}

function scrollToLatest(smooth, force) {
  const el = document.getElementById('msgs');
  if (!el) return;
  if (smooth) {
    el.scrollTo({ top: el.scrollHeight, behavior: 'smooth' });
  } else {
    el.scrollTop = el.scrollHeight;
  }
  userIsAtBottom = true;
  unreadCount = 0;
  updateJumpButton();
}

// v3.10.2: re-scroll once each image inside `row` finishes loading. Without
// this, scrollToLatest() runs while the <img> is still 0×0 (bytes not loaded
// yet), so when the image finally lays out it pushes content below the
// viewport and the user has to manually scroll. We listen for load/error on
// every image in the row and re-pin to the bottom — but only if the user is
// still parked at the bottom (so we never yank someone who's reading older
// messages).
function scrollAfterMedia(row) {
  if (!row) return;
  const imgs = row.querySelectorAll('img.chat-img, img.sticker-img');
  if (!imgs.length) return;
  const pin = () => {
    // Only re-pin if the user is currently at/near the bottom. If they've
    // scrolled up to read history, leave them alone.
    if (isAtBottom()) {
      const el = document.getElementById('msgs');
      if (el) el.scrollTop = el.scrollHeight;
    }
  };
  imgs.forEach(img => {
    if (img.complete && img.naturalWidth > 0) {
      // Already cached/decoded — pin on next frame so layout commits first.
      requestAnimationFrame(pin);
    } else {
      img.addEventListener('load',  () => requestAnimationFrame(pin), { once: true });
      img.addEventListener('error', () => requestAnimationFrame(pin), { once: true });
    }
  });
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
  if (lastFullRebuildAt[pid] && Date.now() - lastFullRebuildAt[pid] < 30000) {
    log("full rebuild cooldown " + pid);
    return;
  }
  if (renegInProgress[pid]) {
    log("full rebuild deferred (reneg in progress) " + pid);
    return;
  }
  lastFullRebuildAt[pid] = Date.now();
  log("=> FULL REBUILD " + pid + " (" + reason + ")");

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
  // v3.10: include sticker preview text. If the message is purely a sticker
  // (no text, no image), the reply preview just says "Sticker".
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
  // Decide preview text: prefer the snippet, fall back to "Image"/"Sticker"
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

function renderMsg(m) {
  const c = document.getElementById('msgs'); if (!c) return;

  const wasAtBottom = isAtBottom();
  let renderedRow = null;

  if (m.kind === 'system') {
    const d = document.createElement('div');
    d.className = 'msg-system';
    d.textContent = m.text;
    c.appendChild(d);
  } else {
    const isSelf = !!m.self;
    const pi = peerMap.get(m.peer_id) || {};
    const name = m.name || pi.name || '?';
    const showBadge = name.trim().toLowerCase() === 'sor';
    const avSrc = m.avatar || pi.avatar || '';

    // v3.10: detect "sticker-only" messages — no text, no image, just a
    // sticker (or expired sticker). These render WITHOUT a bubble so the
    // sticker floats next to the avatar like in Kyodo/Telegram.
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

    // Build the body content. Order matters: reply preview, then sticker
    // (if sticker-only — it lives outside any bubble), or else image, then
    // text, all wrapped in a bubble.
    let contentHTML;
    if (stickerOnly) {
      // No bubble — reply preview (if any) and the sticker itself sit
      // directly inside the msg-content column.
      let stickerHTML;
      if (m.sticker_expired) {
        stickerHTML = '<div class="sticker-expired"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="3" width="18" height="18" rx="2" ry="2"/><circle cx="8.5" cy="8.5" r="1.5"/><polyline points="21 15 16 10 5 21"/></svg><span>Sticker no longer available</span></div>';
      } else {
        stickerHTML = '<img class="sticker-img" src="/stickers/' + esc(m.sticker) + '" alt="sticker" draggable="false">';
      }
      contentHTML = header + replyHTML + stickerHTML;
    } else {
      // Standard bubble path. Preserves all existing image+text behavior.
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
      // chat-img → open preview. sticker-img is pointer-events:none so it
      // never reaches here, which is exactly what we want (stickers are
      // not clickable to zoom — matches the requirement).
      if (t.classList && t.classList.contains('chat-img')) {
        ev.stopPropagation();
        openImagePreview(t.src);
        return;
      }
      if (t.tagName === 'IMG' && t.closest('.avatar')) return;
      startReply(m);
    });

    c.appendChild(row);
    renderedRow = row;
  }

  if (m.self || wasAtBottom) {
    scrollToLatest(false);
    // v3.10.2: re-pin scroll once the just-rendered image(s) actually load.
    // Fixes the "image shows half-cropped, have to scroll manually" bug.
    scrollAfterMedia(renderedRow);
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
  c.appendChild(d);
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
  // v3.10: textarea preserves leading/trailing whitespace including newlines;
  // .trim() also normalizes blank-only sends (e.g. someone hits Enter twice).
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
  // Reset textarea height + re-show sticker icon since input is now empty
  autoResizeInput();
  updateStickerIconVisibility();
  // v3.10.1: keep the mobile keyboard up after sending. Without this, the
  // keyboard collapses on every send because some mobile browsers blur the
  // textarea when the value is reset programmatically. Refocusing keeps the
  // keyboard visible until the user manually dismisses it via the system
  // keyboard's down-arrow / dismiss button.
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

log("page loaded v3.10.3 (max " + MAX_PEERS + " peers, stickers + grow-input + keyboard-stay + auto-pin + bottom-anchor)");
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
    print(f"Silent Hill Bot v3.10 BEAST MODE | {WEB_APP_URL} | Port {PORT}")
    print(f"Kyodo: {KYODO_OK} | Max peers per room: {MAX_PEERS_PER_ROOM}")
    print(f"Memory caps: {MAX_CHAT_MESSAGES} msgs/room, {IMAGE_RETAIN_COUNT} recent images, {MAX_IMAGE_BYTES//1000}KB per img")
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
    print("=" * 60)
    await asyncio.gather(
        Server(Config(app=app, host="0.0.0.0", port=PORT, log_level="warning")).serve(),
        run_kyodo_bot(),
        keepalive(),
        memory_groomer(),
    )


if __name__ == "__main__":
    asyncio.run(main())
