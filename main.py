"""
Silent Hill Voice Call Bot — v3.5 SECRET-KEY-COMPATIBLE
═══════════════════════════════════════════════════════════════════════════════
v3.5 FIX (the actual root cause from Render logs HTTP 401):

  THE REAL PROBLEM: User pasted Metered's SECRET KEY into METERED_API_KEY,
  but the GET /api/v1/turn/credentials?apiKey=... endpoint requires a
  per-credential API KEY (different thing). Result: HTTP 401 every fetch.

  CONFUSING UI: Metered's dashboard "Developers" page prominently shows
  "SECRET KEY" — and warns "never expose it" — which makes users assume
  it's the right value for backend env vars. But the apiKey is actually
  on the separate "TURN Server" page, generated per-credential.

  v3.5 FIX: fetch_metered_creds now tries TWO paths:
    1. POST /credential with secretKey (mints a fresh expiring cred,
       then uses returned apiKey to fetch ICE servers)
    2. GET /credentials with apiKey (original v3.4 behavior, fallback)
  Whichever value the user pasted into METERED_API_KEY now works.

  CREDS REFRESH: Since attempt 1 mints credentials with 1-hour expiry,
  the existing 30-min cache stays in sync — credentials never expire
  while in cache.

═══════════════════════════════════════════════════════════════════════════════
v3.4 FIXES (Metered configured but maybe not loading):

  THE PROBLEM: User has METERED_API_KEY + METERED_DOMAIN env vars set,
  but the log "ICE: 7 server entries loaded" suggests Metered creds
  aren't actually being merged in (expected ~10-12 entries with Metered).

  v3.4 ADDS:
    1. Metered fetch errors are now VISIBLE — every failure mode logs
       a clear reason (HTTP status, JSON parse error, timeout, exception).
       Run the server and watch stdout for "[turn] METERED ..." lines.
    2. Failed premium fetches now cache for only 60s (was 30 min).
       So a transient error doesn't keep Gulf peers broken for half an hour.
    3. New /turn-debug endpoint. Hit it in your browser:
         https://your-app.onrender.com/turn-debug
       Returns sanitized JSON showing exactly which TURN entries are
       loaded right now and whether Metered/CF env vars were detected.
    4. /turn-status now reflects whether premium TURN ACTUALLY loaded
       (by inspecting URLs and usernames), not just whether env vars exist.

  HOW TO USE:
    a. Deploy this version.
    b. Open https://your-app.onrender.com/turn-debug in any browser.
    c. Look for entries with non-public usernames — Metered creds will
       have a long random username, openrelayproject is the public one.
    d. If only Google/openrelay show up, check Render's stdout logs for
       the "[turn] METERED ..." line. It will tell you exactly why.

═══════════════════════════════════════════════════════════════════════════════
v3.3 FIXES (the Oman peer's logs at 22:51:45 / 22:53:42):

  THE BUG: Two peers stuck in ICE=checking for 15s → fail → retry on
           same broken path → fail again. Pattern from log:
             PC afae129b ICE=checking
             CONN-TIMER afae129b still checking, waiting
             PC afae129b ICE=disconnected
             PC afae129b conn=failed
             retry #1 ... (retries with same direct config)

  ROOT CAUSE: Many Gulf/MENA networks (Omani Omantel, some Iraqi/Kuwaiti
              ISPs, corporate WiFi) block direct UDP P2P entirely. The
              connection NEEDS TURN-over-TCP/443 from the first attempt.
              v3.2 only switched to relay after stats showed loss — but
              if the PC never connects, stats never run.

  v3.3 FIXES:
    A. forceIceRestart now FORCES relay-only mode when the PC was
       already failed (not just disconnected). On retry #1 from a
       failed state, we do iceTransportPolicy:'relay' immediately.
    B. scheduleRetry's first retry forces a fresh PC with relay-only
       (was reusing PC config). Eliminates wasted retry on broken path.
    C. New CONN-TIMER threshold: if ICE has been 'checking' for 12s,
       force relay immediately instead of waiting for 'failed'. Saves
       3-5 seconds and prevents the 'checking → failed' dead-end.
    D. New /turn-test endpoint that the client probes on join — if
       the configured TURN server isn't actually reachable, log a
       LOUD warning so the operator knows their TURN is broken.
    E. Increased connection timer to a hard 25s ceiling (3 fires of
       10s but with relay escalation at the 12s checking mark).

  IMPORTANT FOR YOUR OMAN FRIEND: The code can only do so much. You
  MUST set up a real TURN server. Free options (5 min setup):
    - Metered.ca (50GB/mo free) — set METERED_API_KEY + METERED_DOMAIN
    - Cloudflare Realtime (1TB/mo free) — set CF_TURN_TOKEN_ID +
      CF_TURN_API_TOKEN
  The public openrelay.metered.ca fallback is rate-limited and often
  blocked by ISP DPI in Oman/UAE/Saudi.

═══════════════════════════════════════════════════════════════════════════════
v3.2 SURGICAL FIXES (the actual bugs from your 22:05–22:07 log):

  THE TWO REAL BUGS YOUR LOGS SHOW:
  ──────────────────────────────────
  BUG A: Healthy connection KILLED by CONN-TIMEOUT
    Log line:  "PC 3e2e5f10 ICE=connected"  (22:07:44)
               "PC 3e2e5f10 conn=connecting" (22:07:44)
               "CONN-TIMEOUT 3e2e5f10 state=connecting/connected" (22:07:49)
               "FAST ICE restart 3e2e5f10"
    Diagnosis: ICE was already CONNECTED. The PC was just in the
               normal half-second window before conn flips to 'connected'.
               The timer killed a perfectly healthy in-progress connection.
    Fix:       startConnectionTimer skips if iceConnectionState is
               'connected' or 'completed' — full stop. ICE is the truth.

  BUG B: Relay switch happens on a 4s spike, then code never escalates
    Log line:  "STATS!6130c580 dR=805 dL=71 (8.1%) ... AUTO-RELAY"
               then 14 more lines of 5–40% loss ON RELAY, no further action.
    Diagnosis: 4s window is too noisy (a single bad burst triggers relay).
               Once on relay, code never tries another relay server or
               full rebuild even when relay is also bad.
    Fix:       Sliding 12s loss window (EWMA), 6s cooldown after relay
               switch, and a "relay also failing" escalation path that
               rebuilds PC with fresh ICE servers from /turn.

  BONUS FIXES:
  ──────────────
  C: Stats trigger thresholds tuned for real-world cellular jitter
     (10% over 12s instead of 5% over 4s — fewer false relay switches)
  D: After relay switch, "ICE restart on disconnected" cooldown extended
     to 8s (was 5s) to prevent restart storms on flaky networks
  E: Connection timer increased from 6s → 10s (gives bad networks time)
  F: New escalation: if loss > 20% for 16s on relay, full rebuild
  G: /turn endpoint result cached in client memory — refetched on rebuild
     (so a bad TURN server doesn't keep getting reused)

═══════════════════════════════════════════════════════════════════════════════
v3.1 fixes preserved (12 critical bugs from previous version):
  1. Zombie jitter detection (3+ consecutive identical, jitter>0.05, dR=0)
  2. Reneg race window in createOffer
  3. Zombie handler renegInProgress guard
  4. switchPeerToRelay renegInProgress guard
  5. Connection timer signaling state guard (now also iceConnected)
  6. scheduleRetry routes through forceIceRestart (cooldown respected)
  7. Stall trigger >= 2 intervals (8s grace)
  8. Stats interval optional chaining
  9. handleOffer in-place reneg renegInProgress guard
  10. Python asyncio.CancelledError fix
  11. _lastJitters cleared on reconnection
  12. request_relay zombie branch renegInProgress guard
═══════════════════════════════════════════════════════════════════════════════
"""

import asyncio, json, os, time, uuid, hmac, hashlib, base64
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
CHAT_ID = os.getenv("BOT_CHAT_ID", "cmh2gy89r01pvt33exijh1wr3")
CIRCLE_ID = os.getenv("BOT_CIRCLE_ID", "cm9bylrbn00hmux6t43mczt2o")
WEB_APP_URL = os.environ.get("WEB_APP_URL", "http://localhost:8000")
PORT = int(os.environ.get("PORT", "8000"))

# TURN provider env vars (set ONE for production reliability)
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


# ─── TURN CREDENTIAL FETCHING ───────────────────────────────────────────────
async def fetch_metered_creds() -> List[dict]:
    """Fetch fresh TURN creds from Metered.ca.
    v3.5: tries SECRET KEY first (POST /credential), falls back to API KEY
    (GET /credentials). Either way, METERED_API_KEY env var is honored.
    Why: Metered's dashboard 'Developers' page exposes a SECRET KEY,
    while the per-credential 'API Key' is on a different page. People
    naturally paste the secret. We accept both."""
    if not METERED_API_KEY or not METERED_DOMAIN:
        return []

    # ATTEMPT 1: treat as SECRET KEY (POST to create a fresh credential)
    # This is the path most users will hit because the Developers page
    # in the Metered dashboard prominently displays the secret key.
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
                        # Now use the returned apiKey to fetch ICE servers
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
                # 401 here means the value isn't a valid SECRET KEY either —
                # but it might be a valid APIKEY. Fall through to attempt 2.
    except asyncio.TimeoutError:
        print(f"[turn] METERED secret-key POST TIMEOUT — Render can't reach {METERED_DOMAIN}")
    except Exception as e:
        print(f"[turn] METERED secret-key POST EXCEPTION: {type(e).__name__}: {e}")

    # ATTEMPT 2: treat as API KEY (GET — original v3.4 behavior)
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
                    print(f"[turn] !!! In Metered dashboard, go to TURN Server page,")
                    print(f"[turn] !!! click 'Show API Key' on a credential, paste THAT")
                    print(f"[turn] !!! into METERED_API_KEY env var. The Developers")
                    print(f"[turn] !!! page secret should also work — but check it's not")
                    print(f"[turn] !!! truncated or has trailing whitespace.")
                return []
    except asyncio.TimeoutError:
        print(f"[turn] METERED apikey TIMEOUT — Render can't reach {METERED_DOMAIN}")
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
                if r.status == 201 or r.status == 200:
                    data = await r.json()
                    return data.get("iceServers", [])
    except Exception as e:
        print(f"[turn] cf err: {e}")
    return []


async def get_ice_servers() -> List[dict]:
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
    # v3.4: only cache for full 30 min if premium TURN actually loaded.
    # If Metered/CF was configured but failed, retry in 60s — don't
    # serve a 30-min stale fallback that keeps Gulf peers broken.
    metered_configured = bool(METERED_API_KEY and METERED_DOMAIN)
    cf_configured = bool(CF_TURN_TOKEN_ID and CF_TURN_API_TOKEN)
    expected_premium = metered_configured or cf_configured
    got_premium = metered_ok or cf_ok or bool(CUSTOM_TURN_URL)
    if expected_premium and not got_premium:
        print("[turn] !! PREMIUM TURN CONFIGURED BUT FETCH FAILED — short cache (60s)")
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
    return {"ok": True, "rooms": len(rooms), "kyodo": KYODO_OK}


@app.get("/bg.jpg")
async def bg():
    return FileResponse("bg.jpg") if os.path.exists("bg.jpg") else HTMLResponse("", 404)


@app.get("/ci.jpg")
async def ci():
    return FileResponse("ci.jpg") if os.path.exists("ci.jpg") else HTMLResponse("", 404)


@app.get("/turn")
async def turn_endpoint():
    """Serves freshest possible ICE config to clients."""
    servers = await get_ice_servers()
    return JSONResponse({"iceServers": servers})


@app.get("/turn-status")
async def turn_status():
    """Reports whether premium TURN is ACTUALLY working (not just configured)."""
    servers = await get_ice_servers()
    # Inspect what's actually loaded — find any non-public TURN URL
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
    """v3.4: full diagnostic — what TURN config is actually being served?
    Hit this URL in your browser to see if Metered creds are loading."""
    servers = await get_ice_servers()
    # Sanitize: hide actual passwords/secrets in the response
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
    return JSONResponse({
        "total_entries": len(servers),
        "metered_env_set": bool(METERED_API_KEY and METERED_DOMAIN),
        "metered_domain": METERED_DOMAIN if METERED_DOMAIN else "(unset)",
        "cloudflare_env_set": bool(CF_TURN_TOKEN_ID and CF_TURN_API_TOKEN),
        "custom_env_set": bool(CUSTOM_TURN_URL),
        "cache_expires_in": int(_turn_cache.get("expires", 0) - time.time()),
        "entries": sanitized,
    })


@app.get("/call/{room_id}")
async def call_page(room_id: str, t: str = Query(...)):
    tok = tokens.get(t)
    if not tok or tok.get("room_id") != room_id or room_id not in rooms:
        return HTMLResponse("<h1>Invalid link</h1>", 403)
    html = CALL_HTML.replace("__ROOM_ID__", room_id).replace("__TOKEN__", t)
    return HTMLResponse(html)


@app.websocket("/ws/{room_id}")
async def ws_endpoint(ws: WebSocket, room_id: str, t: str = Query(...)):
    tok = tokens.get(t)
    if not tok or tok.get("room_id") != room_id or room_id not in rooms:
        await ws.close(code=4001)
        return
    await ws.accept()
    peer_id = str(uuid.uuid4())[:8]
    room = rooms[room_id]
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

    is_host = tok.get("creator", False) and len(room["peers"]) == 0
    room["peers"][peer_id] = {
        "ws": ws, "name": name, "avatar": avatar,
        "muted": False, "is_host": is_host,
        "joined": time.time(),
    }
    existing = [p for p in room["peers"] if p != peer_id]
    print(f"[WS] {peer_id} ({name}) joined room={room_id} host={is_host} total={len(room['peers'])}")

    await ws.send_json({"type": "your_id", "id": peer_id})
    await ws.send_json({"type": "history", "messages": json_read(room["chat_file"])[-100:]})
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
                if image and (not isinstance(image, str) or len(image) > 600000
                              or not image.startswith("data:image/")):
                    image = ""
                if not text and not image:
                    continue
                reply_to = None
                rt = msg.get("reply_to")
                if isinstance(rt, dict):
                    reply_to = {
                        "id": str(rt.get("id", ""))[:64],
                        "name": str(rt.get("name", ""))[:30],
                        "text": str(rt.get("text", ""))[:80],
                        "has_image": bool(rt.get("has_image")),
                    }
                cm = {"type": "chat", "kind": "user",
                      "id": str(uuid.uuid4())[:12],
                      "peer_id": peer_id,
                      "name": name, "avatar": avatar, "text": text,
                      "time": datetime.now().isoformat()}
                if image:
                    cm["image"] = image
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
    p = f"{rid}_chat.json"
    m = json_read(p, [])
    m.append(msg)
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
.messages{flex:1;overflow-y:auto;padding:12px;display:flex;flex-direction:column;gap:6px;position:relative;z-index:5}
.messages::-webkit-scrollbar{width:0}
.msg-system{text-align:center;color:#8e8e93;font-size:12px;padding:6px 0}
.msg-row{display:flex;gap:8px;max-width:85%;animation:msgIn .2s ease-out;align-items:flex-start;position:relative;overflow:hidden;touch-action:pan-y}
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
.msg-bubble.has-img{padding:4px;overflow:hidden}
.msg-bubble.has-img.has-text{padding-bottom:8px}
.msg-bubble.has-img .msg-text{padding:4px 10px 0}
.msg-reply{border-left:3px solid rgba(255,255,255,0.55);padding:4px 8px;margin-bottom:4px;font-size:12px;background:rgba(255,255,255,0.08);border-radius:6px;display:flex;flex-direction:column;gap:1px;max-width:240px}
.msg-bubble.has-img .msg-reply{margin:4px 4px 4px}
.msg-reply-name{font-weight:600;color:rgba(255,255,255,0.95);overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.msg-reply-text{opacity:0.75;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.reply-bar{position:relative;z-index:10;background:rgba(28,28,30,0.95);border-top:1px solid rgba(255,255,255,0.06);padding:8px 12px;display:flex;align-items:center;gap:8px;flex-shrink:0;animation:msgIn .15s}
.reply-bar-content{flex:1;display:flex;flex-direction:column;gap:2px;min-width:0;border-left:3px solid #007aff;padding-left:8px}
.reply-bar-label{font-size:11px;color:#007aff;font-weight:600}
.reply-bar-text{font-size:13px;color:#8e8e93;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.reply-bar-close{width:28px;height:28px;border-radius:50%;border:none;background:#3a3a3c;color:#fff;font-size:18px;cursor:pointer;display:flex;align-items:center;justify-content:center;flex-shrink:0;line-height:1}
.img-preview-overlay{position:fixed;inset:0;z-index:300;background:rgba(0,0,0,0.95);display:flex;align-items:center;justify-content:center;cursor:pointer;animation:msgIn .2s;padding:20px}
.img-preview-overlay img{max-width:95vw;max-height:90vh;border-radius:8px}
.img-preview-overlay .close-hint{position:absolute;top:20px;right:20px;color:#fff;font-size:32px;opacity:0.7}
.typing-bar{position:relative;z-index:10;background:rgba(13,13,13,0.95);border-top:1px solid rgba(255,255,255,0.06);padding:6px 12px;font-size:12px;color:#8e8e93;flex-shrink:0;animation:msgIn .15s}
.typing-bar.hidden{display:none!important}
.new-msg-pill{position:fixed;bottom:76px;left:50%;transform:translateX(-50%) scale(0.9);background:#007aff;color:#fff;padding:8px 18px;border-radius:20px;font-size:13px;font-weight:600;z-index:20;cursor:pointer;box-shadow:0 4px 14px rgba(0,0,0,0.45);opacity:0;transition:opacity .15s,transform .15s;pointer-events:none}.new-msg-pill.show{opacity:1;transform:translateX(-50%) scale(1);pointer-events:auto}
.msg-row{position:relative;overflow:hidden;touch-action:pan-y}
.input-bar{position:relative;z-index:10;background:rgba(13,13,13,0.95);border-top:1px solid rgba(255,255,255,0.06);padding:8px 12px;display:flex;align-items:center;gap:8px;flex-shrink:0}
.input-attach,.input-send{width:38px;height:38px;border-radius:50%;border:none;display:flex;align-items:center;justify-content:center;cursor:pointer;flex-shrink:0}
.input-attach{background:#2c2c2e;color:#fff;font-size:18px}
.input-field{flex:1;height:38px;border-radius:19px;border:none;background:#1c1c1e;color:#fff;padding:0 14px;font-size:14px;outline:none}
.input-field::placeholder{color:#8e8e93}
.input-send{background:#007aff;color:#fff;font-size:16px}
.input-send:active{transform:scale(.92)}
.mic-btn{width:32px;height:32px;border-radius:50%;border:none;background:#3a3a3c;color:#fff;display:flex;align-items:center;justify-content:center;cursor:pointer;flex-shrink:0;transition:.2s;padding:0}
.mic-btn.muted{background:#ff3b30}
.mic-btn svg{width:16px;height:16px;pointer-events:none}
.leave-header-btn{width:36px;height:36px;display:flex;align-items:center;justify-content:center;background:none;border:none;color:#ff3b30;cursor:pointer;padding:0}
.leave-header-btn svg{width:20px;height:20px}
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
.p-s .dot.connecting{background:#ffcc00;animation:pulse 1.2s infinite}
@keyframes pulse{50%{opacity:0.4}}
.hidden{display:none!important}
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

<div class="new-msg-pill" id="newMsgPill" onclick="catchUpScroll()">new messages</div>
<div class="messages" id="msgs"></div>
<div class="typing-bar hidden" id="typingBar"></div>
<div class="input-bar">
<button class="input-attach" onclick="document.getElementById('imgIn').click()" title="Send image">+</button>
<button class="mic-btn" id="muteBtn" onclick="toggleMute()" title="Mute"><svg id="micIcon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 1a3 3 0 0 0-3 3v8a3 3 0 0 0 6 0V4a3 3 0 0 0-3-3z"/><path d="M19 10v2a7 7 0 0 1-14 0v-2"/><line x1="12" y1="19" x2="12" y2="23"/><line x1="8" y1="23" x2="16" y2="23"/></svg></button>
<input type="file" id="imgIn" accept="image/*" style="display:none" onchange="pickChatImage(event)">
<input type="text" class="input-field" id="msgIn" placeholder="Write a message..." onkeypress="if(event.key==='Enter')sendMsg()">
<button class="input-send" onclick="sendMsg()">&#10148;</button>
</div>
</div>

<script>
// ════════════════════════════════════════════════════════════════════════════
// SILENT HILL CLIENT — BEAST MODE v3.2 GENIUS
// ════════════════════════════════════════════════════════════════════════════
// THE TWO REAL BUGS YOUR LOGS REVEALED:
//
// BUG A (the killer): startConnectionTimer fires while ICE is HEALTHY
//   When ICE is 'connected' or 'completed', the connection IS working.
//   conn=connecting just means it hasn't flipped to connected YET.
//   Killing it here = killing healthy connections.
//   FIX: skip timer if iceConnectionState ∈ {connected, completed}.
//
// BUG B (chronic packet loss): 4s spike triggers relay, relay also bad,
//   no further escalation. Result: stuck on bad relay, audio choppy.
//   FIX: 12s sliding loss window (EWMA), 6s post-relay cooldown,
//        and "relay also failing" → full rebuild with refetched ICE.
//
// Plus all 12 v3.1 fixes preserved.
// ════════════════════════════════════════════════════════════════════════════

const ROOM = "__ROOM_ID__", TOKEN = "__TOKEN__";
let MY_ID = "";
let ws = null, localStream = null, myName = "", myAvatar = "";
let isMuted = false, isHost = false;
let leaving = false;
let wsRetries = 0;
let wakeLock = null;

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

// per-peer guards
const lastMutedAt = {};
const renegInProgress = {};
const lastRelayAt = {};
const lastOfferAt = {};
const lastIceRestartAt = {};
const lastFullRebuildAt = {};       // v3.2: cooldown for full rebuild
const relayConnectedAt = {};        // v3.2: when did relay path stabilize
const lossEwma = {};                // v3.2: smoothed loss per peer
const sustainedBadStart = {};       // v3.2: when sustained bad began
let lastMuteToggleAt = 0;

// ════════════════════════════════════════════════════════════════════════════
// v3.6 FEATURES: auto-scroll lock, swipe-to-reply, wake-lock reacquire, audio self-heal
// ════════════════════════════════════════════════════════════════════════════
let autoScrollEnabled = true;
let newMsgCount = 0;
let swipeState = null;

// frozen-jitter zombie tracking
const frozenJitterCounts = {};
const frozenJitterValues = {};

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
    iceCandidatePoolSize: 4,
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
      ctx.drawImage(img, 0, 0, sz, sz);
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

(function wireTyping() {
  const inEl = document.getElementById('msgIn');
  if (!inEl) return;
  inEl.addEventListener('input', () => {
    if (!ws || ws.readyState !== 1) return;
    ws.send(JSON.stringify({ type: 'typing_start' }));
    if (typingTimer) clearTimeout(typingTimer);
    typingTimer = setTimeout(() => {
      if (ws && ws.readyState === 1) ws.send(JSON.stringify({ type: 'typing_stop' }));
      typingTimer = null;
    }, 2000);
  });
})();

// ════════════════════════════════════════════════════════════════════════════
// FEATURE INIT: scroll lock, swipe-to-reply, wake-lock guard, audio self-heal
// ════════════════════════════════════════════════════════════════════════════
(function initFeatures() {
  const msgsEl = document.getElementById('msgs');
  if (!msgsEl) return;

  // ── scroll lock: detect when user scrolls up ──
  msgsEl.addEventListener('scroll', () => {
    const nearBottom = msgsEl.scrollTop + msgsEl.clientHeight >= msgsEl.scrollHeight - 60;
    if (nearBottom && !autoScrollEnabled) {
      autoScrollEnabled = true;
      newMsgCount = 0;
      const pill = document.getElementById('newMsgPill');
      if (pill) pill.classList.remove('show');
    } else if (!nearBottom && autoScrollEnabled) {
      autoScrollEnabled = false;
    }
  }, { passive: true });

  // ── swipe-to-reply: right on others, left on self ──
  msgsEl.addEventListener('touchstart', e => {
    const row = e.target.closest('.msg-row');
    if (!row || row.classList.contains('system')) return;
    const touch = e.touches[0];
    swipeState = { x: touch.clientX, y: touch.clientY, row, moved: false };
  }, { passive: true });

  msgsEl.addEventListener('touchmove', e => {
    if (!swipeState || !swipeState.row) return;
    const touch = e.touches[0];
    const dx = touch.clientX - swipeState.x;
    const dy = touch.clientY - swipeState.y;
    const isSelf = swipeState.row.classList.contains('self');
    const validDir = (!isSelf && dx > 0) || (isSelf && dx < 0);
    if (validDir && Math.abs(dx) > Math.abs(dy) * 1.5 && Math.abs(dx) > 8) {
      const bubble = swipeState.row.querySelector('.msg-bubble');
      if (bubble) bubble.style.transform = 'translateX(' + (dx * 0.25) + 'px)';
      swipeState.moved = true;
    }
  }, { passive: true });

  msgsEl.addEventListener('touchend', e => {
    if (!swipeState || !swipeState.row) { swipeState = null; return; }
    const touch = e.changedTouches[0];
    const dx = touch.clientX - swipeState.x;
    const dy = touch.clientY - swipeState.y;
    const isSelf = swipeState.row.classList.contains('self');
    // Reset transform
    const bubble = swipeState.row.querySelector('.msg-bubble');
    if (bubble) bubble.style.transform = '';
    const mostlyHorizontal = Math.abs(dx) > Math.abs(dy) * 1.5;
    const threshold = 60;
    if (mostlyHorizontal && Math.abs(dx) > threshold) {
      if (!isSelf && dx > 0) {
        const md = swipeState.row._msgData;
        if (md) startReply(md);
      } else if (isSelf && dx < 0) {
        const md = swipeState.row._msgData;
        if (md) startReply(md);
      }
    }
    swipeState = null;
  }, { passive: true });
})();

async function fetchIceServers()
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
  // v3.4: warn loudly if premium TURN didn't actually load
  try {
    const sr = await fetch('/turn-status', { cache: 'no-store' });
    if (sr.ok) {
      const st = await sr.json();
      if (!st.premium) {
        if (st.metered_configured || st.cloudflare_configured || st.custom_configured) {
          log("!!! TURN ENV SET BUT FETCH FAILED — check /turn-debug");
          log("!!! Server can't reach configured TURN provider");
        } else {
          log("!!! WARN: NO TURN CONFIGURED — Gulf/MENA peers will fail");
        }
      } else {
        log("TURN: premium provider active");
      }
    }
  } catch (e) {}
}

async function acquireWakeLock() {
  try {
    if ('wakeLock' in navigator) {
      wakeLock = await navigator.wakeLock.request('screen');
      log("wakeLock OK");
    }
  } catch (e) { log("wakeLock fail"); }
}
document.addEventListener('visibilitychange', () => {
  if (document.visibilityState === 'visible') {
    if (!wakeLock || wakeLock.released) acquireWakeLock();
    log("foreground — refreshing peer health");
    Object.entries(peers).forEach(([pid, pc]) => {
      if (pc.connectionState === 'disconnected' && MY_ID > pid) {
        log("disconnected peer on resume -> ICE restart " + pid);
        forceIceRestart(pid);
      }
    });
  }
});

// v3.6: wake lock auto-reacquire — iOS Safari silently kills it
setInterval(() => {
  if (document.visibilityState === 'visible' && (!wakeLock || wakeLock.released)) {
    acquireWakeLock();
  }
}, 30000);

// v3.6: audio self-heal — iOS bg suspend can drop audio streams
setInterval(() => {
  Object.entries(audios).forEach(([pid, a]) => {
    if (!a) return;
    const streamDead = !a.srcObject || a.srcObject.getTracks().every(t => t.readyState === 'ended');
    if (a.paused || streamDead) {
      const pc = peers[pid];
      if (pc) {
        const recv = pc.getReceivers().find(r => r.track && r.track.kind === 'audio' && r.track.readyState === 'live');
        if (recv) {
          log("audio self-heal " + pid);
          const fresh = new MediaStream([recv.track]);
          a.srcObject = fresh;
          a.muted = false;
          a.volume = 1.0;
          a.play().catch(() => {});
          startInboundLevel(fresh, pid);
        }
      }
    }
  });
}, 5000);

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
    remoteAudioCtx = new (window.AudioContext || window.webkitAudioContext)();
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

  if (_peerLevelTicker) clearInterval(_peerLevelTicker);
  _peerLevelTicker = setInterval(updPeerLevels, 150);

  connectWS();
}

function connectWS() {
  const p = location.protocol === 'https:' ? 'wss:' : 'ws:';
  const url = p + '//' + location.host + '/ws/' + ROOM + '?t=' + TOKEN;
  log("WS connect (try " + (wsRetries + 1) + ")");

  ws = new WebSocket(url);

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

      case 'your_id':
        MY_ID = m.id;
        log("myId=" + MY_ID);
        break;

      case 'history':
        m.messages.forEach(renderMsg);
        break;

      case 'chat':
        renderMsg(m);
        break;

      case 'peers':
        log("existing peers: " + m.peers.length);
        const currentIds = new Set(m.peers.map(p => p.id));
        for (const [id, _] of peerMap) {
          if (!currentIds.has(id)) {
            nukePeer(id);
            peerMap.delete(id);
          }
        }
        for (const p of m.peers) {
          addPeer(p);
          if (MY_ID > p.id) {
            log("I'm larger (" + MY_ID + ">" + p.id + ") -> offer");
            createOffer(p.id);
          } else {
            log("I'm smaller (" + MY_ID + "<" + p.id + ") -> wait");
          }
        }
        break;

      case 'peer_joined':
        addPeer(m.peer);
        renderSys(m.peer.name + " joined");
        if (MY_ID && MY_ID > m.peer.id) {
          log("late: I'm larger -> offer to " + m.peer.id);
          createOffer(m.peer.id);
        } else if (MY_ID) {
          log("late: I'm smaller -> wait for offer from " + m.peer.id);
        }
        break;

      case 'peer_left':
        nukePeer(m.peer_id);
        peerMap.delete(m.peer_id);
        if (typingUsers.has(m.peer_id)) { typingUsers.delete(m.peer_id); renderTyping(); }
        renderSys(m.name + ' left');
        updCount();
        updPeers();
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
        // v3.3: stuck-checking and retry-after-fail force smaller side
        // to also destroy its PC and wait for the relay-mode offer
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
          // v3.3: smaller side just clears PC and waits for the
          // larger side's relay-mode offer (which is already coming)
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
          if (t && t.readyState === 'live') {
            t.enabled = false;
          }
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
          if (t && t.readyState === 'live') {
            t.enabled = true;
          }
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
            lastMutedAt[m.peer_id] = Date.now();
          }
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
    if (!leaving) {
      const delay = Math.min(1000 * Math.pow(1.5, wsRetries), 15000);
      wsRetries++;
      log("WS reconnect in " + delay + "ms");
      setTimeout(connectWS, delay);
    } else {
      cleanupRTC();
    }
  };

  ws.onerror = e => {
    log("WS err");
  };
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
  document.getElementById('mcount').textContent = (peerMap.size + 1) + ' in call';
}

function updPeers() {
  const el = document.getElementById('pstat');
  let h = '';
  const selfDot = isMuted ? 'fail' : 'conn';
  h += '<div class="p-s"><div class="dot ' + selfDot + '"></div>' + esc(myName) + ' (You)</div>';
  peerMap.forEach((p, id) => {
    let dot = '';
    if (p.connState === 'connected') {
      // v3.2: use smoothed loss for indicator color
      const smoothed = lossEwma[id] !== undefined ? lossEwma[id] : (p.lossPct || 0);
      if (smoothed > 8) dot = 'fail';
      else if (peerRelay[id] || p.usedRelay) dot = 'relay';
      else dot = 'conn';
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

// ════════════════════════════════════════════════════════════════════════════
// ZOMBIE DETECTION (v3.1 fix preserved)
// ════════════════════════════════════════════════════════════════════════════

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

// ════════════════════════════════════════════════════════════════════════════
// v3.2 BUG A FIX: connection timer no longer kills healthy connections
// ════════════════════════════════════════════════════════════════════════════
// Your log showed:
//   PC 3e2e5f10 ICE=connected      ← ICE is GOOD
//   PC 3e2e5f10 conn=connecting    ← PC about to flip to connected
//   CONN-TIMEOUT 3e2e5f10 state=connecting/connected  ← Timer killed it!
//
// The fix: if iceConnectionState is 'connected' or 'completed', the
// connection IS working. Wait for conn to flip. Don't kill it.
// Also bumped 6s → 10s for slow networks.
// ════════════════════════════════════════════════════════════════════════════
function startConnectionTimer(pid) {
  const pc = peers[pid];
  if (!pc || pc._connTimer) return;
  pc._connTimerFires = pc._connTimerFires || 0;
  pc._connTimer = setTimeout(() => {
    pc._connTimer = null;
    if (peers[pid] !== pc || pc.connectionState === 'connected' || pc.connectionState === 'closed') {
      return;
    }
    // v3.2 BUG A FIX: if ICE is already connected/completed, the connection
    // IS working. PC.connectionState just hasn't flipped yet. Wait it out.
    if (pc.iceConnectionState === 'connected' || pc.iceConnectionState === 'completed') {
      log("CONN-TIMER " + pid + " ICE healthy (" + pc.iceConnectionState + "), waiting for conn flip");
      pc._connTimerFires++;
      if (pc._connTimerFires < 3) {
        startConnectionTimer(pid);
      }
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
      // v3.3 FIX C: if we've been checking for ~10-20s, the path is
      // almost certainly blocked. Force relay NOW instead of waiting
      // for 'failed' (which adds another 15-30s of dead time).
      if (pc._connTimerFires >= 1 && !peerRelay[pid] && MY_ID > pid) {
        log("CONN-TIMER " + pid + " stuck checking 10s -> force RELAY (likely blocked UDP)");
        peerRelay[pid] = true;
        lastRelayAt[pid] = Date.now();
        // Tell the other side we're going relay-only
        if (ws && ws.readyState === 1) {
          ws.send(JSON.stringify({ type: 'request_relay', to: pid, reason: 'stuck-checking' }));
        }
        // Rebuild PC with relay-only transport
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
  }, 10000); // v3.2: bumped 6s -> 10s
}

async function forceIceRestart(pid) {
  if (MY_ID <= pid) {
    log("ICE restart ignored (smaller side) " + pid);
    return;
  }
  // v3.2: extended cooldown 5s -> 8s
  if (lastIceRestartAt[pid] && Date.now() - lastIceRestartAt[pid] < 8000) {
    log("ICE restart cooldown " + pid);
    return;
  }
  const pc = peers[pid];
  if (!pc || pc.connectionState === 'closed') return;
  const p = peerMap.get(pid);
  if (!p || p._iceRestarting || p._retrying) return;
  if (pc.iceConnectionState === 'checking' || pc.iceConnectionState === 'connecting') {
    return;
  }
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
        await existing.setLocalDescription({ type: 'rollback' });
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
    try { await pc.addIceCandidate(new RTCIceCandidate(cand)); } catch (e) { /* benign */ }
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
    if (!hasAS) {
      out.splice(audioStart + 1, 0, 'b=AS:40');
    }
  }

  return out.join('\r\n');
}

function startInboundLevel(stream, pid) {
  if (inboundLevelTimers[pid]) {
    clearInterval(inboundLevelTimers[pid]);
    delete inboundLevelTimers[pid];
  }
  try {
    if (!remoteAudioCtx) {
      remoteAudioCtx = new (window.AudioContext || window.webkitAudioContext)();
    }
    if (remoteAudioCtx.state === 'suspended') {
      remoteAudioCtx.resume().catch(() => {});
    }
    const src = remoteAudioCtx.createMediaStreamSource(stream);
    const analyser = remoteAudioCtx.createAnalyser();
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
      capOutboundBitrate(pc, 32);
      startStats(pc, pid);
      delete frozenJitterCounts[pid];
      delete frozenJitterValues[pid];
      delete _lastJitters[pid];
      // v3.2: reset loss tracking on (re)connect
      lossEwma[pid] = 0;
      delete sustainedBadStart[pid];
      // v3.2: mark when relay path stabilized (used for cooldown)
      if (peerRelay[pid]) {
        relayConnectedAt[pid] = Date.now();
      }
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

// ════════════════════════════════════════════════════════════════════════════
// v3.2 STATS — SLIDING WINDOW EWMA, NO MORE 4s SPIKE OVERREACTIONS
// ════════════════════════════════════════════════════════════════════════════
// Old logic (broken): one 4s sample of >5% loss → instant relay switch.
//   Problem: cellular networks naturally spike. Switched too eagerly,
//   then stuck on a relay that was also bad.
//
// New logic (smart):
//   - Maintain exponentially-weighted moving avg of loss (12s window)
//   - Trigger relay only when EWMA > 10% AND sustained for 8s
//   - After relay switch, 6s grace period before any new triggers
//   - If on relay AND EWMA still > 15% AND sustained 16s → full rebuild
//     with fresh ICE servers (refetched from /turn) — may pick different relay
// ════════════════════════════════════════════════════════════════════════════

let _lastJitters = {};

function startStats(pc, pid) {
  if (statsTimers[pid]) clearInterval(statsTimers[pid]);
  let lastRecv = 0, lastLost = 0;
  let lastSent = 0;
  let consecutiveStalled = 0;
  let outboundStall = 0;
  let logTick = 0;

  // v3.2: reset EWMA window for this connection
  lossEwma[pid] = 0;
  delete sustainedBadStart[pid];

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
      if (peerInfo) { peerInfo.lossPct = lossPct; peerInfo.recvRate = dRecv / 4; }

      const peerMuted = peerInfo && peerInfo.muted;
      const mutedAgo = lastMutedAt[pid] ? Date.now() - lastMutedAt[pid] : Infinity;
      const inMuteGrace = mutedAgo < 4000;

      // v3.2: EWMA loss smoothing (alpha=0.33 ~ 12s effective window @ 4s tick)
      // Only update when we have enough samples to be meaningful
      if (total > 10) {
        const alpha = 0.33;
        lossEwma[pid] = alpha * lossPct + (1 - alpha) * (lossEwma[pid] || 0);
      }
      if (peerInfo) peerInfo.lossEwma = lossEwma[pid];

      // ── LOG (anomaly OR heartbeat) ──
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

      // v3.2: post-relay grace period (6s after relay stabilizes)
      const relayJustConnected = relayConnectedAt[pid] && (Date.now() - relayConnectedAt[pid] < 6000);

      // ── TRIGGER 1 (v3.2): sustained EWMA loss → relay switch ──
      // Old: 5% over 4s. New: 10% EWMA sustained 8s.
      if (!peerMuted && !inMuteGrace && !relayJustConnected) {
        if ((lossEwma[pid] || 0) > 10) {
          if (!sustainedBadStart[pid]) sustainedBadStart[pid] = Date.now();
          const sustainedMs = Date.now() - sustainedBadStart[pid];

          if (!peerRelay[pid] && sustainedMs >= 8000) {
            requestRelaySwitch(pid, "sustained loss ewma=" + lossEwma[pid].toFixed(1) + "%");
            sustainedBadStart[pid] = null;
          }
          // v3.2 ESCALATION: if already on relay and STILL bad after 16s, full rebuild
          else if (peerRelay[pid] && (lossEwma[pid] || 0) > 15 && sustainedMs >= 16000) {
            await fullRebuild(pid, "relay also failing ewma=" + lossEwma[pid].toFixed(1) + "%");
            sustainedBadStart[pid] = null;
          }
        } else {
          // EWMA recovered, clear the bad-window timer
          sustainedBadStart[pid] = null;
        }
      }

      // ── TRIGGER 2: complete audio stall (DTX-tolerant) ──
      if (!peerMuted && !inMuteGrace && dRecv === 0) {
        consecutiveStalled++;
        if (consecutiveStalled >= 2 && !peerRelay[pid] && !relayJustConnected) {
          requestRelaySwitch(pid, "stalled (0 pkts/8s)");
          consecutiveStalled = 0;
        } else if (consecutiveStalled >= 3 && peerRelay[pid] && !relayJustConnected) {
          // v3.2: stalled even on relay → full rebuild
          await fullRebuild(pid, "stalled on relay (0 pkts/12s)");
          consecutiveStalled = 0;
        }
      } else {
        consecutiveStalled = 0;
      }

      // ── TRIGGER 3: outbound stall ──
      if (dSent === 0 && !isMuted) {
        outboundStall++;
        if (outboundStall >= 2 && !peerRelay[pid] && !relayJustConnected) {
          requestRelaySwitch(pid, "outbound stalled (0 sent/8s)");
          outboundStall = 0;
        }
      } else {
        outboundStall = 0;
      }

      // ── TRIGGER 4: zombie transceiver ──
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
  }, 4000);
}

// ════════════════════════════════════════════════════════════════════════════
// v3.2 FULL REBUILD — last resort when relay also fails
// ════════════════════════════════════════════════════════════════════════════
// Refetches /turn (gets fresh creds, possibly different relay server),
// destroys PC, rebuilds from scratch with relay-only mode preserved.
// Cooldown: 30s (don't spam rebuilds).
// ════════════════════════════════════════════════════════════════════════════
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

  // Refetch ICE servers — may give us a different/better relay
  try {
    await fetchIceServers();
  } catch (e) {}

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
    try {
      await createOffer(pid);
    } finally {
      // createOffer manages its own renegInProgress
    }
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

  if (p.retries > 4) {
    log("GIVE UP on " + pid);
    p._retrying = false;
    destroyPeer(pid);
    return;
  }

  // v3.3 FIX B: ALWAYS force relay on retry. If direct failed once,
  // the network won't suddenly start allowing P2P. Skip wasting
  // another 15-30s on the same broken path.
  if (!peerRelay[pid]) {
    log("retry forcing RELAY for " + pid + " (direct path failed)");
    peerRelay[pid] = true;
    lastRelayAt[pid] = Date.now();
    if (ws && ws.readyState === 1 && MY_ID > pid) {
      // Tell the smaller side to also go relay
      ws.send(JSON.stringify({ type: 'request_relay', to: pid, reason: 'retry-after-fail' }));
    }
  }

  const delay = Math.min(1000 * p.retries, 4000);
  log("retry #" + p.retries + " in " + delay + "ms -> " + pid + " (RELAY)");
  setTimeout(async () => {
    if (!peerMap.has(pid)) { p._retrying = false; return; }
    if (!ws || ws.readyState !== 1) { p._retrying = false; return; }

    if (MY_ID > pid) {
      // v3.3: don't reuse the failed PC. Always rebuild fresh on retry.
      // The old code did forceIceRestart on retry #1, but iceRestart
      // on a failed PC keeps the same iceTransportPolicy. We need a
      // new PC with relay-only.
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
    const ac = new (window.AudioContext || window.webkitAudioContext)();
    const src = ac.createMediaStreamSource(localStream);
    localAnalyser = ac.createAnalyser();
    localAnalyser.fftSize = 256;
    src.connect(localAnalyser);
    const data = new Uint8Array(localAnalyser.frequencyBinCount);
    let lastSent = 0, lastLevel = 0;
    localLevelTimer = setInterval(() => {
      if (isMuted || !localStream) return;
      localAnalyser.getByteFrequencyData(data);
      let sum = 0;
      for (let i = 0; i < data.length; i++) sum += data[i];
      const level = sum / data.length / 255;
      const now = Date.now();
      const speaking = level > 0.05;
      const wasSpeaking = lastLevel > 0.05;
      if (speaking !== wasSpeaking || (speaking && now - lastSent > 1000)) {
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

  if (realTrack.readyState === 'live') {
    realTrack.enabled = !isMuted;
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

// ─── IMAGE ATTACHMENTS + REPLY-TO-MESSAGE (unchanged) ──────────────────────

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
  const snippet = txt ? txt.slice(0, 80) : (m.image ? 'Image' : '');
  replyingTo = {
    id: m.id || '',
    name: m.name || '?',
    text: snippet,
    has_image: !!m.image
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
  const previewText = (replyingTo.has_image && !replyingTo.text)
    ? 'Image'
    : (replyingTo.text || '');
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
  if (m.kind === 'system') {
    const d = document.createElement('div');
    d.className = 'msg-system';
    d.textContent = m.text;
    c.appendChild(d);
    scroll();
    return;
  }
  const isSelf = !!m.self;
  const pi = peerMap.get(m.peer_id) || {};
  const name = m.name || pi.name || '?';
  const isH = isSelf ? isHost : pi.is_host;
  const showBadge = isH && name === 'Sor';
  const avSrc = m.avatar || pi.avatar || '';
  const row = document.createElement('div');
  row.className = 'msg-row ' + (isSelf ? 'self' : 'other');
  let avHTML;
  if (avSrc) avHTML = '<div class="avatar"><img src="' + esc(avSrc) + '"></div>';
  else avHTML = '<div class="avatar"><span>' + esc(name[0].toUpperCase()) + '</span></div>';
  const header = '<div class="msg-header"><span class="msg-name">' + esc(name) + '</span>' + (showBadge ? '<span class="msg-badge host">Host</span>' : '') + '</div>';

  let replyHTML = '';
  if (m.reply_to) {
    const r = m.reply_to;
    const previewText = (r.has_image && !r.text) ? 'Image' : (r.text || '');
    replyHTML = '<div class="msg-reply">' +
                  '<span class="msg-reply-name">' + esc(r.name || '?') + '</span>' +
                  '<span class="msg-reply-text">' + esc(previewText) + '</span>' +
                '</div>';
  }
  let imgHTML = '';
  if (m.image) {
    imgHTML = '<img class="chat-img" src="' + esc(m.image) + '" alt="image">';
  }
  let textHTML = '';
  if (m.text) {
    textHTML = '<div class="msg-text">' + esc(m.text) + '</div>';
  }
  let bubbleClass = 'msg-bubble';
  if (m.image) bubbleClass += ' has-img';
  if (m.image && m.text) bubbleClass += ' has-text';
  const bubbleInner = replyHTML + imgHTML + (textHTML || (m.image ? '' : esc(m.text)));
  row.innerHTML = avHTML + '<div class="msg-content">' + header +
                  '<div class="' + bubbleClass + '">' + bubbleInner + '</div></div>';

  row.addEventListener('click', (ev) => {
    const t = ev.target;
    if (t.classList && t.classList.contains('chat-img')) {
      ev.stopPropagation();
      openImagePreview(t.src);
      return;
    }
    if (t.tagName === 'IMG' && t.closest('.avatar')) {
      return;
    }
    startReply(m);
  });

  c.appendChild(row);
  row._msgData = m;
  scroll();
}

function renderSys(t) {
  const c = document.getElementById('msgs'); if (!c) return;
  const d = document.createElement('div');
  d.className = 'msg-system';
  d.textContent = t;
  c.appendChild(d);
  scroll();
}

function scroll() {
  const e = document.getElementById('msgs');
  if (!e) return;
  if (autoScrollEnabled) {
    e.scrollTop = e.scrollHeight;
  } else {
    newMsgCount++;
    updateNewMsgPill();
  }
}

function updateNewMsgPill() {
  const pill = document.getElementById('newMsgPill');
  if (!pill) return;
  if (newMsgCount > 0) {
    pill.textContent = '\u2193 ' + newMsgCount + ' new message' + (newMsgCount > 1 ? 's' : '');
    pill.classList.add('show');
  } else {
    pill.classList.remove('show');
  }
}

function catchUpScroll() {
  autoScrollEnabled = true;
  newMsgCount = 0;
  const pill = document.getElementById('newMsgPill');
  if (pill) pill.classList.remove('show');
  const e = document.getElementById('msgs');
  if (e) e.scrollTop = e.scrollHeight;
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

log("page loaded v3.5");
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


async def main():
    print("=" * 60)
    print(f"Silent Hill Bot v3.5 SECRET-KEY-COMPATIBLE | {WEB_APP_URL} | Port {PORT}")
    print(f"Kyodo: {KYODO_OK}")
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
        print("! NO PREMIUM TURN CONFIGURED — Gulf/MENA peers WILL FAIL  !")
        print("!" * 60)
    print("v3.5: METERED_API_KEY now accepts SECRET KEY or API KEY")
    print("v3.5: Auto-mints expiring credentials when secret key is used")
    print("v3.5: Hit /turn-debug to confirm Metered creds are loading")
    print("=" * 60)
    await asyncio.gather(
        Server(Config(app=app, host="0.0.0.0", port=PORT, log_level="warning")).serve(),
        run_kyodo_bot(),
        keepalive(),
    )


if __name__ == "__main__":
    asyncio.run(main())
