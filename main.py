"""
Silent Hill Voice Call Bot — BEAST MODE v2
═══════════════════════════════════════════════════════════════════════════════
KEY UPGRADES vs. previous version:
  • TURN over TLS (turns:443) — punches through aggressive MENA mobile firewalls
  • iceTransportPolicy:'relay' fallback after 1 failure (forces TURN-only faster)
  • Connection escalation: normal → fast ICE restart (6s timeout) → full restart → relay-only
  • Perfect Negotiation pattern (real, not the half-broken rollback-then-destroy)
  • WebSocket auto-reconnect with exponential backoff (network blips don't kill call)
  • Audio: echoCancellation + noiseSuppression + AGC + Opus tuning (DTX, FEC, 32kbps)
  • /turn endpoint — server-side TURN config with optional Metered/Twilio/Cloudflare
  • RTCStats monitoring per peer (packet loss / jitter shown in debug) — INBOUND + OUTBOUND
  • Speaking indicator (audio-level analyser) — shows who is talking
  • Wake Lock so phone screen doesn't sleep mid-call
  • SDP munging for Opus tuning (works on all browsers including iOS Safari)
  • FAST relay fallback — 6s connection timeout + 4s disconnect retry + 4s stall detect
  • Bidirectional relay escalation — both sides switch to TURN when either detects trouble
  • Aggressive ICE error logging (onicecandidateerror) for instant STUN/TURN diagnostics
  • MUTE FIX (v2.1): switched from replaceTrack(null) to track.enabled=false/true
    The replaceTrack approach left the RTP sender in a zombie state on mobile
    so voice would vanish after unmute. track.enabled is the W3C-spec way and
    keeps RTP flowing (DTX silence) so the sender never goes dead.
  • REINFORCEMENTS (v2.2):
      - STATS log throttling — only logs anomalies + a heartbeat every ~32s
      - Network online/offline listeners — auto ICE restart on Wi-Fi↔4G switch
      - Mic watchdog — if another app steals the mic, reacquire & replaceTrack
      - Outbound bitrate hard cap via setParameters (32kbps) on top of SDP cap
      - Visibility resume — kicks fresh stats sample immediately on foreground

═══════════════════════════════════════════════════════════════════════════════
GETTING REAL TURN CREDENTIALS (READ THIS — IT'S WHY ALGERIA FAILS):
  Public TURN (openrelay) is rate-limited and often blocked. For real reliability,
  set ONE of these env vars (free tiers exist for all of them):

  Option A (easiest, free 50GB/mo): Metered.ca
    METERED_API_KEY=xxxxx
    Sign up at https://www.metered.ca/turn-server, copy your API key.
    Server will fetch fresh credentials per session.

  Option B (free 1TB/mo): Cloudflare Realtime TURN
    CF_TURN_TOKEN_ID=xxxxx
    CF_TURN_API_TOKEN=xxxxx
    Sign up at https://dash.cloudflare.com → Calls → TURN.

  Option C (DIY, requires VPS): self-hosted coturn
    CUSTOM_TURN_URL=turns:your-domain.com:443?transport=tcp
    CUSTOM_TURN_USER=username
    CUSTOM_TURN_PASS=password

  If none set, falls back to public TURN (works for ~70% of MENA cases).
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
METERED_DOMAIN = os.environ.get("METERED_DOMAIN", "")  # e.g. "yourapp.metered.live"
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
    """Fetch fresh TURN creds from Metered.ca (free 50GB/mo)."""
    if not METERED_API_KEY or not METERED_DOMAIN:
        return []
    try:
        url = f"https://{METERED_DOMAIN}/api/v1/turn/credentials?apiKey={METERED_API_KEY}"
        async with aiohttp.ClientSession() as s:
            async with s.get(url, timeout=10) as r:
                if r.status == 200:
                    return await r.json()
    except Exception as e:
        print(f"[turn] metered err: {e}")
    return []


async def fetch_cloudflare_creds() -> List[dict]:
    """Fetch fresh TURN creds from Cloudflare Realtime (free 1TB/mo)."""
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
    """Build the best possible ICE config. Caches for 30 min."""
    if _turn_cache["servers"] and time.time() < _turn_cache["expires"]:
        return _turn_cache["servers"]

    servers: List[dict] = [
        # STUN — multiple providers in case one is blocked in some country
        {"urls": ["stun:stun.l.google.com:19302",
                  "stun:stun1.l.google.com:19302",
                  "stun:stun2.l.google.com:19302"]},
        {"urls": "stun:stun.cloudflare.com:3478"},
        {"urls": "stun:global.stun.twilio.com:3478"},
    ]

    # Try premium TURN first (works in 99% of cases)
    metered = await fetch_metered_creds()
    if metered:
        servers.extend(metered)
        print(f"[turn] using Metered.ca ({len(metered)} URLs)")

    cf = await fetch_cloudflare_creds()
    if cf:
        servers.extend(cf)
        print(f"[turn] using Cloudflare ({len(cf)} URLs)")

    if CUSTOM_TURN_URL and CUSTOM_TURN_USER:
        servers.append({"urls": CUSTOM_TURN_URL,
                        "username": CUSTOM_TURN_USER,
                        "credential": CUSTOM_TURN_PASS})
        print(f"[turn] using custom TURN")

    # Fallback: public TURN (NOT reliable for MENA, but better than nothing)
    # CRITICAL: includes turns: (TLS) on 443 — punches through almost every firewall
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
    _turn_cache["expires"] = time.time() + 1800  # 30 min
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
        if init.get("type") == "join":
            name = init.get("name", "Unknown")[:30]
            avatar = init.get("avatar", "")[:200000]  # increased for higher-quality avatars
    except asyncio.TimeoutError:
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

    # Tell peer their own ID first
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

    # Server-side ping task — keeps connection alive through aggressive ISP timeouts
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
                text = msg.get("text", "").strip()[:500]
                if not text:
                    continue
                cm = {"type": "chat", "kind": "user", "peer_id": peer_id,
                      "name": name, "avatar": avatar, "text": text,
                      "time": datetime.now().isoformat()}
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
                # Lightweight active-speaker indicator broadcast
                st = {"type": "speaking", "peer_id": peer_id, "level": msg.get("level", 0)}
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
        ping_task.cancel()
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

        # Defer room cleanup by 60s — handles brief disconnects (mobile network blip)
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
.voice-bar{position:relative;z-index:10;background:rgba(28,28,30,0.95);border-top:1px solid rgba(255,255,255,0.06);padding:10px 16px;display:flex;align-items:center;justify-content:center;gap:14px;flex-shrink:0}
.voice-btn{width:48px;height:48px;border-radius:50%;border:none;display:flex;align-items:center;justify-content:center;font-size:22px;cursor:pointer;transition:.2s}
.voice-btn.mute{background:#3a3a3c;color:#fff}
.voice-btn.mute.muted{background:#ff3b30;color:#fff}
.voice-btn.leave{background:#ff3b30;color:#fff;font-size:20px}
.voice-status{font-size:12px;color:#8e8e93;min-width:80px;text-align:center}
.input-bar{position:relative;z-index:10;background:rgba(13,13,13,0.95);border-top:1px solid rgba(255,255,255,0.06);padding:8px 12px;display:flex;align-items:center;gap:8px;flex-shrink:0}
.input-attach,.input-send{width:38px;height:38px;border-radius:50%;border:none;display:flex;align-items:center;justify-content:center;cursor:pointer;flex-shrink:0}
.input-attach{background:#2c2c2e;color:#fff;font-size:18px}
.input-field{flex:1;height:38px;border-radius:19px;border:none;background:#1c1c1e;color:#fff;padding:0 14px;font-size:14px;outline:none}
.input-field::placeholder{color:#8e8e93}
.input-send{background:#007aff;color:#fff;font-size:16px}
.input-send:active{transform:scale(.92)}
.overlay{position:fixed;inset:0;z-index:100;background:rgba(0,0,0,.88);display:flex;align-items:center;justify-content:center}
.o-box{background:#1c1c1e;border-radius:16px;padding:24px;width:90%;max-width:340px;text-align:center}
.o-box h2{font-size:18px;margin-bottom:8px}
.o-box p{font-size:13px;color:#8e8e93;margin-bottom:14px}
.o-box input{width:100%;height:44px;border-radius:12px;border:1px solid #3a3a3c;background:#2c2c2e;color:#fff;padding:0 14px;font-size:15px;text-align:center;outline:none;margin-bottom:10px}
.o-box button{width:100%;height:44px;border-radius:12px;border:none;background:#007aff;color:#fff;font-size:15px;font-weight:600;cursor:pointer}
.av-preview{width:80px;height:80px;border-radius:50%;margin:0 auto 10px;background:#2c2c2e;display:flex;align-items:center;justify-content:center;font-size:32px;font-weight:600;color:#8e8e93;overflow:hidden;cursor:pointer;border:3px solid #3a3a3c}
.av-preview img{width:100%;height:100%;object-fit:cover}
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
<button class="menu-btn" onclick="document.getElementById('dbg').classList.toggle('show')">&#8942;</button>
</div>

<div class="peer-status" id="pstat"></div>

<div class="messages" id="msgs"></div>
<div class="voice-bar">
<button class="voice-btn mute" id="muteBtn" onclick="toggleMute()" title="Mute">&#127908;</button>
<span class="voice-status" id="vstat">Connecting...</span>
<button class="voice-btn leave" onclick="leaveCall()">&#10005;</button>
</div>
<div class="input-bar">
<button class="input-attach">+</button>
<input type="text" class="input-field" id="msgIn" placeholder="Write a message..." onkeypress="if(event.key==='Enter')sendMsg()">
<button class="input-send" onclick="sendMsg()">&#10148;</button>
</div>
</div>

<script>
// ════════════════════════════════════════════════════════════════════════════
// SILENT HILL CLIENT — BEAST MODE v2.2
// Architecture:
//   • Mesh WebRTC topology (good for ≤6 voice participants)
//   • Server-assigned peer IDs prevent glare
//   • Larger ID = offerer, Smaller ID = answerer (deterministic)
//   • Perfect Negotiation pattern handles edge cases (renegotiation, ICE restart)
//   • Connection escalation: normal → fast ICE restart (6s) → full restart → relay-only
//   • Bidirectional relay: both sides switch to TURN when quality degrades
//   • Outbound stall detection — catches one-way-audio failures instantly
//   • MUTE/UNMUTE FIX (v2.1): uses track.enabled=false/true (W3C-spec mute).
//     The previous replaceTrack(null) approach left the RTP sender in a zombie
//     state on mobile (esp. iOS Safari) — connection stayed "connected" but no
//     packets ever flowed after unmute. track.enabled keeps RTP flowing with
//     DTX comfort-noise so the sender never enters that broken state.
//   • v2.2 REINFORCEMENTS (all defensive, no logic changes):
//     - STATS log throttling: only logs anomalies + heartbeat every ~32s
//     - Online/offline listeners: ICE-restart on Wi-Fi↔cellular switch
//     - Mic watchdog: auto-reacquire if another app steals the mic
//     - Outbound bitrate hard-cap via setParameters (32kbps)
//     - Visibility resume: forces fresh stats sample on foreground
// ════════════════════════════════════════════════════════════════════════════

const ROOM = "__ROOM_ID__", TOKEN = "__TOKEN__";
let MY_ID = "";
let ws = null, localStream = null, myName = "", myAvatar = "";
let isMuted = false, isHost = false;
let leaving = false;
let wsRetries = 0;
let wakeLock = null;

const peers = {};      // pid -> RTCPeerConnection
const audios = {};     // pid -> HTMLAudioElement (PERSISTENT across PC rebuilds)
const peerMap = new Map();    // pid -> {name, avatar, is_host, muted, connState, retries, speaking, recvLevel, lossPct, usedRelay}
const iceBuffer = {};  // pid -> queued candidates received before setRemoteDescription
const statsTimers = {}; // pid -> setInterval handle
const inboundLevelTimers = {}; // pid -> setInterval for incoming audio level monitoring
const lastOfferUfrag = {}; // pid -> last seen ICE ufrag (for duplicate-offer detection)
const peerRelay = {};  // pid -> bool: should this specific peer use TURN-relay-only?
let remoteAudioCtx = null; // Shared AudioContext for analysing INCOMING streams
let audioUnlocked = false; // tracks whether play() has succeeded at least once

let ICE_SERVERS = [
  // Default fallback if /turn endpoint fails — these are hardcoded
  {urls: ['stun:stun.l.google.com:19302', 'stun:stun1.l.google.com:19302']},
  {urls: 'turn:openrelay.metered.ca:443?transport=tcp',
   username: 'openrelayproject', credential: 'openrelayproject'},
  {urls: 'turns:openrelay.metered.ca:443?transport=tcp',
   username: 'openrelayproject', credential: 'openrelayproject'},
];

// ── Audio constraints — modern echo cancel + noise suppression + AGC ──
const AUDIO_CONSTRAINTS = {
  audio: {
    echoCancellation: true,
    noiseSuppression: true,
    autoGainControl: true,
    sampleRate: { ideal: 48000 },
    channelCount: { ideal: 1 } // mono is plenty for voice; halves bandwidth
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

// ── Logging ──
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

// ── Avatar picker ──
function pickAv(e) {
  const f = e.target.files[0]; if (!f) return;
  // Compress avatar to keep WS payloads small
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

// ── Fetch fresh ICE servers from /turn ──
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
}

// ── WakeLock — keep screen alive during call ──
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
    if (!wakeLock) acquireWakeLock();
    // v2.2: kick all stats timers immediately so we don't wait the full 4s tick
    // to notice that something went stale while the screen was off.
    log("👁 foreground — refreshing peer health");
    Object.entries(peers).forEach(([pid, pc]) => {
      if (pc.connectionState === 'disconnected' && MY_ID > pid) {
        log("disconnected peer on resume → ICE restart " + pid);
        forceIceRestart(pid);
      }
    });
  }
});

// ── v2.2 Network change handling ──
// On mobile, switching Wi-Fi ↔ 4G changes your local IP. The existing
// ICE candidate pair becomes invalid silently. Trigger an ICE restart
// proactively so we don't wait for the stall detector to notice.
window.addEventListener('online', () => {
  log("📶 network online — refreshing connections");
  Object.entries(peers).forEach(([pid, pc]) => {
    if (MY_ID > pid && pc.connectionState !== 'closed') {
      // small jitter so multi-peer doesn't restart simultaneously
      setTimeout(() => forceIceRestart(pid), Math.random() * 500);
    }
  });
});
window.addEventListener('offline', () => {
  log("📵 network offline");
});

// ── v2.2 Mic watchdog ──
// Another app (incoming phone call, voice memo, etc.) can steal the mic
// mid-call. The MediaStreamTrack fires 'ended' when this happens. Without
// recovery, your mic dies until you rejoin. With this, we reacquire and
// replaceTrack into every peer so voice resumes seamlessly.
function watchLocalTrack() {
  if (!localStream) return;
  const t = localStream.getAudioTracks()[0];
  if (!t || t._watched) return;
  t._watched = true;
  t.addEventListener('ended', async () => {
    log("⚠ local mic track ended — reacquiring");
    try {
      const newStream = await navigator.mediaDevices.getUserMedia(AUDIO_CONSTRAINTS);
      const newTrack = newStream.getAudioTracks()[0];
      newTrack.enabled = !isMuted;
      // Swap the track into every active peer connection
      Object.values(peers).forEach(pc => {
        pc.getSenders().forEach(s => {
          if (s.track && s.track.kind === 'audio') {
            try { s.replaceTrack(newTrack); } catch (e) { log("reacquire replaceTrack err: " + e.message); }
          }
        });
      });
      // Stop the dead old stream and adopt the new one
      try { localStream.getTracks().forEach(tr => tr.stop()); } catch (e) {}
      localStream = newStream;
      // Re-arm watchdog and level monitor for the new track
      watchLocalTrack();
      setupLocalLevelMonitor();
      log("✓ mic reacquired");
    } catch (e) {
      log("✗ mic reacquire failed: " + e.message);
      document.getElementById('vstat').textContent = 'No mic';
    }
  });
}

// ── Join flow ──
async function doJoin() {
  const n = document.getElementById('nameIn').value.trim();
  if (!n) { alert("Enter name"); return; }
  myName = n;
  document.getElementById('joinBtn').disabled = true;
  document.getElementById('joinBtn').textContent = "...";

  // CRITICAL: Use the join-button click as the gesture to unlock AudioContext.
  // Mobile browsers require a user gesture to start audio playback. Doing this
  // here means by the time remote streams arrive, audio is already unlocked.
  try {
    remoteAudioCtx = new (window.AudioContext || window.webkitAudioContext)();
    if (remoteAudioCtx.state === 'suspended') await remoteAudioCtx.resume();
    // Play a 100ms silent buffer to fully arm autoplay
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
    document.getElementById('vstat').textContent = 'Connected';
    log("mic OK");
    setupLocalLevelMonitor();
    watchLocalTrack(); // v2.2: arm watchdog so we recover if mic gets stolen
  } catch (e) {
    log("mic err: " + e.message);
    document.getElementById('vstat').textContent = 'No mic';
  }
  document.getElementById('joinOvl').classList.add('hidden');
  document.getElementById('app').classList.remove('hidden');
  acquireWakeLock();

  // Start lightweight ticker that updates the audio-level bars next to each peer name
  if (_peerLevelTicker) clearInterval(_peerLevelTicker);
  _peerLevelTicker = setInterval(updPeerLevels, 150);

  connectWS();
}

// ── WebSocket with auto-reconnect ──
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
        // Clean up peers that are no longer present (server is source of truth)
        const currentIds = new Set(m.peers.map(p => p.id));
        for (const [id, _] of peerMap) {
          if (!currentIds.has(id)) {
            nukePeer(id);
            peerMap.delete(id);
          }
        }
        for (const p of m.peers) {
          addPeer(p);
          // Larger ID always offers — guarantees no glare
          if (MY_ID > p.id) {
            log("I'm larger (" + MY_ID + ">" + p.id + ") → offer");
            createOffer(p.id);
          } else {
            log("I'm smaller (" + MY_ID + "<" + p.id + ") → wait");
          }
        }
        break;

      case 'peer_joined':
        addPeer(m.peer);
        renderSys(m.peer.name + " joined");
        if (MY_ID && MY_ID > m.peer.id) {
          log("late: I'm larger → offer to " + m.peer.id);
          createOffer(m.peer.id);
        } else if (MY_ID) {
          log("late: I'm smaller → wait for offer from " + m.peer.id);
        }
        break;

      case 'peer_left':
        nukePeer(m.peer_id);
        peerMap.delete(m.peer_id);
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
        if (!isZombie && MY_ID < m.from) {
          // Normal relay: smaller side ignores (larger side drives relay switch)
          break;
        }
        log("got relay request from " + m.from + " (" + reason + ")");
        peerRelay[m.from] = true;
        if (isZombie && MY_ID > m.from) {
          // Zombie from smaller side: do full rebuild (ICE restart won't fix dead transceiver)
          log("zombie reneg from smaller side → full rebuild " + m.from);
          destroyPeer(m.from);
          setTimeout(() => createOffer(m.from), 300);
        } else {
          await switchPeerToRelay(m.from);
        }
        break;
      }

      case 'mute_cmd': {
        // Server confirms our mute. Use track.enabled=false (W3C-spec mute).
        // RTP keeps flowing with DTX comfort-noise — sender never goes zombie.
        if (localStream) {
          const t = localStream.getAudioTracks()[0];
          if (t) t.enabled = false;
        }
        isMuted = true;
        document.getElementById('muteBtn').classList.add('muted');
        document.getElementById('muteBtn').innerHTML = '&#128263;';
        document.getElementById('vstat').textContent = 'Muted';
        updPeers();
        break;
      }
      case 'unmute_cmd': {
        // Server confirms our unmute. Just flip track.enabled back on.
        // No replaceTrack dance needed — sender was alive the whole time.
        if (localStream) {
          const t = localStream.getAudioTracks()[0];
          if (t) t.enabled = true;
        }
        isMuted = false;
        document.getElementById('muteBtn').classList.remove('muted');
        document.getElementById('muteBtn').innerHTML = '&#127908;';
        document.getElementById('vstat').textContent = 'Connected';
        updPeers();
        break;
      }

      case 'voice_state': {
        const p = peerMap.get(m.peer_id);
        if (p) { p.muted = m.muted; updPeers(); }
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
    }
  };

  ws.onclose = e => {
    log("WS close " + e.code);
    if (!leaving) {
      // Don't tear down RTC immediately — peer might come back fast
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
    document.getElementById('vstat').textContent = 'Reconnecting...';
  };
}

// ── Peer tracking ──
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
      // Once connected, dot color reflects QUALITY:
      //   green  = healthy (loss < 2%)
      //   orange = relay path (working but indirect)
      //   red    = bad quality (loss > 5%)
      if (p.lossPct !== undefined && p.lossPct > 5) dot = 'fail';
      else if (peerRelay[id] || p.usedRelay) dot = 'relay';
      else dot = 'conn';
    } else if (p.connState === 'failed' || p.connState === 'closed') dot = 'fail';
    else if (p.connState === 'connecting' || p.connState === 'checking' || p.connState === 'new') dot = 'connecting';
    // Glow if EITHER sender signals speaking via WS OR we're actually hearing audio.
    // The second condition is the real proof that audio is flowing through WebRTC.
    const speakClass = (p.speaking || p.actuallyHeard) ? ' speaking' : '';
    const muteIcon = p.muted ? ' 🔇' : '';
    // Mini audio level bar shows inbound signal — if this stays empty while
    // the green dot says speaking, audio path is broken (TURN/codec/etc).
    const levelPct = Math.min(100, Math.round((p.recvLevel || 0) * 200));
    const levelBar = p.connState === 'connected'
      ? '<span style="display:inline-block;width:24px;height:4px;background:rgba(255,255,255,0.15);border-radius:2px;margin-left:4px;vertical-align:middle;overflow:hidden"><span style="display:block;width:' + levelPct + '%;height:100%;background:#34c759;transition:width .1s"></span></span>'
      : '';
    h += '<div class="p-s' + speakClass + '" data-pid="' + id + '"><div class="dot ' + dot + '"></div>' + esc(p.name) + muteIcon + levelBar + '</div>';
  });
  el.innerHTML = h;
}

// Lightweight ticker to refresh level bars without full re-render.
// Only updates the inner level <span> width, not the surrounding elements.
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
    // Also toggle the speaking class without rebuilding HTML
    const isActive = p.speaking || p.actuallyHeard;
    if (isActive && !div.classList.contains('speaking')) div.classList.add('speaking');
    else if (!isActive && div.classList.contains('speaking')) div.classList.remove('speaking');
  });
}
let _peerLevelTicker = null;

// ── Zombie peer detection ──
// Detects dead transceivers where connectionState='connected' but no audio.
// With v2.1 mute fix this should rarely fire — but kept as a safety net for
// genuine network/codec breakdowns. Skips muted peers (no audio expected).
let _lastJitters = {}; // pid -> last jitter value
let _zombieCounts = {};  // pid -> consecutive zombie intervals
let _zombieCooldowns = {}; // pid -> timestamp of last zombie action (prevent rapid-fire)

function checkZombiePeers() {
  peerMap.forEach((p, pid) => {
    if (p.connState !== 'connected') return;
    if (p.muted) return; // peer is muted — silence is expected, not a zombie
    // Cooldown: don't act more than once per 10s to prevent reneg storms
    if (_zombieCooldowns[pid] && Date.now() - _zombieCooldowns[pid] < 10000) return;
    // If recvLevel has been 0 for a while and we know peer isn't muted,
    // the transceiver is likely dead. Trigger a renegotiation.
    if (!p.recvLevel || p.recvLevel < 0.005) {
      _zombieCounts[pid] = (_zombieCounts[pid] || 0) + 1;
      if (_zombieCounts[pid] >= 2) {
        log("ZOMBIE detected " + pid + " (no audio despite connected) → renegotiate");
        _zombieCounts[pid] = 0;
        _zombieCooldowns[pid] = Date.now();
        if (MY_ID > pid) {
          // Force a full rebuild (not just ICE restart) to wake transceivers
          destroyPeer(pid);
          setTimeout(() => createOffer(pid), 300);
        } else {
          // Smaller side: destroy and wait for larger side to offer
          destroyPeer(pid);
          // Ask larger side to re-offer via WS
          if (ws && ws.readyState === 1) {
            ws.send(JSON.stringify({ type: 'request_relay', to: pid, reason: 'zombie-reneg' }));
          }
        }
      }
    } else {
      _zombieCounts[pid] = 0;
    }
  });
}

// Also hook into the stats timer to detect frozen jitter (another zombie sign)
function detectFrozenJitter(pid, jitter) {
  // Cooldown: don't act more than once per 10s
  if (_zombieCooldowns[pid] && Date.now() - _zombieCooldowns[pid] < 10000) return false;
  const last = _lastJitters[pid];
  if (last !== undefined && jitter === last && jitter > 0) {
    // Jitter hasn't changed at all across two intervals while conn=connected
    // AND we're receiving 0 packets → zombie confirmed
    const p = peerMap.get(pid);
    if (p && p.connState === 'connected' && !p.muted && (!p.recvLevel || p.recvLevel < 0.005)) {
      _zombieCooldowns[pid] = Date.now();
      return true;
    }
  }
  _lastJitters[pid] = jitter;
  return false;
}

// ════════════════════════════════════════════════════════════════════════════
// WEBRTC — Perfect Negotiation
// ════════════════════════════════════════════════════════════════════════════

function clearConnectionTimer(pc) {
  if (pc && pc._connTimer) {
    clearTimeout(pc._connTimer);
    pc._connTimer = null;
  }
  if (pc) pc._connTimerFires = 0;
}

// Connection timer: fires 6s after a NEW PC is created (via createOffer).
// Does NOT restart on answers or subsequent offers — only on the initial
// connection attempt. This prevents the infinite loop where every answer
// resets the timer and it fires again 6s later.
function startConnectionTimer(pid) {
  const pc = peers[pid];
  if (!pc || pc._connTimer) return; // already running, don't restart
  pc._connTimerFires = pc._connTimerFires || 0;
  pc._connTimer = setTimeout(() => {
    pc._connTimer = null;
    // Only act if this is still the same PC and it's not connected/closed
    if (peers[pid] !== pc || pc.connectionState === 'connected' || pc.connectionState === 'closed') {
      return;
    }
    // If checking/connecting, ICE is still working — give it more time
    if (pc.iceConnectionState === 'checking' || pc.iceConnectionState === 'connecting') {
      log("CONN-TIMEOUT " + pid + " still checking, waiting...");
      return; // let it finish, timer will NOT re-arm (we already cleared it)
    }
    pc._connTimerFires++;
    if (pc._connTimerFires > 2) {
      log("CONN-TIMEOUT " + pid + " max fires reached, scheduling full retry");
      scheduleRetry(pid);
      return;
    }
    log("CONN-TIMEOUT " + pid + " state=" + pc.connectionState + "/" + pc.iceConnectionState);
    forceIceRestart(pid);
  }, 6000);
}

async function forceIceRestart(pid) {
  const pc = peers[pid];
  if (!pc || pc.connectionState === 'closed') return;
  const p = peerMap.get(pid);
  if (!p || p._iceRestarting || p._retrying) return;
  // If already checking/connecting, a restart is already in progress or just started
  if (pc.iceConnectionState === 'checking' || pc.iceConnectionState === 'connecting') {
    return; // wait for it to resolve
  }
  // If we have a remote offer pending, don't collide — let the negotiation complete
  if (pc.signalingState === 'have-remote-offer') {
    log("ICE restart skipped (have-remote-offer) " + pid);
    return;
  }
  p._iceRestarting = true;
  log("FAST ICE restart " + pid);
  try {
    const o = await pc.createOffer({ iceRestart: true });
    o.sdp = preferOpusAndTune(o.sdp);
    await pc.setLocalDescription(o);
    ws.send(JSON.stringify({ type: 'webrtc_offer', to: pid, sdp: pc.localDescription.sdp }));
    // DO NOT restart the connection timer here — we want it to fire once only
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
    try { peers[pid].close(); } catch (e) {}
    delete peers[pid];
  }
  // CRITICAL: We do NOT remove the audio element by default.
  // Reusing the same <audio> element across PC rebuilds means the browser's
  // "user gesture activation" for autoplay carries over — play() will succeed
  // immediately when the new track arrives, instead of getting blocked.
  if (audios[pid] && !keepAudio) {
    try { audios[pid].srcObject = null; } catch (e) {}
    // Note: deliberately not calling .remove() or deleting from `audios` map
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
}

// Full nuke — call ONLY when the peer leaves the call entirely
function nukePeer(pid) {
  destroyPeer(pid);
  if (audios[pid]) {
    try { audios[pid].pause(); audios[pid].srcObject = null; audios[pid].remove(); } catch (e) {}
    delete audios[pid];
  }
  delete peerRelay[pid];
  delete lastOfferUfrag[pid];
}

// Per-peer relay decision. Set automatically by quality monitoring.
// We escalate this peer to relay-only if direct path has been bad.
function shouldForceRelay(pid) {
  if (peerRelay[pid]) return true;
  const p = peerMap.get(pid);
  return p && p.retries >= 1; // was >= 2 — now faster relay fallback
}

async function createOffer(pid) {
  log("offer→" + pid + (shouldForceRelay(pid) ? " (RELAY-ONLY)" : ""));
  destroyPeer(pid);
  const p = peerMap.get(pid);
  if (!p) return;

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
    // Start guard timer — if not connected in 6s, force ICE restart
    startConnectionTimer(pid);
  } catch (e) {
    log("offer FAIL " + pid + ": " + e.message);
  }
}

async function handleOffer(from, sdp) {
  log("offer←" + from);

  // Duplicate-offer guard: extract ICE ufrag (changes per ICE-restart).
  // If the same ufrag arrives twice in quick succession, ignore the second.
  const ufragMatch = (sdp || '').match(/a=ice-ufrag:(\S+)/);
  const ufrag = ufragMatch ? ufragMatch[1] : null;
  if (ufrag && lastOfferUfrag[from] === ufrag) {
    log("duplicate offer ignored " + from + " (ufrag=" + ufrag + ")");
    return;
  }
  if (ufrag) lastOfferUfrag[from] = ufrag;

  const existing = peers[from];

  // Path A: Existing PC is healthy or in-progress → renegotiate IN PLACE.
  // This is the key fix. Even if we get rapid/stray offers, we don't tear
  // down the working connection and the audio element keeps playing.
  if (existing && existing.signalingState !== 'closed' && existing.connectionState !== 'failed') {
    try {
      // Glare guard: if WE have a local offer pending, only the smaller-ID
      // (polite) side rolls back. Larger-ID (impolite) ignores incoming offer.
      if (existing.signalingState === 'have-local-offer') {
        if (MY_ID > from) {
          log("collision: I'm impolite, ignoring offer from " + from);
          return;
        }
        log("collision: polite rollback for " + from);
        await existing.setLocalDescription({ type: 'rollback' });
      }

      await existing.setRemoteDescription(new RTCSessionDescription({ type: 'offer', sdp }));

      // Flush any ICE candidates that arrived before remoteDescription was set
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
      log("in-place reneg FAIL " + from + ": " + e.message + " → fresh PC");
      // Fall through to fresh-PC path
    }
  }

  // Path B: No existing PC (or it's broken) → build fresh one
  destroyPeer(from);
  try {
    const pc = new RTCPeerConnection(getPCConfig(shouldForceRelay(from)));
    setupPC(pc, from);
    peers[from] = pc;

    // setRemoteDescription FIRST so transceivers match the offer
    await pc.setRemoteDescription(new RTCSessionDescription({ type: 'offer', sdp }));

    // Apply trickle ICE that arrived before we had this PC
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
    // Start guard timer on answerer side too (fresh PC from incoming offer)
    startConnectionTimer(from);
  } catch (e) {
    log("answer FAIL " + from + ": " + e.message);
  }
}

async function handleAnswer(from, sdp) {
  log("ans←" + from);
  try {
    const pc = peers[from];
    if (!pc) { log("no PC for ans " + from); return; }
    if (pc.signalingState !== 'have-local-offer') {
      log("ans skipped (state=" + pc.signalingState + ")");
      return;
    }
    await pc.setRemoteDescription(new RTCSessionDescription({ type: 'answer', sdp }));
    log("ans applied " + from);
    // DO NOT restart connection timer on answer — the timer was started when
    // the offer was created. Restarting it here causes infinite loops.
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

// ── SDP munging: prefer Opus with FEC, DTX, mono, capped bitrate ──
function preferOpusAndTune(sdp) {
  if (!sdp) return sdp;
  // Find Opus payload type
  const lines = sdp.split('\r\n');
  let opusPt = null;
  for (const l of lines) {
    const m = l.match(/^a=rtpmap:(\d+) opus\/48000/i);
    if (m) { opusPt = m[1]; break; }
  }
  if (!opusPt) return sdp;

  // Update or insert fmtp for Opus
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
    // Insert fmtp after rtpmap
    for (let i = 0; i < out.length; i++) {
      if (new RegExp('^a=rtpmap:' + opusPt + ' opus').test(out[i])) {
        out.splice(i + 1, 0, 'a=fmtp:' + opusPt + ' minptime=10;useinbandfec=1;usedtx=1;stereo=0;maxaveragebitrate=32000');
        break;
      }
    }
  }

  // Ensure audio section has sendrecv (critical for 2-way audio)
  let audioStart = -1;
  for (let i = 0; i < out.length; i++) {
    if (out[i].startsWith('m=audio')) { audioStart = i; break; }
  }
  if (audioStart !== -1) {
    let dirIdx = -1;
    for (let i = audioStart + 1; i < out.length; i++) {
      if (out[i].startsWith('m=')) break;
      if (out[i].startsWith('a=sendonly') || out[i].startsWith('a=recvonly')) {
        out[i] = 'a=sendrecv';
        dirIdx = i;
        break;
      }
    }
    // Add bandwidth cap to audio section if not present
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

// ── Inbound audio level monitoring ──
// This taps the RECEIVED stream (after WebRTC) and analyses it.
// If level > 0, audio IS arriving and being decoded.
// If level stays 0 despite sender showing as speaking → audio path is broken.
function startInboundLevel(stream, pid) {
  // Stop any previous monitor for this peer
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
    // IMPORTANT: do NOT connect analyser to destination — that would double-play.
    // The <audio> element is responsible for actual playback.
    const data = new Uint8Array(analyser.frequencyBinCount);
    inboundLevelTimers[pid] = setInterval(() => {
      analyser.getByteFrequencyData(data);
      let sum = 0;
      for (let i = 0; i < data.length; i++) sum += data[i];
      const level = sum / data.length / 255;
      const p = peerMap.get(pid);
      if (p) {
        p.recvLevel = level;
        // If we're receiving audio, mark them as actively heard
        p.actuallyHeard = level > 0.02;
      }
    }, 200);
  } catch (e) {
    log("inboundLevel fail " + pid + ": " + e.message);
  }
}

// ── Audio unlock UI ──
// Shows a banner the user can tap to recover from autoplay blocks
function showAudioUnlockUI() {
  let el = document.getElementById('audioUnlock');
  if (el) return;
  el = document.createElement('div');
  el.id = 'audioUnlock';
  el.style.cssText = 'position:fixed;top:50px;left:10px;right:10px;z-index:150;background:#ff9500;color:#000;padding:14px;border-radius:12px;text-align:center;font-weight:600;font-size:14px;cursor:pointer;animation:msgIn .3s';
  el.textContent = '🔊 Tap here to enable sound';
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

// ── PC event wiring ──
function setupPC(pc, pid) {
  pc.onicecandidate = e => {
    if (e.candidate && ws && ws.readyState === 1) {
      ws.send(JSON.stringify({ type: 'webrtc_ice', to: pid, candidate: e.candidate }));
    }
  };

  // NEW: Log STUN/TURN failures for instant diagnostics
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
      a.muted = false; // explicit, in case some browser default flips this
      a.style.display = 'none';
      document.body.appendChild(a);
      audios[pid] = a;
    }
    // Always update srcObject (might be a renegotiated stream)
    a.srcObject = e.streams[0];
    a.muted = false;
    a.volume = 1.0;

    a.play().then(() => {
      log("PLAYING " + pid);
      audioUnlocked = true;
      hideAudioUnlockUI();
      document.getElementById('vstat').textContent = 'Voice OK';
    }).catch(err => {
      log("playBlock " + pid + ": " + err.name);
      // Show the unlock UI so user can tap to enable audio
      showAudioUnlockUI();
    });

    // Start inbound audio-level monitoring for this peer.
    // This is the BEST diagnostic — if level is moving, audio is actually flowing.
    // If sender shows speaking but recvLevel stays 0, audio path is broken.
    startInboundLevel(e.streams[0], pid);
  };

  pc.onconnectionstatechange = () => {
    log("PC " + pid + " conn=" + pc.connectionState);
    const p = peerMap.get(pid);
    if (p) p.connState = pc.connectionState;
    updPeers();

    if (pc.connectionState === 'connected') {
      clearConnectionTimer(pc);
      document.getElementById('vstat').textContent = 'Voice OK';
      if (p) p.retries = 0;
      // Detect whether we're using relay (TURN) vs direct
      detectRelay(pc, pid);
      // v2.2: hard-cap outbound bitrate at the encoder level (32kbps)
      capOutboundBitrate(pc, 32);
      // Start stats monitoring
      startStats(pc, pid);
    }

    if (pc.connectionState === 'failed') {
      log("FAILED " + pid);
      clearConnectionTimer(pc);
      scheduleRetry(pid);
    }

    if (pc.connectionState === 'disconnected') {
      // Don't retry yet — disconnected often recovers on its own within 4s
      log("DISCONNECTED " + pid + " — waiting briefly");
      setTimeout(() => {
        if (peers[pid] && peers[pid].connectionState === 'disconnected') {
          log("still disconnected " + pid + " → retry");
          scheduleRetry(pid);
        }
      }, 4000); // reduced from 8000 for faster recovery
    }
  };

  pc.oniceconnectionstatechange = () => {
    log("PC " + pid + " ICE=" + pc.iceConnectionState);
  };

  // NOTE: onnegotiationneeded intentionally NOT set. For voice-only calls with
  // tracks added at PC creation, it tends to fire spuriously after the answer
  // arrives, causing offer storms and audio dropouts. We use deterministic
  // initial negotiation only.
}

// ── v2.2 Hard-cap outbound audio bitrate via setParameters ──
// Backs up the SDP-level cap (maxaveragebitrate=32000). Some browsers/networks
// ignore the SDP cap; setParameters is enforced at the encoder level so we
// never burst above this on poor connections regardless of what the codec wants.
async function capOutboundBitrate(pc, kbps) {
  try {
    const sender = pc.getSenders().find(s => s.track && s.track.kind === 'audio');
    if (!sender) return;
    const params = sender.getParameters();
    if (!params.encodings || !params.encodings.length) {
      params.encodings = [{}];
    }
    params.encodings[0].maxBitrate = kbps * 1000;
    // Mark as high-priority so the OS scheduler doesn't deprioritize voice RTP
    try { params.encodings[0].priority = 'high'; } catch (e) {}
    try { params.encodings[0].networkPriority = 'high'; } catch (e) {}
    await sender.setParameters(params);
  } catch (e) {
    // Some browsers (older Safari) reject this — non-fatal, SDP cap still applies
  }
}

// ── Detect whether peer connection is going through TURN relay ──
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

// ── Periodic stats with AUTO-QUALITY-DETECTION ──
// Measures per-interval packet loss and triggers auto-relay when bad.
// Trigger conditions:
//   1. Sustained high loss: >5% loss for 8+ seconds (2 intervals at 4s tick)
//   2. Audio stalled: 0 packets received for 4+ seconds while conn=connected
//      (skipped when peer is muted — silence is expected)
//   3. Outbound stalled: 0 packets sent for 8s while connected
//      (skipped when WE are muted — silence is expected)
//   4. Frozen jitter: another zombie sign (skipped when peer is muted)
function startStats(pc, pid) {
  if (statsTimers[pid]) clearInterval(statsTimers[pid]);
  let lastRecv = 0, lastLost = 0;
  let lastSent = 0;
  let consecutiveBad = 0;     // count of consecutive 4s-intervals with >5% loss
  let consecutiveStalled = 0; // count of consecutive intervals with 0 incoming packets
  let outboundStall = 0;      // count of consecutive intervals with 0 outgoing packets
  let logTick = 0;            // counter for log throttling

  statsTimers[pid] = setInterval(async () => {
    if (!peers[pid] || peers[pid].connectionState === 'closed') {
      clearInterval(statsTimers[pid]);
      delete statsTimers[pid];
      return;
    }
    if (peers[pid].connectionState !== 'connected') return; // only monitor while connected

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

      // ── LOG THROTTLING ──
      // Only emit STATS when interesting:
      //   • first 2 ticks (8s of telemetry to confirm path is healthy)
      //   • any anomaly (loss > 1%, unexpected ΔR=0, unexpected ΔS=0, big jitter)
      //   • heartbeat every 8 ticks (~32s) so the log never goes fully silent
      logTick++;
      const isAnomaly = lossPct > 1
        || (dRecv === 0 && !peerMuted)
        || (dSent === 0 && !isMuted)
        || jitter > 0.1;
      const isHeartbeat = logTick <= 2 || logTick % 8 === 0;
      if (isAnomaly || isHeartbeat) {
        const tag = isAnomaly ? "STATS!" : "STATS ";
        log(tag + pid + " ΔR=" + dRecv + " ΔL=" + dLost + " (" + lossPct.toFixed(1) + "%) jit=" + jitter.toFixed(3) + " ΔS=" + dSent);
      }

      // ── TRIGGER 1: Sustained packet loss > 5% ──
      if (lossPct > 5 && total > 30) {
        consecutiveBad++;
        if (consecutiveBad >= 2 && !peerRelay[pid]) { // 2 × 4s = 8s of bad quality (was 12s)
          requestRelaySwitch(pid, "loss=" + lossPct.toFixed(1) + "%");
          consecutiveBad = 0;
        }
      } else {
        consecutiveBad = 0;
      }

      // ── TRIGGER 2: Audio stalled (0 packets for 4s while supposedly connected) ──
      // Skipped when peer is muted — they're intentionally not sending.
      // This catches the silent NAT-timeout failure mode where the connection
      // claims to be "connected" but no packets are flowing.
      if (dRecv === 0 && !peerMuted) {
        consecutiveStalled++;
        if (consecutiveStalled >= 1 && !peerRelay[pid]) { // 1 × 4s = 4s of zero packets
          requestRelaySwitch(pid, "stalled (0 pkts/4s)");
          consecutiveStalled = 0;
        }
      } else {
        consecutiveStalled = 0;
      }

      // ── TRIGGER 3: Outbound stalled (0 packets sent for 8s while connected) ──
      // Skipped when WE are muted — we're intentionally not sending audio.
      // (With v2.1 mute fix, RTP keeps flowing via DTX so this rarely matters,
      //  but the guard prevents any false-positives just in case.)
      if (dSent === 0 && !isMuted) {
        outboundStall++;
        if (outboundStall >= 2 && !peerRelay[pid]) { // 2 × 4s = 8s of zero outbound
          requestRelaySwitch(pid, "outbound stalled (0 sent/8s)");
          outboundStall = 0;
        }
      } else {
        outboundStall = 0;
      }

      // ── TRIGGER 4: Zombie transceiver (frozen jitter + 0 recv while connected) ──
      // Skipped when peer is muted (handled inside detectFrozenJitter).
      if (!peerMuted && detectFrozenJitter(pid, jitter)) {
        log("ZOMBIE jitter frozen on " + pid + " → full renegotiate");
        if (MY_ID > pid) {
          destroyPeer(pid);
          setTimeout(() => createOffer(pid), 300);
        } else {
          destroyPeer(pid);
          if (ws && ws.readyState === 1) {
            ws.send(JSON.stringify({ type: 'request_relay', to: pid, reason: 'zombie-reneg' }));
          }
        }
      }
    } catch (e) {}
  }, 4000); // tick every 4s for fast detection
}

// ── Retry escalation: ICE restart → full restart → relay-only ──
async function scheduleRetry(pid) {
  const p = peerMap.get(pid);
  if (!p) return;
  if (p._retrying) return; // already scheduled
  p._retrying = true;
  p.retries = (p.retries || 0) + 1;

  if (p.retries > 4) {  // was 5
    log("GIVE UP on " + pid);
    p._retrying = false;
    destroyPeer(pid);
    return;
  }

  // ESCALATION: retry 1 = ICE restart, retry 2+ = full rebuild with relay-only
  if (p.retries >= 2 && !peerRelay[pid]) {
    peerRelay[pid] = true;  // force relay for this and all future attempts
  }

  const delay = Math.min(1000 * p.retries, 4000); // faster delays: 1s, 2s, 3s, 4s (was 1500 base)
  log("retry #" + p.retries + " in " + delay + "ms → " + pid);
  setTimeout(async () => {
    if (!peerMap.has(pid)) { p._retrying = false; return; }
    if (!ws || ws.readyState !== 1) { p._retrying = false; return; }

    // Only larger-ID side initiates; smaller-ID side passively receives the new offer
    if (MY_ID > pid) {
      // Try ICE restart on existing PC first (cheaper)
      if (p.retries === 1 && peers[pid] && peers[pid].connectionState !== 'closed') {
        try {
          log("ICE restart " + pid);
          const o = await peers[pid].createOffer({ iceRestart: true });
          o.sdp = preferOpusAndTune(o.sdp);
          await peers[pid].setLocalDescription(o);
          ws.send(JSON.stringify({ type: 'webrtc_offer', to: pid, sdp: peers[pid].localDescription.sdp }));
          startConnectionTimer(pid);
          p._retrying = false;
          return;
        } catch (e) { log("iceRestart fail: " + e.message); }
      }
      await createOffer(pid);
    } else {
      // Smaller side — destroy stale PC and wait for offerer to retry
      destroyPeer(pid);
      log("smaller side: cleared stale PC for " + pid + ", waiting for offer");
    }
    p._retrying = false;
  }, delay);
}

// ── Local mic level monitor → sends "speaking" events ──
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
      // Send only on transitions to avoid flooding
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

// ── UI handlers ──
function cleanupRTC() {
  Object.keys({...peers, ...audios}).forEach(pid => nukePeer(pid));
  if (localStream) {
    localStream.getTracks().forEach(t => t.stop());
    localStream = null;
  }
  if (localLevelTimer) { clearInterval(localLevelTimer); localLevelTimer = null; }
  if (wakeLock) { try { wakeLock.release(); } catch (e) {} wakeLock = null; }
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
  const badge = isH ? 'Host' : 'Co-host';
  const bClass = isH ? 'host' : 'cohost';
  const avSrc = m.avatar || pi.avatar || '';
  const row = document.createElement('div');
  row.className = 'msg-row ' + (isSelf ? 'self' : 'other');
  let avHTML;
  if (avSrc) avHTML = '<div class="avatar"><img src="' + esc(avSrc) + '"></div>';
  else avHTML = '<div class="avatar"><span>' + esc(name[0].toUpperCase()) + '</span></div>';
  const header = '<div class="msg-header"><span class="msg-name">' + esc(name) + '</span><span class="msg-badge ' + bClass + '">' + badge + '</span></div>';
  row.innerHTML = avHTML + '<div class="msg-content">' + header + '<div class="msg-bubble">' + esc(m.text) + '</div></div>';
  c.appendChild(row);
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
  e.scrollTop = e.scrollHeight;
}

function esc(t) {
  const d = document.createElement('div');
  d.textContent = t || '';
  return d.innerHTML;
}

function sendMsg() {
  const inEl = document.getElementById('msgIn');
  const text = inEl.value.trim();
  if (!text || !ws || ws.readyState !== 1) return;
  ws.send(JSON.stringify({ type: 'chat', text: text }));
  inEl.value = '';
}

function toggleMute() {
  if (!localStream) return;
  isMuted = !isMuted;
  // ── THE MUTE FIX (v2.1) ──
  // Use track.enabled = false/true — this is the W3C-spec way to mute.
  // The track keeps producing silence frames (with DTX comfort-noise) so RTP
  // KEEPS FLOWING the entire time. The sender object is never put into a
  // half-broken state, so unmuting is just a flag flip — voice resumes
  // instantly with no negotiation needed.
  //
  // The PREVIOUS approach used sender.replaceTrack(null) on mute and
  // sender.replaceTrack(realTrack) on unmute. On mobile (especially iOS Safari)
  // that left the RTP sender in a zombie state: connection stayed "connected"
  // but no packets ever flowed after unmute. That's why voice would vanish.
  const realTrack = localStream.getAudioTracks()[0];
  if (!realTrack) return;
  realTrack.enabled = !isMuted;

  const b = document.getElementById('muteBtn');
  if (isMuted) {
    b.classList.add('muted');
    b.innerHTML = '&#128263;';
    document.getElementById('vstat').textContent = 'Muted';
  } else {
    b.classList.remove('muted');
    b.innerHTML = '&#127908;';
    document.getElementById('vstat').textContent = 'Connected';
  }
  if (ws && ws.readyState === 1) ws.send(JSON.stringify({ type: isMuted ? 'mute_me' : 'unmute_me' }));
  updPeers();
}

// ════════════════════════════════════════════════════════════════════════════
// AUTO-RELAY: Switch a single peer to TURN-only when quality is bad.
// Triggered by startStats() when packet loss is high or audio stops flowing.
// Cross-peer: smaller-ID side asks larger-ID side via WebSocket.
// ════════════════════════════════════════════════════════════════════════════

async function requestRelaySwitch(pid, reason) {
  const p = peerMap.get(pid);
  if (!p) return;
  if (peerRelay[pid]) return; // already on relay
  peerRelay[pid] = true;
  log("→ AUTO-RELAY for " + pid + " (" + reason + ")");
  if (MY_ID > pid) {
    // I'm the larger ID — I do the actual ICE restart with relay-only
    await switchPeerToRelay(pid);
    // Also tell the smaller side to switch to relay (mutual relay = best reliability)
    if (ws && ws.readyState === 1) {
      ws.send(JSON.stringify({ type: 'request_relay', to: pid, reason: 'mutual-relay:' + reason }));
    }
  } else {
    // I'm the smaller ID — ask my peer (larger) to do it
    if (ws && ws.readyState === 1) {
      ws.send(JSON.stringify({ type: 'request_relay', to: pid, reason: reason }));
    }
  }
}

async function switchPeerToRelay(pid) {
  // Smaller side should NOT create offers — destroy PC and wait for larger side's relay offer
  if (MY_ID < pid) {
    log("relay: smaller side destroying PC for " + pid);
    destroyPeer(pid);
    return;
  }

  const pc = peers[pid];
  if (!pc) {
    await createOffer(pid);
    return;
  }
  try {
    // Update config first, then ICE-restart
    pc.setConfiguration(getPCConfig(true));
    const offer = await pc.createOffer({ iceRestart: true });
    offer.sdp = preferOpusAndTune(offer.sdp);
    await pc.setLocalDescription(offer);
    ws.send(JSON.stringify({ type: 'webrtc_offer', to: pid, sdp: pc.localDescription.sdp }));
    log("relay-mode offer SENT " + pid);
    startConnectionTimer(pid);
  } catch (e) {
    log("setConfig fail " + pid + ": " + e.message + " → full rebuild");
    // Fallback: tear down and rebuild from scratch (peerRelay flag will force relay)
    destroyPeer(pid);
    await createOffer(pid);
  }
}

function leaveCall() {
  log("leave");
  leaving = true;
  if (ws && ws.readyState === 1) ws.close();
  cleanupRTC();
  try { window.close(); } catch (e) {}
  document.body.innerHTML = '<div style="display:flex;align-items:center;justify-content:center;height:100vh;background:#000;color:#fff;font-family:sans-serif"><h2>Left the call</h2></div>';
}

// Page unload: clean up gracefully
window.addEventListener('beforeunload', () => {
  leaving = true;
  cleanupRTC();
});

log("page loaded");
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
    print(f"Silent Hill Bot BEAST MODE v2.2 | {WEB_APP_URL} | Port {PORT}")
    print(f"Kyodo: {KYODO_OK}")
    # Pre-warm TURN cache so first call is fast
    servers = await get_ice_servers()
    print(f"ICE servers configured: {len(servers)} entries")
    if METERED_API_KEY:
        print("✓ Metered.ca TURN configured")
    if CF_TURN_TOKEN_ID:
        print("✓ Cloudflare TURN configured")
    if CUSTOM_TURN_URL:
        print("✓ Custom TURN configured")
    if not (METERED_API_KEY or CF_TURN_TOKEN_ID or CUSTOM_TURN_URL):
        print("⚠ No premium TURN — using public fallback (less reliable in MENA)")
        print("  → see top of file for free TURN setup instructions")
    print("=" * 60)
    await asyncio.gather(
        Server(Config(app=app, host="0.0.0.0", port=PORT, log_level="warning")).serve(),
        run_kyodo_bot(),
        keepalive(),
    )


if __name__ == "__main__":
    asyncio.run(main())
