"""
Silent Hill Voice Call Bot — BEAST MODE v2
═══════════════════════════════════════════════════════════════════════════════
FIXES vs previous version:
  • Smaller side nudges larger side after 8s silence (no more deadlock)
  • request_relay handler always works regardless of who nudges
  • Larger side re-offers immediately on nudge even after giving up
  • Added TRIGGER 3: relay-stalled full rebuild (prevents silent TURN trap)
  • Metered TURN credentials fetched fresh per session

TURN SETUP (required for Algeria/MENA users):
  Set these two env vars on Render:
    METERED_API_KEY=your_key
    METERED_DOMAIN=yourapp.metered.live
  Get them free at https://www.metered.ca/turn-server (50GB/mo free)
═══════════════════════════════════════════════════════════════════════════════
"""

import asyncio, json, os, time, uuid
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
EMAIL    = os.getenv("BOT_EMAIL",     "hadidaoud.ha@gmail.com")
PASSWORD = os.getenv("BOT_PASSWORD",  "yulia123")
DEVICE_ID= os.getenv("BOT_DEVICE_ID","870d649515ce700797d6a56965689f3aaa7d5e82dfdce994b239e00e37238184")
CHAT_ID  = os.getenv("BOT_CHAT_ID",  "cmh2gy89r01pvt33exijh1wr3")
CIRCLE_ID= os.getenv("BOT_CIRCLE_ID","cm9bylrbn00hmux6t43mczt2o")
WEB_APP_URL = os.environ.get("WEB_APP_URL","http://localhost:8000")
PORT     = int(os.environ.get("PORT","8000"))

METERED_API_KEY = os.environ.get("METERED_API_KEY","")
METERED_DOMAIN  = os.environ.get("METERED_DOMAIN","")
CF_TURN_TOKEN_ID  = os.environ.get("CF_TURN_TOKEN_ID","")
CF_TURN_API_TOKEN = os.environ.get("CF_TURN_API_TOKEN","")
CUSTOM_TURN_URL  = os.environ.get("CUSTOM_TURN_URL","")
CUSTOM_TURN_USER = os.environ.get("CUSTOM_TURN_USER","")
CUSTOM_TURN_PASS = os.environ.get("CUSTOM_TURN_PASS","")

tokens: Dict[str, dict] = {}
rooms:  Dict[str, dict] = {}
kyodo_client = None
_turn_cache = {"servers": None, "expires": 0}


def json_write(p, d):
    with open(p,"w",encoding="utf-8") as f: json.dump(d,f,ensure_ascii=False,indent=2)

def json_read(p, default=None):
    if default is None: default=[]
    try:
        if not os.path.exists(p): return default
        with open(p,"r",encoding="utf-8") as f: return json.load(f)
    except Exception: return default


# ─── TURN ───────────────────────────────────────────────────────────────────
async def fetch_metered_creds() -> List[dict]:
    if not METERED_API_KEY or not METERED_DOMAIN: return []
    try:
        url = f"https://{METERED_DOMAIN}/api/v1/turn/credentials?apiKey={METERED_API_KEY}"
        async with aiohttp.ClientSession() as s:
            async with s.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
                if r.status == 200: return await r.json()
    except Exception as e: print(f"[turn] metered err: {e}")
    return []

async def fetch_cloudflare_creds() -> List[dict]:
    if not CF_TURN_TOKEN_ID or not CF_TURN_API_TOKEN: return []
    try:
        url = f"https://rtc.live.cloudflare.com/v1/turn/keys/{CF_TURN_TOKEN_ID}/credentials/generate-ice-servers"
        headers = {"Authorization":f"Bearer {CF_TURN_API_TOKEN}","Content-Type":"application/json"}
        async with aiohttp.ClientSession() as s:
            async with s.post(url,headers=headers,json={"ttl":3600},timeout=aiohttp.ClientTimeout(total=10)) as r:
                if r.status in (200,201):
                    data = await r.json()
                    return data.get("iceServers",[])
    except Exception as e: print(f"[turn] cf err: {e}")
    return []

async def get_ice_servers() -> List[dict]:
    if _turn_cache["servers"] and time.time() < _turn_cache["expires"]:
        return _turn_cache["servers"]

    servers: List[dict] = [
        {"urls":["stun:stun.l.google.com:19302","stun:stun1.l.google.com:19302","stun:stun2.l.google.com:19302"]},
        {"urls":"stun:stun.cloudflare.com:3478"},
        {"urls":"stun:global.stun.twilio.com:3478"},
    ]

    metered = await fetch_metered_creds()
    if metered:
        servers.extend(metered)
        print(f"[turn] using Metered.ca ({len(metered)} URLs)")

    cf = await fetch_cloudflare_creds()
    if cf:
        servers.extend(cf)
        print(f"[turn] using Cloudflare ({len(cf)} URLs)")

    if CUSTOM_TURN_URL and CUSTOM_TURN_USER:
        servers.append({"urls":CUSTOM_TURN_URL,"username":CUSTOM_TURN_USER,"credential":CUSTOM_TURN_PASS})
        print(f"[turn] using custom TURN")

    # Public fallback — unreliable for MENA but better than nothing
    servers.extend([
        {"urls":"turn:openrelay.metered.ca:80",    "username":"openrelayproject","credential":"openrelayproject"},
        {"urls":"turn:openrelay.metered.ca:443",   "username":"openrelayproject","credential":"openrelayproject"},
        {"urls":"turn:openrelay.metered.ca:443?transport=tcp", "username":"openrelayproject","credential":"openrelayproject"},
        {"urls":"turns:openrelay.metered.ca:443?transport=tcp","username":"openrelayproject","credential":"openrelayproject"},
    ])

    _turn_cache["servers"] = servers
    _turn_cache["expires"] = time.time() + 1800
    return servers


# ─── KYODO BOT ──────────────────────────────────────────────────────────────
async def run_kyodo_bot():
    global kyodo_client
    if not KYODO_OK:
        while True: await asyncio.sleep(3600)
    backoff = 5
    while True:
        t0 = time.time()
        try:
            kyodo_client = Client(deviceId=DEVICE_ID)

            @kyodo_client.middleware(EventType.ChatMessage)
            async def _filt(m: ChatMessage):
                if m.author.userId == kyodo_client.userId: return False

            @kyodo_client.event(EventType.ChatMessage)
            async def _on(m: ChatMessage):
                try:
                    c = (m.content or "").strip()
                    if not c or m.chatId != CHAT_ID: return
                    if c.lower() in ("/call","!call","/جلسة"):
                        rid = str(uuid.uuid4())[:8]
                        rooms[rid] = {
                            "peers":{}, "chat_file":f"{rid}_chat.json",
                            "created":datetime.now().isoformat(),
                            "creator_uid":m.author.userId,
                            "creator_name":m.author.nickname,
                        }
                        json_write(f"{rid}_chat.json",[])
                        tok = str(uuid.uuid4())
                        tokens[tok] = {"room_id":rid,"creator":True}
                        link = f"{WEB_APP_URL}/call/{rid}?t={tok}"
                        await kyodo_client.send_message(
                            m.chatId,
                            f"Silent Hill Voice Session\n{link}\nTap to join the call.",
                            m.circleId,
                        )
                except Exception as e: print(f"[Kyodo] err: {e}")

            await kyodo_client.login(EMAIL,PASSWORD)
            print("[Kyodo] Logged in!")
            await kyodo_client.socket_wait()
        except (KeyboardInterrupt,SystemExit): raise
        except Exception as e: print(f"[Kyodo] crash: {e}")
        backoff = 5 if time.time()-t0 > 300 else min(backoff*2,120)
        await asyncio.sleep(backoff)


# ─── FASTAPI ────────────────────────────────────────────────────────────────
app = FastAPI()

@app.get("/")
async def root(): return {"ok":True,"rooms":len(rooms),"kyodo":KYODO_OK}

@app.get("/bg.jpg")
async def bg(): return FileResponse("bg.jpg") if os.path.exists("bg.jpg") else HTMLResponse("",404)

@app.get("/ci.jpg")
async def ci(): return FileResponse("ci.jpg") if os.path.exists("ci.jpg") else HTMLResponse("",404)

@app.get("/turn")
async def turn_endpoint():
    servers = await get_ice_servers()
    return JSONResponse({"iceServers":servers})

@app.get("/call/{room_id}")
async def call_page(room_id:str, t:str=Query(...)):
    tok = tokens.get(t)
    if not tok or tok.get("room_id")!=room_id or room_id not in rooms:
        return HTMLResponse("<h1>Invalid link</h1>",403)
    html = CALL_HTML.replace("__ROOM_ID__",room_id).replace("__TOKEN__",t)
    return HTMLResponse(html)

@app.websocket("/ws/{room_id}")
async def ws_endpoint(ws:WebSocket, room_id:str, t:str=Query(...)):
    tok = tokens.get(t)
    if not tok or tok.get("room_id")!=room_id or room_id not in rooms:
        await ws.close(code=4001); return
    await ws.accept()
    peer_id = str(uuid.uuid4())[:8]
    room = rooms[room_id]
    name,avatar = "Unknown",""
    try:
        init = await asyncio.wait_for(ws.receive_json(),timeout=15)
        if init.get("type")=="join":
            name   = init.get("name","Unknown")[:30]
            avatar = init.get("avatar","")[:200000]
    except asyncio.TimeoutError:
        await ws.close(code=4002); return

    is_host = tok.get("creator",False) and len(room["peers"])==0
    room["peers"][peer_id] = {
        "ws":ws,"name":name,"avatar":avatar,
        "muted":False,"is_host":is_host,"joined":time.time(),
    }
    existing = [p for p in room["peers"] if p!=peer_id]
    print(f"[WS] {peer_id} ({name}) joined room={room_id} host={is_host} total={len(room['peers'])}")

    await ws.send_json({"type":"your_id","id":peer_id})
    await ws.send_json({"type":"history","messages":json_read(room["chat_file"])[-100:]})
    peer_list = [
        {"id":p,"name":room["peers"][p]["name"],"avatar":room["peers"][p]["avatar"],
         "is_host":room["peers"][p]["is_host"],"muted":room["peers"][p]["muted"]}
        for p in existing
    ]
    await ws.send_json({"type":"peers","peers":peer_list})

    join_msg = {"type":"peer_joined","peer":{"id":peer_id,"name":name,"avatar":avatar,"is_host":is_host,"muted":False}}
    for p in existing:
        try: await room["peers"][p]["ws"].send_json(join_msg)
        except Exception: pass

    sys_msg = {"type":"chat","kind":"system","text":f"{name} joined the call","time":datetime.now().isoformat()}
    _append_msg(room_id,sys_msg)
    for p in existing:
        try: await room["peers"][p]["ws"].send_json(sys_msg)
        except Exception: pass

    async def pinger():
        try:
            while True:
                await asyncio.sleep(20)
                try: await ws.send_json({"type":"ping","t":time.time()})
                except Exception: return
        except asyncio.CancelledError: return

    ping_task = asyncio.create_task(pinger())

    try:
        while True:
            msg = await ws.receive_json()
            mt = msg.get("type")

            if mt=="pong": continue

            if mt=="chat":
                text = msg.get("text","").strip()[:500]
                if not text: continue
                cm = {"type":"chat","kind":"user","peer_id":peer_id,"name":name,
                      "avatar":avatar,"text":text,"time":datetime.now().isoformat()}
                _append_msg(room_id,cm)
                for p,pd in room["peers"].items():
                    try:
                        if p==peer_id: await pd["ws"].send_json({**cm,"self":True})
                        else: await pd["ws"].send_json(cm)
                    except Exception: pass

            elif mt in ("webrtc_offer","webrtc_answer","webrtc_ice","request_relay"):
                target = msg.get("to")
                msg["from"] = peer_id
                if target and target in room["peers"]:
                    try: await room["peers"][target]["ws"].send_json(msg)
                    except Exception: pass

            elif mt in ("mute_me","unmute_me"):
                room["peers"][peer_id]["muted"] = (mt=="mute_me")
                cmd = "mute_cmd" if mt=="mute_me" else "unmute_cmd"
                await ws.send_json({"type":cmd})
                st = {"type":"voice_state","peer_id":peer_id,"muted":room["peers"][peer_id]["muted"]}
                for p,pd in room["peers"].items():
                    if p!=peer_id:
                        try: await pd["ws"].send_json(st)
                        except Exception: pass

            elif mt=="speaking":
                st = {"type":"speaking","peer_id":peer_id,"level":msg.get("level",0)}
                for p,pd in room["peers"].items():
                    if p!=peer_id:
                        try: await pd["ws"].send_json(st)
                        except Exception: pass

    except WebSocketDisconnect: pass
    except Exception as e: print(f"[WS] {peer_id} error: {e}")
    finally:
        ping_task.cancel()
        if peer_id in room["peers"]: del room["peers"][peer_id]
        lm = {"type":"peer_left","peer_id":peer_id,"name":name}
        sm = {"type":"chat","kind":"system","text":f"{name} left the call","time":datetime.now().isoformat()}
        for p,pd in list(room["peers"].items()):
            try: await pd["ws"].send_json(lm); await pd["ws"].send_json(sm)
            except Exception: pass
        if not room["peers"]:
            async def cleanup_later():
                await asyncio.sleep(60)
                if room_id in rooms and not rooms[room_id]["peers"]:
                    f = f"{room_id}_chat.json"
                    if os.path.exists(f):
                        try: os.remove(f)
                        except Exception: pass
                    for tk in [k for k,v in tokens.items() if v.get("room_id")==room_id]:
                        del tokens[tk]
                    if room_id in rooms: del rooms[room_id]
                    print(f"[WS] cleaned up empty room {room_id}")
            asyncio.create_task(cleanup_later())


def _append_msg(rid,msg):
    p = f"{rid}_chat.json"
    m = json_read(p,[])
    m.append(msg)
    json_write(p,m)


# ─── CALL UI ────────────────────────────────────────────────────────────────
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
const ROOM = "__ROOM_ID__", TOKEN = "__TOKEN__";
let MY_ID = "";
let ws = null, localStream = null, myName = "", myAvatar = "";
let isMuted = false, isHost = false;
let leaving = false;
let wsRetries = 0;
let wakeLock = null;

const peers = {};
const audios = {};
const peerMap = new Map();
const iceBuffer = {};
const statsTimers = {};
const inboundLevelTimers = {};
const lastOfferUfrag = {};
const peerRelay = {};
let remoteAudioCtx = null;
let audioUnlocked = false;

let ICE_SERVERS = [
  {urls:['stun:stun.l.google.com:19302','stun:stun1.l.google.com:19302']},
  {urls:'turn:openrelay.metered.ca:443?transport=tcp',username:'openrelayproject',credential:'openrelayproject'},
  {urls:'turns:openrelay.metered.ca:443?transport=tcp',username:'openrelayproject',credential:'openrelayproject'},
];

const AUDIO_CONSTRAINTS = {
  audio:{echoCancellation:true,noiseSuppression:true,autoGainControl:true,sampleRate:{ideal:48000},channelCount:{ideal:1}},
  video:false
};

function getPCConfig(forceRelay) {
  return {
    iceServers: ICE_SERVERS,
    bundlePolicy:'max-bundle',
    rtcpMuxPolicy:'require',
    iceCandidatePoolSize:4,
    iceTransportPolicy: forceRelay ? 'relay' : 'all'
  };
}

function log(m) {
  const t = new Date().toLocaleTimeString().split(' ')[0];
  const line = '['+t+'] '+m;
  console.log(line);
  const d = document.getElementById('dbg');
  if(d){
    d.textContent += line+'\n';
    if(d.textContent.length>8000) d.textContent=d.textContent.slice(-6000);
    d.scrollTop=d.scrollHeight;
  }
}

function pickAv(e) {
  const f=e.target.files[0]; if(!f) return;
  const r=new FileReader();
  r.onload=ev=>{
    const img=new Image();
    img.onload=()=>{
      const c=document.createElement('canvas');
      c.width=c.height=128;
      c.getContext('2d').drawImage(img,0,0,128,128);
      myAvatar=c.toDataURL('image/jpeg',0.7);
      document.getElementById('avPrev').innerHTML='<img src="'+myAvatar+'">';
      log("avatar OK ("+Math.round(myAvatar.length/1024)+'kb)');
    };
    img.src=ev.target.result;
  };
  r.readAsDataURL(f);
}
document.getElementById('nameIn').addEventListener('input',e=>{
  myName=e.target.value;
  if(!myAvatar) document.getElementById('avInit').textContent=myName?myName[0].toUpperCase():'?';
});

async function fetchIceServers() {
  try{
    const r=await fetch('/turn',{cache:'no-store'});
    if(r.ok){
      const data=await r.json();
      if(data.iceServers&&data.iceServers.length){
        ICE_SERVERS=data.iceServers;
        log("ICE: "+ICE_SERVERS.length+" server entries loaded");
      }
    }
  }catch(e){log("ICE fetch failed, using fallback");}
}

async function acquireWakeLock() {
  try{
    if('wakeLock' in navigator){wakeLock=await navigator.wakeLock.request('screen');log("wakeLock OK");}
  }catch(e){log("wakeLock fail");}
}
document.addEventListener('visibilitychange',()=>{
  if(document.visibilityState==='visible'&&!wakeLock) acquireWakeLock();
});

async function doJoin() {
  const n=document.getElementById('nameIn').value.trim();
  if(!n){alert("Enter name");return;}
  myName=n;
  document.getElementById('joinBtn').disabled=true;
  document.getElementById('joinBtn').textContent="...";

  try{
    remoteAudioCtx=new(window.AudioContext||window.webkitAudioContext)();
    if(remoteAudioCtx.state==='suspended') await remoteAudioCtx.resume();
    const buf=remoteAudioCtx.createBuffer(1,remoteAudioCtx.sampleRate*0.1,remoteAudioCtx.sampleRate);
    const src=remoteAudioCtx.createBufferSource();
    src.buffer=buf; src.connect(remoteAudioCtx.destination); src.start();
    audioUnlocked=true; log("audio unlocked");
  }catch(e){log("audio unlock fail: "+e.message);}

  await fetchIceServers();

  try{
    localStream=await navigator.mediaDevices.getUserMedia(AUDIO_CONSTRAINTS);
    document.getElementById('vstat').textContent='Connected';
    log("mic OK");
    setupLocalLevelMonitor();
  }catch(e){
    log("mic err: "+e.message);
    document.getElementById('vstat').textContent='No mic';
  }
  document.getElementById('joinOvl').classList.add('hidden');
  document.getElementById('app').classList.remove('hidden');
  acquireWakeLock();

  if(_peerLevelTicker) clearInterval(_peerLevelTicker);
  _peerLevelTicker=setInterval(updPeerLevels,150);
  connectWS();
}

function connectWS() {
  const p=location.protocol==='https:'?'wss:':'ws:';
  const url=p+'//'+location.host+'/ws/'+ROOM+'?t='+TOKEN;
  log("WS connect (try "+(wsRetries+1)+")");
  ws=new WebSocket(url);

  ws.onopen=()=>{
    log("WS open"); wsRetries=0;
    ws.send(JSON.stringify({type:'join',name:myName,avatar:myAvatar}));
  };

  ws.onmessage=async(ev)=>{
    let m; try{m=JSON.parse(ev.data);}catch(e){return;}
    switch(m.type){
      case 'ping': ws.send(JSON.stringify({type:'pong'})); break;
      case 'your_id': MY_ID=m.id; log("myId="+MY_ID); break;
      case 'history': m.messages.forEach(renderMsg); break;
      case 'chat': renderMsg(m); break;

      case 'peers':
        log("existing peers: "+m.peers.length);
        for(const p of m.peers){
          addPeer(p);
          if(MY_ID>p.id){log("I'm larger → offer"); createOffer(p.id);}
          else log("I'm smaller → wait");
        }
        break;

      case 'peer_joined':
        addPeer(m.peer);
        renderSys(m.peer.name+" joined");
        if(MY_ID&&MY_ID>m.peer.id){log("late: larger → offer to "+m.peer.id); createOffer(m.peer.id);}
        else if(MY_ID) log("late: smaller → wait for offer from "+m.peer.id);
        break;

      case 'peer_left':
        nukePeer(m.peer_id); peerMap.delete(m.peer_id);
        renderSys(m.name+' left'); updCount(); updPeers();
        break;

      case 'webrtc_offer':  await handleOffer(m.from,m.sdp); break;
      case 'webrtc_answer': await handleAnswer(m.from,m.sdp); break;
      case 'webrtc_ice':    await handleIce(m.from,m.candidate); break;

      // ── FIX: request_relay now also serves as a "nudge" from smaller side ──
      // The smaller side sends this when the larger side has gone silent.
      // The larger side ALWAYS re-offers (with relay) when it receives this.
      case 'request_relay':
        log("relay request/nudge from "+m.from+" ("+(m.reason||'')+")");
        peerRelay[m.from]=true;
        if(MY_ID>m.from){
          // I'm the larger ID → I'm in charge of offering → do it
          await switchPeerToRelay(m.from);
        } else {
          // I'm the smaller ID → they're asking me to rebuild as answerer
          // This covers the case where both sides failed and smaller nudges
          destroyPeer(m.from);
          log("smaller: rebuilt PC, waiting for offer from "+m.from);
        }
        break;

      case 'mute_cmd':
        if(localStream) localStream.getAudioTracks().forEach(t=>t.enabled=false);
        break;
      case 'unmute_cmd':
        if(localStream) localStream.getAudioTracks().forEach(t=>t.enabled=true);
        break;
      case 'voice_state': {
        const p=peerMap.get(m.peer_id);
        if(p){p.muted=m.muted; updPeers();}
        break;
      }
      case 'speaking': {
        const p=peerMap.get(m.peer_id);
        if(p){p.speaking=m.level>0.05; updPeers();}
        break;
      }
    }
  };

  ws.onclose=e=>{
    log("WS close "+e.code);
    if(!leaving){
      const delay=Math.min(1000*Math.pow(1.5,wsRetries),15000);
      wsRetries++;
      log("WS reconnect in "+delay+"ms");
      setTimeout(connectWS,delay);
    }else{cleanupRTC();}
  };
  ws.onerror=()=>{
    log("WS err");
    document.getElementById('vstat').textContent='Reconnecting...';
  };
}

function addPeer(p) {
  peerMap.set(p.id,{
    name:p.name,avatar:p.avatar||'',is_host:p.is_host,muted:p.muted,
    connState:'new',retries:0,speaking:false,usedRelay:false
  });
  updCount(); updPeers();
}

function updCount(){
  document.getElementById('mcount').textContent=(peerMap.size+1)+' in call';
}

function updPeers(){
  const el=document.getElementById('pstat');
  let h='';
  const selfDot=isMuted?'fail':'conn';
  h+='<div class="p-s"><div class="dot '+selfDot+'"></div>'+esc(myName)+' (You)</div>';
  peerMap.forEach((p,id)=>{
    let dot='';
    if(p.connState==='connected'){
      if(p.lossPct!==undefined&&p.lossPct>5) dot='fail';
      else if(peerRelay[id]||p.usedRelay) dot='relay';
      else dot='conn';
    }else if(p.connState==='failed'||p.connState==='closed') dot='fail';
    else dot='connecting';
    const speakClass=(p.speaking||p.actuallyHeard)?' speaking':'';
    const muteIcon=p.muted?' 🔇':'';
    const levelPct=Math.min(100,Math.round((p.recvLevel||0)*200));
    const levelBar=p.connState==='connected'
      ?'<span style="display:inline-block;width:24px;height:4px;background:rgba(255,255,255,0.15);border-radius:2px;margin-left:4px;vertical-align:middle;overflow:hidden"><span style="display:block;width:'+levelPct+'%;height:100%;background:#34c759;transition:width .1s"></span></span>'
      :'';
    h+='<div class="p-s'+speakClass+'" data-pid="'+id+'"><div class="dot '+dot+'"></div>'+esc(p.name)+muteIcon+levelBar+'</div>';
  });
  el.innerHTML=h;
}

function updPeerLevels(){
  const el=document.getElementById('pstat'); if(!el) return;
  el.querySelectorAll('[data-pid]').forEach(div=>{
    const pid=div.getAttribute('data-pid');
    const p=peerMap.get(pid); if(!p) return;
    const innerBar=div.querySelector('span > span');
    if(innerBar){
      const lvl=Math.min(100,Math.round((p.recvLevel||0)*200));
      innerBar.style.width=lvl+'%';
    }
    const isActive=p.speaking||p.actuallyHeard;
    if(isActive&&!div.classList.contains('speaking')) div.classList.add('speaking');
    else if(!isActive&&div.classList.contains('speaking')) div.classList.remove('speaking');
  });
}
let _peerLevelTicker=null;

// ════════════════════════════════════════════════════════════════════════════
// WebRTC
// ════════════════════════════════════════════════════════════════════════════

function destroyPeer(pid,keepAudio){
  if(peers[pid]){try{peers[pid].close();}catch(e){} delete peers[pid];}
  if(statsTimers[pid]){clearInterval(statsTimers[pid]); delete statsTimers[pid];}
  if(inboundLevelTimers[pid]){clearInterval(inboundLevelTimers[pid]); delete inboundLevelTimers[pid];}
  delete iceBuffer[pid];
}

function nukePeer(pid){
  destroyPeer(pid);
  if(audios[pid]){
    try{audios[pid].pause();audios[pid].srcObject=null;audios[pid].remove();}catch(e){}
    delete audios[pid];
  }
  delete peerRelay[pid];
  delete lastOfferUfrag[pid];
}

function shouldForceRelay(pid){
  if(peerRelay[pid]) return true;
  const p=peerMap.get(pid);
  return p&&p.retries>=2;
}

async function createOffer(pid){
  log("offer→"+pid+(shouldForceRelay(pid)?" (RELAY-ONLY)":""));
  destroyPeer(pid);
  const p=peerMap.get(pid); if(!p) return;
  try{
    const pc=new RTCPeerConnection(getPCConfig(shouldForceRelay(pid)));
    setupPC(pc,pid);
    peers[pid]=pc;
    if(localStream) localStream.getTracks().forEach(t=>pc.addTrack(t,localStream));
    const offer=await pc.createOffer();
    offer.sdp=preferOpusAndTune(offer.sdp);
    await pc.setLocalDescription(offer);
    ws.send(JSON.stringify({type:'webrtc_offer',to:pid,sdp:pc.localDescription.sdp}));
    log("offer SENT "+pid);
  }catch(e){log("offer FAIL "+pid+": "+e.message);}
}

async function handleOffer(from,sdp){
  log("offer←"+from);
  const ufragMatch=(sdp||'').match(/a=ice-ufrag:(\S+)/);
  const ufrag=ufragMatch?ufragMatch[1]:null;
  if(ufrag&&lastOfferUfrag[from]===ufrag){log("duplicate offer ignored "+from); return;}
  if(ufrag) lastOfferUfrag[from]=ufrag;

  const existing=peers[from];
  if(existing&&existing.signalingState!=='closed'&&existing.connectionState!=='failed'){
    try{
      if(existing.signalingState==='have-local-offer'){
        if(MY_ID>from){log("collision: impolite, ignore "+from); return;}
        log("collision: polite rollback "+from);
        await existing.setLocalDescription({type:'rollback'});
      }
      await existing.setRemoteDescription(new RTCSessionDescription({type:'offer',sdp}));
      if(iceBuffer[from]){
        for(const c of iceBuffer[from]){try{await existing.addIceCandidate(new RTCIceCandidate(c));}catch(e){}}
        delete iceBuffer[from];
      }
      const ans=await existing.createAnswer();
      ans.sdp=preferOpusAndTune(ans.sdp);
      await existing.setLocalDescription(ans);
      ws.send(JSON.stringify({type:'webrtc_answer',to:from,sdp:existing.localDescription.sdp}));
      log("in-place answer SENT "+from); return;
    }catch(e){log("in-place reneg FAIL "+from+": "+e.message+" → fresh PC");}
  }

  destroyPeer(from);
  try{
    const pc=new RTCPeerConnection(getPCConfig(shouldForceRelay(from)));
    setupPC(pc,from); peers[from]=pc;
    await pc.setRemoteDescription(new RTCSessionDescription({type:'offer',sdp}));
    if(iceBuffer[from]){
      for(const c of iceBuffer[from]){try{await pc.addIceCandidate(new RTCIceCandidate(c));}catch(e){}}
      delete iceBuffer[from];
    }
    if(localStream) localStream.getTracks().forEach(t=>pc.addTrack(t,localStream));
    const ans=await pc.createAnswer();
    ans.sdp=preferOpusAndTune(ans.sdp);
    await pc.setLocalDescription(ans);
    ws.send(JSON.stringify({type:'webrtc_answer',to:from,sdp:pc.localDescription.sdp}));
    log("answer SENT "+from);
  }catch(e){log("answer FAIL "+from+": "+e.message);}
}

async function handleAnswer(from,sdp){
  log("ans←"+from);
  try{
    const pc=peers[from];
    if(!pc){log("no PC for ans "+from); return;}
    if(pc.signalingState!=='have-local-offer'){log("ans skipped (state="+pc.signalingState+")"); return;}
    await pc.setRemoteDescription(new RTCSessionDescription({type:'answer',sdp}));
    log("ans applied "+from);
  }catch(e){log("ansErr "+from+": "+e.message); scheduleRetry(from);}
}

async function handleIce(from,cand){
  const pc=peers[from];
  if(pc&&pc.remoteDescription&&cand){
    try{await pc.addIceCandidate(new RTCIceCandidate(cand));}catch(e){}
  }else if(cand){
    if(!iceBuffer[from]) iceBuffer[from]=[];
    iceBuffer[from].push(cand);
  }
}

function preferOpusAndTune(sdp){
  if(!sdp) return sdp;
  const lines=sdp.split('\r\n');
  let opusPt=null;
  for(const l of lines){const m=l.match(/^a=rtpmap:(\d+) opus\/48000/i);if(m){opusPt=m[1];break;}}
  if(!opusPt) return sdp;
  let found=false;
  const out=lines.map(l=>{
    const m=l.match(new RegExp('^a=fmtp:'+opusPt+' (.*)$'));
    if(m){
      found=true;let p=m[1];
      const merge=(key,val)=>{
        if(new RegExp('(^|;)'+key+'=').test(p)) p=p.replace(new RegExp(key+'=[^;]*'),key+'='+val);
        else p+=';'+key+'='+val;
      };
      merge('useinbandfec','1');merge('usedtx','1');merge('stereo','0');
      merge('maxaveragebitrate','32000');merge('cbr','0');
      return 'a=fmtp:'+opusPt+' '+p;
    }
    return l;
  });
  if(!found){
    for(let i=0;i<out.length;i++){
      if(new RegExp('^a=rtpmap:'+opusPt+' opus').test(out[i])){
        out.splice(i+1,0,'a=fmtp:'+opusPt+' minptime=10;useinbandfec=1;usedtx=1;stereo=0;maxaveragebitrate=32000');
        break;
      }
    }
  }
  return out.join('\r\n');
}

function startInboundLevel(stream,pid){
  if(inboundLevelTimers[pid]){clearInterval(inboundLevelTimers[pid]);delete inboundLevelTimers[pid];}
  try{
    if(!remoteAudioCtx) remoteAudioCtx=new(window.AudioContext||window.webkitAudioContext)();
    if(remoteAudioCtx.state==='suspended') remoteAudioCtx.resume().catch(()=>{});
    const src=remoteAudioCtx.createMediaStreamSource(stream);
    const analyser=remoteAudioCtx.createAnalyser();
    analyser.fftSize=256; src.connect(analyser);
    const data=new Uint8Array(analyser.frequencyBinCount);
    inboundLevelTimers[pid]=setInterval(()=>{
      analyser.getByteFrequencyData(data);
      let sum=0; for(let i=0;i<data.length;i++) sum+=data[i];
      const level=sum/data.length/255;
      const p=peerMap.get(pid);
      if(p){p.recvLevel=level; p.actuallyHeard=level>0.02;}
    },200);
  }catch(e){log("inboundLevel fail "+pid+": "+e.message);}
}

function showAudioUnlockUI(){
  let el=document.getElementById('audioUnlock'); if(el) return;
  el=document.createElement('div');
  el.id='audioUnlock';
  el.style.cssText='position:fixed;top:50px;left:10px;right:10px;z-index:150;background:#ff9500;color:#000;padding:14px;border-radius:12px;text-align:center;font-weight:600;font-size:14px;cursor:pointer;animation:msgIn .3s';
  el.textContent='🔊 Tap here to enable sound';
  el.onclick=()=>{
    log("audio unlock tapped");
    if(remoteAudioCtx&&remoteAudioCtx.state==='suspended') remoteAudioCtx.resume().catch(()=>{});
    Object.values(audios).forEach(a=>{try{a.muted=false;a.volume=1.0;a.play().catch(()=>{});}catch(e){}});
    audioUnlocked=true; hideAudioUnlockUI();
  };
  document.body.appendChild(el);
}
function hideAudioUnlockUI(){const el=document.getElementById('audioUnlock');if(el) el.remove();}

function setupPC(pc,pid){
  pc.onicecandidate=e=>{
    if(e.candidate&&ws&&ws.readyState===1)
      ws.send(JSON.stringify({type:'webrtc_ice',to:pid,candidate:e.candidate}));
  };

  pc.ontrack=e=>{
    log("TRACK "+pid+" streams="+e.streams.length);
    let a=audios[pid];
    if(!a){
      a=document.createElement('audio');
      a.autoplay=true; a.playsInline=true; a.volume=1.0; a.muted=false;
      a.style.display='none';
      document.body.appendChild(a);
      audios[pid]=a;
    }
    a.srcObject=e.streams[0]; a.muted=false; a.volume=1.0;
    const tryPlay=()=>a.play().then(()=>{
      log("PLAYING "+pid); audioUnlocked=true;
      hideAudioUnlockUI();
      document.getElementById('vstat').textContent='Voice OK';
    }).catch(err=>{
      log("playBlock "+pid+": "+err.name);
      showAudioUnlockUI();
    });
    tryPlay();
    startInboundLevel(e.streams[0],pid);
  };

  pc.onconnectionstatechange=()=>{
    log("PC "+pid+" conn="+pc.connectionState);
    const p=peerMap.get(pid);
    if(p) p.connState=pc.connectionState;
    updPeers();

    if(pc.connectionState==='connected'){
      document.getElementById('vstat').textContent='Voice OK';
      if(p) p.retries=0;
      detectRelay(pc,pid);
      startStats(pc,pid);
    }
    if(pc.connectionState==='failed'){
      log("FAILED "+pid);
      scheduleRetry(pid);
    }
    if(pc.connectionState==='disconnected'){
      log("DISCONNECTED "+pid+" — waiting briefly");
      setTimeout(()=>{
        if(peers[pid]&&peers[pid].connectionState==='disconnected'){
          log("still disconnected "+pid+" → retry");
          scheduleRetry(pid);
        }
      },8000);
    }
  };

  pc.oniceconnectionstatechange=()=>{log("PC "+pid+" ICE="+pc.iceConnectionState);};
}

async function detectRelay(pc,pid){
  try{
    const stats=await pc.getStats();
    let isRelay=false, candidatePair=null;
    stats.forEach(r=>{if(r.type==='candidate-pair'&&r.state==='succeeded'&&r.nominated) candidatePair=r;});
    if(candidatePair) stats.forEach(r=>{if(r.id===candidatePair.localCandidateId&&r.candidateType==='relay') isRelay=true;});
    const p=peerMap.get(pid);
    if(p){p.usedRelay=isRelay; updPeers();}
    log("PC "+pid+" path="+(isRelay?"RELAY":"DIRECT"));
  }catch(e){}
}

function startStats(pc,pid){
  if(statsTimers[pid]) clearInterval(statsTimers[pid]);
  let lastRecv=0,lastLost=0,consecutiveBad=0,consecutiveStalled=0;
  statsTimers[pid]=setInterval(async()=>{
    if(!peers[pid]||peers[pid].connectionState==='closed'){clearInterval(statsTimers[pid]);delete statsTimers[pid];return;}
    if(peers[pid].connectionState!=='connected') return;
    try{
      const stats=await peers[pid].getStats();
      let recv=0,lost=0,jitter=0;
      stats.forEach(r=>{if(r.type==='inbound-rtp'&&r.kind==='audio'){recv=r.packetsReceived||0;lost=r.packetsLost||0;jitter=r.jitter||0;}});
      const dRecv=recv-lastRecv, dLost=lost-lastLost;
      const total=dRecv+dLost;
      const lossPct=total>0?(dLost/total)*100:0;
      lastRecv=recv; lastLost=lost;
      const p=peerMap.get(pid);
      if(p){p.lossPct=lossPct; p.recvRate=dRecv/4;}
      log("STATS "+pid+" ΔR="+dRecv+" ΔL="+dLost+" ("+lossPct.toFixed(1)+"%) jit="+jitter.toFixed(3));

      // TRIGGER 1: Sustained packet loss > 5%
      if(lossPct>5&&total>30){
        consecutiveBad++;
        if(consecutiveBad>=3&&!peerRelay[pid]){requestRelaySwitch(pid,"loss="+lossPct.toFixed(1)+"%"); consecutiveBad=0;}
      }else{consecutiveBad=0;}

      // TRIGGER 2: Audio stalled — 0 packets for 8s
      if(dRecv===0){
        consecutiveStalled++;
        if(consecutiveStalled>=2&&!peerRelay[pid]){requestRelaySwitch(pid,"stalled"); consecutiveStalled=0;}
      }else{consecutiveStalled=0;}

      // TRIGGER 3: Already on relay and still stalled → full rebuild
      if(dRecv===0&&peerRelay[pid]&&consecutiveStalled>=4){
        log("RELAY ALSO STALLED "+pid+" — rebuilding peer");
        consecutiveStalled=0;
        scheduleRetry(pid);
      }
    }catch(e){}
  },4000);
}

// ── FIX: scheduleRetry — smaller side now nudges larger side after 8s silence ──
async function scheduleRetry(pid){
  const p=peerMap.get(pid); if(!p) return;
  p.retries=(p.retries||0)+1;

  if(p.retries>5){log("GIVE UP on "+pid); destroyPeer(pid); return;}

  const delay=Math.min(1500*p.retries,6000);
  log("retry #"+p.retries+" in "+delay+"ms → "+pid);
  setTimeout(async()=>{
    if(!peerMap.has(pid)) return;
    if(!ws||ws.readyState!==1) return;

    if(MY_ID>pid){
      // Larger side → I offer
      if(p.retries===1&&peers[pid]&&peers[pid].connectionState!=='closed'){
        try{
          log("ICE restart "+pid);
          const o=await peers[pid].createOffer({iceRestart:true});
          o.sdp=preferOpusAndTune(o.sdp);
          await peers[pid].setLocalDescription(o);
          ws.send(JSON.stringify({type:'webrtc_offer',to:pid,sdp:peers[pid].localDescription.sdp}));
          return;
        }catch(e){log("iceRestart fail: "+e.message);}
      }
      await createOffer(pid);
    }else{
      // Smaller side → clear stale PC, wait for larger side to re-offer
      // FIX: if larger side stays silent for 8s, nudge them via WS
      destroyPeer(pid);
      log("smaller side: cleared stale PC for "+pid+", waiting for offer");
      setTimeout(()=>{
        if(peerMap.has(pid)&&!peers[pid]){
          log("larger side silent → nudge "+pid);
          if(ws&&ws.readyState===1){
            ws.send(JSON.stringify({type:'request_relay',to:pid,reason:'nudge_from_smaller'}));
          }
        }
      },8000);
    }
  },delay);
}

async function requestRelaySwitch(pid,reason){
  const p=peerMap.get(pid); if(!p) return;
  if(peerRelay[pid]) return;
  peerRelay[pid]=true;
  log("→ AUTO-RELAY for "+pid+" ("+reason+")");
  if(MY_ID>pid){
    await switchPeerToRelay(pid);
  }else{
    if(ws&&ws.readyState===1){
      ws.send(JSON.stringify({type:'request_relay',to:pid,reason:reason}));
      log("relay switch requested from "+pid);
    }
  }
}

async function switchPeerToRelay(pid){
  const pc=peers[pid];
  if(!pc){if(MY_ID>pid) await createOffer(pid); return;}
  try{
    pc.setConfiguration({
      iceServers:ICE_SERVERS, bundlePolicy:'max-bundle',
      rtcpMuxPolicy:'require', iceTransportPolicy:'relay'
    });
    const offer=await pc.createOffer({iceRestart:true});
    offer.sdp=preferOpusAndTune(offer.sdp);
    await pc.setLocalDescription(offer);
    ws.send(JSON.stringify({type:'webrtc_offer',to:pid,sdp:pc.localDescription.sdp}));
    log("relay-mode offer SENT "+pid);
  }catch(e){
    log("setConfig fail "+pid+": "+e.message+" → full rebuild");
    destroyPeer(pid);
    if(MY_ID>pid) await createOffer(pid);
  }
}

let localAnalyser=null,localLevelTimer=null;
function setupLocalLevelMonitor(){
  if(!localStream) return;
  try{
    const ac=new(window.AudioContext||window.webkitAudioContext)();
    const src=ac.createMediaStreamSource(localStream);
    localAnalyser=ac.createAnalyser(); localAnalyser.fftSize=256;
    src.connect(localAnalyser);
    const data=new Uint8Array(localAnalyser.frequencyBinCount);
    let lastSent=0,lastLevel=0;
    localLevelTimer=setInterval(()=>{
      if(isMuted||!localStream) return;
      localAnalyser.getByteFrequencyData(data);
      let sum=0; for(let i=0;i<data.length;i++) sum+=data[i];
      const level=sum/data.length/255;
      const now=Date.now();
      const speaking=level>0.05, wasSpeaking=lastLevel>0.05;
      if(speaking!==wasSpeaking||(speaking&&now-lastSent>1000)){
        if(ws&&ws.readyState===1){
          ws.send(JSON.stringify({type:'speaking',level:speaking?level:0}));
          lastSent=now;
        }
      }
      lastLevel=level;
    },200);
  }catch(e){log("levelMon fail");}
}

function cleanupRTC(){
  Object.keys({...peers,...audios}).forEach(pid=>nukePeer(pid));
  if(localStream){localStream.getTracks().forEach(t=>t.stop()); localStream=null;}
  if(localLevelTimer){clearInterval(localLevelTimer); localLevelTimer=null;}
  if(wakeLock){try{wakeLock.release();}catch(e){} wakeLock=null;}
}

function renderMsg(m){
  const c=document.getElementById('msgs'); if(!c) return;
  if(m.kind==='system'){
    const d=document.createElement('div');
    d.className='msg-system'; d.textContent=m.text;
    c.appendChild(d); scroll(); return;
  }
  const isSelf=!!m.self;
  const pi=peerMap.get(m.peer_id)||{};
  const name=m.name||pi.name||'?';
  const isH=isSelf?isHost:pi.is_host;
  const badge=isH?'Host':'Co-host';
  const bClass=isH?'host':'cohost';
  const avSrc=m.avatar||pi.avatar||'';
  const row=document.createElement('div');
  row.className='msg-row '+(isSelf?'self':'other');
  let avHTML=avSrc
    ?'<div class="avatar"><img src="'+esc(avSrc)+'"></div>'
    :'<div class="avatar"><span>'+esc(name[0].toUpperCase())+'</span></div>';
  const header='<div class="msg-header"><span class="msg-name">'+esc(name)+'</span><span class="msg-badge '+bClass+'">'+badge+'</span></div>';
  row.innerHTML=avHTML+'<div class="msg-content">'+header+'<div class="msg-bubble">'+esc(m.text)+'</div></div>';
  c.appendChild(row); scroll();
}

function renderSys(t){
  const c=document.getElementById('msgs'); if(!c) return;
  const d=document.createElement('div'); d.className='msg-system'; d.textContent=t;
  c.appendChild(d); scroll();
}

function scroll(){const e=document.getElementById('msgs'); e.scrollTop=e.scrollHeight;}

function esc(t){const d=document.createElement('div'); d.textContent=t||''; return d.innerHTML;}

function sendMsg(){
  const inEl=document.getElementById('msgIn');
  const text=inEl.value.trim();
  if(!text||!ws||ws.readyState!==1) return;
  ws.send(JSON.stringify({type:'chat',text:text}));
  inEl.value='';
}

function toggleMute(){
  if(!localStream) return;
  isMuted=!isMuted;
  localStream.getAudioTracks().forEach(t=>t.enabled=!isMuted);
  const b=document.getElementById('muteBtn');
  if(isMuted){
    b.classList.add('muted'); b.innerHTML='&#128263;';
    document.getElementById('vstat').textContent='Muted';
  }else{
    b.classList.remove('muted'); b.innerHTML='&#127908;';
    document.getElementById('vstat').textContent='Connected';
  }
  if(ws&&ws.readyState===1) ws.send(JSON.stringify({type:isMuted?'mute_me':'unmute_me'}));
  updPeers();
}

function leaveCall(){
  log("leave"); leaving=true;
  if(ws&&ws.readyState===1) ws.close();
  cleanupRTC();
  try{window.close();}catch(e){}
  document.body.innerHTML='<div style="display:flex;align-items:center;justify-content:center;height:100vh;background:#000;color:#fff;font-family:sans-serif"><h2>Left the call</h2></div>';
}

window.addEventListener('beforeunload',()=>{leaving=true; cleanupRTC();});
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
                    async with s.get(url,timeout=aiohttp.ClientTimeout(total=15)) as r:
                        print(f"[keepalive] {r.status}")
            except Exception as e:
                print(f"[keepalive] err: {e}")


async def main():
    print("="*60)
    print(f"Silent Hill Bot v2 | {WEB_APP_URL} | Port {PORT}")
    print(f"Kyodo: {KYODO_OK}")
    servers = await get_ice_servers()
    print(f"ICE servers configured: {len(servers)} entries")
    if METERED_API_KEY:  print("✓ Metered.ca TURN configured")
    if CF_TURN_TOKEN_ID: print("✓ Cloudflare TURN configured")
    if CUSTOM_TURN_URL:  print("✓ Custom TURN configured")
    if not(METERED_API_KEY or CF_TURN_TOKEN_ID or CUSTOM_TURN_URL):
        print("⚠ No premium TURN — using public fallback (unreliable in MENA)")
    print("="*60)
    await asyncio.gather(
        Server(Config(app=app,host="0.0.0.0",port=PORT,log_level="warning")).serve(),
        run_kyodo_bot(),
        keepalive(),
    )


if __name__ == "__main__":
    asyncio.run(main())
