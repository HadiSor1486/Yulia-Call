"""
Silent Hill Voice Call Bot - Kyodo + WebRTC + Chat UI
Debug-heavy version with all fixes.
"""

import asyncio
import json
import os
import time
import uuid
import traceback
from datetime import datetime
from typing import Any, Dict, List

import aiohttp
import httpx
from fastapi import FastAPI, Query, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from uvicorn import Config, Server

# ═══════════════════════════════════════════════════════════
# KYODO IMPORT
# ═══════════════════════════════════════════════════════════
try:
    from kyodo import ChatMessage, EventType, KyodoObjectTypes, AsyncClient as Client
    KYODO_OK = True
    print("[INIT] kyodo imported OK")
except ImportError as e:
    KYODO_OK = False
    print("[INIT] kyodo import FAILED: " + str(e))

# ═══════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════
EMAIL = os.getenv("BOT_EMAIL", "hadidaoud.ha@gmail.com")
PASSWORD = os.getenv("BOT_PASSWORD", "yulia123")
DEVICE_ID = os.getenv("BOT_DEVICE_ID", "870d649515ce700797d6a56965689f3aaa7d5e82dfdce994b239e00e37238184")
CHAT_ID = os.getenv("BOT_CHAT_ID", "cmh2gy89r01pvt33exijh1wr3")
CIRCLE_ID = os.getenv("BOT_CIRCLE_ID", "cm9bylrbn00hmux6t43mczt2o")
WEB_APP_URL = os.environ.get("WEB_APP_URL", "http://localhost:8000")
PORT = int(os.environ.get("PORT", "8000"))

# ═══════════════════════════════════════════════════════════
# STATE
# ═══════════════════════════════════════════════════════════
tokens: Dict[str, dict] = {}
rooms: Dict[str, dict] = {}
kyodo_client = None

# ═══════════════════════════════════════════════════════════
# JSON HELPERS
# ═══════════════════════════════════════════════════════════

def json_write(path: str, data: Any):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def json_read(path: str, default: Any = None):
    if default is None:
        default = []
    try:
        if not os.path.exists(path):
            return default
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default

# ═══════════════════════════════════════════════════════════
# KYODO BOT
# ═══════════════════════════════════════════════════════════

async def run_kyodo_bot():
    global kyodo_client
    if not KYODO_OK:
        print("[Kyodo] Library not available, bot task idle")
        while True:
            await asyncio.sleep(3600)

    backoff = 5
    while True:
        t0 = time.time()
        try:
            kyodo_client = Client(deviceId=DEVICE_ID)
            print("[Kyodo] Client created")

            @kyodo_client.middleware(EventType.ChatMessage)
            async def self_filter(message: ChatMessage):
                if message.author.userId == kyodo_client.userId:
                    return False

            @kyodo_client.event(EventType.ChatMessage)
            async def on_msg(message: ChatMessage):
                try:
                    content = (message.content or "").strip()
                    if not content or message.chatId != CHAT_ID:
                        return
                    print(f"[Kyodo] msg from {message.author.nickname}: {content[:50]}")

                    if content.lower() in ("/call", "!call", "/جلسة"):
                        room_id = str(uuid.uuid4())[:8]
                        chat_file = f"{room_id}_chat.json"
                        rooms[room_id] = {
                            "peers": {},
                            "chat_file": chat_file,
                            "created": datetime.now().isoformat(),
                            "creator_uid": message.author.userId,
                            "creator_name": message.author.nickname,
                        }
                        json_write(chat_file, [])
                        token = str(uuid.uuid4())
                        tokens[token] = {"room_id": room_id, "creator": True}
                        link = f"{WEB_APP_URL}/call/{room_id}?t={token}"
                        await kyodo_client.send_message(
                            message.chatId,
                            "Silent Hill Voice Session\n" + link + "\nTap to join the call.",
                            message.circleId,
                        )
                        print(f"[Kyodo] Room {room_id} created, link sent")
                except Exception as e:
                    print(f"[Kyodo] error: {e}")
                    traceback.print_exc()

            await kyodo_client.login(EMAIL, PASSWORD)
            print("[Kyodo] Logged in!")
            await kyodo_client.socket_wait()
            print("[Kyodo] Socket closed, will reconnect")
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception as e:
            print(f"[Kyodo] crash: {e}")
            traceback.print_exc()
        backoff = 5 if time.time() - t0 > 300 else min(backoff * 2, 120)
        print(f"[Kyodo] reconnect in {backoff}s...")
        await asyncio.sleep(backoff)


# ═══════════════════════════════════════════════════════════
# FASTAPI
# ═══════════════════════════════════════════════════════════

app = FastAPI()

@app.get("/")
async def root():
    return {"ok": True, "time": str(datetime.now())}

@app.get("/bg.jpg")
async def bg():
    return FileResponse("bg.jpg") if os.path.exists("bg.jpg") else HTMLResponse("", 404)

@app.get("/ci.jpg")
async def ci():
    return FileResponse("ci.jpg") if os.path.exists("ci.jpg") else HTMLResponse("", 404)

@app.get("/api/room/{room_id}/history")
async def room_history(room_id: str):
    return JSONResponse(get_chat_history(room_id) if room_id in rooms else [])

@app.get("/call/{room_id}")
async def call_page(room_id: str, t: str = Query(...)):
    tok = tokens.get(t)
    if not tok or tok.get("room_id") != room_id or room_id not in rooms:
        return HTMLResponse("<h1>Invalid or expired link</h1>", 403)
    html = CALL_HTML.replace("__ROOM_ID__", room_id).replace("__TOKEN__", t)
    return HTMLResponse(html)

@app.websocket("/ws/{room_id}")
async def ws_endpoint(websocket: WebSocket, room_id: str, t: str = Query(...)):
    tok = tokens.get(t)
    if not tok or tok.get("room_id") != room_id or room_id not in rooms:
        print(f"[WS] Rejected connection: bad token or room")
        await websocket.close(code=4001)
        return

    await websocket.accept()
    print(f"[WS] Connection accepted for room {room_id}")

    peer_id = str(uuid.uuid4())[:8]
    room = rooms[room_id]
    display_name = "Unknown"
    avatar_data = ""

    try:
        init_msg = await asyncio.wait_for(websocket.receive_json(), timeout=10)
        print(f"[WS] Peer {peer_id} init: {json.dumps(init_msg)}")
        if init_msg.get("type") == "join":
            display_name = init_msg.get("name", "Unknown")[:30]
            avatar_data = init_msg.get("avatar", "")[:100000]
    except asyncio.TimeoutError:
        print(f"[WS] Peer {peer_id} init timeout")
        await websocket.close(code=4002)
        return

    is_host = tok.get("creator", False) and len(room["peers"]) == 0
    room["peers"][peer_id] = {
        "ws": websocket, "name": display_name,
        "avatar": avatar_data, "muted": False, "is_host": is_host,
    }
    existing = [pid for pid in room["peers"].keys() if pid != peer_id]
    print(f"[WS] Peer {peer_id} ({display_name}) joined. Host={is_host}. Existing: {len(existing)}")

    # Send history
    history = get_chat_history(room_id)
    await websocket.send_json({"type": "history", "messages": history})
    print(f"[WS] Sent {len(history)} history messages")

    # Send existing peers (new peer creates offers to them)
    peers_info = [{"id": pid, "name": room["peers"][pid]["name"],
                   "avatar": room["peers"][pid]["avatar"],
                   "is_host": room["peers"][pid]["is_host"],
                   "muted": room["peers"][pid]["muted"]}
                  for pid in existing]
    await websocket.send_json({"type": "peers", "peers": peers_info})
    print(f"[WS] Sent {len(peers_info)} existing peers to {peer_id}")

    # Notify existing peers
    join_msg = {"type": "peer_joined",
                "peer": {"id": peer_id, "name": display_name,
                         "avatar": avatar_data, "is_host": is_host, "muted": False}}
    for pid in existing:
        try:
            await room["peers"][pid]["ws"].send_json(join_msg)
        except Exception as e:
            print(f"[WS] Failed to notify peer {pid}: {e}")

    # System message
    sys_msg = {"type": "chat", "kind": "system",
               "text": f"{display_name} joined the call",
               "time": datetime.now().isoformat()}
    append_chat_message(room_id, sys_msg)
    for pid in existing:
        try:
            await room["peers"][pid]["ws"].send_json(sys_msg)
        except Exception:
            pass

    # Main loop
    try:
        while True:
            msg = await websocket.receive_json()
            mtype = msg.get("type")
            print(f"[WS] Peer {peer_id} sent: type={mtype}")

            if mtype == "chat":
                text = msg.get("text", "").strip()[:500]
                if not text:
                    continue
                chat_msg = {
                    "type": "chat", "kind": "user", "peer_id": peer_id,
                    "name": display_name, "avatar": avatar_data,
                    "text": text, "time": datetime.now().isoformat(),
                }
                append_chat_message(room_id, chat_msg)
                # Echo to sender with self=true
                try:
                    await websocket.send_json({**chat_msg, "self": True})
                    print(f"[WS] Echoed chat to sender {peer_id}")
                except Exception as e:
                    print(f"[WS] Echo failed: {e}")
                # Broadcast to others
                for pid, pdata in room["peers"].items():
                    if pid != peer_id:
                        try:
                            await pdata["ws"].send_json(chat_msg)
                        except Exception:
                            pass
                print(f"[WS] Chat broadcasted to {len(room['peers'])-1} peers")

            elif mtype in ("webrtc_offer", "webrtc_answer", "webrtc_ice"):
                target = msg.get("to")
                msg["from"] = peer_id
                if target and target in room["peers"]:
                    try:
                        await room["peers"][target]["ws"].send_json(msg)
                        print(f"[WS] Relayed {mtype} from {peer_id} to {target}")
                    except Exception as e:
                        print(f"[WS] Relay failed: {e}")

            elif mtype == "mute_me":
                room["peers"][peer_id]["muted"] = True
                await websocket.send_json({"type": "mute_cmd"})
                for pid, pdata in room["peers"].items():
                    if pid != peer_id:
                        try:
                            await pdata["ws"].send_json({"type": "voice_state", "peer_id": peer_id, "muted": True})
                        except Exception:
                            pass

            elif mtype == "unmute_me":
                room["peers"][peer_id]["muted"] = False
                await websocket.send_json({"type": "unmute_cmd"})
                for pid, pdata in room["peers"].items():
                    if pid != peer_id:
                        try:
                            await pdata["ws"].send_json({"type": "voice_state", "peer_id": peer_id, "muted": False})
                        except Exception:
                            pass
    except WebSocketDisconnect:
        print(f"[WS] Peer {peer_id} disconnected")
    except Exception as e:
        print(f"[WS] Peer {peer_id} error: {e}")
    finally:
        if peer_id in room["peers"]:
            del room["peers"][peer_id]

        left_msg = {"type": "peer_left", "peer_id": peer_id, "name": display_name}
        sys_msg = {"type": "chat", "kind": "system",
                   "text": f"{display_name} left the call",
                   "time": datetime.now().isoformat()}
        append_chat_message(room_id, sys_msg)
        for pid, pdata in list(room["peers"].items()):
            try:
                await pdata["ws"].send_json(left_msg)
                await pdata["ws"].send_json(sys_msg)
            except Exception:
                pass

        if not room["peers"]:
            path = f"{room_id}_chat.json"
            if os.path.exists(path):
                os.remove(path)
            expired = [tk for tk, d in tokens.items() if d.get("room_id") == room_id]
            for tk in expired:
                del tokens[tk]
            del rooms[room_id]
            print(f"[Room {room_id}] closed")


def get_chat_history(room_id: str, limit: int = 100):
    path = f"{room_id}_chat.json"
    msgs = json_read(path, [])
    return msgs[-limit:]


def append_chat_message(room_id: str, msg: dict):
    path = f"{room_id}_chat.json"
    msgs = json_read(path, [])
    msgs.append(msg)
    json_write(path, msgs)


# ═══════════════════════════════════════════════════════════
# HTML FRONTEND — All fixes applied
# ═══════════════════════════════════════════════════════════

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
.bg::after{content:'';position:absolute;inset:0;background:linear-gradient(180deg,rgba(0,0,0,0.6) 0%,rgba(0,0,0,0.3) 50%,rgba(0,0,0,0.7) 100%)}
.header{position:relative;z-index:10;background:rgba(13,13,13,0.95);backdrop-filter:blur(20px);border-bottom:1px solid rgba(255,255,255,0.06);padding:8px 12px;display:flex;align-items:center;gap:10px;flex-shrink:0}
.back-btn,.menu-btn{width:36px;height:36px;display:flex;align-items:center;justify-content:center;background:none;border:none;color:#fff;font-size:20px;cursor:pointer}
.group-icon{width:36px;height:36px;border-radius:50%;object-fit:cover;border:1px solid rgba(255,255,255,0.1)}
.group-info{flex:1;min-width:0}
.group-name{font-size:15px;font-weight:600;color:#fff;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.group-meta{font-size:12px;color:#8e8e93;margin-top:1px}
.messages{flex:1;overflow-y:auto;padding:12px;display:flex;flex-direction:column;gap:6px;position:relative;z-index:5;scroll-behavior:smooth}
.messages::-webkit-scrollbar{width:0}
.msg-system{text-align:center;color:#8e8e93;font-size:12px;padding:6px 0;opacity:0.8}
.msg-row{display:flex;gap:8px;max-width:85%;animation:msgIn 0.2s ease-out;align-items:flex-start}
.msg-row.self{align-self:flex-end;flex-direction:row-reverse}
.msg-row.other{align-self:flex-start}
@keyframes msgIn{from{opacity:0;transform:translateY(6px)}to{opacity:1;transform:translateY(0)}}
.avatar{width:36px;height:36px;border-radius:50%;object-fit:cover;border:2px solid rgba(255,255,255,0.1);flex-shrink:0;background:#2c2c2e;display:flex;align-items:center;justify-content:center;font-size:14px;font-weight:600;color:#8e8e93;overflow:hidden}
.avatar img{width:100%;height:100%;object-fit:cover}
.msg-content{display:flex;flex-direction:column;gap:3px;min-width:0;max-width:260px}
.msg-header{display:flex;align-items:center;gap:6px;flex-wrap:wrap}
.msg-row.self .msg-header{flex-direction:row-reverse;justify-content:flex-start}
.msg-name{font-size:12px;font-weight:600;color:#fff;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:120px}
.msg-badge{font-size:9px;font-weight:700;padding:2px 8px;border-radius:10px;text-transform:uppercase;letter-spacing:0.5px}
.msg-badge.host{background:#007aff;color:#fff}
.msg-badge.cohost{background:#3a3a3c;color:#fff}
.msg-bubble{padding:8px 14px;border-radius:18px;font-size:14px;line-height:1.4;word-break:break-word;white-space:pre-wrap}
.msg-row.other .msg-bubble{background:#2c2c2e;color:#fff;border-bottom-left-radius:4px}
.msg-row.self .msg-bubble{background:#007aff;color:#fff;border-bottom-right-radius:4px}
.voice-bar{position:relative;z-index:10;background:rgba(28,28,30,0.95);backdrop-filter:blur(20px);border-top:1px solid rgba(255,255,255,0.06);padding:10px 16px;display:flex;align-items:center;justify-content:center;gap:24px;flex-shrink:0}
.voice-btn{width:48px;height:48px;border-radius:50%;border:none;display:flex;align-items:center;justify-content:center;font-size:22px;cursor:pointer;transition:all 0.2s}
.voice-btn.mute{background:#3a3a3c;color:#fff}
.voice-btn.mute.muted{background:#ff3b30;color:#fff}
.voice-btn.leave{background:#ff3b30;color:#fff;font-size:20px}
.voice-status{font-size:12px;color:#8e8e93;min-width:60px;text-align:center}
.input-bar{position:relative;z-index:10;background:rgba(13,13,13,0.95);backdrop-filter:blur(20px);border-top:1px solid rgba(255,255,255,0.06);padding:8px 12px;display:flex;align-items:center;gap:8px;flex-shrink:0}
.input-attach{width:36px;height:36px;border-radius:50%;border:none;background:#2c2c2e;color:#fff;font-size:18px;display:flex;align-items:center;justify-content:center;cursor:pointer;flex-shrink:0}
.input-field{flex:1;height:38px;border-radius:19px;border:none;background:#1c1c1e;color:#fff;padding:0 14px;font-size:14px;outline:none}
.input-field::placeholder{color:#8e8e93}
.input-send{width:38px;height:38px;border-radius:50%;border:none;background:#007aff;color:#fff;font-size:16px;display:flex;align-items:center;justify-content:center;cursor:pointer;flex-shrink:0}
.input-send:active{transform:scale(0.92)}
.overlay{position:fixed;inset:0;z-index:100;background:rgba(0,0,0,0.88);display:flex;align-items:center;justify-content:center;backdrop-filter:blur(10px)}
.overlay-box{background:#1c1c1e;border-radius:16px;padding:24px;width:90%;max-width:340px;text-align:center}
.overlay-box h2{font-size:18px;margin-bottom:8px;color:#fff}
.overlay-box p{font-size:13px;color:#8e8e93;margin-bottom:16px}
.overlay-box input{width:100%;height:44px;border-radius:12px;border:1px solid #3a3a3c;background:#2c2c2e;color:#fff;padding:0 14px;font-size:15px;text-align:center;outline:none;margin-bottom:10px}
.overlay-box input:focus{border-color:#007aff}
.overlay-box button{width:100%;height:44px;border-radius:12px;border:none;background:#007aff;color:#fff;font-size:15px;font-weight:600;cursor:pointer}
.overlay-box button:disabled{opacity:0.5}
.avatar-preview{width:80px;height:80px;border-radius:50%;object-fit:cover;border:3px solid #3a3a3c;margin:0 auto 12px;background:#2c2c2e;display:flex;align-items:center;justify-content:center;font-size:32px;font-weight:600;color:#8e8e93;overflow:hidden;cursor:pointer}
.avatar-preview img{width:100%;height:100%;object-fit:cover}
.avatar-input{display:none}
.file-label{display:block;color:#007aff;font-size:13px;margin-bottom:10px;cursor:pointer}
.debug-panel{position:fixed;top:0;left:0;right:0;z-index:200;background:rgba(255,0,0,0.9);color:#fff;font-size:11px;padding:4px;font-family:monospace;max-height:120px;overflow-y:auto;display:none}
.debug-panel.show{display:block}
.hidden{display:none!important}
</style>
</head>
<body>
<div class="bg"></div>
<div class="debug-panel" id="debugPanel"></div>

<!-- Join overlay -->
<div class="overlay" id="joinOverlay">
<div class="overlay-box">
<h2>Join Silent Hill</h2>
<div class="avatar-preview" id="avatarPreview" onclick="document.getElementById('avatarInput').click()">
<span id="avatarLetter">?</span>
</div>
<input type="file" class="avatar-input" id="avatarInput" accept="image/*" onchange="onAvatarPick(event)">
<label class="file-label" onclick="document.getElementById('avatarInput').click()">Tap circle to add photo</label>
<input type="text" id="nameInput" placeholder="Your name" maxlength="20" autocomplete="off" onkeypress="if(event.key==='Enter')doJoin()">
<button id="joinBtn" onclick="doJoin()">Join Call</button>
</div>
</div>

<!-- Main app -->
<div class="app" id="app" style="display:none">
<div class="header">
<button class="back-btn" onclick="leaveCall()">&#8249;</button>
<img class="group-icon" src="/ci.jpg" onerror="this.style.display='none'" alt="">
<div class="group-info">
<div class="group-name">Silent Hill</div>
<div class="group-meta" id="memberCount">0 in call</div>
</div>
<button class="menu-btn" onclick="toggleDebug()">&#9949;</button>
</div>

<div class="messages" id="messages"></div>

<div class="voice-bar">
<button class="voice-btn mute" id="muteBtn" onclick="toggleMute()">&#127908;</button>
<span class="voice-status" id="voiceStatus">Connecting...</span>
<button class="voice-btn leave" onclick="leaveCall()">&#10005;</button>
</div>

<div class="input-bar">
<button class="input-attach" onclick="document.getElementById('debugPanel').classList.toggle('show')">+</button>
<input type="text" class="input-field" id="msgInput" placeholder="Write a message..." onkeypress="if(event.key==='Enter')sendMsg()">
<button class="input-send" onclick="sendMsg()">&#10148;</button>
</div>
</div>

<script>
// ═══════════════════════════════════════════
// CONFIG
// ═══════════════════════════════════════════
const ROOM="__ROOM_ID__",TOKEN="__TOKEN__";

// ═══════════════════════════════════════════
// STATE
// ═══════════════════════════════════════════
let ws=null,localStream=null,myName="",myAvatar="",isMuted=false,isHost=false;
const peers={},audios={};
const peerMap=new Map();
const ICE={iceServers:[{urls:'stun:stun.l.google.com:19302'},{urls:'stun:stun1.l.google.com:19302'}]};
let debugLines=[];

// ═══════════════════════════════════════════
// DEBUG LOGGING
// ═══════════════════════════════════════════
function log(msg){
const ts=new Date().toLocaleTimeString();
const line=`[${ts}] ${msg}`;
console.log(line);
debugLines.push(line);
if(debugLines.length>50)debugLines.shift();
const panel=document.getElementById('debugPanel');
if(panel)panel.innerHTML=debugLines.join('<br>');
}
function toggleDebug(){document.getElementById('debugPanel').classList.toggle('show');}

// ═══════════════════════════════════════════
// AVATAR UPLOAD
// ═══════════════════════════════════════════
function onAvatarPick(e){
const f=e.target.files[0];
if(!f)return;
log("Avatar selected: "+f.name+" ("+Math.round(f.size/1024)+"KB)");
const r=new FileReader();
r.onload=ev=>{
const data=ev.target.result;
myAvatar=data;
document.getElementById('avatarPreview').innerHTML='<img src="'+data+'">';
document.getElementById('avatarLetter').style.display='none';
log("Avatar loaded (base64)");
};
r.readAsDataURL(f);
}

function updateAvatarPreview(){
const el=document.getElementById('avatarPreview');
if(myAvatar){
el.innerHTML='<img src="'+myAvatar+'">';
}else{
const initial=myName?myName.charAt(0).toUpperCase():'?';
el.innerHTML='<span>'+initial+'</span>';
}
}

document.getElementById('nameInput').addEventListener('input',e=>{
myName=e.target.value;
if(!myAvatar)updateAvatarPreview();
});

// ═══════════════════════════════════════════
// JOIN
// ═══════════════════════════════════════════
async function doJoin(){
const name=document.getElementById('nameInput').value.trim();
if(!name){alert("Enter your name");return;}
myName=name;
log("Joining as: "+name);
document.getElementById('joinBtn').disabled=true;
document.getElementById('joinBtn').textContent="Connecting...";

try{
localStream=await navigator.mediaDevices.getUserMedia({audio:true});
log("Mic permission granted");
document.getElementById('voiceStatus').textContent='Connected';
}catch(e){
log("Mic error: "+e.message);
document.getElementById('voiceStatus').textContent='No mic';
}

document.getElementById('joinOverlay').classList.add('hidden');
document.getElementById('app').style.display='';
connectWS();
}

// ═══════════════════════════════════════════
// WEBSOCKET
// ═══════════════════════════════════════════
function connectWS(){
const proto=window.location.protocol==='https:'?'wss:':'ws:';
const url=proto+'//'+window.location.host+'/ws/'+ROOM+'?t='+TOKEN;
log("WS connecting to: "+url.replace(/t=[^&]+/,'t=***'));

ws=new WebSocket(url);

ws.onopen=()=>{
log("WS connected");
ws.send(JSON.stringify({type:'join',name:myName,avatar:myAvatar}));
log("Sent join message");
};

ws.onmessage=async(ev)=>{
let m;
try{m=JSON.parse(ev.data);}catch(e){log("Bad JSON: "+ev.data);return;}
log("WS recv: type="+m.type);

switch(m.type){
case 'history':
log("Loading "+m.messages.length+" history messages");
m.messages.forEach(renderMsg);
break;
case 'chat':
renderMsg(m);
break;
case 'peers':
log("Got "+m.peers.length+" existing peers — creating offers");
m.peers.forEach(p=>{
addPeerInfo(p);
// NEW PEER CREATES OFFERS TO EXISTING PEERS
log("Creating offer to peer "+p.id);
createOffer(p.id);
});
break;
case 'peer_joined':
log("Peer joined: "+m.peer.name+" ("+m.peer.id+")");
addPeerInfo(m.peer);
renderSys(m.peer.name+" joined the call");
break;
case 'peer_left':
log("Peer left: "+(m.name||m.peer_id));
removePeerAudio(m.peer_id);
peerMap.delete(m.peer_id);
renderSys((m.name||'Someone')+' left the call');
updateMemberCount();
break;
case 'webrtc_offer':
log("Got offer from "+m.from);
await handleOffer(m.from,m.sdp);
break;
case 'webrtc_answer':
log("Got answer from "+m.from);
await handleAnswer(m.from,m.sdp);
break;
case 'webrtc_ice':
log("Got ICE from "+m.from);
await handleIce(m.from,m.candidate);
break;
case 'mute_cmd':
log("Muted by server");
if(localStream)localStream.getAudioTracks().forEach(t=>t.enabled=false);
break;
case 'unmute_cmd':
log("Unmuted by server");
if(localStream)localStream.getAudioTracks().forEach(t=>t.enabled=true);
break;
case 'voice_state':
{
const p=peerMap.get(m.peer_id);
if(p){p.muted=m.muted;log("Peer "+m.peer_id+" muted="+m.muted);}
}
break;
default:
log("Unknown msg type: "+m.type);
}
};

ws.onclose=(ev)=>{
log("WS closed: code="+ev.code+" reason="+ev.reason);
cleanupRTC();
};
ws.onerror=(e)=>{
log("WS error");
document.getElementById('voiceStatus').textContent='Error';
};
}

// ═══════════════════════════════════════════
// PEER INFO
// ═══════════════════════════════════════════
function addPeerInfo(p){
peerMap.set(p.id,{name:p.name,avatar:p.avatar||'',is_host:p.is_host,muted:p.muted||false});
if(p.is_host)log("Peer "+p.id+" is HOST");
updateMemberCount();
}

function updateMemberCount(){
document.getElementById('memberCount').textContent=(peerMap.size+1)+' in call';
}

// ═══════════════════════════════════════════
// WEBRTC — NEW PEER CREATES OFFERS (mesh)
// ═══════════════════════════════════════════
async function createOffer(peerId){
log("createOffer -> "+peerId);
try{
const pc=new RTCPeerConnection(ICE);
setupPC(pc,peerId);
peers[peerId]=pc;
if(localStream){
localStream.getTracks().forEach(t=>pc.addTrack(t,localStream));
log("Added local tracks to PC");
}
const o=await pc.createOffer();
await pc.setLocalDescription(o);
log("Offer created, sending to "+peerId);
ws.send(JSON.stringify({type:'webrtc_offer',to:peerId,sdp:o.sdp}));
}catch(e){
log("createOffer FAILED: "+e.message);
}
}

async function handleOffer(from,sdp){
log("handleOffer from "+from);
try{
const pc=new RTCPeerConnection(ICE);
setupPC(pc,from);
peers[from]=pc;
if(localStream){
localStream.getTracks().forEach(t=>pc.addTrack(t,localStream));
log("Added local tracks to answer PC");
}
await pc.setRemoteDescription(new RTCSessionDescription({type:'offer',sdp}));
const a=await pc.createAnswer();
await pc.setLocalDescription(a);
log("Answer created, sending to "+from);
ws.send(JSON.stringify({type:'webrtc_answer',to:from,sdp:a.sdp}));
}catch(e){
log("handleOffer FAILED: "+e.message);
}
}

async function handleAnswer(from,sdp){
log("handleAnswer from "+from);
try{
const pc=peers[from];
if(!pc){log("No PC for "+from);return;}
await pc.setRemoteDescription(new RTCSessionDescription({type:'answer',sdp}));
log("Remote description set for "+from);
}catch(e){
log("handleAnswer FAILED: "+e.message);
}
}

async function handleIce(from,candidate){
try{
const pc=peers[from];
if(pc&&candidate){
await pc.addIceCandidate(new RTCIceCandidate(candidate));
log("ICE added for "+from);
}
}catch(e){
log("handleIce FAILED: "+e.message);
}
}

function setupPC(pc,peerId){
// ICE candidate forwarding
pc.onicecandidate=e=>{
if(e.candidate&&ws&&ws.readyState===1){
ws.send(JSON.stringify({type:'webrtc_ice',to:peerId,candidate:e.candidate}));
}
};

// Receive audio track
pc.ontrack=e=>{
log("ontrack from "+peerId+" — streams="+e.streams.length);
let a=audios[peerId];
if(!a){
a=new Audio();
a.autoplay=true;
a.volume=1.0;
audios[peerId]=a;
log("Created audio element for "+peerId);
}
a.srcObject=e.streams[0];
a.play().catch(err=>log("Audio play failed: "+err.message));
log("Audio stream attached for "+peerId);
};

// Connection state changes
pc.onconnectionstatechange=()=>{
log("PC "+peerId+" state: "+pc.connectionState);
if(pc.connectionState==='connected'){
log("PEER CONNECTED: "+peerId+" — audio should flow now!");
document.getElementById('voiceStatus').textContent='Voice OK';
}
if(pc.connectionState==='failed'||pc.connectionState==='closed'){
log("PC "+peerId+" "+pc.connectionState);
removePeerAudio(peerId);
}
};

pc.oniceconnectionstatechange=()=>{
log("PC "+peerId+" ICE: "+pc.iceConnectionState);
};
}

function removePeerAudio(peerId){
if(peers[peerId]){peers[peerId].close();delete peers[peerId];}
if(audios[peerId]){
audios[peerId].pause();
audios[peerId].srcObject=null;
delete audios[peerId];
}
}

function cleanupRTC(){
Object.keys(peers).forEach(removePeerAudio);
if(localStream){localStream.getTracks().forEach(t=>t.stop());localStream=null;}
}

// ═══════════════════════════════════════════
// CHAT RENDERING
// ═══════════════════════════════════════════
function renderMsg(m){
log("renderMsg kind="+m.kind+" self="+m.self+" name="+(m.name||'?'));
const container=document.getElementById('messages');
if(!container){log("No messages container!");return;}

if(m.kind==='system'){
const d=document.createElement('div');
d.className='msg-system';
d.textContent=m.text;
container.appendChild(d);
scrollBottom();
return;
}

const isSelf=!!m.self;
const peerInfo=peerMap.get(m.peer_id)||{};
const displayName=m.name||peerInfo.name||'Unknown';
const isPeerHost=peerInfo.is_host||false;
const avatarSrc=m.avatar||peerInfo.avatar||'';

// CSS class: self or other
const rowClass=isSelf?'self':'other';
// Badge
const badgeText=isSelf?(isHost?'Host':'Co-host'):(isPeerHost?'Host':'Co-host');
const badgeClass=isSelf?(isHost?'host':'cohost'):(isPeerHost?'host':'cohost');

const row=document.createElement('div');
row.className='msg-row '+rowClass;

// Avatar
let avatarHTML='';
if(avatarSrc){
avatarHTML='<div class="avatar"><img src="'+esc(avatarSrc)+'" alt=""></div>';
}else{
const initial=esc(displayName.charAt(0).toUpperCase());
avatarHTML='<div class="avatar"><span>'+initial+'</span></div>';
}

// Name + badge
let headerHTML='';
if(isSelf){
headerHTML='<div class="msg-header"><span class="msg-name">'+esc(displayName)+'</span><span class="msg-badge '+badgeClass+'">'+badgeText+'</span></div>';
}else{
headerHTML='<div class="msg-header"><span class="msg-name">'+esc(displayName)+'</span><span class="msg-badge '+badgeClass+'">'+badgeText+'</span></div>';
}

row.innerHTML=avatarHTML+'<div class="msg-content">'+headerHTML+'<div class="msg-bubble">'+esc(m.text)+'</div></div>';
container.appendChild(row);
scrollBottom();
log("Message rendered: "+displayName+": "+m.text.substring(0,30));
}

function renderSys(text){
log("renderSys: "+text);
const c=document.getElementById('messages');
if(!c)return;
const d=document.createElement('div');
d.className='msg-system';
d.textContent=text;
c.appendChild(d);
scrollBottom();
}

function scrollBottom(){
const el=document.getElementById('messages');
el.scrollTop=el.scrollHeight;
}

function esc(t){
const d=document.createElement('div');
d.textContent=t||'';
return d.innerHTML;
}

// ═══════════════════════════════════════════
// SEND MESSAGE
// ═══════════════════════════════════════════
function sendMsg(){
const input=document.getElementById('msgInput');
const text=input.value.trim();
if(!text){log("Empty message, not sending");return;}
if(!ws||ws.readyState!==1){log("WS not ready, cannot send");return;}

log("Sending chat: "+text.substring(0,30));
ws.send(JSON.stringify({type:'chat',text:text}));
input.value='';

// Optimistic render
renderMsg({
type:'chat',kind:'user',peer_id:'self',
name:myName,avatar:myAvatar,text:text,
time:new Date().toISOString(),self:true
});
}

// ═══════════════════════════════════════════
// MUTE / LEAVE
// ═══════════════════════════════════════════
function toggleMute(){
if(!localStream)return;
isMuted=!isMuted;
localStream.getAudioTracks().forEach(t=>t.enabled=!isMuted);
const btn=document.getElementById('muteBtn');
if(isMuted){
btn.classList.add('muted');
btn.innerHTML='&#128263;';
document.getElementById('voiceStatus').textContent='Muted';
log("Muted");
}else{
btn.classList.remove('muted');
btn.innerHTML='&#127908;';
document.getElementById('voiceStatus').textContent='Connected';
log("Unmuted");
}
if(ws&&ws.readyState===1)ws.send(JSON.stringify({type:isMuted?'mute_me':'unmute_me'}));
}

function leaveCall(){
log("Leaving call");
if(ws&&ws.readyState===1)ws.close();
cleanupRTC();
try{window.close();}catch(e){}
document.body.innerHTML='<div style="display:flex;align-items:center;justify-content:center;height:100vh;background:#000;color:#fff;font-family:sans-serif;"><h2>You left the call</h2></div>';
}

// ═══════════════════════════════════════════
// INIT
// ═══════════════════════════════════════════
document.getElementById('nameInput').focus();
log("Page loaded, room="+ROOM);
</script>
</body>
</html>"""


# ═══════════════════════════════════════════════════════════
# RENDER KEEPALIVE
# ═══════════════════════════════════════════════════════════

async def keepalive():
    await asyncio.sleep(30)
    url = WEB_APP_URL
    if "onrender.com" not in url and "localhost" not in url:
        url = None
    while True:
        await asyncio.sleep(600)
        if url:
            try:
                async with aiohttp.ClientSession() as s:
                    async with s.get(url, timeout=15) as r:
                        print(f"[keepalive] ping -> {r.status}")
            except Exception as e:
                print(f"[keepalive] error: {e}")


# ═══════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════

async def web_task():
    cfg = Config(app=app, host="0.0.0.0", port=PORT, log_level="warning")
    srv = Server(cfg)
    await srv.serve()


async def main():
    print("=" * 50)
    print("Silent Hill Voice Call Bot")
    print(f"Web URL: {WEB_APP_URL}")
    print(f"Port: {PORT}")
    print(f"Kyodo: {'YES' if KYODO_OK else 'NO'}")
    print("=" * 50)
    await asyncio.gather(web_task(), run_kyodo_bot(), keepalive())


if __name__ == "__main__":
    asyncio.run(main())
