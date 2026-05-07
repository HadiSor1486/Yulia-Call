"""
Silent Hill Voice Call Bot — Kyodo + WebRTC + Chat UI
Run: python main.py
"""

import asyncio
import json
import os
import sys
import time
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

import httpx
from fastapi import FastAPI, Query, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from uvicorn import Config, Server

# ═══════════════════════════════════════════════════════════
# Try importing Kyodo client
# ═══════════════════════════════════════════════════════════
try:
    from kyodo import ChatMessage, Client as KyodoClient, EventType
    from kyodo.objects.args import ChatMessageTypes, MediaTarget
    KYODO_AVAILABLE = True
except ImportError:
    KYODO_AVAILABLE = False
    print("[WARN] kyodo library not found — bot will not send links to Kyodo")

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
tokens: Dict[str, dict] = {}      # token -> {room_id, display_name, kyodo_uid?}
rooms: Dict[str, dict] = {}       # room_id -> {peers: {}, chat_file: str, created: str}
user_cache: Dict[str, dict] = {}  # kyodo_uid -> {name, avatar}
kyodo_client = None

# ═══════════════════════════════════════════════════════════
# KYODO BOT
# ═══════════════════════════════════════════════════════════

async def setup_kyodo():
    """Start Kyodo client and listen for /call command."""
    global kyodo_client
    if not KYODO_AVAILABLE:
        return

    kyodo_client = KyodoClient(deviceId=DEVICE_ID)

    @kyodo_client.event(EventType.ChatMessage)
    async def on_msg(message: ChatMessage):
        try:
            if message.author.userId == kyodo_client.userId:
                return
            content = (message.content or "").strip()
            if not content or message.chatId != CHAT_ID:
                return

            if content.lower() in ("/call", "!call", "/جلسة"):
                # Create room
                room_id = str(uuid.uuid4())[:8]
                chat_file = f"{room_id}_chat.json"
                rooms[room_id] = {
                    "peers": {},
                    "chat_file": chat_file,
                    "created": datetime.now().isoformat(),
                    "creator_uid": message.author.userId,
                    "creator_name": message.author.nickname,
                }
                # Create empty chat file
                json_write(chat_file, [])

                # Generate token
                token = str(uuid.uuid4())
                tokens[token] = {"room_id": room_id, "creator": True}

                link = f"{WEB_APP_URL}/call/{room_id}?t={token}"

                await kyodo_client.send_message(
                    message.chatId,
                    "Silent Hill Voice Session\n" + link + "\nTap to join the call.",
                    message.circleId,
                )
        except Exception as e:
            print(f"[Kyodo] error: {e}")

    # Login and run
    try:
        await kyodo_client.login(EMAIL, PASSWORD)
        print("[Kyodo] Bot logged in!")
        await kyodo_client.socket_wait()
    except Exception as e:
        print(f"[Kyodo] login error: {e}")


# ═══════════════════════════════════════════════════════════
# CHAT PERSISTENCE (JSON)
# ═══════════════════════════════════════════════════════════

def json_read(path: str, default: Any = None) -> Any:
    if default is None:
        default = []
    try:
        if not os.path.exists(path):
            return default
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def json_write(path: str, data: Any):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def append_chat_message(room_id: str, msg: dict):
    path = f"{room_id}_chat.json"
    msgs = json_read(path, [])
    msgs.append(msg)
    json_write(path, msgs)


def get_chat_history(room_id: str, limit: int = 100) -> List[dict]:
    path = f"{room_id}_chat.json"
    msgs = json_read(path, [])
    return msgs[-limit:]


def delete_chat_history(room_id: str):
    path = f"{room_id}_chat.json"
    if os.path.exists(path):
        os.remove(path)


# ═══════════════════════════════════════════════════════════
# USER PROFILE FETCHING
# ═══════════════════════════════════════════════════════════

async def fetch_kyodo_user(kyodo_uid: str) -> Optional[dict]:
    """Fetch user name + avatar from Kyodo. Returns cached if available."""
    if kyodo_uid in user_cache:
        return user_cache[kyodo_uid]
    # Try getting from client if available
    if kyodo_client and hasattr(kyodo_client, 'get_user'):
        try:
            user = await kyodo_client.get_user(kyodo_uid)
            info = {
                "name": getattr(user, 'nickname', 'Unknown'),
                "avatar": getattr(user, 'avatar_url', '') or getattr(user, 'avatar', ''),
            }
            user_cache[kyodo_uid] = info
            return info
        except Exception:
            pass
    return None


# ═══════════════════════════════════════════════════════════
# FASTAPI APP
# ═══════════════════════════════════════════════════════════

app = FastAPI()

@app.get("/")
async def root():
    return {"ok": True, "time": str(datetime.now())}


@app.get("/bg.jpg")
async def bg():
    if os.path.exists("bg.jpg"):
        return FileResponse("bg.jpg")
    return HTMLResponse("", status_code=404)


@app.get("/ci.jpg")
async def ci():
    if os.path.exists("ci.jpg"):
        return FileResponse("ci.jpg")
    return HTMLResponse("", status_code=404)


@app.get("/api/room/{room_id}/history")
async def room_history(room_id: str):
    if room_id not in rooms:
        return JSONResponse([], status_code=404)
    return JSONResponse(get_chat_history(room_id))


@app.get("/call/{room_id}")
async def call_page(room_id: str, t: str = Query(...)):
    """Serve the Silent Hill chat+voice page."""
    tok = tokens.get(t)
    if not tok or tok.get("room_id") != room_id:
        return HTMLResponse("<h1>Link expired or invalid</h1>", status_code=403)
    if room_id not in rooms:
        return HTMLResponse("<h1>Room closed</h1>", status_code=404)

    # Inject room data into HTML
    html = CALL_HTML.replace("__ROOM_ID__", room_id).replace("__TOKEN__", t).replace("__WS_URL__", WEB_APP_URL)
    return HTMLResponse(html)


@app.websocket("/ws/{room_id}")
async def ws_endpoint(websocket: WebSocket, room_id: str, t: str = Query(...)):
    """Unified WebSocket: WebRTC signaling + Chat + Voice state."""
    tok = tokens.get(t)
    if not tok or tok.get("room_id") != room_id:
        await websocket.close(code=4001)
        return
    if room_id not in rooms:
        await websocket.close(code=4004)
        return

    await websocket.accept()

    peer_id = str(uuid.uuid4())[:8]
    room = rooms[room_id]

    # Wait for join message with display name
    display_name = "Unknown"
    kyodo_uid = ""
    is_host = False

    try:
        init_msg = await asyncio.wait_for(websocket.receive_json(), timeout=10)
        if init_msg.get("type") == "join":
            display_name = init_msg.get("name", "Unknown")[:30]
            kyodo_uid = init_msg.get("kyodo_uid", "")
            is_host = tok.get("creator", False) and len(room["peers"]) == 0
    except asyncio.TimeoutError:
        await websocket.close(code=4002)
        return

    # Fetch user profile from cache/Kyodo
    avatar_url = ""
    if kyodo_uid:
        info = await fetch_kyodo_user(kyodo_uid)
        if info:
            avatar_url = info.get("avatar", "")
            display_name = info.get("name", display_name)

    # Register peer
    room["peers"][peer_id] = {
        "ws": websocket,
        "name": display_name,
        "avatar": avatar_url,
        "kyodo_uid": kyodo_uid,
        "muted": False,
        "is_host": is_host,
    }

    existing = [pid for pid in room["peers"].keys() if pid != peer_id]

    # Send chat history
    history = get_chat_history(room_id)
    await websocket.send_json({"type": "history", "messages": history})

    # Tell new peer about existing peers
    await websocket.send_json({"type": "peers", "peers": [
        {"id": pid, "name": room["peers"][pid]["name"], "avatar": room["peers"][pid]["avatar"], "is_host": room["peers"][pid]["is_host"], "muted": room["peers"][pid]["muted"]}
        for pid in existing
    ]})

    # Notify existing peers
    join_msg = {
        "type": "peer_joined",
        "peer": {"id": peer_id, "name": display_name, "avatar": avatar_url, "is_host": is_host, "muted": False},
    }
    for pid in existing:
        try:
            await room["peers"][pid]["ws"].send_json(join_msg)
        except Exception:
            pass

    # Add system message to chat
    sys_msg = {
        "type": "chat",
        "kind": "system",
        "text": f"{display_name} joined the call",
        "time": datetime.now().isoformat(),
    }
    append_chat_message(room_id, sys_msg)
    for pid in existing:
        try:
            await room["peers"][pid]["ws"].send_json(sys_msg)
        except Exception:
            pass

    # Main message loop
    try:
        while True:
            msg = await websocket.receive_json()
            mtype = msg.get("type")

            if mtype == "chat":
                text = msg.get("text", "").strip()[:500]
                if not text:
                    continue
                chat_msg = {
                    "type": "chat",
                    "kind": "user",
                    "peer_id": peer_id,
                    "name": display_name,
                    "avatar": avatar_url,
                    "text": text,
                    "time": datetime.now().isoformat(),
                }
                append_chat_message(room_id, chat_msg)
                for pid, pdata in room["peers"].items():
                    if pid != peer_id:
                        try:
                            await pdata["ws"].send_json(chat_msg)
                        except Exception:
                            pass
                # Echo back to sender
                await websocket.send_json({**chat_msg, "self": True})

            elif mtype in ("webrtc_offer", "webrtc_answer", "webrtc_ice"):
                target = msg.get("to")
                msg["from"] = peer_id
                if target and target in room["peers"]:
                    try:
                        await room["peers"][target]["ws"].send_json(msg)
                    except Exception:
                        pass

            elif mtype == "voice_state":
                muted = msg.get("muted", False)
                room["peers"][peer_id]["muted"] = muted
                state_msg = {
                    "type": "voice_state",
                    "peer_id": peer_id,
                    "muted": muted,
                }
                for pid, pdata in room["peers"].items():
                    if pid != peer_id:
                        try:
                            await pdata["ws"].send_json(state_msg)
                        except Exception:
                            pass

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
        pass
    finally:
        # Cleanup
        if peer_id in room["peers"]:
            del room["peers"][peer_id]

        # Notify others
        left_msg = {
            "type": "peer_left",
            "peer_id": peer_id,
            "name": display_name,
        }
        sys_msg = {
            "type": "chat",
            "kind": "system",
            "text": f"{display_name} left the call",
            "time": datetime.now().isoformat(),
        }
        append_chat_message(room_id, sys_msg)

        for pid, pdata in list(room["peers"].items()):
            try:
                await pdata["ws"].send_json(left_msg)
                await pdata["ws"].send_json(sys_msg)
            except Exception:
                pass

        # If room empty, delete everything
        if not room["peers"]:
            delete_chat_history(room_id)
            expired = [tk for tk, d in tokens.items() if d.get("room_id") == room_id]
            for tk in expired:
                del tokens[tk]
            del rooms[room_id]
            print(f"[Room {room_id}] closed — all users left")


# ═══════════════════════════════════════════════════════════
# HTML TEMPLATE — Silent Hill Chat + Voice UI
# ═══════════════════════════════════════════════════════════

CALL_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0, user-scalable=no, viewport-fit=cover">
<meta name="apple-mobile-web-app-capable" content="yes">
<meta name="mobile-web-app-capable" content="yes">
<meta name="apple-mobile-web-app-status-bar-style" content="black-translucent">
<title>Silent Hill</title>
<style>
*{margin:0;padding:0;box-sizing:border-box;-webkit-tap-highlight-color:transparent}
html,body{height:100%;overflow:hidden;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;background:#0a0a0a;color:#fff}

/* Main app container */
.app{display:flex;flex-direction:column;height:100vh;height:100dvh;position:relative}

/* Background image */
.bg{position:fixed;inset:0;z-index:0;background:url('/bg.jpg') center/cover no-repeat;opacity:0.4}
.bg::after{content:'';position:absolute;inset:0;background:linear-gradient(180deg,rgba(0,0,0,0.6) 0%,rgba(0,0,0,0.3) 50%,rgba(0,0,0,0.7) 100%)}

/* Header */
.header{position:relative;z-index:10;background:rgba(13,13,13,0.95);backdrop-filter:blur(20px);border-bottom:1px solid rgba(255,255,255,0.06);padding:8px 12px;display:flex;align-items:center;gap:10px;flex-shrink:0}
.back-btn{width:36px;height:36px;display:flex;align-items:center;justify-content:center;background:none;border:none;color:#fff;font-size:20px;cursor:pointer}
.group-icon{width:36px;height:36px;border-radius:50%;object-fit:cover;border:1px solid rgba(255,255,255,0.1)}
.group-info{flex:1;min-width:0}
.group-name{font-size:15px;font-weight:600;color:#fff;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.group-meta{font-size:12px;color:#8e8e93;margin-top:1px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.menu-btn{width:36px;height:36px;display:flex;align-items:center;justify-content:center;background:none;border:none;color:#fff;font-size:18px;cursor:pointer}

/* Messages area */
.messages{flex:1;overflow-y:auto;padding:12px;display:flex;flex-direction:column;gap:4px;position:relative;z-index:5;scroll-behavior:smooth}
.messages::-webkit-scrollbar{width:0}

/* System message */
.msg-system{text-align:center;color:#8e8e93;font-size:12px;padding:8px 0;opacity:0.8}

/* Message row */
.msg-row{display:flex;gap:8px;max-width:85%;animation:msgIn 0.25s ease-out}
.msg-row.left{align-self:flex-start}
.msg-row.right{align-self:flex-end;flex-direction:row-reverse}

@keyframes msgIn{from{opacity:0;transform:translateY(10px)}to{opacity:1;transform:translateY(0)}}

/* Avatar */
.avatar{width:36px;height:36px;border-radius:50%;object-fit:cover;border:2px solid rgba(255,255,255,0.1);flex-shrink:0;align-self:flex-start}
.avatar-ring{animation:pulse 2s infinite}
@keyframes pulse{0%,100%{box-shadow:0 0 0 0 rgba(52,199,89,0.4)}50%{box-shadow:0 0 0 4px rgba(52,199,89,0)}}

/* Message content */
.msg-content{display:flex;flex-direction:column;gap:2px;min-width:0}
.msg-header{display:flex;align-items:center;gap:6px;flex-wrap:wrap}
.msg-row.right .msg-header{flex-direction:row-reverse}

.msg-name{font-size:12px;font-weight:600;color:#fff;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:120px}
.msg-badge{font-size:9px;font-weight:700;padding:2px 8px;border-radius:10px;text-transform:uppercase;letter-spacing:0.5px}
.msg-badge.host{background:#007aff;color:#fff}
.msg-badge.cohost{background:#3a3a3c;color:#fff}

/* Message bubble */
.msg-bubble{padding:8px 12px;border-radius:16px;font-size:14px;line-height:1.4;word-break:break-word;max-width:260px;width:fit-content}
.msg-row.left .msg-bubble{background:#2c2c2e;color:#fff;border-bottom-left-radius:4px}
.msg-row.right .msg-bubble{background:#007aff;color:#fff;border-bottom-right-radius:4px;margin-left:auto}

/* Reply styling */
.msg-reply{border-left:3px solid #007aff;padding-left:8px;margin-bottom:4px;opacity:0.7;font-size:12px}
.msg-reply-name{font-weight:600;margin-bottom:2px}

/* Voice bar */
.voice-bar{position:relative;z-index:10;background:rgba(28,28,30,0.95);backdrop-filter:blur(20px);border-top:1px solid rgba(255,255,255,0.06);padding:8px 16px;display:flex;align-items:center;justify-content:center;gap:20px;flex-shrink:0}
.voice-btn{width:44px;height:44px;border-radius:50%;border:none;display:flex;align-items:center;justify-content:center;font-size:20px;cursor:pointer;transition:all 0.2s}
.voice-btn.mute{background:#3a3a3c;color:#fff}
.voice-btn.mute.active{background:#ff3b30;color:#fff}
.voice-btn.leave{background:#ff3b30;color:#fff;transform:rotate(135deg)}
.voice-status{font-size:12px;color:#8e8e93;min-width:80px;text-align:center}

/* Participants bar */
.participants{display:flex;gap:8px;align-items:center;overflow-x:auto;padding:0 4px}
.participants::-webkit-scrollbar{display:none}
.part-avatar{width:28px;height:28px;border-radius:50%;object-fit:cover;border:2px solid transparent;transition:border-color 0.3s}
.part-avatar.speaking{border-color:#34c759}
.part-avatar.muted{opacity:0.5}

/* Input bar */
.input-bar{position:relative;z-index:10;background:rgba(13,13,13,0.95);backdrop-filter:blur(20px);border-top:1px solid rgba(255,255,255,0.06);padding:8px 12px;display:flex;align-items:center;gap:8px;flex-shrink:0}
.input-attach{width:36px;height:36px;border-radius:50%;border:none;background:#2c2c2e;color:#fff;font-size:18px;display:flex;align-items:center;justify-content:center;cursor:pointer;flex-shrink:0}
.input-field{flex:1;height:36px;border-radius:18px;border:none;background:#1c1c1e;color:#fff;padding:0 14px;font-size:14px;outline:none}
.input-field::placeholder{color:#8e8e93}
.input-send{width:36px;height:36px;border-radius:50%;border:none;background:#007aff;color:#fff;font-size:16px;display:flex;align-items:center;justify-content:center;cursor:pointer;flex-shrink:0;transition:transform 0.1s}
.input-send:active{transform:scale(0.9)}

/* Name prompt overlay */
.name-overlay{position:fixed;inset:0;z-index:100;background:rgba(0,0,0,0.85);display:flex;align-items:center;justify-content:center;backdrop-filter:blur(10px)}
.name-box{background:#1c1c1e;border-radius:16px;padding:24px;width:85%;max-width:320px;text-align:center}
.name-box h2{font-size:18px;margin-bottom:8px}
.name-box p{font-size:13px;color:#8e8e93;margin-bottom:16px}
.name-box input{width:100%;height:44px;border-radius:12px;border:1px solid #3a3a3c;background:#2c2c2e;color:#fff;padding:0 14px;font-size:15px;text-align:center;outline:none;margin-bottom:12px}
.name-box input:focus{border-color:#007aff}
.name-box button{width:100%;height:44px;border-radius:12px;border:none;background:#007aff;color:#fff;font-size:15px;font-weight:600;cursor:pointer}

/* Hidden */
.hidden{display:none!important}
</style>
</head>
<body>
<div class="bg"></div>

<!-- Name prompt -->
<div class="name-overlay" id="nameOverlay">
<div class="name-box">
<h2>Join Silent Hill Call</h2>
<p>Enter your name to join the voice session</p>
<input type="text" id="nameInput" placeholder="Your name" maxlength="20" autocomplete="off">
<button onclick="joinRoom()">Join</button>
</div>
</div>

<div class="app">
<!-- Header -->
<div class="header">
<button class="back-btn" onclick="leaveCall()">&#8249;</button>
<img class="group-icon" src="/ci.jpg" onerror="this.style.display='none'" alt="">
<div class="group-info">
<div class="group-name">Silent Hill</div>
<div class="group-meta" id="memberCount">0 in call</div>
</div>
<button class="menu-btn">&#8942;</button>
</div>

<!-- Messages -->
<div class="messages" id="messages"></div>

<!-- Voice bar -->
<div class="voice-bar" id="voiceBar">
<button class="voice-btn mute" id="muteBtn" onclick="toggleMute()">&#127908;</button>
<span class="voice-status" id="voiceStatus">Connecting...</span>
<button class="voice-btn leave" onclick="leaveCall()">&#9742;</button>
</div>

<!-- Participants -->
<div class="participants hidden" id="participants"></div>

<!-- Input bar -->
<div class="input-bar">
<button class="input-attach">+</button>
<input type="text" class="input-field" id="msgInput" placeholder="Write a message..." onkeypress="if(event.key==='Enter')sendMsg()">
<button class="input-send" onclick="sendMsg()">&#10148;</button>
</div>
</div>

<script>
// ═══════════════════════════════════════════
// CONFIG
// ═══════════════════════════════════════════
const ROOM = "__ROOM_ID__";
const TOKEN = "__TOKEN__";
const WS_URL = "__WS_URL__".replace(/^http/,'ws');

// ═══════════════════════════════════════════
// STATE
// ═══════════════════════════════════════════
let ws=null,localStream=null,myName="",myPeerId="",isMuted=false,isHost=false;
const peers={},audios={};
const peerMap = new Map(); // peer_id -> {name,avatar,is_host,muted}
const ICE={iceServers:[{urls:'stun:stun.l.google.com:19302'},{urls:'stun:stun1.l.google.com:19302'}]};

// ═══════════════════════════════════════════
// JOIN FLOW
// ═══════════════════════════════════════════
document.getElementById('nameInput').addEventListener('keypress',e=>{if(e.key==='Enter')joinRoom()});

async function joinRoom(){
    const name=document.getElementById('nameInput').value.trim();
    if(!name)return;
    myName=name;
    document.getElementById('nameOverlay').classList.add('hidden');

    // Get mic
    try{
        localStream=await navigator.mediaDevices.getUserMedia({audio:true});
        document.getElementById('voiceStatus').textContent='Connected';
    }catch(e){
        document.getElementById('voiceStatus').textContent='No mic';
    }

    connectWS();
}

// ═══════════════════════════════════════════
// WEBSOCKET
// ═══════════════════════════════════════════
function connectWS(){
    const proto=window.location.protocol==='https:'?'wss:':'ws:';
    const url=`${proto}//${window.location.host}/ws/${ROOM}?t=${TOKEN}`;
    ws=new WebSocket(url);

    ws.onopen=()=>{
        ws.send(JSON.stringify({type:'join',name:myName}));
    };

    ws.onmessage=async(e)=>{
        const m=JSON.parse(e.data);
        const handlers={
            history:()=>m.messages.forEach(renderMsg),
            chat:()=>renderMsg(m),
            peers:()=>m.peers.forEach(p=>addPeerInfo(p)),
            peer_joined:()=>{addPeerInfo(m.peer);renderSys(`${m.peer.name} joined`);},
            peer_left:()=>{removePeerUI(m.peer_id);renderSys(`${m.name||'Someone'} left`);},
            webrtc_offer:()=>handleOffer(m.from,m.sdp),
            webrtc_answer:()=>handleAnswer(m.from,m.sdp),
            webrtc_ice:()=>handleIce(m.from,m.candidate),
            voice_state:()=>updateVoiceState(m.peer_id,m.muted),
            mute_cmd:()=>{if(localStream)localStream.getAudioTracks().forEach(t=>t.enabled=false);},
            unmute_cmd:()=>{if(localStream)localStream.getAudioTracks().forEach(t=>t.enabled=true);},
        };
        if(handlers[m.type])handlers[m.type]();
    };

    ws.onclose=()=>cleanup();
    ws.onerror=()=>document.getElementById('voiceStatus').textContent='Error';
}

// ═══════════════════════════════════════════
// PEER INFO
// ═══════════════════════════════════════════
function addPeerInfo(p){
    peerMap.set(p.id,{name:p.name,avatar:p.avatar,is_host:p.is_host,muted:p.muted});
    updateParticipants();
}

function removePeerUI(peerId){
    if(peers[peerId]){peers[peerId].close();delete peers[peerId];}
    if(audios[peerId]){audios[peerId].srcObject=null;delete audios[peerId];}
    peerMap.delete(peerId);
    updateParticipants();
    updateMemberCount();
}

function updateVoiceState(peerId,muted){
    const p=peerMap.get(peerId);
    if(p){p.muted=muted;updateParticipants();}
}

function updateParticipants(){
    const el=document.getElementById('participants');
    if(peerMap.size===0){el.classList.add('hidden');return;}
    el.classList.remove('hidden');
    let html='';
    // Self
    html+=`<img class="part-avatar${isMuted?' muted':''}" src="https://ui-avatars.com/api/?name=${encodeURIComponent(myName)}&background=007aff&color=fff&size=64" title="${myName}(You)">`;
    peerMap.forEach((p,id)=>{
        const src=p.avatar||`https://ui-avatars.com/api/?name=${encodeURIComponent(p.name)}&background=2c2c2e&color=fff&size=64`;
        html+=`<img class="part-avatar${p.muted?' muted':''}" src="${src}" title="${p.name}" data-peer="${id}">`;
    });
    el.innerHTML=html;
    updateMemberCount();
}

function updateMemberCount(){
    const count=peerMap.size+1;
    document.getElementById('memberCount').textContent=count+' in call';
}

// ═══════════════════════════════════════════
// WEBRTC
// ═══════════════════════════════════════════
async function createOffer(peerId){
    const pc=new RTCPeerConnection(ICE);
    setupPC(pc,peerId);
    peers[peerId]=pc;
    if(localStream)localStream.getTracks().forEach(t=>pc.addTrack(t,localStream));
    const o=await pc.createOffer();
    await pc.setLocalDescription(o);
    ws.send(JSON.stringify({type:'webrtc_offer',to:peerId,sdp:o.sdp}));
}

async function handleOffer(from,sdp){
    const pc=new RTCPeerConnection(ICE);
    setupPC(pc,from);
    peers[from]=pc;
    if(localStream)localStream.getTracks().forEach(t=>pc.addTrack(t,localStream));
    await pc.setRemoteDescription(new RTCSessionDescription({type:'offer',sdp}));
    const a=await pc.createAnswer();
    await pc.setLocalDescription(a);
    ws.send(JSON.stringify({type:'webrtc_answer',to:from,sdp:a.sdp}));
}

async function handleAnswer(from,sdp){
    const pc=peers[from];
    if(pc)await pc.setRemoteDescription(new RTCSessionDescription({type:'answer',sdp}));
}

async function handleIce(from,candidate){
    const pc=peers[from];
    if(pc&&candidate)await pc.addIceCandidate(new RTCIceCandidate(candidate));
}

function setupPC(pc,peerId){
    pc.onicecandidate=e=>{if(e.candidate&&ws.readyState===1)ws.send(JSON.stringify({type:'webrtc_ice',to:peerId,candidate:e.candidate}));};
    pc.ontrack=e=>{
        let a=audios[peerId];
        if(!a){a=new Audio();a.autoplay=true;audios[peerId]=a;}
        a.srcObject=e.streams[0];
    };
    pc.onconnectionstatechange=()=>{if(pc.connectionState==='failed')removePeerUI(peerId);};
}

// ═══════════════════════════════════════════
// UI RENDERING
// ═══════════════════════════════════════════
function renderMsg(m){
    const container=document.getElementById('messages');

    if(m.kind==='system'){
        const div=document.createElement('div');
        div.className='msg-system';
        div.textContent=m.text;
        container.appendChild(div);
        scrollBottom();
        return;
    }

    const isSelf=m.self===true;
    const peerInfo=peerMap.get(m.peer_id)||{name:m.name,avatar:m.avatar,is_host:false,muted:false};
    const avatar=m.avatar||`https://ui-avatars.com/api/?name=${encodeURIComponent(m.name)}&background=${isSelf?'007aff':'2c2c2e'}&color=fff&size=128`;
    const badge=isSelf?(isHost?'host':'cohost'):(peerInfo.is_host?'host':'cohost');
    const badgeText=isSelf?(isHost?'Host':'Co-host'):(peerInfo.is_host?'Host':'Co-host');

    const row=document.createElement('div');
    row.className=`msg-row ${isSelf?'right':'left'}`;

    row.innerHTML=`<img class="avatar" src="${avatar}" alt="">
<div class="msg-content">
<div class="msg-header">
${isSelf?`<span class="msg-badge ${badge}">${badgeText}</span><span class="msg-name">${escapeHtml(m.name)}</span>`:`<span class="msg-name">${escapeHtml(m.name)}</span><span class="msg-badge ${badge}">${badgeText}</span>`}
</div>
<div class="msg-bubble">${escapeHtml(m.text)}</div>
</div>`;

    container.appendChild(row);
    scrollBottom();
}

function renderSys(text){
    const container=document.getElementById('messages');
    const div=document.createElement('div');
    div.className='msg-system';
    div.textContent=text;
    container.appendChild(div);
    scrollBottom();
}

function scrollBottom(){
    const el=document.getElementById('messages');
    el.scrollTop=el.scrollHeight;
}

function escapeHtml(t){
    const d=document.createElement('div');
    d.textContent=t;
    return d.innerHTML;
}

// ═══════════════════════════════════════════
// CHAT INPUT
// ═══════════════════════════════════════════
function sendMsg(){
    const input=document.getElementById('msgInput');
    const text=input.value.trim();
    if(!text||!ws||ws.readyState!==1)return;
    ws.send(JSON.stringify({type:'chat',text:text}));
    input.value='';

    // Optimistic render for self
    renderMsg({type:'chat',kind:'user',peer_id:'self',name:myName,avatar:'',text:text,time:new Date().toISOString(),self:true});
}

// ═══════════════════════════════════════════
// VOICE CONTROLS
// ═══════════════════════════════════════════
function toggleMute(){
    if(!localStream)return;
    isMuted=!isMuted;
    localStream.getAudioTracks().forEach(t=>t.enabled=!isMuted);
    const btn=document.getElementById('muteBtn');
    if(isMuted){
        btn.classList.add('active');
        btn.innerHTML='&#128263;'; // muted mic
        document.getElementById('voiceStatus').textContent='Muted';
    }else{
        btn.classList.remove('active');
        btn.innerHTML='&#127908;'; // unmuted mic
        document.getElementById('voiceStatus').textContent='Connected';
    }
    if(ws&&ws.readyState===1){
        ws.send(JSON.stringify({type:'voice_state',muted:isMuted}));
    }
    updateParticipants();
}

function leaveCall(){
    if(ws&&ws.readyState===1)ws.close();
    cleanup();
    window.close();
    // Fallback if window.close() blocked
    document.body.innerHTML='<div style="display:flex;align-items:center;justify-content:center;height:100vh;color:#fff;background:#000;font-family:sans-serif;"><div style="text-align:center;"><h2>Left the call</h2><p style="color:#8e8e93;margin-top:8px;">You can close this tab</p></div></div>';
}

// ═══════════════════════════════════════════
// CLEANUP
// ═══════════════════════════════════════════
function cleanup(){
    Object.keys(peers).forEach(id=>{if(peers[id])peers[id].close();});
    if(localStream){localStream.getTracks().forEach(t=>t.stop());localStream=null;}
}

// Auto-focus name input
document.getElementById('nameInput').focus();
</script>
</body>
</html>"""


# ═══════════════════════════════════════════════════════════
# MAIN — Starts Kyodo Bot + Web Server
# ═══════════════════════════════════════════════════════════

async def kyodo_task():
    """Run Kyodo bot in background."""
    if not KYODO_AVAILABLE:
        print("[Kyodo] Library not available, skipping bot task")
        while True:
            await asyncio.sleep(3600)
    while True:
        try:
            await setup_kyodo()
        except Exception as e:
            print(f"[Kyodo] crashed: {e}")
        await asyncio.sleep(10)


async def web_task():
    """Run FastAPI web server."""
    cfg = Config(app=app, host="0.0.0.0", port=PORT, log_level="warning")
    srv = Server(cfg)
    await srv.serve()


async def main():
    print("=" * 50)
    print("Silent Hill Voice Call Bot")
    print(f"Web URL: {WEB_APP_URL}")
    print(f"Port: {PORT}")
    print(f"Kyodo: {'YES' if KYODO_AVAILABLE else 'NO'}")
    print("=" * 50)

    await asyncio.gather(
        web_task(),
        kyodo_task(),
    )


if __name__ == "__main__":
    asyncio.run(main())
