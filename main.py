"""
Silent Hill Voice Call Bot - Kyodo + WebRTC + Chat UI
Updated for new Kyodo AsyncClient API | Render-ready
Run: python main.py
"""

import asyncio
import json
import os
import time
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

import aiohttp
import httpx
from fastapi import FastAPI, Query, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from uvicorn import Config, Server

# ═══════════════════════════════════════════════════════════
# KYODO IMPORT (kyodo 1.7.2 — exact match to working bot)
# ═══════════════════════════════════════════════════════════
try:
    from kyodo import ChatMessage, EventType, KyodoObjectTypes, AsyncClient as Client
    KYODO_AVAILABLE = True
except ImportError as _e:
    KYODO_AVAILABLE = False
    print("[WARN] kyodo import failed: " + str(_e))

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
# KYODO BOT - /call command creates voice room
# ═══════════════════════════════════════════════════════════

async def run_kyodo_bot():
    """Run Kyodo client and listen for commands."""
    global kyodo_client
    if not KYODO_AVAILABLE:
        print("[Kyodo] Library not available, bot not started")
        while True:
            await asyncio.sleep(3600)
    
    kyodo_client = Client(deviceId=DEVICE_ID)
    
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
            
            if content.lower() in ("/call", "!call", "/call"):
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
        except Exception as e:
            print(f"[Kyodo] error: {e}")
    
    backoff = 5
    while True:
        t0 = time.time()
        try:
            await kyodo_client.login(EMAIL, PASSWORD)
            print("[Kyodo] Bot logged in!")
            await kyodo_client.socket_wait()
            print("[Kyodo] Session ended, reconnecting...")
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception as e:
            print(f"[Kyodo] crash: {e}")
        backoff = 5 if time.time() - t0 > 300 else min(backoff * 2, 120)
        print(f"[Kyodo] reconnecting in {backoff}s...")
        await asyncio.sleep(backoff)


# ═══════════════════════════════════════════════════════════
# CHAT PERSISTENCE (JSON)
# ═══════════════════════════════════════════════════════════

def json_write(path: str, data: Any):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


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
    tok = tokens.get(t)
    if not tok or tok.get("room_id") != room_id:
        return HTMLResponse("<h1>Link expired or invalid</h1>", status_code=403)
    if room_id not in rooms:
        return HTMLResponse("<h1>Room closed</h1>", status_code=404)
    html = CALL_HTML.replace("__ROOM_ID__", room_id).replace("__TOKEN__", t)
    return HTMLResponse(html)


@app.websocket("/ws/{room_id}")
async def ws_endpoint(websocket: WebSocket, room_id: str, t: str = Query(...)):
    tok = tokens.get(t)
    if not tok or tok.get("room_id") != room_id or room_id not in rooms:
        await websocket.close(code=4001)
        return
    
    await websocket.accept()
    peer_id = str(uuid.uuid4())[:8]
    room = rooms[room_id]
    
    # Wait for join message
    display_name = "Unknown"
    try:
        init_msg = await asyncio.wait_for(websocket.receive_json(), timeout=10)
        if init_msg.get("type") == "join":
            display_name = init_msg.get("name", "Unknown")[:30]
    except asyncio.TimeoutError:
        await websocket.close(code=4002)
        return
    
    is_host = tok.get("creator", False) and len(room["peers"]) == 0
    room["peers"][peer_id] = {"ws": websocket, "name": display_name, "muted": False, "is_host": is_host}
    existing = [pid for pid in room["peers"].keys() if pid != peer_id]
    
    # Send chat history
    await websocket.send_json({"type": "history", "messages": get_chat_history(room_id)})
    
    # Tell new peer about existing peers (they create offers)
    await websocket.send_json({"type": "peers", "peers": [
        {"id": pid, "name": room["peers"][pid]["name"], "is_host": room["peers"][pid]["is_host"], "muted": room["peers"][pid]["muted"]}
        for pid in existing
    ]})
    
    # Notify existing peers
    join_msg = {"type": "peer_joined", "peer": {"id": peer_id, "name": display_name, "is_host": is_host, "muted": False}}
    for pid in existing:
        try:
            await room["peers"][pid]["ws"].send_json(join_msg)
        except Exception:
            pass
    
    # System message
    sys_msg = {"type": "chat", "kind": "system", "text": f"{display_name} joined the call", "time": datetime.now().isoformat()}
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
            
            if mtype == "chat":
                text = msg.get("text", "").strip()[:500]
                if not text:
                    continue
                chat_msg = {
                    "type": "chat", "kind": "user", "peer_id": peer_id,
                    "name": display_name, "text": text,
                    "time": datetime.now().isoformat(),
                }
                append_chat_message(room_id, chat_msg)
                for pid, pdata in room["peers"].items():
                    if pid != peer_id:
                        try:
                            await pdata["ws"].send_json(chat_msg)
                        except Exception:
                            pass
                await websocket.send_json({**chat_msg, "self": True})
            
            elif mtype in ("webrtc_offer", "webrtc_answer", "webrtc_ice"):
                target = msg.get("to")
                msg["from"] = peer_id
                if target and target in room["peers"]:
                    try:
                        await room["peers"][target]["ws"].send_json(msg)
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
        if peer_id in room["peers"]:
            del room["peers"][peer_id]
        
        left_msg = {"type": "peer_left", "peer_id": peer_id, "name": display_name}
        sys_msg = {"type": "chat", "kind": "system", "text": f"{display_name} left the call", "time": datetime.now().isoformat()}
        append_chat_message(room_id, sys_msg)
        
        for pid, pdata in list(room["peers"].items()):
            try:
                await pdata["ws"].send_json(left_msg)
                await pdata["ws"].send_json(sys_msg)
            except Exception:
                pass
        
        if not room["peers"]:
            delete_chat_history(room_id)
            expired = [tk for tk, d in tokens.items() if d.get("room_id") == room_id]
            for tk in expired:
                del tokens[tk]
            del rooms[room_id]
            print(f"[Room {room_id}] closed - all users left")


# ═══════════════════════════════════════════════════════════
# HTML TEMPLATE - Silent Hill Chat + Voice UI
# ═══════════════════════════════════════════════════════════

CALL_HTML = """<!DOCTYPE html>
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
.messages{flex:1;overflow-y:auto;padding:12px;display:flex;flex-direction:column;gap:4px;position:relative;z-index:5;scroll-behavior:smooth}
.messages::-webkit-scrollbar{width:0}
.msg-system{text-align:center;color:#8e8e93;font-size:12px;padding:8px 0;opacity:0.8}
.msg-row{display:flex;gap:8px;max-width:85%;animation:msgIn 0.25s ease-out}
.msg-row.left{align-self:flex-start}
.msg-row.right{align-self:flex-end;flex-direction:row-reverse}
@keyframes msgIn{from{opacity:0;transform:translateY(10px)}to{opacity:1;transform:translateY(0)}}
.avatar{width:36px;height:36px;border-radius:50%;object-fit:cover;border:2px solid rgba(255,255,255,0.1);flex-shrink:0;align-self:flex-start;background:#2c2c2e}
.msg-content{display:flex;flex-direction:column;gap:2px;min-width:0}
.msg-header{display:flex;align-items:center;gap:6px;flex-wrap:wrap}
.msg-row.right .msg-header{flex-direction:row-reverse}
.msg-name{font-size:12px;font-weight:600;color:#fff;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:120px}
.msg-badge{font-size:9px;font-weight:700;padding:2px 8px;border-radius:10px;text-transform:uppercase;letter-spacing:0.5px}
.msg-badge.host{background:#007aff;color:#fff}
.msg-badge.cohost{background:#3a3a3c;color:#fff}
.msg-bubble{padding:8px 12px;border-radius:16px;font-size:14px;line-height:1.4;word-break:break-word;max-width:260px;width:fit-content}
.msg-row.left .msg-bubble{background:#2c2c2e;color:#fff;border-bottom-left-radius:4px}
.msg-row.right .msg-bubble{background:#007aff;color:#fff;border-bottom-right-radius:4px;margin-left:auto}
.voice-bar{position:relative;z-index:10;background:rgba(28,28,30,0.95);backdrop-filter:blur(20px);border-top:1px solid rgba(255,255,255,0.06);padding:8px 16px;display:flex;align-items:center;justify-content:center;gap:20px;flex-shrink:0}
.voice-btn{width:44px;height:44px;border-radius:50%;border:none;display:flex;align-items:center;justify-content:center;font-size:20px;cursor:pointer;transition:all 0.2s}
.voice-btn.mute{background:#3a3a3c;color:#fff}
.voice-btn.mute.active{background:#ff3b30;color:#fff}
.voice-btn.leave{background:#ff3b30;color:#fff;transform:rotate(135deg)}
.voice-status{font-size:12px;color:#8e8e93;min-width:80px;text-align:center}
.input-bar{position:relative;z-index:10;background:rgba(13,13,13,0.95);backdrop-filter:blur(20px);border-top:1px solid rgba(255,255,255,0.06);padding:8px 12px;display:flex;align-items:center;gap:8px;flex-shrink:0}
.input-attach{width:36px;height:36px;border-radius:50%;border:none;background:#2c2c2e;color:#fff;font-size:18px;display:flex;align-items:center;justify-content:center;cursor:pointer;flex-shrink:0}
.input-field{flex:1;height:36px;border-radius:18px;border:none;background:#1c1c1e;color:#fff;padding:0 14px;font-size:14px;outline:none}
.input-field::placeholder{color:#8e8e93}
.input-send{width:36px;height:36px;border-radius:50%;border:none;background:#007aff;color:#fff;font-size:16px;display:flex;align-items:center;justify-content:center;cursor:pointer;flex-shrink:0}
.name-overlay{position:fixed;inset:0;z-index:100;background:rgba(0,0,0,0.85);display:flex;align-items:center;justify-content:center;backdrop-filter:blur(10px)}
.name-box{background:#1c1c1e;border-radius:16px;padding:24px;width:85%;max-width:320px;text-align:center}
.name-box h2{font-size:18px;margin-bottom:8px}
.name-box p{font-size:13px;color:#8e8e93;margin-bottom:16px}
.name-box input{width:100%;height:44px;border-radius:12px;border:1px solid #3a3a3c;background:#2c2c2e;color:#fff;padding:0 14px;font-size:15px;text-align:center;outline:none;margin-bottom:12px}
.name-box input:focus{border-color:#007aff}
.name-box button{width:100%;height:44px;border-radius:12px;border:none;background:#007aff;color:#fff;font-size:15px;font-weight:600;cursor:pointer}
.hidden{display:none!important}
</style>
</head>
<body>
<div class="bg"></div>

<div class="name-overlay" id="nameOverlay">
<div class="name-box">
<h2>Join Silent Hill Call</h2>
<p>Enter your name to join the voice session</p>
<input type="text" id="nameInput" placeholder="Your name" maxlength="20" autocomplete="off">
<button onclick="joinRoom()">Join</button>
</div>
</div>

<div class="app">
<div class="header">
<button class="back-btn" onclick="leaveCall()">&#8249;</button>
<img class="group-icon" src="/ci.jpg" onerror="this.style.display='none'" alt="">
<div class="group-info">
<div class="group-name">Silent Hill</div>
<div class="group-meta" id="memberCount">0 in call</div>
</div>
<button class="menu-btn">&#8942;</button>
</div>

<div class="messages" id="messages"></div>

<div class="voice-bar" id="voiceBar">
<button class="voice-btn mute" id="muteBtn" onclick="toggleMute()">&#127908;</button>
<span class="voice-status" id="voiceStatus">Connecting...</span>
<button class="voice-btn leave" onclick="leaveCall()">&#9742;</button>
</div>

<div class="input-bar">
<button class="input-attach">+</button>
<input type="text" class="input-field" id="msgInput" placeholder="Write a message..." onkeypress="if(event.key==='Enter')sendMsg()">
<button class="input-send" onclick="sendMsg()">&#10148;</button>
</div>
</div>

<script>
const ROOM="__ROOM_ID__",TOKEN="__TOKEN__";
let ws=null,localStream=null,myName="",myPeerId="",isMuted=false;
const peers={},audios={};
const peerMap=new Map();
const ICE={iceServers:[{urls:'stun:stun.l.google.com:19302'},{urls:'stun:stun1.l.google.com:19302'}]};

document.getElementById('nameInput').addEventListener('keypress',e=>{if(e.key==='Enter')joinRoom()});

async function joinRoom(){
const name=document.getElementById('nameInput').value.trim();
if(!name)return;
myName=name;
document.getElementById('nameOverlay').classList.add('hidden');
try{
localStream=await navigator.mediaDevices.getUserMedia({audio:true});
document.getElementById('voiceStatus').textContent='Connected';
}catch(e){
document.getElementById('voiceStatus').textContent='No mic';
}
connectWS();
}

function connectWS(){
const proto=window.location.protocol==='https:'?'wss:':'ws:';
const url=`${proto}//${window.location.host}/ws/${ROOM}?t=${TOKEN}`;
ws=new WebSocket(url);

ws.onopen=()=>{ws.send(JSON.stringify({type:'join',name:myName}));};

ws.onmessage=async(e)=>{
const m=JSON.parse(e.data);
if(m.type==='history')m.messages.forEach(renderMsg);
else if(m.type==='chat')renderMsg(m);
else if(m.type==='peers')m.peers.forEach(p=>addPeerInfo(p));
else if(m.type==='peer_joined'){addPeerInfo(m.peer);renderSys(m.peer.name+' joined');}
else if(m.type==='peer_left'){removePeerUI(m.peer_id);renderSys((m.name||'Someone')+' left');}
else if(m.type==='webrtc_offer')handleOffer(m.from,m.sdp);
else if(m.type==='webrtc_answer')handleAnswer(m.from,m.sdp);
else if(m.type==='webrtc_ice')handleIce(m.from,m.candidate);
else if(m.type==='mute_cmd'){if(localStream)localStream.getAudioTracks().forEach(t=>t.enabled=false);}
else if(m.type==='unmute_cmd'){if(localStream)localStream.getAudioTracks().forEach(t=>t.enabled=true);}
};

ws.onclose=cleanup;
ws.onerror=()=>document.getElementById('voiceStatus').textContent='Error';
}

function addPeerInfo(p){peerMap.set(p.id,{name:p.name,is_host:p.is_host,muted:p.muted});updateCount();}
function removePeerUI(peerId){
if(peers[peerId]){peers[peerId].close();delete peers[peerId];}
if(audios[peerId]){audios[peerId].srcObject=null;delete audios[peerId];}
peerMap.delete(peerId);
updateCount();
}
function updateCount(){document.getElementById('memberCount').textContent=(peerMap.size+1)+' in call';}

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

function renderMsg(m){
const c=document.getElementById('messages');
if(m.kind==='system'){
const d=document.createElement('div');
d.className='msg-system';
d.textContent=m.text;
c.appendChild(d);
scrollBottom();
return;
}
const isSelf=m.self===true;
const p=peerMap.get(m.peer_id)||{name:m.name||'Unknown',is_host:false};
const badge=isSelf?'Co-host':(p.is_host?'Host':'Co-host');
const row=document.createElement('div');
row.className='msg-row '+(isSelf?'right':'left');
row.innerHTML=`<div class="avatar" style="display:flex;align-items:center;justify-content:center;font-size:14px;font-weight:600;color:#8e8e93;">${(isSelf?myName:p.name).charAt(0).toUpperCase()}</div><div class="msg-content"><div class="msg-header">${isSelf?`<span class="msg-badge ${isHost?'host':'cohost'}">${badge}</span><span class="msg-name">${esc(isSelf?myName:p.name)}</span>`:`<span class="msg-name">${esc(p.name)}</span><span class="msg-badge ${p.is_host?'host':'cohost'}">${badge}</span>`}</div><div class="msg-bubble">${esc(m.text)}</div></div>`;
c.appendChild(row);
scrollBottom();
}

function renderSys(text){
const c=document.getElementById('messages');
const d=document.createElement('div');
d.className='msg-system';
d.textContent=text;
c.appendChild(d);
scrollBottom();
}

function scrollBottom(){const el=document.getElementById('messages');el.scrollTop=el.scrollHeight;}
function esc(t){const d=document.createElement('div');d.textContent=t;return d.innerHTML;}

function sendMsg(){
const input=document.getElementById('msgInput');
const text=input.value.trim();
if(!text||!ws||ws.readyState!==1)return;
ws.send(JSON.stringify({type:'chat',text:text}));
input.value='';
renderMsg({type:'chat',kind:'user',peer_id:'self',name:myName,text:text,time:new Date().toISOString(),self:true});
}

function toggleMute(){
if(!localStream)return;
isMuted=!isMuted;
localStream.getAudioTracks().forEach(t=>t.enabled=!isMuted);
const btn=document.getElementById('muteBtn');
if(isMuted){btn.classList.add('active');btn.innerHTML='&#128263;';document.getElementById('voiceStatus').textContent='Muted';}
else{btn.classList.remove('active');btn.innerHTML='&#127908;';document.getElementById('voiceStatus').textContent='Connected';}
if(ws&&ws.readyState===1)ws.send(JSON.stringify({type:'mute_me'}));
}

function leaveCall(){
if(ws&&ws.readyState===1)ws.close();
cleanup();
window.close();
document.body.innerHTML='<div style="display:flex;align-items:center;justify-content:center;height:100vh;color:#fff;background:#000;font-family:sans-serif;"><div style="text-align:center;"><h2>Left the call</h2></div></div>';
}

function cleanup(){
Object.keys(peers).forEach(id=>{if(peers[id])peers[id].close();});
if(localStream){localStream.getTracks().forEach(t=>t.stop());localStream=null;}
}

document.getElementById('nameInput').focus();
</script>
</body>
</html>"""


# ═══════════════════════════════════════════════════════════
# RENDER KEEPALIVE (prevents free tier from sleeping)
# ═══════════════════════════════════════════════════════════

async def keepalive():
    """Self-ping every 10 minutes to keep Render alive."""
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
    print(f"Kyodo: {'YES' if KYODO_AVAILABLE else 'NO'}")
    print("=" * 50)
    await asyncio.gather(web_task(), run_kyodo_bot(), keepalive())


if __name__ == "__main__":
    asyncio.run(main())
