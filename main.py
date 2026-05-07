"""
Silent Hill Voice Call Bot — Kyodo + WebRTC + Chat UI
GLARE FIX v2: Server assigns peer IDs — both sides compare same IDs.
"""

import asyncio, json, os, time, uuid, traceback
from datetime import datetime
from typing import Any, Dict

import aiohttp
from fastapi import FastAPI, Query, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, HTMLResponse
from uvicorn import Config, Server

try:
    from kyodo import ChatMessage, EventType, KyodoObjectTypes, AsyncClient as Client
    KYODO_OK = True
except ImportError as e:
    KYODO_OK = False

EMAIL = os.getenv("BOT_EMAIL", "hadidaoud.ha@gmail.com")
PASSWORD = os.getenv("BOT_PASSWORD", "yulia123")
DEVICE_ID = os.getenv("BOT_DEVICE_ID", "870d649515ce700797d6a56965689f3aaa7d5e82dfdce994b239e00e37238184")
CHAT_ID = os.getenv("BOT_CHAT_ID", "cmh2gy89r01pvt33exijh1wr3")
CIRCLE_ID = os.getenv("BOT_CIRCLE_ID", "cm9bylrbn00hmux6t43mczt2o")
WEB_APP_URL = os.environ.get("WEB_APP_URL", "http://localhost:8000")
PORT = int(os.environ.get("PORT", "8000"))

tokens: Dict[str, dict] = {}
rooms: Dict[str, dict] = {}
kyodo_client = None

def json_write(p: str, d: Any):
    with open(p, "w", encoding="utf-8") as f:
        json.dump(d, f, ensure_ascii=False, indent=2)

def json_read(p: str, default=None):
    if default is None: default = []
    try:
        if not os.path.exists(p): return default
        with open(p, "r", encoding="utf-8") as f: return json.load(f)
    except Exception: return default

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
                    if c.lower() in ("/call", "!call", "/جلسة"):
                        rid = str(uuid.uuid4())[:8]
                        rooms[rid] = {"peers": {}, "chat_file": f"{rid}_chat.json",
                                      "created": datetime.now().isoformat(),
                                      "creator_uid": m.author.userId, "creator_name": m.author.nickname}
                        json_write(f"{rid}_chat.json", [])
                        tok = str(uuid.uuid4())
                        tokens[tok] = {"room_id": rid, "creator": True}
                        link = f"{WEB_APP_URL}/call/{rid}?t={tok}"
                        await kyodo_client.send_message(m.chatId,
                            f"Silent Hill Voice Session\n{link}\nTap to join the call.", m.circleId)
                except Exception as e:
                    print(f"[Kyodo] err: {e}")
            await kyodo_client.login(EMAIL, PASSWORD)
            print("[Kyodo] Logged in!")
            await kyodo_client.socket_wait()
        except (KeyboardInterrupt, SystemExit): raise
        except Exception as e:
            print(f"[Kyodo] crash: {e}")
        backoff = 5 if time.time()-t0 > 300 else min(backoff*2, 120)
        await asyncio.sleep(backoff)

app = FastAPI()

@app.get("/")
async def root(): return {"ok": True}

@app.get("/bg.jpg")
async def bg():
    return FileResponse("bg.jpg") if os.path.exists("bg.jpg") else HTMLResponse("", 404)

@app.get("/ci.jpg")
async def ci():
    return FileResponse("ci.jpg") if os.path.exists("ci.jpg") else HTMLResponse("", 404)

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
        await ws.close(code=4001); return
    await ws.accept()
    peer_id = str(uuid.uuid4())[:8]
    room = rooms[room_id]
    name, avatar = "Unknown", ""
    try:
        init = await asyncio.wait_for(ws.receive_json(), timeout=10)
        if init.get("type") == "join":
            name = init.get("name", "Unknown")[:30]
            avatar = init.get("avatar", "")[:50000]
    except asyncio.TimeoutError:
        await ws.close(code=4002); return

    is_host = tok.get("creator", False) and len(room["peers"]) == 0
    room["peers"][peer_id] = {"ws": ws, "name": name, "avatar": avatar,
                              "muted": False, "is_host": is_host}
    existing = [p for p in room["peers"] if p != peer_id]
    print(f"[WS] {peer_id} ({name}) joined host={is_host}")

    # Send peer their own ID first — critical for glare-free signaling
    await ws.send_json({"type": "your_id", "id": peer_id})

    await ws.send_json({"type": "history", "messages": json_read(room["chat_file"])[-100:]})
    peer_list = [{"id": p, "name": room["peers"][p]["name"], "avatar": room["peers"][p]["avatar"],
                  "is_host": room["peers"][p]["is_host"], "muted": room["peers"][p]["muted"]}
                 for p in existing]
    await ws.send_json({"type": "peers", "peers": peer_list})

    join_msg = {"type": "peer_joined",
                "peer": {"id": peer_id, "name": name, "avatar": avatar,
                         "is_host": is_host, "muted": False}}
    for p in existing:
        try: await room["peers"][p]["ws"].send_json(join_msg)
        except: pass

    sys_msg = {"type": "chat", "kind": "system", "text": f"{name} joined the call",
               "time": datetime.now().isoformat()}
    _append_msg(room_id, sys_msg)
    for p in existing:
        try: await room["peers"][p]["ws"].send_json(sys_msg)
        except: pass

    try:
        while True:
            msg = await ws.receive_json()
            mt = msg.get("type")
            if mt == "chat":
                text = msg.get("text", "").strip()[:500]
                if not text: continue
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
                    except: pass
            elif mt in ("webrtc_offer", "webrtc_answer", "webrtc_ice"):
                target = msg.get("to")
                msg["from"] = peer_id
                if target and target in room["peers"]:
                    try: await room["peers"][target]["ws"].send_json(msg)
                    except: pass
            elif mt in ("mute_me", "unmute_me"):
                room["peers"][peer_id]["muted"] = (mt == "mute_me")
                cmd = "mute_cmd" if mt == "mute_me" else "unmute_cmd"
                await ws.send_json({"type": cmd})
                st = {"type": "voice_state", "peer_id": peer_id,
                      "muted": room["peers"][peer_id]["muted"]}
                for p, pd in room["peers"].items():
                    if p != peer_id:
                        try: await pd["ws"].send_json(st)
                        except: pass
    except WebSocketDisconnect:
        pass
    finally:
        if peer_id in room["peers"]: del room["peers"][peer_id]
        lm = {"type": "peer_left", "peer_id": peer_id, "name": name}
        sm = {"type": "chat", "kind": "system", "text": f"{name} left the call",
              "time": datetime.now().isoformat()}
        for p, pd in list(room["peers"].items()):
            try:
                await pd["ws"].send_json(lm)
                await pd["ws"].send_json(sm)
            except: pass
        if not room["peers"]:
            for f in [f"{room_id}_chat.json"]:
                if os.path.exists(f): os.remove(f)
            for tk in [k for k, v in tokens.items() if v.get("room_id") == room_id]: del tokens[tk]
            del rooms[room_id]

def _append_msg(rid, msg):
    p = f"{rid}_chat.json"
    m = json_read(p, [])
    m.append(msg)
    json_write(p, m)


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
.voice-bar{position:relative;z-index:10;background:rgba(28,28,30,0.95);border-top:1px solid rgba(255,255,255,0.06);padding:10px 16px;display:flex;align-items:center;justify-content:center;gap:20px;flex-shrink:0}
.voice-btn{width:48px;height:48px;border-radius:50%;border:none;display:flex;align-items:center;justify-content:center;font-size:22px;cursor:pointer;transition:.2s}
.voice-btn.mute{background:#3a3a3c;color:#fff}
.voice-btn.mute.muted{background:#ff3b30;color:#fff}
.voice-btn.leave{background:#ff3b30;color:#fff;font-size:20px}
.voice-status{font-size:12px;color:#8e8e93;min-width:60px;text-align:center}
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
.debug{position:fixed;top:0;left:0;right:0;z-index:200;background:rgba(0,0,0,.92);color:#0f0;font:11px monospace;padding:4px;max-height:120px;overflow-y:auto;display:none;white-space:pre-wrap}
.debug.show{display:block}
.peer-status{display:flex;gap:6px;align-items:center;overflow-x:auto;padding:4px 12px}
.peer-status::-webkit-scrollbar{display:none}
.p-s{flex-shrink:0;display:flex;align-items:center;gap:4px;background:rgba(255,255,255,0.08);padding:4px 10px;border-radius:12px;font-size:11px}
.p-s .dot{width:8px;height:8px;border-radius:50%;background:#8e8e93}
.p-s .dot.conn{background:#34c759}
.p-s .dot.fail{background:#ff3b30}
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
<button class="voice-btn mute" id="muteBtn" onclick="toggleMute()">&#127908;</button>
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
const ROOM="__ROOM_ID__",TOKEN="__TOKEN__";
let MY_ID=""; // Set by server via your_id message — SAME on both sides
let ws=null,localStream=null,myName="",myAvatar="",isMuted=false,isHost=false;
const peers={},audios={};
const peerMap=new Map();
const iceBuffer={}; // buffer trickle ICE candidates arriving before setRemoteDescription

// STUN + TURN — expanded for mobile network compatibility
const ICE_SERVERS=[
  {urls:'stun:stun.l.google.com:19302'},
  {urls:'stun:stun1.l.google.com:19302'},
  {urls:'stun:stun2.l.google.com:19302'},
  {urls:'stun:stun3.l.google.com:19302'},
  {urls:'stun:stun4.l.google.com:19302'},
  {urls:'turn:openrelay.metered.ca:80',username:'openrelayproject',credential:'openrelayproject'},
  {urls:'turn:openrelay.metered.ca:443?transport=tcp',username:'openrelayproject',credential:'openrelayproject'},
  {urls:'turn:turn.anyfirewall.com:443?transport=tcp',username:'webrtc',credential:'webrtc'},
];
function getPCConfig(){return{iceServers:ICE_SERVERS,bundlePolicy:'max-bundle',rtcpMuxPolicy:'require',iceCandidatePoolSize:10};}

// ── DEBUG ──
function log(m){
  const t=new Date().toLocaleTimeString().split(' ')[0];
  const line='['+t+'] '+m;
  console.log(line);
  const d=document.getElementById('dbg');
  if(d)d.textContent+=line+'\n';
}

// ── AVATAR ──
function pickAv(e){
  const f=e.target.files[0]; if(!f)return;
  const r=new FileReader();
  r.onload=ev=>{myAvatar=ev.target.result; document.getElementById('avPrev').innerHTML='<img src="'+myAvatar+'">'; log("avatar OK")};
  r.readAsDataURL(f);
}
document.getElementById('nameIn').addEventListener('input',e=>{myName=e.target.value; if(!myAvatar)document.getElementById('avInit').textContent=myName?myName[0].toUpperCase():'?';});

// ── JOIN ──
async function doJoin(){
  const n=document.getElementById('nameIn').value.trim();
  if(!n){alert("Enter name");return;}
  myName=n;
  document.getElementById('joinBtn').disabled=true;
  document.getElementById('joinBtn').textContent="...";
  try{
    localStream=await navigator.mediaDevices.getUserMedia({audio:true});
    document.getElementById('vstat').textContent='Connected';
    log("mic OK");
  }catch(e){log("mic err: "+e.message); document.getElementById('vstat').textContent='No mic';}
  document.getElementById('joinOvl').classList.add('hidden');
  document.getElementById('app').classList.remove('hidden');
  connectWS();
}

// ── WEBSOCKET ──
function connectWS(){
  const p=location.protocol==='https:'?'wss:':'ws:';
  const url=p+'//'+location.host+'/ws/'+ROOM+'?t='+TOKEN;
  log("WS connect");
  ws=new WebSocket(url);

  ws.onopen=()=>{
    log("WS open");
    ws.send(JSON.stringify({type:'join',name:myName,avatar:myAvatar}));
  };

  ws.onmessage=async(ev)=>{
    let m; try{m=JSON.parse(ev.data);}catch(e){return;}
    switch(m.type){
      case 'your_id':
        // CRITICAL: Server tells us our ID. Both peers compare SAME IDs.
        MY_ID = m.id;
        log("myId="+MY_ID);
        break;

      case 'history': m.messages.forEach(renderMsg); break;
      case 'chat': renderMsg(m); break;

      case 'peers':
        log("peers:"+m.peers.length);
        m.peers.forEach(p=>{
          addPeer(p);
          // GLARE FIX: Compare SAME server-assigned IDs
          if(MY_ID > p.id){
            log("LARGER ("+MY_ID+">"+p.id+") offer->"+p.id);
            createOffer(p.id);
          }else{
            log("SMALLER ("+MY_ID+"<"+p.id+") wait");
          }
        });
        break;

      case 'peer_joined':
        addPeer(m.peer);
        renderSys(m.peer.name+" joined");
        if(MY_ID && MY_ID > m.peer.id){
          log("late LARGER offer->"+m.peer.id);
          createOffer(m.peer.id);
        }else if(MY_ID){
          log("late SMALLER wait");
        }
        break;

      case 'peer_left': destroyPeer(m.peer_id); peerMap.delete(m.peer_id); renderSys(m.name+' left'); updCount(); updPeers(); break;
      case 'webrtc_offer': await handleOffer(m.from,m.sdp); break;
      case 'webrtc_answer': await handleAnswer(m.from,m.sdp); break;
      case 'webrtc_ice': await handleIce(m.from,m.candidate); break;
      case 'mute_cmd': if(localStream)localStream.getAudioTracks().forEach(t=>t.enabled=false); break;
      case 'unmute_cmd': if(localStream)localStream.getAudioTracks().forEach(t=>t.enabled=true); break;
      case 'voice_state': {const p=peerMap.get(m.peer_id); if(p)p.muted=m.muted;} break;
    }
  };
  ws.onclose=e=>{log("WS close "+e.code); cleanupRTC();};
  ws.onerror=e=>{log("WS err"); document.getElementById('vstat').textContent='Error';};
}

function addPeer(p){peerMap.set(p.id,{name:p.name,avatar:p.avatar||'',is_host:p.is_host,muted:p.muted,connState:'new',retries:0}); if(p.is_host)log("peer "+p.id+" HOST"); updCount(); updPeers();}
function updCount(){document.getElementById('mcount').textContent=(peerMap.size+1)+' in call';}

// ── PEER STATUS BAR ──
function updPeers(){
  const el=document.getElementById('pstat');
  let h='';
  const selfDot=isMuted?'fail':'conn';
  h+='<div class="p-s"><div class="dot '+selfDot+'"></div>'+esc(myName)+'(You)</div>';
  peerMap.forEach((p,id)=>{
    const dot=p.connState==='connected'?'conn':(p.connState==='failed'?'fail':'');
    h+='<div class="p-s"><div class="dot '+dot+'"></div>'+esc(p.name)+'</div>';
  });
  el.innerHTML=h;
}

// ═══════════════════════════════════════════════════════
// WEBRTC — GLARE-FREE using server-assigned IDs
// ═══════════════════════════════════════════════════════

// Wait for ICE gathering to complete (with timeout) — 8s for mobile networks
function iceGatherComplete(pc,timeout=8000){
  return new Promise(resolve=>{
    if(pc.iceGatheringState==='complete'){resolve();return;}
    const t=setTimeout(()=>{pc.removeEventListener('icegatheringstatechange',check);resolve();},timeout);
    const check=()=>{
      if(pc.iceGatheringState==='complete'){
        clearTimeout(t);
        pc.removeEventListener('icegatheringstatechange',check);
        resolve();
      }
    };
    pc.addEventListener('icegatheringstatechange',check);
  });
}

function destroyPeer(pid){
  if(peers[pid]){try{peers[pid].close();}catch(e){} delete peers[pid];}
  if(audios[pid]){try{audios[pid].pause(); audios[pid].srcObject=null; audios[pid].remove();}catch(e){} delete audios[pid];}
}

async function createOffer(pid,iceRestart=false){
  log("offer->"+pid+(iceRestart?" iceRestart":""));
  destroyPeer(pid);
  const p=peerMap.get(pid); if(p) p.retries=0; // reset retry count on fresh offer
  try{
    const pc=new RTCPeerConnection(getPCConfig());
    setupPC(pc,pid);
    peers[pid]=pc;
    if(localStream)localStream.getTracks().forEach(t=>pc.addTrack(t,localStream));
    const o=await pc.createOffer({iceRestart});
    await pc.setLocalDescription(o);
    // FIX: wait for ICE gathering before sending (ensures candidates in SDP)
    await iceGatherComplete(pc);
    ws.send(JSON.stringify({type:'webrtc_offer',to:pid,sdp:pc.localDescription.sdp}));
    log("offer SENT "+pid+" ice="+pc.iceGatheringState);
  }catch(e){log("offer FAIL "+pid+": "+e.message);}
}

async function handleOffer(from,sdp){
  log("offer<-"+from);
  destroyPeer(from);
  try{
    const pc=new RTCPeerConnection(getPCConfig());
    setupPC(pc,from);
    peers[from]=pc;
    // FIX: setRemoteDescription BEFORE addTrack so transceivers match the offer
    await pc.setRemoteDescription(new RTCSessionDescription({type:'offer',sdp}));
    // Apply any buffered trickle ICE candidates that arrived before setRemoteDescription
    if(iceBuffer[from]){
      for(const cand of iceBuffer[from]){
        try{await pc.addIceCandidate(new RTCIceCandidate(cand));}catch(e){}
      }
      delete iceBuffer[from];
    }
    if(localStream)localStream.getTracks().forEach(t=>pc.addTrack(t,localStream));
    const a=await pc.createAnswer();
    await pc.setLocalDescription(a);
    // FIX: wait for ICE gathering before sending (ensures candidates in SDP)
    await iceGatherComplete(pc);
    ws.send(JSON.stringify({type:'webrtc_answer',to:from,sdp:pc.localDescription.sdp}));
    log("answer SENT "+from+" ice="+pc.iceGatheringState);
  }catch(e){log("ans FAIL "+from+": "+e.message);}
}

async function handleAnswer(from,sdp){
  log("ans<-"+from);
  try{
    const pc=peers[from]; if(!pc){log("no PC for ans"+from);return;}
    await pc.setRemoteDescription(new RTCSessionDescription({type:'answer',sdp}));
    log("ans OK "+from);
  }catch(e){log("ansErr "+from+": "+e.message); destroyPeer(from);}
}

async function handleIce(from,cand){
  const pc=peers[from];
  if(pc&&cand){
    try{await pc.addIceCandidate(new RTCIceCandidate(cand));}catch(e){}
  }else if(cand){
    // Buffer candidate until PC is created by setRemoteDescription
    if(!iceBuffer[from]) iceBuffer[from]=[];
    iceBuffer[from].push(cand);
  }
}

function setupPC(pc,pid){
  pc.onicecandidate=e=>{
    if(e.candidate&&ws&&ws.readyState===1){
      ws.send(JSON.stringify({type:'webrtc_ice',to:pid,candidate:e.candidate}));
    }
  };

  pc.onicegatheringstatechange=()=>{
    log("PC "+pid+" ICEgather="+pc.iceGatheringState);
  };

  pc.ontrack=e=>{
    log("TRACK "+pid+" streams="+e.streams.length);
    let a=audios[pid];
    if(!a){
      a=new Audio();
      a.autoplay=true;
      a.playsInline=true;
      a.volume=1.0;
      a.style.display='none';
      document.body.appendChild(a); // add to DOM for mobile compatibility
      try{const C=window.AudioContext||window.webkitAudioContext;const x=new C();x.resume().then(()=>x.close());}catch(e){}
      audios[pid]=a;
    }
    a.srcObject=e.streams[0];
    a.play().then(()=>{
      log("PLAYING "+pid);
      document.getElementById('vstat').textContent='Voice OK';
    }).catch(err=>{
      log("playBlock "+pid);
      const r=()=>{a.play().then(()=>log("retryOK "+pid)).catch(()=>{});};
      document.addEventListener('touchstart',r,{once:true});
      document.addEventListener('click',r,{once:true});
    });
  };

  pc.onconnectionstatechange=()=>{
    log("PC "+pid+" conn="+pc.connectionState+" ice="+pc.iceConnectionState);
    const p=peerMap.get(pid); if(p)p.connState=pc.connectionState;
    updPeers();
    if(pc.connectionState==='connected'){
      document.getElementById('vstat').textContent='Voice OK';
      log("CONNECTED "+pid+"!!");
      const p=peerMap.get(pid); if(p) p.retries=0; // reset retry count on success
    }
    if(pc.connectionState==='failed'){
      log("FAILED "+pid);
      destroyPeer(pid);
      // FIX: auto-retry with iceRestart, max 2 retries, 1s delay
      const p=peerMap.get(pid);
      if(p) p.retries=(p.retries||0)+1;
      setTimeout(()=>{
        if(MY_ID && pid && ws && ws.readyState===1){
          const peer=peerMap.get(pid);
          if(MY_ID > pid && peer && peer.retries < 3){
            log("RETRY offer->"+pid+" (#"+peer.retries+")");
            createOffer(pid,true); // iceRestart=true
          }
        }
      },1000);
    }
  };

  pc.oniceconnectionstatechange=()=>{
    log("PC "+pid+" ICE="+pc.iceConnectionState);
  };
}

function cleanupRTC(){
  Object.keys(peers).forEach(pid=>destroyPeer(pid));
  if(localStream){localStream.getTracks().forEach(t=>t.stop()); localStream=null;}
}

// ── CHAT ──
function renderMsg(m){
  const c=document.getElementById('msgs'); if(!c)return;
  if(m.kind==='system'){const d=document.createElement('div');d.className='msg-system';d.textContent=m.text;c.appendChild(d);scroll();return;}
  const isSelf=!!m.self;
  const pi=peerMap.get(m.peer_id)||{};
  const name=m.name||pi.name||'?';
  const isH=isSelf?isHost:pi.is_host;
  const badge=isH?'Host':'Co-host';
  const bClass=isH?'host':'cohost';
  const avSrc=m.avatar||pi.avatar||'';
  const row=document.createElement('div');
  row.className='msg-row '+(isSelf?'self':'other');
  let avHTML;
  if(avSrc)avHTML='<div class="avatar"><img src="'+esc(avSrc)+'"></div>';
  else avHTML='<div class="avatar"><span>'+esc(name[0].toUpperCase())+'</span></div>';
  const header='<div class="msg-header"><span class="msg-name">'+esc(name)+'</span><span class="msg-badge '+bClass+'">'+badge+'</span></div>';
  row.innerHTML=avHTML+'<div class="msg-content">'+header+'<div class="msg-bubble">'+esc(m.text)+'</div></div>';
  c.appendChild(row);
  scroll();
}
function renderSys(t){const c=document.getElementById('msgs');if(!c)return;const d=document.createElement('div');d.className='msg-system';d.textContent=t;c.appendChild(d);scroll();}
function scroll(){const e=document.getElementById('msgs');e.scrollTop=e.scrollHeight;}
function esc(t){const d=document.createElement('div');d.textContent=t||'';return d.innerHTML;}

function sendMsg(){
  const inEl=document.getElementById('msgIn');
  const text=inEl.value.trim();
  if(!text||!ws||ws.readyState!==1)return;
  log("send: "+text.substring(0,30));
  ws.send(JSON.stringify({type:'chat',text:text}));
  inEl.value='';
}

function toggleMute(){
  if(!localStream)return;
  isMuted=!isMuted;
  localStream.getAudioTracks().forEach(t=>t.enabled=!isMuted);
  const b=document.getElementById('muteBtn');
  if(isMuted){b.classList.add('muted');b.innerHTML='&#128263;';document.getElementById('vstat').textContent='Muted';log("muted");}
  else{b.classList.remove('muted');b.innerHTML='&#127908;';document.getElementById('vstat').textContent='Connected';log("unmuted");}
  if(ws&&ws.readyState===1)ws.send(JSON.stringify({type:isMuted?'mute_me':'unmute_me'}));
  updPeers();
}

function leaveCall(){
  log("leave");
  if(ws&&ws.readyState===1)ws.close();
  cleanupRTC();
  try{window.close();}catch(e){}
  document.body.innerHTML='<div style="display:flex;align-items:center;justify-content:center;height:100vh;background:#000;color:#fff;font-family:sans-serif"><h2>Left the call</h2></div>';
}

log("page loaded");
</script>
</body>
</html>"""

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
    print("=" * 50)
    print(f"Silent Hill Bot | {WEB_APP_URL} | Port {PORT} | Kyodo: {KYODO_OK}")
    print("=" * 50)
    await asyncio.gather(
        Server(Config(app=app, host="0.0.0.0", port=PORT, log_level="warning")).serve(),
        run_kyodo_bot(),
        keepalive(),
    )

if __name__ == "__main__":
    asyncio.run(main())
