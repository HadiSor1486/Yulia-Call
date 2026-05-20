"""
call_system.py — LiveKit-backed voice call system
═══════════════════════════════════════════════════════════════════════════════
Replaces the old P2P mesh WebRTC + TURN code with a LiveKit SFU.

WHY: The old mesh approach had every peer connect to every other peer directly.
This works for 2-4 people on a fast LAN but falls apart on mobile / Gulf
networks with 6+ peers. LiveKit is an SFU (Selective Forwarding Unit) —
every peer connects to one media server instead of N-1 other peers. Same
architecture Discord, Zoom, ChatGPT Voice all use.

WHAT THIS MODULE DOES:
  • Mints short-lived JWT access tokens for clients to connect to LiveKit
  • Exposes a single function get_call_token(room_id, identity, name) -> dict
  • Exposes a FastAPI sub-app helper register_routes(app) that adds the
    /livekit-token endpoint to your main FastAPI app
  • Centralizes all voice-related config so future tweaks happen here only

WHAT THIS MODULE INTENTIONALLY DOES NOT DO:
  • Peer presence tracking — that stays in your main.py rooms dict (your
    chat/games/seat UI all depend on it).
  • Mute / speaking broadcasts — LiveKit emits these client-side; you just
    relay them through your existing WS for UI consistency. (Or you can
    use LiveKit's webhooks if you want server-authoritative presence.)
  • Audio mixing or recording — supported by LiveKit but not enabled here
    by default. Add later if you want.

SETUP (one-time):
  1. Sign up at https://livekit.io → create a project → copy these 3 values
     to your Render env vars:
        LIVEKIT_URL             wss://YOUR-PROJECT.livekit.cloud
        LIVEKIT_API_KEY         APIxxxxxxxxxxx
        LIVEKIT_API_SECRET      <long secret>
  2. Add `livekit-api>=0.6.0` to requirements.txt
  3. Redeploy. That's it — clients fetch a token from /livekit-token and
     join the LiveKit room with the same name as your call room_id.

FREE TIER: 100 concurrent participants, 5 GB egress/month. Plenty for a
6-15-person friend group.
═══════════════════════════════════════════════════════════════════════════════
"""

import os
import time
from datetime import timedelta
from typing import Optional, Dict, Any

from fastapi import HTTPException, Query
from fastapi.responses import JSONResponse

# LiveKit server SDK. If not installed we fail loud so the operator
# notices instead of running with broken voice.
try:
    from livekit import api as lk_api
    LIVEKIT_OK = True
except ImportError:
    LIVEKIT_OK = False
    print("[call_system] !! livekit-api NOT installed. Voice calls will fail.")
    print("[call_system] !! Add 'livekit-api>=0.6.0' to requirements.txt and redeploy.")


# ─── CONFIG ─────────────────────────────────────────────────────────────────
LIVEKIT_URL    = os.environ.get("LIVEKIT_URL", "")        # wss://your-project.livekit.cloud
LIVEKIT_API_KEY    = os.environ.get("LIVEKIT_API_KEY", "")
LIVEKIT_API_SECRET = os.environ.get("LIVEKIT_API_SECRET", "")

# Token lifetime. 6h is generous — covers a long call without forcing
# the client to refresh mid-conversation. LiveKit lets you re-issue
# without dropping the connection if you ever need longer sessions.
TOKEN_TTL_SECONDS = int(os.environ.get("LIVEKIT_TOKEN_TTL", str(6 * 3600)))

# Maximum identity length on LiveKit side. Names get truncated to keep
# tokens small and avoid display issues.
MAX_IDENTITY_LEN = 80
MAX_NAME_LEN     = 64


def is_configured() -> bool:
    """True if all three LiveKit env vars are set AND the SDK is importable."""
    return bool(LIVEKIT_OK and LIVEKIT_URL and LIVEKIT_API_KEY and LIVEKIT_API_SECRET)


def get_call_token(room_id: str,
                   identity: str,
                   display_name: Optional[str] = None,
                   is_admin: bool = False) -> Dict[str, Any]:
    """
    Mint a LiveKit access token for a peer joining `room_id`.

    Args:
        room_id: must match the room name on both ends (we reuse your
                 existing call room_id verbatim — no separate LiveKit
                 room namespace).
        identity: stable per-peer string. We use your peer_id so the
                  same identity isn't reused across peers in one room.
        display_name: shown in LiveKit's metadata (your UI uses its own
                      name from the join handshake anyway).
        is_admin: if True, grant room_admin privileges. Only your
                  hidden-suffix admin user (Sor-) should get this.

    Returns:
        { "url": "wss://...", "token": "<jwt>", "room": room_id,
          "identity": identity, "expires_at": <unix ts> }

    Raises:
        HTTPException 503 if LiveKit isn't configured.
        HTTPException 400 on bad input.
    """
    if not is_configured():
        raise HTTPException(
            status_code=503,
            detail="Voice service not configured. Set LIVEKIT_URL/KEY/SECRET env vars.",
        )
    if not room_id or not identity:
        raise HTTPException(status_code=400, detail="room_id and identity required")

    identity = identity[:MAX_IDENTITY_LEN]
    name     = (display_name or identity)[:MAX_NAME_LEN]

    grants = lk_api.VideoGrants(
        room_join=True,
        room=room_id,
        can_publish=True,        # peer can publish their mic
        can_subscribe=True,      # peer can hear others
        can_publish_data=True,   # for LiveKit data channel (handy for future)
        room_admin=bool(is_admin),
    )

    token = (
        lk_api.AccessToken(LIVEKIT_API_KEY, LIVEKIT_API_SECRET)
        .with_identity(identity)
        .with_name(name)
        .with_grants(grants)
        .with_ttl(timedelta(seconds=TOKEN_TTL_SECONDS))
        .to_jwt()
    )

    return {
        "url": LIVEKIT_URL,
        "token": token,
        "room": room_id,
        "identity": identity,
        "expires_at": int(time.time() + TOKEN_TTL_SECONDS),
    }


def register_routes(app, *, validate_room=None):
    """
    Attach call-system HTTP routes to your existing FastAPI app.

    Args:
        app: the FastAPI instance from main.py
        validate_room: optional callable(room_id, token_str) -> bool.
                       If provided, /livekit-token will reject requests
                       whose room_id isn't valid per your token table.
                       This keeps voice access gated by the same invite
                       tokens that already gate /call/{room_id}.
    """

    @app.get("/livekit-token")
    async def livekit_token_endpoint(
        room: str = Query(..., min_length=1, max_length=100),
        identity: str = Query(..., min_length=1, max_length=MAX_IDENTITY_LEN),
        name: str = Query("", max_length=MAX_NAME_LEN),
        t: str = Query("", max_length=200),           # main.py's invite token
        is_admin: int = Query(0),
    ):
        # Optional gating: only mint tokens for rooms backed by a valid
        # invite token. Same security model your /call/{room_id} uses.
        if validate_room is not None:
            ok = False
            try:
                ok = validate_room(room, t)
            except Exception as e:
                print(f"[call_system] validate_room raised: {e}")
                ok = False
            if not ok:
                raise HTTPException(status_code=403, detail="Invalid room or token")

        out = get_call_token(
            room_id=room,
            identity=identity,
            display_name=name or identity,
            is_admin=bool(is_admin),
        )
        return JSONResponse(out)

    @app.get("/livekit-status")
    async def livekit_status_endpoint():
        """Diagnostic endpoint. Hit /livekit-status in the browser to verify
        the server has working LiveKit creds. Never echoes secrets."""
        return JSONResponse({
            "configured":     is_configured(),
            "sdk_installed":  LIVEKIT_OK,
            "url_set":        bool(LIVEKIT_URL),
            "url_host":       LIVEKIT_URL.split("//", 1)[-1] if LIVEKIT_URL else "",
            "api_key_set":    bool(LIVEKIT_API_KEY),
            "api_secret_set": bool(LIVEKIT_API_SECRET),
            "token_ttl":      TOKEN_TTL_SECONDS,
        })


def boot_log():
    """Pretty banner for main.py startup so you can see at a glance whether
    voice is wired up correctly. Call this from your main() before
    Server.serve()."""
    print("=" * 60)
    print("CALL SYSTEM (LiveKit-backed)")
    print(f"  SDK installed:      {LIVEKIT_OK}")
    print(f"  LIVEKIT_URL set:    {bool(LIVEKIT_URL)}")
    print(f"  LIVEKIT_API_KEY:    {bool(LIVEKIT_API_KEY)}")
    print(f"  LIVEKIT_API_SECRET: {bool(LIVEKIT_API_SECRET)}")
    print(f"  Token TTL:          {TOKEN_TTL_SECONDS}s")
    if not is_configured():
        print("  !! NOT CONFIGURED — clients will fail to join voice.")
        print("  !! Set LIVEKIT_URL, LIVEKIT_API_KEY, LIVEKIT_API_SECRET on Render.")
    print("=" * 60)
