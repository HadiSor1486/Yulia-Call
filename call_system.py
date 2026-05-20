"""
call_system.py — LiveKit SFU voice for Silent Hill
═══════════════════════════════════════════════════════════════════════════════
Mints short-lived JWT access tokens for clients to connect to a LiveKit room.
All voice transport (SDP, ICE, TURN, media forwarding) is handled by LiveKit;
this module just gates token issuance with the same invite-token table that
protects /call/{room_id}.

ENV VARS (Render → Environment):
    LIVEKIT_URL              wss://YOUR-PROJECT.livekit.cloud
    LIVEKIT_API_KEY          APIxxxxxxxxxxx
    LIVEKIT_API_SECRET       <long secret>
    LIVEKIT_TOKEN_TTL        (optional, seconds, default 21600 = 6h)

REQUIREMENTS:
    livekit-api>=1.0.0       (add to requirements.txt)

FREE TIER: 100 concurrent participants, 5 GB egress/month. Plenty for our cap.
═══════════════════════════════════════════════════════════════════════════════
"""

import os
import time
from datetime import timedelta
from typing import Optional, Dict, Any

from fastapi import HTTPException, Query
from fastapi.responses import JSONResponse

try:
    from livekit import api as lk_api
    LIVEKIT_OK = True
except ImportError:
    LIVEKIT_OK = False
    print("[call_system] !! livekit-api NOT installed — voice calls will fail.")
    print("[call_system] !! Add 'livekit-api>=1.0.0' to requirements.txt and redeploy.")


# ─── CONFIG ─────────────────────────────────────────────────────────────────
LIVEKIT_URL        = os.environ.get("LIVEKIT_URL", "")
LIVEKIT_API_KEY    = os.environ.get("LIVEKIT_API_KEY", "")
LIVEKIT_API_SECRET = os.environ.get("LIVEKIT_API_SECRET", "")
TOKEN_TTL_SECONDS  = int(os.environ.get("LIVEKIT_TOKEN_TTL", str(6 * 3600)))

MAX_IDENTITY_LEN = 80
MAX_NAME_LEN     = 64


def is_configured() -> bool:
    """True iff SDK loaded AND all three env vars are set."""
    return bool(LIVEKIT_OK and LIVEKIT_URL and LIVEKIT_API_KEY and LIVEKIT_API_SECRET)


def get_call_token(room_id: str,
                   identity: str,
                   display_name: Optional[str] = None,
                   is_admin: bool = False) -> Dict[str, Any]:
    """Mint a LiveKit access token for `identity` joining `room_id`.

    Returns: { url, token, room, identity, expires_at }
    Raises HTTPException 503 if LiveKit env vars aren't set.
    """
    if not is_configured():
        raise HTTPException(
            status_code=503,
            detail="Voice service not configured. Set LIVEKIT_URL/KEY/SECRET on the server.",
        )
    if not room_id or not identity:
        raise HTTPException(status_code=400, detail="room_id and identity required")

    identity = identity[:MAX_IDENTITY_LEN]
    name     = (display_name or identity)[:MAX_NAME_LEN]

    grants = lk_api.VideoGrants(
        room_join=True,
        room=room_id,
        can_publish=True,
        can_subscribe=True,
        can_publish_data=True,
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
    """Attach the /livekit-token and /livekit-status routes.

    validate_room: optional callable(room_id, invite_token) -> bool.
        When provided, /livekit-token rejects requests whose invite_token
        isn't valid for `room_id`. This keeps voice access gated by the
        same tokens that protect /call/{room_id}.
    """

    @app.get("/livekit-token")
    async def livekit_token_endpoint(
        room: str = Query(..., min_length=1, max_length=100),
        identity: str = Query(..., min_length=1, max_length=MAX_IDENTITY_LEN),
        name: str = Query("", max_length=MAX_NAME_LEN),
        t: str = Query("", max_length=200),
        is_admin: int = Query(0),
    ):
        if validate_room is not None:
            try:
                ok = bool(validate_room(room, t))
            except Exception as e:
                print(f"[call_system] validate_room raised: {e}")
                ok = False
            if not ok:
                raise HTTPException(status_code=403, detail="Invalid room or token")

        return JSONResponse(get_call_token(
            room_id=room,
            identity=identity,
            display_name=name or identity,
            is_admin=bool(is_admin),
        ))

    @app.get("/livekit-status")
    async def livekit_status_endpoint():
        """Diagnostic — hit this in a browser to verify LiveKit creds.
        Never echoes the actual secrets."""
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
    """Print LiveKit config status at server startup."""
    ok = is_configured()
    print(f"LiveKit: {'OK' if ok else 'NOT CONFIGURED'} | "
          f"SDK={LIVEKIT_OK} URL={bool(LIVEKIT_URL)} "
          f"KEY={bool(LIVEKIT_API_KEY)} SECRET={bool(LIVEKIT_API_SECRET)} "
          f"TTL={TOKEN_TTL_SECONDS}s")
    if not ok:
        print("LiveKit: !! Voice will not work — set LIVEKIT_URL, "
              "LIVEKIT_API_KEY, LIVEKIT_API_SECRET on Render.")
