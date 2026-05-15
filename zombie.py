"""
zombie.py — Old Maid / "Zombie!" game engine for Silent Hill Voice Call Bot
═══════════════════════════════════════════════════════════════════════════════

Inspired by Plato's "Old Zombie". Self-contained module modeled after uno.py;
exposes the same three lifecycle hooks so main.py can wire it in with
practically zero ceremony:
    handle_ws(room_id, peer_id, peer_name, peer_avatar, room, msg, send)
    on_peer_leave(room_id, peer_id, room, send)
    on_room_cleanup(room_id)

GAME RULES (per the user's reference screenshots):
    • 33-card deck = 16 matched pairs (4 suits × 8 ranks: 7,8,9,10,J,Q,K,A)
      + 1 unique ZOMBIE card. Pairs match by RANK+COLOR (♥/♦ are red;
      ♠/♣ are black — so e.g. K♥ pairs with K♦, K♠ pairs with K♣).
    • Cards dealt round-robin until none left. Whoever ends up with the
      Zombie has one extra card, but they don't know who it is yet
      since pairs auto-discard.
    • On setup, every player auto-discards any pairs from their starting
      hand.
    • Turn: pick one HIDDEN card from the player to your right (in seat
      order). If the picked card pairs with one in your hand, both auto-
      discard. Then your hand reshuffles (so you can't remember positions)
      and the turn moves clockwise.
    • If a player empties their hand → they're OUT, but as a WINNER.
    • Game continues until only one player remains holding the Zombie.
      That player is the Zombie / loser.

The "1 loser, everyone else wins" framing is what makes this so good for
voice chat — the entire table laughs at whoever ends up with the Zombie.

Wire protocol (client ↔ server, over the existing WS):

    Client → Server:
        zomb_create        {players: 2..5}
        zomb_join          {}
        zomb_leave         {}
        zomb_start         {}                          # host force-start
        zomb_pick          {target_pid, card_idx}      # pick a hidden card
        zomb_close         {}                          # close finished game
        zomb_play_again    {}                          # spin up fresh lobby

    Server → All in room:
        zomb_state         {phase, host, players, max_players, turn,
                            target, hand_counts, finished[], winner_count,
                            zombie_pid, turn_started_at, turn_timeout,
                            server_now}
        zomb_event         {kind, text, peer_id?, ...}

    Server → Single peer:
        zomb_hand          {hand: [Card, ...]}         # ONLY your own
        zomb_error         {text}
        zomb_closed        {text}

Card shape:
    {id: "Kh-x1y2", rank: "7|8|9|10|J|Q|K|A|Z", suit: "h|d|s|c|z"}
    where suit "z" + rank "Z" is the Zombie card.
"""

import asyncio
import random
import time
import uuid
from typing import Any, Awaitable, Callable, Dict, List, Optional

# room_id → Game
games: Dict[str, "Game"] = {}

# Per-room turn-timeout asyncio task (same pattern as uno.py)
_turn_timers: Dict[str, "asyncio.Task"] = {}

# Card definitions
RANKS = ("7", "8", "9", "10", "J", "Q", "K", "A")
SUITS = ("h", "d", "s", "c")  # hearts, diamonds, spades, clubs
# Color partition for matching: red = h+d, black = s+c.
# A pair = same rank AND same color. So J♥ ↔ J♦, J♠ ↔ J♣. (4 pairs per rank.)
# Wait — that's the standard Old Maid pairing too. Re-check: 8 ranks × 2
# color-pairs = 16 pairs = 32 cards. + 1 Zombie = 33. ✓

MIN_PLAYERS = 2
MAX_PLAYERS = 5
TURN_TIMEOUT_SECONDS = 25  # picks are simple, tighter than Uno


def _color_of(suit: str) -> str:
    return "r" if suit in ("h", "d") else "b"


def _build_deck() -> List[dict]:
    """33-card deck: 4 suits × 8 ranks (32) + 1 Zombie."""
    deck: List[dict] = []
    for rank in RANKS:
        for suit in SUITS:
            deck.append({
                "id": f"{rank}{suit}-{uuid.uuid4().hex[:5]}",
                "rank": rank,
                "suit": suit,
            })
    # The Zombie. We use rank="Z" suit="z" as sentinels so it never
    # accidentally pairs with anything.
    deck.append({
        "id": f"ZZ-{uuid.uuid4().hex[:5]}",
        "rank": "Z",
        "suit": "z",
    })
    random.shuffle(deck)
    return deck


def _is_zombie(card: dict) -> bool:
    return card["rank"] == "Z"


def _pairs_with(a: dict, b: dict) -> bool:
    """Two non-Zombie cards pair iff same rank and same color."""
    if _is_zombie(a) or _is_zombie(b):
        return False
    return (a["rank"] == b["rank"]
            and _color_of(a["suit"]) == _color_of(b["suit"]))


def _extract_pairs(hand: List[dict]) -> List[List[dict]]:
    """Remove all pairs from hand IN PLACE. Returns list of pair-pairs
    that were discarded (each inner list is 2 cards). This is called on
    setup (initial dealt hand) and after every pick that produced a pair.
    Greedy single-pass: for each card, find another in hand that pairs
    with it. Since each rank+color has exactly 2 instances in the deck,
    there's at most one pair candidate per card — so order doesn't
    matter."""
    out: List[List[dict]] = []
    i = 0
    while i < len(hand):
        ci = hand[i]
        if _is_zombie(ci):
            i += 1
            continue
        match_j = -1
        for j in range(i + 1, len(hand)):
            if _pairs_with(ci, hand[j]):
                match_j = j
                break
        if match_j != -1:
            cj = hand[match_j]
            # Remove both. Pop the higher index first so the lower one
            # stays valid.
            hand.pop(match_j)
            hand.pop(i)
            out.append([ci, cj])
            # Don't increment i — the next card slid into this slot.
        else:
            i += 1
    return out


# ── Game ────────────────────────────────────────────────────────────────────
class Game:
    __slots__ = (
        "room_id", "max_players", "host_pid", "phase",
        "players",                # ordered seat list (rotation order)
        "hands",                  # pid -> [card,...]
        "finished_pids",          # players who emptied their hand (winners)
        "turn_idx",               # which seat is currently picking
        "winner_pid",             # the Zombie (loser) at end of game
        "created_at", "turn_started_at",
    )

    def __init__(self, room_id: str, host_pid: str, max_players: int) -> None:
        self.room_id = room_id
        self.max_players = max(MIN_PLAYERS, min(MAX_PLAYERS, int(max_players)))
        self.host_pid = host_pid
        self.phase: str = "lobby"
        self.players: List[Dict[str, str]] = []
        self.hands: Dict[str, List[dict]] = {}
        self.finished_pids: List[str] = []
        self.turn_idx: int = 0
        self.winner_pid: Optional[str] = None
        self.created_at = time.time()
        self.turn_started_at: float = 0.0

    # ── lobby ──
    def add_player(self, pid: str, name: str, avatar: str) -> bool:
        if self.phase != "lobby":
            return False
        if any(p["pid"] == pid for p in self.players):
            return True
        if len(self.players) >= self.max_players:
            return False
        self.players.append({"pid": pid, "name": name, "avatar": avatar})
        return True

    def remove_player(self, pid: str) -> str:
        idx = next((i for i, p in enumerate(self.players) if p["pid"] == pid), -1)
        if idx == -1:
            return "noop"

        if self.phase == "lobby":
            self.players.pop(idx)
            if pid == self.host_pid:
                if self.players:
                    self.host_pid = self.players[0]["pid"]
                    return "left_lobby"
                return "host_left_lobby"
            return "left_lobby"

        if self.phase == "playing":
            # Mid-game leave: drop their hand and remove them. If they had
            # the Zombie, it's gone with them and the game becomes
            # unfinishable — so we end it with no Zombie (everyone wins).
            had_zombie = any(_is_zombie(c) for c in self.hands.get(pid, []))
            self.hands.pop(pid, None)
            self.players.pop(idx)
            # Adjust active turn index
            if idx < self.turn_idx:
                self.turn_idx -= 1
            if self.players:
                self.turn_idx %= len(self.players)

            if had_zombie:
                # Zombie left the game — nobody can lose. Everyone else
                # is declared a winner.
                for p in self.players:
                    if p["pid"] not in self.finished_pids:
                        self.finished_pids.append(p["pid"])
                self.phase = "finished"
                self.winner_pid = None  # no loser
                return "zombie_left_everyone_wins"

            if len(self.players) < 2:
                # Need at least 2 players to keep going.
                self.phase = "finished"
                if self.players:
                    # The remaining player holds the Zombie by elimination.
                    last_pid = self.players[0]["pid"]
                    if any(_is_zombie(c) for c in self.hands.get(last_pid, [])):
                        self.winner_pid = last_pid
                    else:
                        # They had no Zombie either (other left took it
                        # implicitly). Treat as winner.
                        if last_pid not in self.finished_pids:
                            self.finished_pids.append(last_pid)
                return "game_aborted"

            return "left_mid_game"

        # finished phase
        self.players.pop(idx)
        return "left_lobby"

    def can_start(self) -> bool:
        return (self.phase == "lobby"
                and MIN_PLAYERS <= len(self.players) <= self.max_players)

    def is_full(self) -> bool:
        return len(self.players) >= self.max_players

    # ── start ──
    def start(self) -> List[dict]:
        """Deals out the deck and auto-discards starting pairs. Returns
        a list of public events describing the initial discard counts
        per player (no card identities revealed)."""
        assert self.can_start()
        deck = _build_deck()
        self.hands = {p["pid"]: [] for p in self.players}
        # Round-robin deal until deck is empty.
        i = 0
        while deck:
            pid = self.players[i % len(self.players)]["pid"]
            self.hands[pid].append(deck.pop())
            i += 1

        events: List[dict] = []
        for p in self.players:
            pairs = _extract_pairs(self.hands[p["pid"]])
            if pairs:
                events.append({
                    "kind": "initial_discard",
                    "peer_id": p["pid"],
                    "n": len(pairs),
                    "text": f"{p['name']} discarded {len(pairs)} starting pair"
                            + ("s" if len(pairs) != 1 else ""),
                })

        self.turn_idx = 0
        self.finished_pids = []
        self.winner_pid = None
        self.phase = "playing"
        self.turn_started_at = time.time()

        # If a player started with NO cards (extremely unlikely but
        # possible with very tiny hands), they win immediately.
        self._check_finished_during_setup(events)
        # If only one remains after setup, end the game.
        self._maybe_finish(events)
        return events

    def _check_finished_during_setup(self, events: List[dict]) -> None:
        for p in self.players:
            pid = p["pid"]
            if pid in self.finished_pids:
                continue
            if not self.hands.get(pid):
                self.finished_pids.append(pid)
                events.append({"kind": "out", "peer_id": pid,
                               "text": f"{p['name']} is out — clean win!"})

    def _maybe_finish(self, events: List[dict]) -> bool:
        """If exactly one player remains with cards, the game ends and
        that player is the Zombie. Returns True if game ended."""
        if self.phase != "playing":
            return False
        active = [p for p in self.players if p["pid"] not in self.finished_pids]
        if len(active) <= 1:
            self.phase = "finished"
            if active:
                self.winner_pid = active[0]["pid"]
                name = active[0]["name"]
                events.append({
                    "kind": "zombie", "peer_id": self.winner_pid,
                    "text": f"🧟 {name} is the Zombie!"
                })
            return True
        return False

    # ── play ──
    def current_pid(self) -> str:
        return self._active_at(self.turn_idx)

    def _active_at(self, idx: int) -> str:
        """Get the pid at seat index idx, skipping finished players."""
        if not self.players:
            return ""
        n = len(self.players)
        for i in range(n):
            cand = self.players[(idx + i) % n]
            if cand["pid"] not in self.finished_pids:
                return cand["pid"]
        return ""

    def _next_active_idx(self, idx: int) -> int:
        """Step idx forward until landing on a player with cards."""
        n = len(self.players)
        for i in range(1, n + 1):
            j = (idx + i) % n
            cand = self.players[j]
            if cand["pid"] not in self.finished_pids and self.hands.get(cand["pid"]):
                return j
        return idx  # fallback (game should end before this)

    def target_pid(self) -> str:
        """The player to the RIGHT of the current player is the pick
        target. In our list, 'right' = next index (clockwise)."""
        n = len(self.players)
        if n == 0:
            return ""
        # The current player's seat index.
        cur_pid = self.current_pid()
        cur_idx = next((i for i, p in enumerate(self.players)
                        if p["pid"] == cur_pid), 0)
        # Next active player with cards.
        for i in range(1, n + 1):
            j = (cur_idx + i) % n
            cand = self.players[j]
            if (cand["pid"] not in self.finished_pids
                    and self.hands.get(cand["pid"])):
                return cand["pid"]
        return ""

    def pick(self, picker_pid: str, target_pid: str, card_idx: int) -> Dict[str, Any]:
        """The picker takes the hidden card at position card_idx from the
        target's hand. If the picked card pairs with something in the
        picker's hand, both discard. Then both hands are reshuffled (so
        positions can't be memorized) and the turn passes clockwise."""
        if self.phase != "playing":
            return {"ok": False, "error": "Game not in play"}
        if picker_pid != self.current_pid():
            return {"ok": False, "error": "Not your turn"}
        legal_target = self.target_pid()
        if target_pid != legal_target:
            return {"ok": False, "error": "Pick from the player on your right"}
        target_hand = self.hands.get(target_pid, [])
        if not target_hand:
            return {"ok": False, "error": "That player has no cards"}
        if not isinstance(card_idx, int) or card_idx < 0 or card_idx >= len(target_hand):
            return {"ok": False, "error": "Invalid card index"}

        card = target_hand.pop(card_idx)
        picker_hand = self.hands.get(picker_pid, [])
        picker_hand.append(card)

        events: List[dict] = []
        picker_name = next(p["name"] for p in self.players if p["pid"] == picker_pid)
        target_name = next(p["name"] for p in self.players if p["pid"] == target_pid)

        # Look for a pair with the just-picked card.
        paired = False
        if not _is_zombie(card):
            # Find any card in picker_hand that pairs with `card` (and is
            # not `card` itself — the last appended item).
            for k in range(len(picker_hand) - 1):
                if _pairs_with(card, picker_hand[k]):
                    # Discard both. Pop the higher index first.
                    last_idx = len(picker_hand) - 1
                    picker_hand.pop(last_idx)
                    picker_hand.pop(k)
                    paired = True
                    events.append({
                        "kind": "pair", "peer_id": picker_pid,
                        "text": f"{picker_name} paired and discarded "
                                f"{card['rank']}s",
                    })
                    break

        if not paired:
            events.append({
                "kind": "pick", "peer_id": picker_pid,
                "target_pid": target_pid,
                "text": f"{picker_name} picked from {target_name}",
            })

        # Reshuffle both affected hands so card positions reset.
        random.shuffle(self.hands[picker_pid])
        random.shuffle(self.hands[target_pid])

        # Check if either player just emptied their hand → winner.
        if not self.hands[picker_pid] and picker_pid not in self.finished_pids:
            self.finished_pids.append(picker_pid)
            events.append({
                "kind": "out", "peer_id": picker_pid,
                "text": f"🎉 {picker_name} is out — winner!"
            })
        if not self.hands[target_pid] and target_pid not in self.finished_pids:
            self.finished_pids.append(target_pid)
            events.append({
                "kind": "out", "peer_id": target_pid,
                "text": f"🎉 {target_name} is out — winner!"
            })

        # End-game check
        if self._maybe_finish(events):
            return {"ok": True, "events": events}

        # Advance turn to next active player.
        # cur_idx → next active idx
        cur_idx = next((i for i, p in enumerate(self.players)
                        if p["pid"] == picker_pid), 0)
        self.turn_idx = self._next_active_idx(cur_idx)
        self.turn_started_at = time.time()
        return {"ok": True, "events": events}

    # ── snapshot ──
    def public_state(self) -> dict:
        return {
            "phase": self.phase,
            "host": self.host_pid,
            "players": [
                {"pid": p["pid"], "name": p["name"], "avatar": p["avatar"]}
                for p in self.players
            ],
            "max_players": self.max_players,
            "turn": self.current_pid() if self.phase == "playing" else "",
            "target": self.target_pid() if self.phase == "playing" else "",
            "hand_counts": {p["pid"]: len(self.hands.get(p["pid"], []))
                            for p in self.players},
            "finished": list(self.finished_pids),
            "zombie_pid": self.winner_pid,  # who has the Zombie at end
            "turn_started_at": self.turn_started_at,
            "turn_timeout": TURN_TIMEOUT_SECONDS,
            "server_now": time.time(),
        }


# ── WS plumbing ─────────────────────────────────────────────────────────────
SendFn = Callable[[Optional[str], dict], Awaitable[None]]


def _cancel_turn_timer(room_id: str) -> None:
    t = _turn_timers.pop(room_id, None)
    if t is not None and not t.done():
        t.cancel()


async def _turn_timeout_runner(room_id: str, expected_turn_at: float,
                                room: dict, send: SendFn) -> None:
    try:
        await asyncio.sleep(TURN_TIMEOUT_SECONDS + 0.5)
    except asyncio.CancelledError:
        return
    g = games.get(room_id)
    if not g or g.phase != "playing":
        return
    if g.turn_started_at != expected_turn_at:
        return
    # Auto-pick a random card from the target.
    picker = g.current_pid()
    target = g.target_pid()
    if not target:
        return
    target_hand = g.hands.get(target, [])
    if not target_hand:
        return
    idx = random.randrange(len(target_hand))
    name = next((p["name"] for p in g.players if p["pid"] == picker), "?")
    events: List[dict] = [{
        "kind": "timeout", "peer_id": picker,
        "text": f"⏱ {name} timed out — auto-picked",
    }]
    res = g.pick(picker, target, idx)
    events.extend(res.get("events", []))
    await _broadcast_state(room, room_id, send)
    await _send_private_hands(room, room_id, send)  # everyone (counts changed)
    await _emit_events(room, room_id, send, events)


def _schedule_turn_timer(room_id: str, room: dict, send: SendFn) -> None:
    g = games.get(room_id)
    if not g or g.phase != "playing":
        _cancel_turn_timer(room_id)
        return
    existing = _turn_timers.get(room_id)
    if existing is not None and not existing.done():
        if getattr(existing, "_zomb_turn_at", None) == g.turn_started_at:
            return
        existing.cancel()
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        return
    task = loop.create_task(
        _turn_timeout_runner(room_id, g.turn_started_at, room, send)
    )
    task._zomb_turn_at = g.turn_started_at  # type: ignore[attr-defined]
    _turn_timers[room_id] = task


async def _broadcast_state(room: dict, room_id: str, send: SendFn) -> None:
    g = games.get(room_id)
    if not g:
        _cancel_turn_timer(room_id)
        return
    state = g.public_state()
    await send(None, {"type": "zomb_state", "state": state})
    if g.phase == "playing":
        _schedule_turn_timer(room_id, room, send)
    else:
        _cancel_turn_timer(room_id)


async def _send_private_hands(room: dict, room_id: str, send: SendFn,
                              only_pid: Optional[str] = None) -> None:
    g = games.get(room_id)
    if not g:
        return
    targets = [only_pid] if only_pid else list(g.hands.keys())
    for pid in targets:
        hand = g.hands.get(pid, [])
        await send(pid, {"type": "zomb_hand", "hand": hand})


async def _emit_events(room: dict, room_id: str, send: SendFn,
                       events: List[dict]) -> None:
    for ev in events:
        await send(None, {"type": "zomb_event", **ev})


async def handle_ws(room_id: str, peer_id: str, peer_name: str,
                    peer_avatar: str, room: dict, msg: dict,
                    send: SendFn) -> None:
    mt = msg.get("type", "")

    if mt == "zomb_create":
        if room_id in games and games[room_id].phase != "finished":
            await send(peer_id, {"type": "zomb_error",
                                  "text": "A game is already running"})
            return
        try:
            n = int(msg.get("players", 2))
        except (TypeError, ValueError):
            n = 2
        if n < MIN_PLAYERS or n > MAX_PLAYERS:
            await send(peer_id, {"type": "zomb_error",
                                  "text": f"Players must be {MIN_PLAYERS}-{MAX_PLAYERS}"})
            return
        g = Game(room_id, peer_id, n)
        _cancel_turn_timer(room_id)
        games[room_id] = g
        g.add_player(peer_id, peer_name, peer_avatar)
        await _broadcast_state(room, room_id, send)
        await _emit_events(room, room_id, send, [{
            "kind": "lobby_open", "peer_id": peer_id,
            "text": f"{peer_name} opened a Zombie lobby ({n} players)"
        }])
        return

    g = games.get(room_id)
    if not g:
        await send(peer_id, {"type": "zomb_error",
                              "text": "No active game. Create one first."})
        return

    if mt == "zomb_join":
        if g.phase != "lobby":
            await send(peer_id, {"type": "zomb_error",
                                  "text": "Game already started"})
            return
        ok = g.add_player(peer_id, peer_name, peer_avatar)
        if not ok:
            await send(peer_id, {"type": "zomb_error",
                                  "text": "Lobby is full"})
            return
        await _broadcast_state(room, room_id, send)
        await _emit_events(room, room_id, send, [{
            "kind": "join", "peer_id": peer_id,
            "text": f"{peer_name} joined the Zombie game"
        }])
        if g.is_full():
            events = g.start()
            await _broadcast_state(room, room_id, send)
            await _send_private_hands(room, room_id, send)
            await _emit_events(room, room_id, send,
                               [{"kind": "start", "text": "Game started!"}] + events)
        return

    if mt == "zomb_leave":
        leaver_name = next((p["name"] for p in g.players
                            if p["pid"] == peer_id), peer_name)
        result = g.remove_player(peer_id)
        if result == "noop":
            return
        if result == "host_left_lobby":
            del games[room_id]
            _cancel_turn_timer(room_id)
            await send(None, {"type": "zomb_closed",
                              "text": f"{leaver_name} closed the Zombie lobby"})
            return
        await _broadcast_state(room, room_id, send)
        if result == "left_lobby":
            await _emit_events(room, room_id, send, [{
                "kind": "leave", "peer_id": peer_id,
                "text": f"{leaver_name} left the lobby"}])
        elif result == "left_mid_game":
            await _emit_events(room, room_id, send, [{
                "kind": "leave", "peer_id": peer_id,
                "text": f"{leaver_name} left the game"}])
            await _send_private_hands(room, room_id, send)
        elif result == "zombie_left_everyone_wins":
            await _emit_events(room, room_id, send, [{
                "kind": "win_all",
                "text": f"🧟 {leaver_name} left WITH the Zombie — everyone wins!"
            }])
        elif result == "game_aborted":
            await _emit_events(room, room_id, send, [{
                "kind": "abort",
                "text": "Game ended — not enough players"
            }])
        return

    if mt == "zomb_start":
        if peer_id != g.host_pid:
            await send(peer_id, {"type": "zomb_error",
                                  "text": "Only the host can start"})
            return
        if not g.can_start():
            await send(peer_id, {"type": "zomb_error",
                                  "text": "Need at least 2 players"})
            return
        events = g.start()
        await _broadcast_state(room, room_id, send)
        await _send_private_hands(room, room_id, send)
        await _emit_events(room, room_id, send,
                           [{"kind": "start", "text": "Game started!"}] + events)
        return

    if mt == "zomb_pick":
        target = str(msg.get("target_pid", ""))[:32]
        try:
            idx = int(msg.get("card_idx", -1))
        except (TypeError, ValueError):
            idx = -1
        res = g.pick(peer_id, target, idx)
        if not res.get("ok"):
            await send(peer_id, {"type": "zomb_error",
                                  "text": res.get("error", "Invalid pick")})
            return
        await _broadcast_state(room, room_id, send)
        # Both picker and target's hands may have changed.
        await _send_private_hands(room, room_id, send, only_pid=peer_id)
        await _send_private_hands(room, room_id, send, only_pid=target)
        await _emit_events(room, room_id, send, res.get("events", []))
        return

    if mt == "zomb_close":
        if g.phase != "finished" and peer_id != g.host_pid:
            await send(peer_id, {"type": "zomb_error",
                                  "text": "Only the host can close"})
            return
        del games[room_id]
        _cancel_turn_timer(room_id)
        await send(None, {"type": "zomb_closed",
                          "text": "Zombie game closed"})
        return

    if mt == "zomb_play_again":
        if g.phase != "finished":
            await send(peer_id, {"type": "zomb_error",
                                  "text": "Can only restart after a game"})
            return
        if not any(p["pid"] == peer_id for p in g.players):
            await send(peer_id, {"type": "zomb_error",
                                  "text": "Only previous players can restart"})
            return
        prev_max = g.max_players
        del games[room_id]
        _cancel_turn_timer(room_id)
        new_g = Game(room_id, peer_id, prev_max)
        games[room_id] = new_g
        new_g.add_player(peer_id, peer_name, peer_avatar)
        await _broadcast_state(room, room_id, send)
        await _emit_events(room, room_id, send, [{
            "kind": "lobby_open", "peer_id": peer_id,
            "text": f"{peer_name} started a new Zombie lobby ({prev_max} players)"
        }])
        return


async def on_peer_leave(room_id: str, peer_id: str,
                        room: Optional[dict], send: SendFn) -> None:
    g = games.get(room_id)
    if not g or room is None:
        return
    in_game = any(p["pid"] == peer_id for p in g.players)
    if not in_game:
        return
    leaver_name = next((p["name"] for p in g.players
                        if p["pid"] == peer_id), "?")
    result = g.remove_player(peer_id)
    if result == "host_left_lobby":
        del games[room_id]
        _cancel_turn_timer(room_id)
        await send(None, {"type": "zomb_closed",
                          "text": f"{leaver_name} (host) left — lobby closed"})
        return
    if result == "noop":
        return
    await _broadcast_state(room, room_id, send)
    if result == "zombie_left_everyone_wins":
        await _emit_events(room, room_id, send, [{
            "kind": "win_all",
            "text": f"🧟 {leaver_name} disconnected WITH the Zombie — everyone wins!"
        }])
        return
    if result == "game_aborted":
        await _emit_events(room, room_id, send, [{
            "kind": "abort",
            "text": "Game ended — not enough players"
        }])
        return
    await _emit_events(room, room_id, send, [{
        "kind": "leave", "peer_id": peer_id,
        "text": f"{leaver_name} left the Zombie game"
    }])
    if result == "left_mid_game":
        await _send_private_hands(room, room_id, send)


def on_room_cleanup(room_id: str) -> None:
    if room_id in games:
        del games[room_id]
    _cancel_turn_timer(room_id)
