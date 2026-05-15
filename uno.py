"""
uno.py — Classic Uno game engine for Silent Hill Voice Call Bot
═══════════════════════════════════════════════════════════════════════════════
Self-contained module. main.py wires three small touchpoints:

  1) In the WS receive loop, if the message type starts with "uno_", call
     uno.handle_ws(room_id, peer_id, peer_name, peer_avatar, room, msg).
  2) In the WS finally/disconnect block, call uno.on_peer_leave(room_id, peer_id).
  3) When a room is fully cleaned up, call uno.on_room_cleanup(room_id).

State lives only in this module's `games` dict. No file persistence — games are
short-lived and survive only as long as the room itself.

Wire protocol (client ↔ server, all carried over the existing WebSocket):

  Client → Server (msg.type):
    uno_create        {players: 2|3|4}                # open a lobby
    uno_join          {}                              # join open lobby
    uno_leave         {}                              # leave lobby/game
    uno_start         {}                              # host force-start (or full)
    uno_play          {card_id, chosen_color?}        # play a card from hand
    uno_draw          {}                              # draw 1 from pile
    uno_pass          {}                              # end turn after drawing
                                                       # if drawn card unplayable
    uno_call_uno      {}                              # announce UNO

  Server → All in room (broadcast):
    uno_state         {phase, host, players[], turn,
                       direction, top_card, color,
                       draw_pile_count, hand_counts,
                       max_players, winner?}
    uno_event         {kind, text, peer_id?, ...}

  Server → Single peer (private):
    uno_hand          {hand: [Card, ...]}             # only your own
    uno_error         {text}

Card shape (JSON):
    {id: "r7-a", color: "r"|"y"|"g"|"b"|"w",
     value: "0".."9" | "skip" | "rev" | "+2" | "wild" | "+4"}
"""

import random
import time
import uuid
from typing import Any, Awaitable, Callable, Dict, List, Optional

# Public: room_id → Game. main.py is allowed to peek (e.g. for /health) but
# should not mutate this directly.
games: Dict[str, "Game"] = {}

# ── Constants ──────────────────────────────────────────────────────────────
COLORS = ("r", "y", "g", "b")          # red, yellow, green, blue
NUMBERS = tuple(str(i) for i in range(10))
ACTIONS = ("skip", "rev", "+2")
WILDS = ("wild", "+4")

MIN_PLAYERS = 2
MAX_PLAYERS = 4
INITIAL_HAND = 7
LOBBY_TTL_SECONDS = 600   # auto-close abandoned lobbies (10 min)


def _build_deck() -> List[dict]:
    """Standard 108-card Uno deck.
      For each color: one 0, two of each 1-9, two skip, two reverse, two +2.
      Plus 4 wild and 4 +4. Total = 4*(1 + 18 + 2 + 2 + 2) + 4 + 4 = 108.
    """
    deck: List[dict] = []

    def add(color: str, value: str) -> None:
        # id uses a random suffix so duplicate face cards have distinct ids
        # — crucial for client-side selection ("which Red 7 did I tap?").
        deck.append({
            "id": f"{color}{value}-{uuid.uuid4().hex[:4]}",
            "color": color,
            "value": value,
        })

    for c in COLORS:
        add(c, "0")
        for n in NUMBERS[1:]:
            add(c, n)
            add(c, n)
        for a in ACTIONS:
            add(c, a)
            add(c, a)
    for _ in range(4):
        add("w", "wild")
        add("w", "+4")

    random.shuffle(deck)
    return deck


def _is_action(card: dict) -> bool:
    return card["value"] in ACTIONS


def _is_wild(card: dict) -> bool:
    return card["color"] == "w"


def _can_play(card: dict, top: dict, active_color: str) -> bool:
    """Classic Uno match rule:
      - Wilds (wild, +4) can always be played.
      - Otherwise, match on the ACTIVE color, OR on the value of the top card.
    `active_color` is what the previous player declared after a wild, or the
    top card's natural color otherwise.
    """
    if _is_wild(card):
        return True
    if card["color"] == active_color:
        return True
    if card["value"] == top["value"] and not _is_wild(top):
        # Same value (number or action) — color jump is allowed.
        return True
    return False


# ── Game ────────────────────────────────────────────────────────────────────
class Game:
    """One Uno game / lobby. Tied to a room. Self-contained state."""

    __slots__ = (
        "room_id", "max_players", "host_pid", "phase",
        "players", "hands", "draw_pile", "discard",
        "turn_idx", "direction", "active_color",
        "draw_stack",
        "must_draw_then_act", "drawn_this_turn",
        "uno_called", "winner_pid",
        "created_at",
    )

    def __init__(self, room_id: str, host_pid: str, max_players: int) -> None:
        self.room_id = room_id
        self.max_players = max(MIN_PLAYERS, min(MAX_PLAYERS, int(max_players)))
        self.host_pid = host_pid
        self.phase: str = "lobby"  # lobby | playing | finished

        # players: ordered list of {pid, name, avatar}. Order is seat order.
        self.players: List[Dict[str, str]] = []

        # hands: pid → list[Card]
        self.hands: Dict[str, List[dict]] = {}

        self.draw_pile: List[dict] = []
        self.discard: List[dict] = []     # top of discard is discard[-1]

        self.turn_idx: int = 0
        self.direction: int = 1           # +1 clockwise, -1 counter
        self.active_color: str = ""       # set after every play; for wilds, chosen

        # Stacked draw penalty waiting to be applied at the start of next
        # player's turn (classic non-stacking rule: applied immediately to
        # the NEXT player who must draw N and lose their turn). We keep it
        # simple and non-stackable: a +2 on top forces next player to draw 2
        # and skip; a +4 forces 4 and skip.
        self.draw_stack: int = 0

        # If the current player drew a card, they get exactly one chance to
        # play THAT drawn card if legal; otherwise they pass. This flag
        # gates that behavior.
        self.drawn_this_turn: bool = False
        # If a card was drawn but is unplayable, the player must explicitly
        # pass — this prevents endless drawing.
        self.must_draw_then_act: bool = False

        # Set of pids who currently have "Uno!" declared (have 1 card and
        # legally called it). If a player drops to 1 card without calling,
        # any other player tapping "Catch" would penalize them — we don't
        # implement catch in v1, just the call.
        self.uno_called: set = set()

        self.winner_pid: Optional[str] = None
        self.created_at = time.time()

    # ── lobby ──
    def add_player(self, pid: str, name: str, avatar: str) -> bool:
        if self.phase != "lobby":
            return False
        if any(p["pid"] == pid for p in self.players):
            return True  # already in
        if len(self.players) >= self.max_players:
            return False
        self.players.append({"pid": pid, "name": name, "avatar": avatar})
        return True

    def remove_player(self, pid: str) -> str:
        """Returns one of: 'noop', 'left_lobby', 'host_left_lobby',
        'left_mid_game', 'game_aborted', 'game_won_by_remaining'."""
        idx = next((i for i, p in enumerate(self.players)
                    if p["pid"] == pid), -1)
        if idx == -1:
            return "noop"

        if self.phase == "lobby":
            self.players.pop(idx)
            if pid == self.host_pid:
                # Promote next player to host, or close the lobby.
                if self.players:
                    self.host_pid = self.players[0]["pid"]
                    return "left_lobby"
                return "host_left_lobby"
            return "left_lobby"

        if self.phase == "playing":
            # Mid-game leave: return their cards to the bottom of the draw
            # pile (shuffled) so the deck still has roughly the right size,
            # then remove them from rotation.
            hand = self.hands.pop(pid, [])
            self.draw_pile = hand + self.draw_pile
            random.shuffle(self.draw_pile)
            self.uno_called.discard(pid)
            self.players.pop(idx)

            if len(self.players) < 2:
                # Only one player left → they win by default.
                if self.players:
                    self.winner_pid = self.players[0]["pid"]
                    self.phase = "finished"
                    return "game_won_by_remaining"
                self.phase = "finished"
                return "game_aborted"

            # Fix up turn index: if the leaver was before the current turn,
            # the indices shift left; if they WERE the current turn, the
            # turn passes to the same idx (which is now the next player).
            if idx < self.turn_idx:
                self.turn_idx -= 1
            self.turn_idx %= len(self.players)
            self.drawn_this_turn = False
            self.must_draw_then_act = False
            return "left_mid_game"

        # finished: just drop them from the listing.
        self.players.pop(idx)
        return "left_lobby"

    def can_start(self) -> bool:
        return (self.phase == "lobby"
                and len(self.players) >= MIN_PLAYERS
                and len(self.players) <= self.max_players)

    def is_full(self) -> bool:
        return len(self.players) >= self.max_players

    # ── deal ──
    def start(self) -> None:
        assert self.can_start()
        self.draw_pile = _build_deck()
        self.discard = []
        self.hands = {p["pid"]: [] for p in self.players}

        # Deal 7 cards each.
        for _ in range(INITIAL_HAND):
            for p in self.players:
                self.hands[p["pid"]].append(self.draw_pile.pop())

        # Flip first card. House rule: if first card is a +4, put it back
        # and reshuffle (per official Mattel rules). We also handle other
        # action openers reasonably: wild → first player picks color when
        # they take their turn (we randomize for simplicity here); +2 → first
        # player draws 2 and is skipped; skip → first player skipped;
        # reverse → direction flips before first move.
        while True:
            first = self.draw_pile.pop()
            if first["value"] == "+4":
                # Insert back into the pile and reshuffle.
                self.draw_pile.insert(0, first)
                random.shuffle(self.draw_pile)
                continue
            self.discard.append(first)
            break

        self.direction = 1
        self.turn_idx = 0
        self.draw_stack = 0
        self.drawn_this_turn = False
        self.must_draw_then_act = False
        self.uno_called.clear()
        self.winner_pid = None
        self.phase = "playing"

        # Resolve opener effects.
        top = self.discard[-1]
        if top["color"] == "w":  # wild opener — pick a random color
            self.active_color = random.choice(COLORS)
        else:
            self.active_color = top["color"]

        if top["value"] == "skip":
            self._advance_turn(skip=True)
        elif top["value"] == "rev":
            self.direction *= -1
            # With 2 players, reverse acts like skip. Otherwise just flip.
            if len(self.players) == 2:
                self._advance_turn(skip=True)
        elif top["value"] == "+2":
            self.draw_stack = 2  # applied on first player's turn-start

    # ── play helpers ──
    def _advance_turn(self, skip: bool = False) -> None:
        step = self.direction * (2 if skip else 1)
        self.turn_idx = (self.turn_idx + step) % len(self.players)
        self.drawn_this_turn = False
        self.must_draw_then_act = False

    def _refill_pile_if_needed(self) -> None:
        if self.draw_pile:
            return
        if len(self.discard) <= 1:
            # Nothing to reshuffle. Just leave the pile empty; subsequent
            # draws will be no-ops (caller checks).
            return
        top = self.discard[-1]
        rest = self.discard[:-1]
        # Wild cards in the discard lose their chosen color when reshuffled
        # back in — they become plain wilds again.
        for c in rest:
            if c["color"] == "w":
                # color stays "w"; nothing to reset for value
                pass
        random.shuffle(rest)
        self.draw_pile = rest
        self.discard = [top]

    def _draw_n(self, pid: str, n: int) -> List[dict]:
        drawn: List[dict] = []
        for _ in range(n):
            self._refill_pile_if_needed()
            if not self.draw_pile:
                break
            card = self.draw_pile.pop()
            self.hands[pid].append(card)
            drawn.append(card)
        # Drawing >1 means they got a forced penalty draw → they no longer
        # have a single card → uno status drops.
        if len(self.hands[pid]) != 1:
            self.uno_called.discard(pid)
        return drawn

    def current_pid(self) -> str:
        return self.players[self.turn_idx]["pid"]

    def apply_pending_draw_stack(self) -> Optional[dict]:
        """Called at the start of a turn. If a +2/+4 was just played, the
        current player must draw and lose their turn. Returns an event dict
        to broadcast, or None if no penalty was pending."""
        if self.draw_stack <= 0:
            return None
        pid = self.current_pid()
        n = self.draw_stack
        self._draw_n(pid, n)
        self.draw_stack = 0
        name = next(p["name"] for p in self.players if p["pid"] == pid)
        ev = {"kind": "penalty", "peer_id": pid, "n": n,
              "text": f"{name} drew {n} and is skipped"}
        self._advance_turn(skip=False)  # the draw itself ends their turn
        return ev

    # ── play card ──
    def play_card(self, pid: str, card_id: str,
                  chosen_color: str = "") -> Dict[str, Any]:
        """Returns: {ok: bool, error?: str, events?: [event dicts]}."""
        if self.phase != "playing":
            return {"ok": False, "error": "Game not in play"}
        if pid != self.current_pid():
            return {"ok": False, "error": "Not your turn"}
        if self.draw_stack > 0:
            return {"ok": False, "error": "You must draw first"}

        hand = self.hands.get(pid, [])
        idx = next((i for i, c in enumerate(hand) if c["id"] == card_id), -1)
        if idx == -1:
            return {"ok": False, "error": "Card not in hand"}
        card = hand[idx]
        top = self.discard[-1]

        if not _can_play(card, top, self.active_color):
            return {"ok": False, "error": "Card doesn't match"}

        # If they drew this turn, they're only allowed to play the just-drawn
        # card (the last card appended to their hand).
        if self.drawn_this_turn and idx != len(hand) - 1:
            return {"ok": False, "error": "Only the drawn card can be played"}

        # Wilds require a chosen color.
        if _is_wild(card):
            if chosen_color not in COLORS:
                return {"ok": False, "error": "Choose a color for the wild"}

        # Commit the play.
        hand.pop(idx)
        self.discard.append(card)
        if _is_wild(card):
            self.active_color = chosen_color
        else:
            self.active_color = card["color"]

        events: List[dict] = []
        name = next(p["name"] for p in self.players if p["pid"] == pid)
        events.append({
            "kind": "play", "peer_id": pid,
            "text": f"{name} played {_card_label(card)}"
                    + (f" → {_color_word(chosen_color)}"
                       if _is_wild(card) else ""),
        })

        # If their hand size is now 1 and they didn't call UNO already,
        # we just leave it — catching is a player-initiated action; v1 has
        # call only. (Adding catch later: any other player taps a "Catch"
        # button → if uno_called doesn't include them → penalty 2 cards.)
        if len(hand) == 1 and pid not in self.uno_called:
            # Auto-soft-flag: still legal, but visible. The client uses this
            # to highlight the UNO button.
            events.append({"kind": "almost_uno", "peer_id": pid,
                           "text": f"{name} has 1 card left!"})

        # Win check.
        if len(hand) == 0:
            self.winner_pid = pid
            self.phase = "finished"
            events.append({"kind": "win", "peer_id": pid,
                           "text": f"🏆 {name} wins!"})
            return {"ok": True, "events": events}

        # Apply card effect on the NEXT player.
        v = card["value"]
        if v == "skip":
            self._advance_turn(skip=True)
            events.append({"kind": "skip", "text": "Skipped!"})
        elif v == "rev":
            self.direction *= -1
            if len(self.players) == 2:
                # In 2-player, reverse = skip.
                self._advance_turn(skip=True)
                events.append({"kind": "reverse_skip",
                               "text": "Reverse (acts as skip)"})
            else:
                self._advance_turn(skip=False)
                events.append({"kind": "reverse", "text": "Direction reversed"})
        elif v == "+2":
            self.draw_stack = 2
            self._advance_turn(skip=False)
            events.append({"kind": "draw_pending",
                           "text": "Next player draws 2"})
        elif v == "+4":
            self.draw_stack = 4
            self._advance_turn(skip=False)
            events.append({"kind": "draw_pending",
                           "text": "Next player draws 4"})
        else:
            self._advance_turn(skip=False)

        return {"ok": True, "events": events}

    # ── draw / pass ──
    def draw_card(self, pid: str) -> Dict[str, Any]:
        if self.phase != "playing":
            return {"ok": False, "error": "Game not in play"}
        if pid != self.current_pid():
            return {"ok": False, "error": "Not your turn"}
        if self.draw_stack > 0:
            # Apply the stack penalty: draw N + skip turn.
            n = self.draw_stack
            drew = self._draw_n(pid, n)
            self.draw_stack = 0
            name = next(p["name"] for p in self.players if p["pid"] == pid)
            self._advance_turn(skip=False)
            return {"ok": True, "events": [
                {"kind": "penalty", "peer_id": pid, "n": n,
                 "text": f"{name} drew {n} and is skipped"}
            ], "drew": drew}
        if self.drawn_this_turn:
            return {"ok": False, "error": "You already drew this turn"}
        drew = self._draw_n(pid, 1)
        self.drawn_this_turn = True
        self.must_draw_then_act = True
        name = next(p["name"] for p in self.players if p["pid"] == pid)
        return {"ok": True, "events": [
            {"kind": "draw", "peer_id": pid, "text": f"{name} drew a card"}
        ], "drew": drew}

    def pass_turn(self, pid: str) -> Dict[str, Any]:
        if self.phase != "playing":
            return {"ok": False, "error": "Game not in play"}
        if pid != self.current_pid():
            return {"ok": False, "error": "Not your turn"}
        if not self.drawn_this_turn:
            return {"ok": False, "error": "Draw a card first"}
        self._advance_turn(skip=False)
        return {"ok": True, "events": []}

    def call_uno(self, pid: str) -> Dict[str, Any]:
        if self.phase != "playing":
            return {"ok": False, "error": "Game not in play"}
        if pid not in self.hands:
            return {"ok": False, "error": "Not a player"}
        if len(self.hands[pid]) != 1:
            return {"ok": False, "error": "Only callable at 1 card"}
        if pid in self.uno_called:
            return {"ok": False, "error": "Already called"}
        self.uno_called.add(pid)
        name = next(p["name"] for p in self.players if p["pid"] == pid)
        return {"ok": True, "events": [
            {"kind": "uno", "peer_id": pid, "text": f"UNO! — {name}"}
        ]}

    # ── snapshot for broadcast ──
    def public_state(self) -> dict:
        top = self.discard[-1] if self.discard else None
        return {
            "phase": self.phase,
            "host": self.host_pid,
            "players": [
                {"pid": p["pid"], "name": p["name"], "avatar": p["avatar"]}
                for p in self.players
            ],
            "max_players": self.max_players,
            "turn": (self.players[self.turn_idx]["pid"]
                     if self.phase == "playing" and self.players else ""),
            "direction": self.direction,
            "top_card": top,
            "color": self.active_color,
            "draw_pile_count": len(self.draw_pile),
            "discard_count": len(self.discard),
            "hand_counts": {p["pid"]: len(self.hands.get(p["pid"], []))
                            for p in self.players},
            "uno_called": list(self.uno_called),
            "draw_pending": self.draw_stack,
            "must_pass": self.must_draw_then_act,
            "winner": self.winner_pid,
        }


# ── labeling helpers (used in event texts) ──
def _color_word(c: str) -> str:
    return {"r": "Red", "y": "Yellow", "g": "Green",
            "b": "Blue", "w": "Wild"}.get(c, c)


def _card_label(card: dict) -> str:
    v = card["value"]
    if v == "wild":
        return "Wild"
    if v == "+4":
        return "Wild +4"
    if v == "skip":
        return f"{_color_word(card['color'])} Skip"
    if v == "rev":
        return f"{_color_word(card['color'])} Reverse"
    if v == "+2":
        return f"{_color_word(card['color'])} +2"
    return f"{_color_word(card['color'])} {v}"


# ── WS plumbing (called from main.py) ──────────────────────────────────────
# A "send" callable is passed in by main.py so this module stays decoupled
# from FastAPI. Signature:
#   await send(target_peer_id_or_None_for_broadcast, message_dict)
# When target is None, message is broadcast to every peer in room["peers"].

SendFn = Callable[[Optional[str], dict], Awaitable[None]]


async def _broadcast_state(room: dict, room_id: str, send: SendFn) -> None:
    g = games.get(room_id)
    if not g:
        return
    state = g.public_state()
    await send(None, {"type": "uno_state", "state": state})


async def _send_private_hands(room: dict, room_id: str, send: SendFn,
                              only_pid: Optional[str] = None) -> None:
    g = games.get(room_id)
    if not g or g.phase != "playing":
        return
    targets = [only_pid] if only_pid else list(g.hands.keys())
    for pid in targets:
        hand = g.hands.get(pid, [])
        await send(pid, {"type": "uno_hand", "hand": hand})


async def _emit_events(room: dict, room_id: str, send: SendFn,
                       events: List[dict]) -> None:
    for ev in events:
        await send(None, {"type": "uno_event", **ev})


async def handle_ws(room_id: str, peer_id: str, peer_name: str,
                    peer_avatar: str, room: dict, msg: dict,
                    send: SendFn) -> None:
    """Dispatch for any message whose type starts with 'uno_'. main.py
    forwards here. We do NOT touch room["peers"] or any non-uno state.
    """
    mt = msg.get("type", "")

    # ── lobby create ──
    if mt == "uno_create":
        if room_id in games and games[room_id].phase != "finished":
            await send(peer_id, {"type": "uno_error",
                                  "text": "A game is already running"})
            return
        try:
            n = int(msg.get("players", 2))
        except (TypeError, ValueError):
            n = 2
        if n < MIN_PLAYERS or n > MAX_PLAYERS:
            await send(peer_id, {"type": "uno_error",
                                  "text": f"Players must be {MIN_PLAYERS}-{MAX_PLAYERS}"})
            return
        g = Game(room_id, peer_id, n)
        games[room_id] = g
        g.add_player(peer_id, peer_name, peer_avatar)
        await _broadcast_state(room, room_id, send)
        await _emit_events(room, room_id, send, [{
            "kind": "lobby_open", "peer_id": peer_id,
            "text": f"{peer_name} opened an Uno lobby ({n} players)"
        }])
        return

    # All other commands need an existing game.
    g = games.get(room_id)
    if not g:
        await send(peer_id, {"type": "uno_error",
                              "text": "No active game. Create one first."})
        return

    if mt == "uno_join":
        if g.phase != "lobby":
            await send(peer_id, {"type": "uno_error",
                                  "text": "Game already started"})
            return
        ok = g.add_player(peer_id, peer_name, peer_avatar)
        if not ok:
            await send(peer_id, {"type": "uno_error",
                                  "text": "Lobby is full"})
            return
        await _broadcast_state(room, room_id, send)
        await _emit_events(room, room_id, send, [{
            "kind": "join", "peer_id": peer_id,
            "text": f"{peer_name} joined the Uno game"
        }])
        # Auto-start when full.
        if g.is_full():
            g.start()
            await _broadcast_state(room, room_id, send)
            await _send_private_hands(room, room_id, send)
            await _emit_events(room, room_id, send, [{
                "kind": "start", "text": "Game started!"
            }])
        return

    if mt == "uno_leave":
        result = g.remove_player(peer_id)
        if result == "noop":
            return
        if result == "host_left_lobby":
            del games[room_id]
            await send(None, {"type": "uno_closed",
                              "text": f"{peer_name} closed the Uno lobby"})
            return
        await _broadcast_state(room, room_id, send)
        if result == "left_lobby":
            await _emit_events(room, room_id, send, [{
                "kind": "leave", "peer_id": peer_id,
                "text": f"{peer_name} left the lobby"}])
        elif result == "left_mid_game":
            await _emit_events(room, room_id, send, [{
                "kind": "leave", "peer_id": peer_id,
                "text": f"{peer_name} left the game"}])
            await _send_private_hands(room, room_id, send)
        elif result == "game_won_by_remaining":
            winner = g.winner_pid
            wname = next((p["name"] for p in g.players
                          if p["pid"] == winner), "?")
            await _emit_events(room, room_id, send, [{
                "kind": "win", "peer_id": winner,
                "text": f"🏆 {wname} wins (others left)"}])
        elif result == "game_aborted":
            del games[room_id]
            await send(None, {"type": "uno_closed",
                              "text": "Game aborted — not enough players"})
        return

    if mt == "uno_start":
        if peer_id != g.host_pid:
            await send(peer_id, {"type": "uno_error",
                                  "text": "Only the host can start"})
            return
        if not g.can_start():
            await send(peer_id, {"type": "uno_error",
                                  "text": "Need at least 2 players"})
            return
        g.start()
        await _broadcast_state(room, room_id, send)
        await _send_private_hands(room, room_id, send)
        await _emit_events(room, room_id, send, [{
            "kind": "start", "text": "Game started!"
        }])
        return

    if mt == "uno_play":
        card_id = str(msg.get("card_id", ""))[:32]
        chosen = str(msg.get("chosen_color", ""))[:1]
        res = g.play_card(peer_id, card_id, chosen)
        if not res.get("ok"):
            await send(peer_id, {"type": "uno_error",
                                  "text": res.get("error", "Invalid play")})
            return
        await _broadcast_state(room, room_id, send)
        await _send_private_hands(room, room_id, send, only_pid=peer_id)
        await _emit_events(room, room_id, send, res.get("events", []))
        # Apply any pending draw stack at the start of the new turn — but
        # we do NOT auto-apply it; the next player must press DRAW. This
        # makes the +2/+4 visible to everyone and gives the victim a beat
        # to react. (Some Uno variants stack here; we don't, per user's
        # "classic Uno" request.)
        return

    if mt == "uno_draw":
        res = g.draw_card(peer_id)
        if not res.get("ok"):
            await send(peer_id, {"type": "uno_error",
                                  "text": res.get("error", "Can't draw")})
            return
        await _broadcast_state(room, room_id, send)
        await _send_private_hands(room, room_id, send, only_pid=peer_id)
        await _emit_events(room, room_id, send, res.get("events", []))
        return

    if mt == "uno_pass":
        res = g.pass_turn(peer_id)
        if not res.get("ok"):
            await send(peer_id, {"type": "uno_error",
                                  "text": res.get("error", "Can't pass")})
            return
        await _broadcast_state(room, room_id, send)
        await _emit_events(room, room_id, send, res.get("events", []))
        return

    if mt == "uno_call_uno":
        res = g.call_uno(peer_id)
        if not res.get("ok"):
            await send(peer_id, {"type": "uno_error",
                                  "text": res.get("error", "Can't call UNO")})
            return
        await _broadcast_state(room, room_id, send)
        await _emit_events(room, room_id, send, res.get("events", []))
        return

    # uno_close: host force-closes (useful for ending a finished game)
    if mt == "uno_close":
        if peer_id != g.host_pid and g.phase != "finished":
            await send(peer_id, {"type": "uno_error",
                                  "text": "Only the host can close"})
            return
        del games[room_id]
        await send(None, {"type": "uno_closed",
                          "text": "Uno game closed"})
        return


async def on_peer_leave(room_id: str, peer_id: str,
                        room: Optional[dict], send: SendFn) -> None:
    """Called from main.py's WS finally block. If the leaving peer was in
    the game, remove them and broadcast updates."""
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
        await send(None, {"type": "uno_closed",
                          "text": f"{leaver_name} (host) left — lobby closed"})
        return
    if result == "noop":
        return
    await _broadcast_state(room, room_id, send)
    if result == "game_aborted":
        del games[room_id]
        await send(None, {"type": "uno_closed",
                          "text": "Game aborted — not enough players"})
        return
    if result == "game_won_by_remaining":
        winner = g.winner_pid
        wname = next((p["name"] for p in g.players
                      if p["pid"] == winner), "?")
        await _emit_events(room, room_id, send, [{
            "kind": "win", "peer_id": winner,
            "text": f"🏆 {wname} wins (others left)"
        }])
        return
    await _emit_events(room, room_id, send, [{
        "kind": "leave", "peer_id": peer_id,
        "text": f"{leaver_name} left the Uno game"
    }])
    if result == "left_mid_game":
        await _send_private_hands(room, room_id, send)


def on_room_cleanup(room_id: str) -> None:
    """Called from main.py when a room is fully torn down."""
    if room_id in games:
        del games[room_id]
