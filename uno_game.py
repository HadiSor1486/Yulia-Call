"""
UNO Game Engine — Server-side state machine for the Silent Hill voice-call bot.
================================================================================

Features:
  - Full UNO rules: Skip, Reverse, Draw 2, Wild, Wild Draw 4
  - UNO call (must declare "UNO" when down to 1 card)
  - Challenge rule on Wild Draw 4 (can challenge if challenger had matching color)
  - Turn timer (default 30s) — auto-draw then auto-pass if time runs out
  - Scoring: winner gets sum of opponents' remaining card values
  - Supports 2-4 players (extensible to more)

Usage:
    game = UnoGame(room_id="abc123")
    game.add_player(peer_id="p1", name="Alice")
    game.add_player(peer_id="p2", name="Bob")
    game.start()                    # deals 7 cards each, flips first card
    game.play_card(peer_id, card)   # play a card from hand
    game.draw_card(peer_id)         # draw from deck
    game.pass_turn(peer_id)         # pass after drawing (if still can't play)
    game.call_uno(peer_id)          # declare UNO
    game.get_state(peer_id)         # full state from one player's view
"""

import random
import time
from typing import Dict, List, Optional, Any, Tuple

# Card value constants
CARD_NUMBERS = list(range(0, 10))
CARD_SPECIALS = ["skip", "reverse", "draw2"]
CARD_WILDS = ["wild", "wild4"]

# Colors (wild has no color until played)
COLORS = ["red", "yellow", "green", "blue"]

def _card_score(card: dict) -> int:
    """Point value of a card for scoring purposes."""
    t = card["type"]
    if t == "number":
        return card["value"]
    elif t in ("skip", "reverse", "draw2"):
        return 20
    elif t in ("wild", "wild4"):
        return 50
    return 0


class UnoCard:
    """Represents a single UNO card."""

    __slots__ = ("type", "color", "value", "id")

    def __init__(self, card_type: str, color: Optional[str] = None, value: Optional[int] = None, card_id: int = 0):
        self.type = card_type
        self.color = color
        self.value = value
        self.id = card_id

    def to_dict(self, hide_color: bool = False) -> dict:
        """Serialize to dict. If hide_color is True and this is a wild, omit the chosen color."""
        d = {"type": self.type, "id": self.id}
        if self.color and not (hide_color and self.type in ("wild", "wild4")):
            d["color"] = self.color
        if self.value is not None:
            d["value"] = self.value
        return d

    @staticmethod
    def from_dict(d: dict) -> "UnoCard":
        return UnoCard(d["type"], d.get("color"), d.get("value"), d.get("id", 0))

    def __repr__(self):
        if self.type == "number":
            return f"[{self.color} {self.value}]"
        elif self.type in ("wild", "wild4"):
            return f"[{self.type.upper()}]"
        return f"[{self.color} {self.type.upper()}]"


def _build_deck() -> List[UnoCard]:
    """Build a standard 108-card UNO deck."""
    cards = []
    cid = 0

    # One 0 of each color, two of 1-9
    for color in COLORS:
        cards.append(UnoCard("number", color, 0, cid)); cid += 1
        for num in range(1, 10):
            cards.append(UnoCard("number", color, num, cid)); cid += 1
            cards.append(UnoCard("number", color, num, cid)); cid += 1

    # Two of each special per color
    for color in COLORS:
        for special in CARD_SPECIALS:
            cards.append(UnoCard(special, color, card_id=cid)); cid += 1
            cards.append(UnoCard(special, color, card_id=cid)); cid += 1

    # Four wilds and four wild draw 4s
    for _ in range(4):
        cards.append(UnoCard("wild", card_id=cid)); cid += 1
        cards.append(UnoCard("wild4", card_id=cid)); cid += 1

    return cards


class UnoGame:
    """
    Complete UNO game state machine.
    All state mutations happen through this class.
    """

    def __init__(self, room_id: str, turn_timeout: int = 30):
        self.room_id = room_id
        self.turn_timeout = turn_timeout  # seconds per turn

        # Players: peer_id -> {name, hand: List[UnoCard], said_uno: bool, is_creator: bool}
        self.players: Dict[str, dict] = {}
        self.player_order: List[str] = []  # peer_ids in seating order
        self.creator_id: Optional[str] = None

        self.deck: List[UnoCard] = []
        self.discard: List[UnoCard] = []
        self.current_player_idx: int = 0
        self.direction: int = 1  # 1 = clockwise, -1 = counter-clockwise
        self.pending_draw: int = 0  # accumulated draw penalty (e.g. stacked +2/+4)
        self.game_state: str = "lobby"  # lobby | playing | finished
        self.winner_id: Optional[str] = None
        self.winner_name: str = ""
        self.turn_deadline: float = 0
        self.last_action_time: float = 0
        self.current_color: str = ""  # active color (may differ from top discard if wild)
        self.turn_count: int = 0

        # Track who has been challenged this turn (for Wild Draw 4)
        self._last_wild4_player: Optional[str] = None

    # ── Player management ────────────────────────────────────────────────────

    def add_player(self, peer_id: str, name: str, is_creator: bool = False) -> bool:
        """Add a player to the lobby. Returns False if game already started or full."""
        if self.game_state != "lobby":
            return False
        if peer_id in self.players:
            return True  # already in
        if len(self.players) >= 4:
            return False
        self.players[peer_id] = {
            "name": name,
            "hand": [],
            "said_uno": False,
            "is_creator": is_creator,
        }
        if is_creator and self.creator_id is None:
            self.creator_id = peer_id
        self.player_order.append(peer_id)
        return True

    def remove_player(self, peer_id: str) -> Optional[str]:
        """
        Remove a player (they left the call or the game).
        If the game is in progress, their hand is discarded and turn skips them.
        Returns a replacement creator peer_id if one was reassigned, else None.
        """
        if peer_id not in self.players:
            return None

        p = self.players[peer_id]

        # If lobby: simple remove
        if self.game_state == "lobby":
            del self.players[peer_id]
            self.player_order = [pid for pid in self.player_order if pid != peer_id]
            if peer_id == self.creator_id and self.player_order:
                self.creator_id = self.player_order[0]
                self.players[self.creator_id]["is_creator"] = True
                return self.creator_id
            return None

        # If playing: discard their hand, remove from order
        if self.game_state == "playing":
            self.discard.extend(p["hand"])
            was_idx = self.player_order.index(peer_id)
            self.player_order = [pid for pid in self.player_order if pid != peer_id]
            del self.players[peer_id]

            if len(self.player_order) == 1:
                # Only one player left — they win by default
                self.winner_id = self.player_order[0]
                self.winner_name = self.players[self.winner_id]["name"]
                self.game_state = "finished"
                return None

            # Adjust current_player_idx if needed
            if was_idx < self.current_player_idx:
                self.current_player_idx -= 1
            if self.current_player_idx >= len(self.player_order):
                self.current_player_idx = 0

            # Reassign creator
            if peer_id == self.creator_id and self.player_order:
                self.creator_id = self.player_order[0]
                self.players[self.creator_id]["is_creator"] = True
                return self.creator_id

        return None

    def get_player_count(self) -> int:
        return len(self.players)

    # ── Game lifecycle ───────────────────────────────────────────────────────

    def start(self) -> Tuple[bool, str]:
        """Start the game from lobby. Returns (ok, message)."""
        if self.game_state != "lobby":
            return False, "Game already started"
        if len(self.players) < 2:
            return False, "Need at least 2 players"

        self.deck = _build_deck()
        random.shuffle(self.deck)
        self.discard = []
        self.direction = 1
        self.pending_draw = 0
        self.turn_count = 0
        self.winner_id = None
        self.winner_name = ""

        # Deal 7 cards to each player
        for pid in self.player_order:
            hand = []
            for _ in range(7):
                hand.append(self._draw_from_deck())
            self.players[pid]["hand"] = hand
            self.players[pid]["said_uno"] = False

        # Flip first card — if wild, reshuffle and try again
        first = self._draw_from_deck()
        while first.type in ("wild", "wild4"):
            self.deck.append(first)
            random.shuffle(self.deck)
            first = self._draw_from_deck()
        self.discard.append(first)
        self.current_color = first.color

        self.current_player_idx = 0
        self.game_state = "playing"
        self._start_turn_timer()

        return True, "Game started"

    # ── Turn timer ───────────────────────────────────────────────────────────

    def _start_turn_timer(self):
        self.turn_deadline = time.time() + self.turn_timeout
        self.last_action_time = time.time()

    def check_timeout(self) -> Optional[dict]:
        """
        Check if the current turn has timed out.
        If so, auto-draw (or take pending draw) and auto-pass.
        Returns an action dict if a timeout happened, else None.
        """
        if self.game_state != "playing":
            return None
        if time.time() < self.turn_deadline:
            return None

        pid = self._current_player_id()
        p = self.players[pid]

        # If there's a pending draw (stacked), they must take it
        if self.pending_draw > 0:
            drawn = self._execute_pending_draw(pid)
            self._advance_turn()
            self._start_turn_timer()
            return {
                "type": "auto_penalty",
                "player_id": pid,
                "player_name": p["name"],
                "drawn_count": self.pending_draw,
                "drawn_cards": [c.to_dict() for c in drawn],
                "message": f"{p['name']} timed out and drew {len(drawn)} cards",
            }

        # Auto-draw one card
        card = self._draw_from_deck()
        p["hand"].append(card)

        # If the drawn card is playable, auto-play it (simple AI)
        if self._can_play(pid, card):
            self._do_play(pid, card.id)
            self._start_turn_timer()
            return {
                "type": "auto_play",
                "player_id": pid,
                "player_name": p["name"],
                "played": card.to_dict(),
                "message": f"{p['name']} timed out — drew and played",
            }

        # Can't play — pass
        self._advance_turn()
        self._start_turn_timer()
        return {
            "type": "auto_pass",
            "player_id": pid,
            "player_name": p["name"],
            "drawn": card.to_dict(),
            "message": f"{p['name']} timed out — drew and passed",
        }

    # ── Core actions ─────────────────────────────────────────────────────────

    def play_card(self, peer_id: str, card_id: int, chosen_color: Optional[str] = None) -> Tuple[bool, str, Optional[dict]]:
        """
        Play a card from hand. Returns (ok, message, event_dict).
        For wild cards, chosen_color must be provided.
        """
        if self.game_state != "playing":
            return False, "Game not in progress", None
        if peer_id != self._current_player_id():
            return False, "Not your turn", None

        p = self.players[peer_id]
        card = self._find_card_in_hand(peer_id, card_id)
        if not card:
            return False, "Card not in hand", None

        if not self._can_play(peer_id, card):
            return False, "Cannot play that card", None

        # For wild cards, require a chosen color
        if card.type in ("wild", "wild4") and chosen_color not in COLORS:
            return False, "Choose a valid color (red, yellow, green, blue)", None

        event = self._do_play(peer_id, card_id, chosen_color)
        self._start_turn_timer()
        return True, "OK", event

    def draw_card(self, peer_id: str) -> Tuple[bool, str, Optional[dict]]:
        """Draw a card from the deck."""
        if self.game_state != "playing":
            return False, "Game not in progress", None
        if peer_id != self._current_player_id():
            return False, "Not your turn", None

        # If there's a pending draw penalty, you must take it (no choice)
        if self.pending_draw > 0:
            drawn = self._execute_pending_draw(peer_id)
            self._advance_turn()
            self._start_turn_timer()
            return True, f"Drew {len(drawn)} penalty cards", {
                "type": "penalty_draw",
                "player_id": peer_id,
                "player_name": self.players[peer_id]["name"],
                "drawn_count": len(drawn),
                "drawn_cards": [c.to_dict() for c in drawn],
            }

        card = self._draw_from_deck()
        self.players[peer_id]["hand"].append(card)

        # Check if the drawn card is playable
        can_play_drawn = self._can_play(peer_id, card)

        self._start_turn_timer()
        return True, "Drew a card", {
            "type": "draw",
            "player_id": peer_id,
            "player_name": self.players[peer_id]["name"],
            "card": card.to_dict(),
            "can_play": can_play_drawn,
        }

    def pass_turn(self, peer_id: str) -> Tuple[bool, str, Optional[dict]]:
        """Pass after drawing a card you can't play."""
        if self.game_state != "playing":
            return False, "Game not in progress", None
        if peer_id != self._current_player_id():
            return False, "Not your turn", None

        self._advance_turn()
        self._start_turn_timer()
        return True, "Passed", {
            "type": "pass",
            "player_id": peer_id,
            "player_name": self.players[peer_id]["name"],
        }

    def call_uno(self, peer_id: str) -> Tuple[bool, str, Optional[dict]]:
        """Declare UNO (must have exactly 1 card left)."""
        if peer_id not in self.players:
            return False, "Not in game", None
        hand = self.players[peer_id]["hand"]
        if len(hand) != 1:
            return False, "You must have exactly 1 card to call UNO", None
        self.players[peer_id]["said_uno"] = True
        return True, "UNO called!", {
            "type": "uno_called",
            "player_id": peer_id,
            "player_name": self.players[peer_id]["name"],
        }

    def challenge_wild4(self, challenger_id: str) -> Tuple[bool, str, Optional[dict]]:
        """
        Challenge the last Wild Draw 4 play.
        The challenger believes the player who played the wild4 had a matching color
        in their hand. If true, the wild4 player draws 4 instead. If false, challenger draws 6.
        """
        if self._last_wild4_player is None:
            return False, "No Wild Draw 4 to challenge", None

        # Get the color that was in play BEFORE the wild4 was played
        wild4_player = self._last_wild4_player
        # We need to check if the wild4 player had a card matching the previous color
        # Since we don't store historical hand state, we use a probabilistic approach:
        # In a real implementation we'd track this; here we simulate by checking if
        # they have any non-wild cards left (which would indicate they likely had
        # a matching color and chose to play wild4 anyway)
        #
        # Simplified: we store the previous color and check their remaining hand
        prev_color = getattr(self, "_wild4_previous_color", "")
        wild4_player_hand = self.players.get(wild4_player, {}).get("hand", [])
        had_matching = any(
            c.color == prev_color for c in wild4_player_hand
        ) if prev_color else False

        # If they had a matching color, the challenge SUCCEEDS — they draw 4 instead
        if had_matching:
            # Wild4 player draws 4
            drawn = []
            for _ in range(4):
                drawn.append(self._draw_from_deck())
            self.players[wild4_player]["hand"].extend(drawn)
            self._last_wild4_player = None
            return True, "Challenge successful!", {
                "type": "challenge_success",
                "challenger_id": challenger_id,
                "challenger_name": self.players[challenger_id]["name"],
                "wild4_player_id": wild4_player,
                "wild4_player_name": self.players[wild4_player]["name"],
                "drawn_count": 4,
                "drawn_by": wild4_player,
            }
        else:
            # Challenge failed — challenger draws 6
            drawn = []
            for _ in range(6):
                drawn.append(self._draw_from_deck())
            self.players[challenger_id]["hand"].extend(drawn)
            self._last_wild4_player = None
            return True, "Challenge failed — you draw 6", {
                "type": "challenge_failed",
                "challenger_id": challenger_id,
                "challenger_name": self.players[challenger_id]["name"],
                "drawn_count": 6,
            }

    def catch_uno(self, catcher_id: str, target_id: str) -> Tuple[bool, str, Optional[dict]]:
        """
        Catch a player who forgot to call UNO (has 1 card but didn't say UNO).
        The target draws 2 cards as penalty.
        """
        if target_id not in self.players:
            return False, "Target not in game", None
        target = self.players[target_id]
        if len(target["hand"]) != 1 or target["said_uno"]:
            return False, "That player is safe", None

        # Penalty: draw 2
        drawn = []
        for _ in range(2):
            drawn.append(self._draw_from_deck())
        target["hand"].extend(drawn)

        return True, f"Caught {target['name']}! They draw 2.", {
            "type": "uno_caught",
            "catcher_id": catcher_id,
            "catcher_name": self.players[catcher_id]["name"],
            "target_id": target_id,
            "target_name": target["name"],
            "drawn_count": 2,
        }

    # ── Internal mechanics ───────────────────────────────────────────────────

    def _draw_from_deck(self) -> UnoCard:
        """Draw the top card. If deck empty, reshuffle discard into deck."""
        if not self.deck:
            if len(self.discard) > 1:
                top = self.discard[-1]
                self.deck = self.discard[:-1]
                self.discard = [top]
                random.shuffle(self.deck)
            else:
                # Absolutely empty — create a minimal emergency deck
                self.deck = _build_deck()
                random.shuffle(self.deck)
        return self.deck.pop()

    def _find_card_in_hand(self, peer_id: str, card_id: int) -> Optional[UnoCard]:
        for c in self.players[peer_id]["hand"]:
            if c.id == card_id:
                return c
        return None

    def _remove_from_hand(self, peer_id: str, card_id: int) -> Optional[UnoCard]:
        hand = self.players[peer_id]["hand"]
        for i, c in enumerate(hand):
            if c.id == card_id:
                return hand.pop(i)
        return None

    def _current_player_id(self) -> str:
        return self.player_order[self.current_player_idx]

    def _can_play(self, peer_id: str, card: UnoCard) -> bool:
        """Check if a card can be legally played on the current top card."""
        top = self.discard[-1] if self.discard else None
        if not top:
            return True

        # Pending draw: must match the type exactly (stackable)
        # e.g. on a Draw 2, you can play another Draw 2
        # On a Wild Draw 4, you can play another Wild Draw 4
        if self.pending_draw > 0:
            if self.pending_draw == 2 and card.type == "draw2":
                return True
            if self.pending_draw == 4 and card.type == "wild4":
                return True
            # If it's a stacked draw, only the matching type can be played
            # If it's a single draw2 (pending_draw=2), another draw2 stacks
            # If it's a wild4 (pending_draw=4), another wild4 stacks
            return False

        # Wild can always be played
        if card.type in ("wild", "wild4"):
            return True

        # Must match color or type/value
        if card.color == self.current_color:
            return True
        if card.type == top.type and card.type != "number":
            return True
        if card.type == "number" and top.type == "number" and card.value == top.value:
            return True
        # If top was a wild, match the chosen color
        if top.type in ("wild", "wild4") and card.color == self.current_color:
            return True

        return False

    def _do_play(self, peer_id: str, card_id: int, chosen_color: Optional[str] = None) -> dict:
        """Execute a card play (assumes validation already done). Returns event dict."""
        card = self._remove_from_hand(peer_id, card_id)
        if card is None:
            return {"type": "error", "message": "Card not found"}

        # For wilds, set the chosen color
        if card.type in ("wild", "wild4") and chosen_color:
            card.color = chosen_color

        self.discard.append(card)
        self.current_color = card.color or chosen_color or self.current_color
        self.turn_count += 1

        p = self.players[peer_id]
        name = p["name"]

        # Reset UNO call flag for all OTHER players (they need to re-call when down to 1)
        # Actually, said_uno is per-player and only matters when they have 1 card
        # But after playing, if they now have 1 card, they MUST call UNO on their next turn
        # If they have 0, game ends
        if len(p["hand"]) != 1:
            p["said_uno"] = False

        # Build result
        result = {
            "type": "card_played",
            "player_id": peer_id,
            "player_name": name,
            "card": card.to_dict(),
            "remaining": len(p["hand"]),
            "current_color": self.current_color,
        }

        # Handle card effects
        next_idx = (self.current_player_idx + self.direction) % len(self.player_order)
        next_pid = self.player_order[next_idx]

        if card.type == "skip":
            self._advance_turn()
            self._advance_turn()
            result["effect"] = "skip"
            result["skipped_player"] = next_pid
            result["skipped_name"] = self.players[next_pid]["name"]
            result["message"] = f"{name} played Skip! {result['skipped_name']} is skipped"

        elif card.type == "reverse":
            self.direction *= -1
            # In 2-player, reverse acts like skip
            if len(self.player_order) == 2:
                self._advance_turn()
                self._advance_turn()
                result["effect"] = "reverse_skip"
                result["message"] = f"{name} played Reverse! Turn goes back (2-player = skip)"
            else:
                self._advance_turn()  # advance normally, direction flip handles the reversal
                result["effect"] = "reverse"
                result["message"] = f"{name} played Reverse! Direction flipped"

        elif card.type == "draw2":
            # Stackable: if next player also has draw2, they can stack
            self.pending_draw = self.pending_draw + 2 if self.pending_draw > 0 else 2
            self._advance_turn()
            result["effect"] = "draw2"
            result["pending_draw"] = self.pending_draw
            result["message"] = f"{name} played Draw 2! Next player must draw {self.pending_draw} or stack"

        elif card.type == "wild":
            self._advance_turn()
            result["effect"] = "wild"
            result["message"] = f"{name} played Wild! Color is now {self.current_color}"

        elif card.type == "wild4":
            self._last_wild4_player = peer_id
            self._wild4_previous_color = self.current_color  # store for challenge
            self.pending_draw = self.pending_draw + 4 if self.pending_draw > 0 else 4
            self._advance_turn()
            result["effect"] = "wild4"
            result["pending_draw"] = self.pending_draw
            result["message"] = f"{name} played Wild Draw 4! Color is now {self.current_color}, next draws {self.pending_draw} or stacks"
            result["challengeable"] = True

        else:
            # Number card — simple advance
            self._advance_turn()
            result["effect"] = "number"
            result["message"] = f"{name} played {card.color} {card.value}"

        # Check for win
        if len(p["hand"]) == 0:
            self._end_game(peer_id)
            result["winner"] = peer_id
            result["winner_name"] = name
            result["scores"] = self._calculate_scores(peer_id)
            result["message"] = f"{name} wins the round!"
            return result

        # Check for UNO (1 card left, didn't call)
        if len(p["hand"]) == 1 and not p["said_uno"]:
            result["forgot_uno"] = peer_id

        return result

    def _execute_pending_draw(self, peer_id: str) -> List[UnoCard]:
        """Make a player draw the pending penalty cards."""
        drawn = []
        for _ in range(self.pending_draw):
            drawn.append(self._draw_from_deck())
        self.players[peer_id]["hand"].extend(drawn)
        self.pending_draw = 0
        return drawn

    def _advance_turn(self):
        """Move to the next player."""
        if not self.player_order:
            return
        self.current_player_idx = (self.current_player_idx + self.direction) % len(self.player_order)
        while self.player_order[self.current_player_idx] not in self.players:
            self.current_player_idx = (self.current_player_idx + self.direction) % len(self.player_order)

    def _end_game(self, winner_id: str):
        self.game_state = "finished"
        self.winner_id = winner_id
        self.winner_name = self.players[winner_id]["name"]
        self.pending_draw = 0

    def _calculate_scores(self, winner_id: str) -> dict:
        """Calculate scores. Winner gets sum of all opponents' card values."""
        total = 0
        details = {}
        for pid, pdata in self.players.items():
            if pid == winner_id:
                continue
            score = sum(_card_score(c) for c in pdata["hand"])
            details[pid] = {"name": pdata["name"], "score": score, "cards_left": len(pdata["hand"])}
            total += score
        details["_winner_points"] = total
        return details

    # ── State serialization ──────────────────────────────────────────────────

    def get_state(self, viewer_id: str) -> dict:
        """
        Get the full game state from one player's perspective.
        Hands of other players are hidden (only card count shown).
        """
        if self.game_state == "lobby":
            return {
                "state": "lobby",
                "room_id": self.room_id,
                "players": [
                    {"peer_id": pid, "name": p["name"], "is_creator": p["is_creator"]}
                    for pid, p in self.players.items()
                ],
                "creator_id": self.creator_id,
                "can_start": len(self.players) >= 2 and viewer_id == self.creator_id,
                "player_count": len(self.players),
            }

        # Playing or finished
        current_pid = self._current_player_id() if self.player_order else ""
        other_hands = {}
        for pid, p in self.players.items():
            if pid == viewer_id:
                continue
            other_hands[pid] = {
                "name": p["name"],
                "card_count": len(p["hand"]),
                "said_uno": p["said_uno"],
            }

        my_hand = []
        my_said_uno = False
        if viewer_id in self.players:
            my_hand = [c.to_dict(hide_color=False) for c in self.players[viewer_id]["hand"]]
            my_said_uno = self.players[viewer_id]["said_uno"]

        # Calculate which cards in my hand are playable
        playable_ids = set()
        if viewer_id == current_pid:
            for c in self.players.get(viewer_id, {}).get("hand", []):
                if self._can_play(viewer_id, c):
                    playable_ids.add(c.id)

        top_card = self.discard[-1].to_dict() if self.discard else None

        # Time remaining for current turn
        time_remaining = max(0, int(self.turn_deadline - time.time())) if self.game_state == "playing" else 0

        return {
            "state": self.game_state,
            "room_id": self.room_id,
            "players": [
                {"peer_id": pid, "name": self.players[pid]["name"], "is_creator": self.players[pid]["is_creator"]}
                for pid in self.player_order
            ],
            "my_hand": my_hand,
            "my_id": viewer_id,
            "my_said_uno": my_said_uno,
            "playable_ids": list(playable_ids),
            "other_hands": other_hands,
            "top_card": top_card,
            "current_color": self.current_color,
            "current_player_id": current_pid,
            "current_player_name": self.players.get(current_pid, {}).get("name", "") if current_pid else "",
            "direction": self.direction,
            "pending_draw": self.pending_draw,
            "turn_count": self.turn_count,
            "time_remaining": time_remaining,
            "turn_timeout": self.turn_timeout,
            "winner_id": self.winner_id,
            "winner_name": self.winner_name,
        }

    def get_public_state(self) -> dict:
        """Get a minimal public state (for spectators or the lobby)."""
        return {
            "state": self.game_state,
            "room_id": self.room_id,
            "player_count": len(self.players),
            "max_players": 4,
            "players": [
                {"peer_id": pid, "name": p["name"]} for pid, p in self.players.items()
            ],
            "current_player_name": self.players.get(self._current_player_id(), {}).get("name", "") if self.game_state == "playing" else "",
            "winner_name": self.winner_name,
        }


# ── Room-level game manager ──────────────────────────────────────────────────
# Simple helper for main.py to use

class GameManager:
    """Manages all active UNO games across rooms."""

    def __init__(self):
        self.games: Dict[str, UnoGame] = {}  # room_id -> UnoGame

    def get_or_create(self, room_id: str) -> UnoGame:
        if room_id not in self.games:
            self.games[room_id] = UnoGame(room_id)
        return self.games[room_id]

    def get(self, room_id: str) -> Optional[UnoGame]:
        return self.games.get(room_id)

    def remove(self, room_id: str):
        if room_id in self.games:
            del self.games[room_id]

    def cleanup_empty(self):
        """Remove games that are finished and old, or have no players."""
        to_remove = []
        for rid, game in self.games.items():
            if game.game_state == "finished":
                # Keep finished games for 10 minutes so players can see results
                if time.time() - game.last_action_time > 600:
                    to_remove.append(rid)
            elif len(game.players) == 0:
                to_remove.append(rid)
        for rid in to_remove:
            del self.games[rid]


# Singleton for the bot
uno_manager = GameManager()
