import asyncio
import json
import sys
import random
import os
import re
import secrets
import base64
from pathlib import Path
from datetime import datetime, date
from aiohttp import web, WSMsgType, ClientConnectionResetError
import chess

# Windows compatibility
if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


# ========================= JSON DATABASE =========================
class JSONUserDatabase:
    def __init__(self, json_path="assets/database/chess_users.json"):
        self.json_path = json_path
        self._lock = asyncio.Lock()
        self.users = {}
        self.token_to_username = {}
        self._next_id = 1
        self._load()

    def _load(self):
        if os.path.exists(self.json_path):
            try:
                with open(self.json_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.users = data.get('users', {})
                    self.token_to_username = {}
                    for username, record in self.users.items():
                        if 'gems' not in record:
                            record['gems'] = 100
                        if 'last_daily_award' not in record:
                            record['last_daily_award'] = None
                        token = record.get('session_token')
                        if token:
                            self.token_to_username[token] = username
                        if 'id' in record and record['id'] >= self._next_id:
                            self._next_id = record['id'] + 1
            except (json.JSONDecodeError, IOError):
                self.users = {}
                self.token_to_username = {}
                self._next_id = 1
        else:
            self.users = {}
            self.token_to_username = {}
            self._next_id = 1

    async def delete_user(self, username):
        """Delete user from database completely."""
        async with self._lock:
            user = self.users.pop(username, None)
            if not user:
                return False
            token = user.get('session_token')
            if token:
                self.token_to_username.pop(token, None)
            await self._save()
            return True
        
    async def change_password(self, username, current_password, new_password):
        async with self._lock:
            user = self.users.get(username)
            if not user:
                return False, "User not found"
            if user['password'] != current_password:
                return False, "Current password is incorrect"
            if len(new_password) < 4:
                return False, "New password must be at least 4 characters"
            user['password'] = new_password
            await self._save()
            return True, "Password updated successfully"
        
    async def get_user_stats(self, username):
        async with self._lock:
            user = self.users.get(username)
            if not user:
                return {"games_played": 0, "games_won": 0, "win_percentage": 0}
            played = user.get("games_played", 0)
            won = user.get("games_won", 0)
            win_percentage = (won / played * 100) if played > 0 else 0
            return {
                "games_played": played,
                "games_won": won,
                "win_percentage": round(win_percentage, 1)
            }
            
    async def _save(self):
        data = {'users': self.users, 'next_id': self._next_id}
        temp_path = self.json_path + '.tmp'
        with open(temp_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        os.replace(temp_path, self.json_path)

    def _generate_token(self):
        return secrets.token_urlsafe(32)

    async def register_user(self, username, password):
        if not re.match(r'^[a-zA-Z0-9_]{3,20}$', username):
            return False, None, None, "Invalid username format."
        if len(password) < 4:
            return False, None, None, "Password must be at least 4 characters."
        async with self._lock:
            if username in self.users:
                return False, None, None, "Username already exists."
            token = self._generate_token()
            now = datetime.now().isoformat()
            user_id = self._next_id
            self._next_id += 1
            user_record = {
                "id": user_id, "username": username, "password": password,
                "profile_pic": None, "session_token": token,
                "created_at": now, "last_login": now,
                "games_played": 0, "games_won": 0,
                "gems": 100,
                "last_daily_award": None
            }
            self.users[username] = user_record
            self.token_to_username[token] = username
            await self._save()
            user_info = {
                "id": user_id, "username": username, "profile_pic": None,
                "created_at": now, "last_login": now
            }
            return True, user_info, token, None

    async def login_user(self, username, password):
        async with self._lock:
            user = self.users.get(username)
            if not user:
                return False, None, None, "User not found."
            if user['password'] != password:
                return False, None, None, "Invalid password."
            new_token = self._generate_token()
            old_token = user['session_token']
            if old_token:
                self.token_to_username.pop(old_token, None)
            user['session_token'] = new_token
            now = datetime.now().isoformat()
            user['last_login'] = now
            self.token_to_username[new_token] = username
            await self._save()
            user_info = {
                "id": user['id'], "username": user['username'],
                "profile_pic": user['profile_pic'],
                "created_at": user['created_at'], "last_login": now
            }
            return True, user_info, new_token, None

    async def authenticate_token(self, token):
        if not token:
            return None, False
        async with self._lock:
            username = self.token_to_username.get(token)
            if not username:
                return None, False
            user = self.users.get(username)
            if not user or user['session_token'] != token:
                return None, False
            user_info = {
                "id": user['id'], "username": user['username'],
                "profile_pic": user['profile_pic'],
                "created_at": user['created_at'], "last_login": user['last_login']
            }
            return user_info, True

    async def update_profile_pic(self, username, base64_data):
        async with self._lock:
            user = self.users.get(username)
            if user:
                user['profile_pic'] = base64_data
                await self._save()

    async def update_game_stats(self, username, won=False):
        async with self._lock:
            user = self.users.get(username)
            if user:
                user['games_played'] = user.get('games_played', 0) + 1
                if won:
                    user['games_won'] = user.get('games_won', 0) + 1
                await self._save()

    async def get_user_gems(self, username):
        async with self._lock:
            user = self.users.get(username)
            return user.get('gems', 0) if user else 0

    async def deduct_gems(self, username, amount):
        async with self._lock:
            user = self.users.get(username)
            if not user or user.get('gems', 0) < amount:
                return False
            user['gems'] = user.get('gems', 0) - amount
            await self._save()
            return True

    async def add_gems(self, username, amount):
        async with self._lock:
            user = self.users.get(username)
            if user:
                user['gems'] = user.get('gems', 0) + amount
                await self._save()

    async def award_daily_gems(self, username):
        async with self._lock:
            user = self.users.get(username)
            if not user:
                return False, 0
            today = date.today()
            last_award = user.get('last_daily_award')
            if last_award:
                last_date = datetime.fromisoformat(last_award).date()
                if last_date >= today:
                    return False, user.get('gems', 0)
            user['gems'] = user.get('gems', 0) + 50
            user['last_daily_award'] = datetime.now().isoformat()
            await self._save()
            return True, user['gems']


class ChessGame:
    def __init__(self, game_id, white_ws, black_ws, white_username, black_username, stake, matchmaker_ref):
        self.id = game_id
        self.white = white_ws
        self.black = black_ws
        self.white_username = white_username
        self.black_username = black_username
        self.stake = stake
        self.board = chess.Board()
        self.game_over = False
        self.result = None
        self.matchmaker = matchmaker_ref

        # Missed move counters (consecutive timeouts per player)
        self.missed_white = 0
        self.missed_black = 0

        # Timer tasks for each player (None = no active timer)
        self.white_timer_task = None
        self.black_timer_task = None
        self.TIMEOUT_SECONDS = 60

        # Start initial timer (White moves first)
        self._start_timer('white')
        
    async def resign(self, ws):
        """Player with WebSocket `ws` resigns → opponent wins."""
        if self.game_over:
            return False, "Game already finished."

        # Determine who resigned and who wins
        if ws == self.white:
            loser = self.white_username
            winner = self.black_username
        elif ws == self.black:
            loser = self.black_username
            winner = self.white_username
        else:
            return False, "Unknown player"

        self.game_over = True
        self.result = f"{winner} wins by resignation"
        self._cancel_all_timers()

        # Broadcast the final result
        self.broadcast(self.state_packet({"result": self.result}))

        # Settle gem transfers (winner takes both stakes)
        await self.matchmaker.settle_game_gems(self)

        # Remove game from matchmaker
        self.matchmaker.remove_game(self.id)
        return True, None

    async def force_resign_by_username(self, username):
        """Force resignation of a specific player (used for account deletion)."""
        if self.game_over:
            return False, "Game already finished."
        if username == self.white_username:
            loser = self.white_username
            winner = self.black_username
        elif username == self.black_username:
            loser = self.black_username
            winner = self.white_username
        else:
            return False, "Username not in this game"
        
        self.game_over = True
        self.result = f"{winner} wins (opponent account deleted)"
        self._cancel_all_timers()
        self.broadcast(self.state_packet({"result": self.result}))
        await self.matchmaker.settle_game_gems(self)   # winner gets full stake
        self.matchmaker.remove_game(self.id)
        return True, winner
    
    def opponent(self, ws):
        return self.black if ws == self.white else self.white

    def opponent_username(self, ws):
        return self.black_username if ws == self.white else self.white_username

    def broadcast(self, message, exclude=None):
        for sock in (self.white, self.black):
            if sock and sock != exclude:
                asyncio.create_task(self._safe_send(sock, message))

    def state_packet(self, extra=None):
        pkt = {
            "type": "state",
            "fen": self.board.fen(),
            "turn": "white" if self.board.turn == chess.WHITE else "black",
            "legal_moves": [m.uci() for m in self.board.legal_moves],
            "in_check": self.board.is_check(),
            "game_over": self.game_over,
        }
        if extra:
            pkt.update(extra)
        if self.result:
            pkt["result"] = self.result
        return pkt

    async def _safe_send(self, ws, message):
        try:
            if ws and not ws.closed:
                await ws.send_json(message)
        except Exception:
            pass

    def _start_timer(self, color):
        """Cancel any existing timer for this color and start a new one."""
        self._cancel_timer(color)
        if self.game_over:
            return

        async def timeout():
            await asyncio.sleep(self.TIMEOUT_SECONDS)
            if not self.game_over:
                await self._on_timeout(color)

        task = asyncio.create_task(timeout())
        if color == 'white':
            self.white_timer_task = task
        else:
            self.black_timer_task = task

    def _cancel_timer(self, color):
        task = self.white_timer_task if color == 'white' else self.black_timer_task
        if task and not task.done():
            task.cancel()
        if color == 'white':
            self.white_timer_task = None
        else:
            self.black_timer_task = None

    def _cancel_all_timers(self):
        self._cancel_timer('white')
        self._cancel_timer('black')

    async def _on_timeout(self, color):
        """Called when a player's 60 seconds expire.
        - If this is the 3rd consecutive timeout by this player → they lose.
        - Otherwise, switch turn to opponent and continue.
        """
        if self.game_over:
            return

        # Increment missed counter for this player
        if color == 'white':
            self.missed_white += 1
            missed_count = self.missed_white
            opponent_color = 'black'
            opponent_username = self.black_username
        else:
            self.missed_black += 1
            missed_count = self.missed_black
            opponent_color = 'white'
            opponent_username = self.white_username

        # Cancel the expired timer (already done by the timeout task, but ensure)
        self._cancel_timer(color)

        # If player missed 3 times in a row → game over, opponent wins
        if missed_count >= 3:
            loser = self.white_username if color == 'white' else self.black_username
            winner = self.black_username if color == 'white' else self.white_username
            result = f"{winner} wins by timeout (inactive player)"

            self.game_over = True
            self.result = result

            # Broadcast final result
            self.broadcast(self.state_packet({"result": self.result}))
            self._cancel_all_timers()

            # Settle gem transfers (winner takes both stakes)
            await self.matchmaker.settle_game_gems(self)
            self.matchmaker.remove_game(self.id)  # Remove finished game
            return

        # --- Not game over: just switch turn to opponent ---
        # Flip the board's turn (the player who missed loses their turn)
        self.board.turn = not self.board.turn   # chess.WHITE <-> chess.BLACK

        # Cancel opponent's timer (if any) and start a fresh one for them
        self._cancel_timer(opponent_color)
        self._start_timer(opponent_color)

        # Broadcast an informational message and the updated state
        msg = f"{color.capitalize()} missed the move. {opponent_color.capitalize()}'s turn."
        self.broadcast({"type": "info", "message": msg})
        self.broadcast(self.state_packet())

    async def apply_move(self, move_uci, colour):
        """Manual move from a player."""
        if self.game_over:
            return False, "Game already finished."
        if colour != ("white" if self.board.turn == chess.WHITE else "black"):
            return False, "Not your turn."

        try:
            move = chess.Move.from_uci(move_uci)
            if move not in self.board.legal_moves:
                return False, "Illegal move."

            # Cancel timer for the player who just moved
            self._cancel_timer(colour)

            # Reset missed counter for the moving player
            if colour == 'white':
                self.missed_white = 0
            else:
                self.missed_black = 0

            # Apply the move
            self.board.push(move)
        except Exception:
            return False, f"Invalid move: {move_uci}"

        # Check for game over
        if self.board.is_game_over():
            self.game_over = True
            self.result = self._get_result()
            self._cancel_all_timers()
            self.broadcast(self.state_packet({"result": self.result}))
            await self.matchmaker.settle_game_gems(self)
            self.matchmaker.remove_game(self.id)  # Remove finished game
        else:
            # Start timer for the opponent
            next_color = "white" if self.board.turn == chess.WHITE else "black"
            self._start_timer(next_color)
            self.broadcast(self.state_packet())

        return True, None

    def _get_result(self):
        b = self.board
        if b.is_checkmate():
            winner = "Black" if b.turn == chess.WHITE else "White"
            return f"{winner} wins by checkmate!"
        if b.is_stalemate():
            return "Draw — Stalemate."
        if b.is_insufficient_material():
            return "Draw — Insufficient material."
        if b.is_seventyfive_moves():
            return "Draw — 75-move rule."
        if b.is_fivefold_repetition():
            return "Draw — Fivefold repetition."
        return "Game over."


# ========================= MATCHMAKER =========================
class Matchmaker:
    def __init__(self):
        self.waiting = []           # each entry: (ws, username, stake)
        self.games = {}
        self.ws_to_game = {}
        self.ws_color = {}
        self.ws_username = {}
        self.ws_token = {}
        self.next_game_id = 1
        self.db = JSONUserDatabase()
        self.active_connections = {}
        self.ws_avatar = {}
        self.pending_disconnect = {}
        self.disconnect_time = {}
        self.pending_confirmations = {}

    def remove_game(self, game_id):
        game = self.games.pop(game_id, None)
        if game:
            # Remove WebSocket mappings
            for ws in (game.white, game.black):
                if ws:
                    self.ws_to_game.pop(ws, None)
                    self.ws_color.pop(ws, None)

    async def _safe_send_external(self, ws, message):
        try:
            if ws and not ws.closed:
                await ws.send_json(message)
        except Exception:
            pass

    async def _force_disconnect_user(self, ws, username, reason="logged in elsewhere"):
        self.remove_from_queue(ws)
        if username in self.pending_disconnect:
            self.pending_disconnect[username].cancel()
            del self.pending_disconnect[username]
            del self.disconnect_time[username]
        game = self.ws_to_game.get(ws)
        if game:
            opponent_ws = game.white if game.white_username != username else game.black
            if opponent_ws and not opponent_ws.closed:
                await self._safe_send_external(opponent_ws, {
                    "type": "error",
                    "message": f"{username} disconnected ({reason}). Game over."
                })
            for sock in (game.white, game.black):
                if sock:
                    self.ws_to_game.pop(sock, None)
                    self.ws_color.pop(sock, None)
            self.games.pop(game.id, None)
        if self.active_connections.get(username) == ws:
            del self.active_connections[username]
        self.ws_username.pop(ws, None)
        self.ws_token.pop(ws, None)
        self.ws_avatar.pop(ws, None)
        self.ws_to_game.pop(ws, None)
        self.ws_color.pop(ws, None)
        if not ws.closed:
            await ws.close(code=4001, message=reason)

    async def request_confirmation_or_replace(self, ws, username, token, user_info):
        old_ws = self.active_connections.get(username)
        if old_ws and old_ws != ws and not old_ws.closed:
            conf_id = secrets.token_urlsafe(16)
            future = asyncio.Future()
            self.pending_confirmations[conf_id] = (username, token, user_info, future)
            await self._safe_send_external(old_ws, {
                "type": "force_logout_request",
                "confirmation_id": conf_id,
                "message": "Another device is trying to log into your account. Allow it?"
            })
            try:
                confirmed = await asyncio.wait_for(future, timeout=60.0)
            except asyncio.TimeoutError:
                confirmed = False
            self.pending_confirmations.pop(conf_id, None)
            if not confirmed:
                return False, None, None, "Login rejected by active session."
        return True, token, user_info, None

    async def replace_session(self, username, new_token, user_info):
        async with self.db._lock:
            user = self.db.users.get(username)
            if user:
                old_token = user.get('session_token')
                if old_token:
                    self.db.token_to_username.pop(old_token, None)
                user['session_token'] = new_token
                user['last_login'] = datetime.now().isoformat()
                self.db.token_to_username[new_token] = username
                await self.db._save()

    async def authenticate(self, ws, token):
        if ws.closed:
            return False

        user_info, success = await self.db.authenticate_token(token)
        self.ws_avatar[ws] = user_info.get("profile_pic") if user_info else None
        if not success:
            if not ws.closed:
                await ws.send_json({"type": "auth_error", "message": "Invalid or expired session. Please login again."})
            return False

        username = user_info["username"]

        # --- Check for pending disconnect and resume game ---
        if username in self.pending_disconnect:
            # Cancel the scheduled removal
            task = self.pending_disconnect.pop(username)
            task.cancel()
            self.disconnect_time.pop(username, None)

            # Find the existing game
            game = None
            for g in self.games.values():
                if g.white_username == username or g.black_username == username:
                    game = g
                    break

            if game and game.game_over:
                # Game already finished – clean up and continue normal auth
                self.remove_game(game.id)
                # Remove pending disconnect entry already popped above
                # Fall through to normal authentication
            elif game:
                # Determine the player's color
                color = 'white' if game.white_username == username else 'black'
                # Update mapping: replace old ws with new ws
                old_ws = game.white if color == 'white' else game.black
                if old_ws:
                    self.ws_to_game.pop(old_ws, None)
                    self.ws_color.pop(old_ws, None)
                if color == 'white':
                    game.white = ws
                else:
                    game.black = ws
                self.ws_to_game[ws] = game
                self.ws_color[ws] = color

                # Update active connection
                self.active_connections[username] = ws
                self.ws_username[ws] = username
                self.ws_token[ws] = token

                # Send resume info without versus animation
                opponent_name = game.black_username if color == 'white' else game.white_username
                opponent_avatar = self.ws_avatar.get(game.white if color == 'black' else game.black)
                await ws.send_json({
                    "type": "resume_game",
                    "color": color,
                    "fen": game.board.fen(),
                    "opponent": opponent_name,
                    "opponentAvatar": opponent_avatar,
                    "game_id": game.id,
                    "your_gems": await self.db.get_user_gems(username)
                })
                # Send full state
                await ws.send_json(game.state_packet())
                stats = await self.db.get_user_stats(username)
                await ws.send_json({"type": "user_stats", "stats": stats})
                return True
            # else (no game found) – fall through to normal auth

        # --- Existing check for already logged in elsewhere ---
        old_ws = self.active_connections.get(username)
        if old_ws and old_ws != ws and not old_ws.closed:
            if not ws.closed:
                await ws.send_json({
                    "type": "auth_error",
                    "message": "Account already logged in on another device or tab."
                })
            return False

        # Normal authentication (no pending game)
        self.active_connections[username] = ws
        self.ws_username[ws] = username
        self.ws_token[ws] = token

        awarded, new_balance = await self.db.award_daily_gems(username)
        if not ws.closed:
            await ws.send_json({
                "type": "auth_success",
                "message": f"Welcome back {username}!",
                "user": user_info
            })
            await ws.send_json({"type": "gem_balance", "balance": new_balance})
            stats = await self.db.get_user_stats(username)
            await ws.send_json({"type": "user_stats", "stats": stats})
        return True

    async def delete_account(self, username):
        """Permanently delete user account, handling active games and queue."""
        # Remove from waiting queue
        for i, (q_ws, q_name, _) in enumerate(self.waiting):
            if q_name == username:
                self.waiting.pop(i)
                break
        
        # Handle active game
        game = None
        for g in self.games.values():
            if g.white_username == username or g.black_username == username:
                game = g
                break
        if game:
            await game.force_resign_by_username(username)
        
        # Remove from active connections
        ws = self.active_connections.pop(username, None)
        if ws:
            self.ws_username.pop(ws, None)
            self.ws_token.pop(ws, None)
            self.ws_avatar.pop(ws, None)
            # Close WebSocket with reason
            if not ws.closed:
                await ws.close(code=4002, message="Account deleted")
        
        # Cancel any pending disconnect task
        if username in self.pending_disconnect:
            self.pending_disconnect[username].cancel()
            del self.pending_disconnect[username]
            del self.disconnect_time[username]
        
        # Delete from database
        success = await self.db.delete_user(username)
        return success
    
    def remove_from_queue(self, ws):
        for i, (q_ws, _, _) in enumerate(self.waiting):
            if q_ws == ws:
                self.waiting.pop(i)
                return True
        return False

    async def add_to_queue(self, ws, username, stake):
        for (q_ws, _, q_stake) in self.waiting:
            if q_ws == ws:
                await ws.send_json({"type": "info", "message": "Already searching..."})
                return

        gems = await self.db.get_user_gems(username)
        if gems < stake:
            await ws.send_json({
                "type": "error",
                "message": f"❌ You need {stake} gems to play this stake. You have {gems} gems."
            })
            return

        self.waiting.append((ws, username, stake))
        await ws.send_json({
            "type": "info",
            "message": f"Searching for {stake} gems stake... Players in queue: {len(self.waiting)}"
        })

        matched = None
        for i, (q_ws, q_name, q_stake) in enumerate(self.waiting):
            if q_ws == ws:
                continue
            if q_stake == stake:
                matched = (q_ws, q_name, q_stake, i)
                break

        if matched:
            p2_ws, p2_name, p2_stake, idx2 = matched
            self.waiting.pop(idx2)
            self.remove_from_queue(ws)

            async with self.db._lock:
                white_gems = self.db.users.get(username, {}).get('gems', 0)
                black_gems = self.db.users.get(p2_name, {}).get('gems', 0)
                if white_gems < stake or black_gems < stake:
                    await ws.send_json({"type": "error", "message": "Match cancelled: insufficient gems."})
                    await p2_ws.send_json({"type": "error", "message": "Match cancelled: insufficient gems."})
                    return
                self.db.users[username]['gems'] -= stake
                self.db.users[p2_name]['gems'] -= stake
                await self.db._save()
                new_white_gems = self.db.users[username]['gems']
                new_black_gems = self.db.users[p2_name]['gems']

            white_ws, black_ws = (ws, p2_ws) if random.choice([True, False]) else (p2_ws, ws)
            white_name, black_name = (username, p2_name) if white_ws == ws else (p2_name, username)

            game_id = self.next_game_id
            self.next_game_id += 1
            game = ChessGame(game_id, white_ws, black_ws, white_name, black_name, stake, self)
            self.games[game_id] = game
            self.ws_to_game[white_ws] = game
            self.ws_to_game[black_ws] = game
            self.ws_color[white_ws] = 'white'
            self.ws_color[black_ws] = 'black'
            white_avatar = self.ws_avatar.get(white_ws)
            black_avatar = self.ws_avatar.get(black_ws)

            await white_ws.send_json({
                "type": "match_start", "game_id": game_id, "color": "white",
                "fen": game.board.fen(), "opponent": black_name, "opponentAvatar": black_avatar,
                "your_gems": new_white_gems
            })
            await black_ws.send_json({
                "type": "match_start", "game_id": game_id, "color": "black",
                "fen": game.board.fen(), "opponent": white_name, "opponentAvatar": white_avatar,
                "your_gems": new_black_gems
            })
            await white_ws.send_json(game.state_packet())
            await black_ws.send_json(game.state_packet())

    async def settle_game_gems(self, game):
        white = game.white_username
        black = game.black_username
        stake = game.stake
        result = game.result

        if not game.game_over:
            return

        # Helper to detect winner from result string
        def get_winner_loser(result):
            if "wins by" in result:
                winner_str = result.split(" wins by")[0].strip()
                if winner_str == white or winner_str == "White":
                    return white, black
                elif winner_str == black or winner_str == "Black":
                    return black, white
            return None, None

        winner, loser = get_winner_loser(result)

        if winner is not None:
            # Winner takes both stakes
            await self.db.update_game_stats(winner, won=True)
            await self.db.update_game_stats(loser, won=False)
            await self.db.add_gems(winner, 2 * stake)
        elif "Draw" in result or "Stalemate" in result or "Insufficient material" in result:
            # Draw: each gets stake back
            await self.db.update_game_stats(white, won=False)
            await self.db.update_game_stats(black, won=False)
            await self.db.add_gems(white, stake)
            await self.db.add_gems(black, stake)
        else:
            # Fallback (should not happen) – treat as draw
            print(f"Unrecognized game result: {result}")
            await self.db.update_game_stats(white, won=False)
            await self.db.update_game_stats(black, won=False)
            await self.db.add_gems(white, stake)
            await self.db.add_gems(black, stake)

        # Send updated balances and stats to both players
        new_white = await self.db.get_user_gems(white)
        new_black = await self.db.get_user_gems(black)
        stats_white = await self.db.get_user_stats(white)
        stats_black = await self.db.get_user_stats(black)

        for ws, bal, stats in [(game.white, new_white, stats_white), (game.black, new_black, stats_black)]:
            if ws and not ws.closed:
                await ws.send_json({"type": "gem_balance", "balance": bal})
                await ws.send_json({"type": "user_stats", "stats": stats})
                    
    async def _remove_user_from_game(self, username, ws):
        game = None
        for g in self.games.values():
            if g.white_username == username or g.black_username == username:
                game = g
                break
        if not game:
            return
        # Only send error if the game is not already over
        if not game.game_over:
            opponent_ws = game.white if game.white_username != username else game.black
            if opponent_ws and not opponent_ws.closed:
                await self._safe_send_external(opponent_ws, {
                    "type": "error", "message": f"{username} disconnected and did not return. Game over."
                })
        for sock in (game.white, game.black):
            if sock:
                self.ws_to_game.pop(sock, None)
                self.ws_color.pop(sock, None)
        self.games.pop(game.id, None)

    async def cleanup_connection(self, ws):
        username = self.ws_username.get(ws)
        if not username:
            return
        self.remove_from_queue(ws)
        game = self.ws_to_game.get(ws)
        if game:
            # If game already over, clean up immediately
            if game.game_over:
                await self._remove_user_from_game(username, ws)
                if self.active_connections.get(username) == ws:
                    del self.active_connections[username]
                self.ws_username.pop(ws, None)
                self.ws_token.pop(ws, None)
                self.ws_avatar.pop(ws, None)
                return

            async def delayed_cleanup():
                await asyncio.sleep(30)   # changed from 15 to 30 seconds
                if username in self.pending_disconnect:
                    del self.pending_disconnect[username]
                    del self.disconnect_time[username]
                    # If the user didn't reconnect, declare opponent win
                    game = self.ws_to_game.get(ws)
                    if game and not game.game_over:
                        # Determine opponent
                        if game.white_username == username:
                            opponent_username = game.black_username
                            winner = "Black"
                        else:
                            opponent_username = game.white_username
                            winner = "White"
                        game.game_over = True
                        game.result = f"{winner} wins by opponent disconnection"
                        # Broadcast to both players (if opponent still connected)
                        game.broadcast(game.state_packet({"result": game.result}))
                        # Cancel timers
                        game._cancel_all_timers()
                        # Settle gems
                        await self.settle_game_gems(game)
                        self.remove_game(game.id)  # Remove finished game
                    # Clean up mappings
                    await self._remove_user_from_game(username, ws)
            task = asyncio.create_task(delayed_cleanup())
            self.pending_disconnect[username] = task
            self.disconnect_time[username] = asyncio.get_event_loop().time()
        if self.active_connections.get(username) == ws:
            del self.active_connections[username]
        self.ws_username.pop(ws, None)
        self.ws_token.pop(ws, None)
        self.ws_avatar.pop(ws, None)

    async def handle_message(self, ws, data):
        msg_type = data.get("type")

        if msg_type == "ping":
            if not ws.closed:
                await ws.send_json({"type": "pong"})
            return

        if ws not in self.ws_username:
            if msg_type == "auth":
                token = data.get("token", "")
                await self.authenticate(ws, token)
            else:
                if not ws.closed:
                    await ws.send_json({"type": "auth_error", "message": "Please authenticate first."})
            return

        username = self.ws_username[ws]

        if msg_type == "force_logout_confirm":
            conf_id = data.get("confirmation_id")
            confirm = data.get("confirm", False)
            if conf_id in self.pending_confirmations:
                entry_username, new_token, user_info, future = self.pending_confirmations[conf_id]
                if not future.done():
                    future.set_result(confirm)
                if confirm:
                    try:
                        if not ws.closed:
                            await ws.send_json({"type": "info", "message": "Confirmation received. Logging out..."})
                    except Exception:
                        pass
                    await self._force_disconnect_user(ws, username, "logged in elsewhere")
                else:
                    if not ws.closed:
                        await ws.send_json({"type": "info", "message": "Login rejected."})
            else:
                if not ws.closed:
                    await ws.send_json({"type": "error", "message": "Invalid confirmation ID"})
            return

        for (q_ws, _, _) in self.waiting:
            if q_ws == ws:
                if msg_type == "cancel_match":
                    if self.remove_from_queue(ws):
                        await ws.send_json({"type": "info", "message": "Matchmaking cancelled."})
                    else:
                        await ws.send_json({"type": "error", "message": "Not in queue."})
                else:
                    await ws.send_json({"type": "error", "message": "Still searching. Cancel first."})
                return

        game = self.ws_to_game.get(ws)
        if game:
            colour = self.ws_color[ws]
            if msg_type == "move":
                move = data.get("move", "")
                success, err = await game.apply_move(move, colour)
                if not success and not ws.closed:
                    await ws.send_json({"type": "error", "message": err})
                # Note: gems are already settled inside apply_move when game ends
            elif msg_type == "chat":
                text = data.get("text", "")[:200]
                game.broadcast({"type": "chat", "from": colour, "text": text})
            elif msg_type == "resign":
                success, err = await game.resign(ws)
                if not success and not ws.closed:
                    await ws.send_json({"type": "error", "message": err})
            else:
                if not ws.closed:
                    await ws.send_json({"type": "error", "message": "Invalid action in game."})
        else:
            if msg_type == "find_match":
                stake = data.get("stake")
                if not stake or not isinstance(stake, int) or stake <= 0:
                    await ws.send_json({"type": "error", "message": "Invalid stake amount."})
                    return
                await self.add_to_queue(ws, username, stake)
            elif msg_type == "get_gems":
                gems = await self.db.get_user_gems(username)
                await ws.send_json({"type": "gem_balance", "balance": gems})
            else:
                await ws.send_json({"type": "error", "message": "Send 'find_match' with stake to start searching."})


# ========================= HTTP HANDLERS =========================
matchmaker = Matchmaker()

async def register_handler(request):
    try:
        data = await request.json()
        username = data.get('username', '').strip()
        password = data.get('password', '')
        success, user_info, token, error = await matchmaker.db.register_user(username, password)
        if success:
            return web.json_response({"success": True, "user": user_info, "token": token})
        else:
            return web.json_response({"success": False, "message": error}, status=400)
    except Exception as e:
        return web.json_response({"success": False, "message": str(e)}, status=500)

async def delete_account_handler(request):
    auth_header = request.headers.get('Authorization', '')
    token = None
    if auth_header.startswith('Bearer '):
        token = auth_header[7:]
    if not token:
        return web.json_response({"success": False, "message": "Missing token"}, status=401)
    
    user_info, valid = await matchmaker.db.authenticate_token(token)
    if not valid:
        return web.json_response({"success": False, "message": "Invalid token"}, status=401)
    
    username = user_info['username']
    success = await matchmaker.delete_account(username)
    if success:
        return web.json_response({"success": True, "message": "Account permanently deleted."})
    else:
        return web.json_response({"success": False, "message": "Account deletion failed."}, status=500)

async def login_handler(request):
    try:
        data = await request.json()
        username = data.get('username', '').strip()
        password = data.get('password', '')
        success, user_info, token, error = await matchmaker.db.login_user(username, password)
        if not success:
            return web.json_response({"success": False, "message": error}, status=401)

        old_ws = matchmaker.active_connections.get(username)
        if old_ws and not old_ws.closed:
            return web.json_response({
                "success": False,
                "message": "Already logged in on another device. Please logout from there first."
            }, status=409)

        return web.json_response({"success": True, "user": user_info, "token": token})
    except Exception as e:
        return web.json_response({"success": False, "message": str(e)}, status=500)

async def token_login_handler(request):
    try:
        data = await request.json()
        token = data.get('token', '')
        user_info, success = await matchmaker.db.authenticate_token(token)
        if not success:
            return web.json_response({"success": False, "message": "Invalid token"}, status=401)

        username = user_info['username']
        old_ws = matchmaker.active_connections.get(username)
        if old_ws and not old_ws.closed:
            return web.json_response({
                "success": False,
                "message": "Already logged in on another device. Please logout from there first."
            }, status=409)

        return web.json_response({"success": True, "user": user_info, "token": token})
    except Exception as e:
        return web.json_response({"success": False, "message": str(e)}, status=500)

async def get_gems_handler(request):
    auth_header = request.headers.get('Authorization', '')
    token = None
    if auth_header.startswith('Bearer '):
        token = auth_header[7:]
    if not token:
        return web.json_response({"success": False, "message": "Missing token"}, status=401)
    user_info, valid = await matchmaker.db.authenticate_token(token)
    if not valid:
        return web.json_response({"success": False, "message": "Invalid token"}, status=401)
    gems = await matchmaker.db.get_user_gems(user_info['username'])
    return web.json_response({"balance": gems})

async def upload_avatar_handler(request):
    auth_header = request.headers.get('Authorization', '')
    token = None
    if auth_header.startswith('Bearer '):
        token = auth_header[7:]
    if not token:
        return web.json_response({"success": False, "message": "Missing token"}, status=401)
    user_info, valid = await matchmaker.db.authenticate_token(token)
    if not valid:
        return web.json_response({"success": False, "message": "Invalid token"}, status=401)
    reader = await request.multipart()
    field = await reader.next()
    if field.name != 'avatar':
        return web.json_response({"success": False, "message": "Expected field 'avatar'"}, status=400)
    content = await field.read()
    mime_type = field.headers.get('Content-Type', 'image/png')
    if mime_type not in ['image/png', 'image/jpeg', 'image/jpg']:
        mime_type = 'image/png'
    b64_data = f"data:{mime_type};base64," + base64.b64encode(content).decode('utf-8')
    await matchmaker.db.update_profile_pic(user_info['username'], b64_data)
    return web.json_response({"success": True, "profile_pic": b64_data})

async def change_password_handler(request):
    auth_header = request.headers.get('Authorization', '')
    token = None
    if auth_header.startswith('Bearer '):
        token = auth_header[7:]
    if not token:
        return web.json_response({"success": False, "message": "Missing token"}, status=401)

    user_info, valid = await matchmaker.db.authenticate_token(token)
    if not valid:
        return web.json_response({"success": False, "message": "Invalid token"}, status=401)

    try:
        data = await request.json()
        current_password = data.get('currentPassword', '')
        new_password = data.get('newPassword', '')
        confirm_password = data.get('confirmPassword', '')

        if new_password != confirm_password:
            return web.json_response({"success": False, "message": "New passwords do not match"}, status=400)

        success, message = await matchmaker.db.change_password(
            user_info['username'], current_password, new_password
        )
        if success:
            return web.json_response({"success": True, "message": message})
        else:
            return web.json_response({"success": False, "message": message}, status=400)
    except Exception as e:
        return web.json_response({"success": False, "message": str(e)}, status=500)
    
async def websocket_handler(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)

    async def heartbeat(ws):
        while True:
            await asyncio.sleep(30)
            if ws.closed:
                break
            try:
                await ws.ping()
            except:
                break

    heartbeat_task = asyncio.create_task(heartbeat(ws))
    try:
        async for msg in ws:
            if msg.type == WSMsgType.TEXT:
                try:
                    data = json.loads(msg.data)
                except:
                    if not ws.closed:
                        await ws.send_json({"type": "error", "message": "Invalid JSON"})
                    continue
                try:
                    await matchmaker.handle_message(ws, data)
                except ClientConnectionResetError:
                    pass
                except Exception as e:
                    print(f"Error in handle_message: {e}")
    finally:
        heartbeat_task.cancel()
        await matchmaker.cleanup_connection(ws)
    return ws

async def index_handler(request):
    with open("index.html", "r", encoding="utf-8") as f:
        return web.Response(text=f.read(), content_type='text/html')


# ========================= SERVER SETUP =========================
app = web.Application(client_max_size=50 * 1024 * 1024)
assets_path = Path(__file__).parent / 'assets'
if assets_path.exists():
    app.router.add_static('/assets', str(assets_path))
app.router.add_post('/api/register', register_handler)
app.router.add_post('/api/login', login_handler)
app.router.add_post('/api/token_login', token_login_handler)
app.router.add_get('/api/get_gems', get_gems_handler)
app.router.add_post('/api/upload_avatar', upload_avatar_handler)
app.router.add_post('/api/change_password', change_password_handler)
app.router.add_post('/api/delete_account', delete_account_handler)
app.router.add_get('/', index_handler)
app.router.add_get('/ws', websocket_handler)

if __name__ == '__main__':
    PORT = int(os.environ.get("PORT", 5050))
    print(f"Open: http://localhost:{PORT}")
    web.run_app(app, host='0.0.0.0', port=PORT)