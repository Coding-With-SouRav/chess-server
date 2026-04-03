import asyncio
import json
import sys
import random
import os
import re
import secrets
import base64
from pathlib import Path
from datetime import datetime
from aiohttp import web, WSMsgType
import chess

# Windows compatibility
if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


# ========================= JSON DATABASE =========================
class JSONUserDatabase:
    def __init__(self, json_path="chess_users.json"):
        self.json_path = json_path
        self._lock = asyncio.Lock()
        self.users = {}          # username -> user record
        self.token_to_username = {}  # session_token -> username
        self._next_id = 1
        self._load()

    def _load(self):
        """Load users from JSON file into memory."""
        if os.path.exists(self.json_path):
            try:
                with open(self.json_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.users = data.get('users', {})
                    self.token_to_username = {}
                    for username, record in self.users.items():
                        token = record.get('session_token')
                        if token:
                            self.token_to_username[token] = username
                        # Ensure numeric id is int
                        if 'id' in record:
                            if record['id'] >= self._next_id:
                                self._next_id = record['id'] + 1
            except (json.JSONDecodeError, IOError):
                self.users = {}
                self.token_to_username = {}
                self._next_id = 1
        else:
            self.users = {}
            self.token_to_username = {}
            self._next_id = 1

    async def _save(self):
        """Atomically save users to JSON file."""
        data = {
            'users': self.users,
            'next_id': self._next_id
        }
        # Write to temporary file then replace
        temp_path = self.json_path + '.tmp'
        with open(temp_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        os.replace(temp_path, self.json_path)

    def _generate_token(self):
        return secrets.token_urlsafe(32)

    async def register_user(self, username, password):
        """Register a new user. Returns (success, user_info, token, error)"""
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
                "id": user_id,
                "username": username,
                "password": password,
                "profile_pic": None,
                "session_token": token,
                "created_at": now,
                "last_login": now,
                "games_played": 0,
                "games_won": 0
            }
            self.users[username] = user_record
            self.token_to_username[token] = username
            await self._save()

            user_info = {
                "id": user_id,
                "username": username,
                "profile_pic": None,
                "created_at": now,
                "last_login": now
            }
            return True, user_info, token, None

    async def login_user(self, username, password):
        """Login user. Returns (success, user_info, token, error)"""
        async with self._lock:
            user = self.users.get(username)
            if not user:
                return False, None, None, "User not found."
            if user['password'] != password:
                return False, None, None, "Invalid password."

            # Generate new token
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
                "id": user['id'],
                "username": user['username'],
                "profile_pic": user['profile_pic'],
                "created_at": user['created_at'],
                "last_login": now
            }
            return True, user_info, new_token, None

    async def authenticate_token(self, token):
        """Validate session token. Returns (user_info, success)"""
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
                "id": user['id'],
                "username": user['username'],
                "profile_pic": user['profile_pic'],
                "created_at": user['created_at'],
                "last_login": user['last_login']
            }
            return user_info, True

    async def update_profile_pic(self, username, base64_data):
        """Update user's profile picture (base64 string)"""
        async with self._lock:
            user = self.users.get(username)
            if not user:
                return
            user['profile_pic'] = base64_data
            await self._save()

    async def update_game_stats(self, username, won=False):
        """Update user's game statistics"""
        async with self._lock:
            user = self.users.get(username)
            if not user:
                return
            user['games_played'] = user.get('games_played', 0) + 1
            if won:
                user['games_won'] = user.get('games_won', 0) + 1
            await self._save()


# ========================= GAME =========================
class ChessGame:
    def __init__(self, game_id, white_ws, black_ws, white_username, black_username):
        self.id = game_id
        self.white = white_ws
        self.black = black_ws
        self.white_username = white_username
        self.black_username = black_username
        self.board = chess.Board()
        self.game_over = False
        self.result = None

    def opponent(self, ws):
        return self.black if ws == self.white else self.white

    def opponent_username(self, ws):
        return self.black_username if ws == self.white else self.white_username

    def broadcast(self, message, exclude=None):
        for sock in (self.white, self.black):
            if sock and sock != exclude:
                asyncio.create_task(self.safe_send(sock, message))

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
    
    async def safe_send(self, ws, message):
        try:
            if ws and not ws.closed:
                await ws.send_json(message)
        except Exception:
            pass
        
    def apply_move(self, move_uci, colour):
        if self.game_over:
            return False, "Game already finished."

        if colour != ("white" if self.board.turn == chess.WHITE else "black"):
            return False, "Not your turn."

        try:
            move = chess.Move.from_uci(move_uci)
            if move not in self.board.legal_moves:
                return False, "Illegal move."
            self.board.push(move)
        except Exception:
            return False, f"Invalid move: {move_uci}"

        if self.board.is_game_over():
            self.game_over = True
            self.result = self._get_result()
            self.broadcast(self.state_packet({"result": self.result}))
        else:
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
        self.waiting = []  # List of (ws, username)
        self.games = {}
        self.ws_to_game = {}
        self.ws_color = {}
        self.ws_username = {}
        self.ws_token = {}
        self.next_game_id = 1
        self.db = JSONUserDatabase()
        self.active_connections = {}  # username -> ws (for duplicate prevention)
        self.ws_avatar = {}
        # New attributes for delayed cleanup (reconnection grace period)
        self.pending_disconnect = {}   # username -> asyncio.Task
        self.disconnect_time = {}      # username -> timestamp

    async def _safe_send_external(self, ws, message):
        try:
            if ws and not ws.closed:
                await ws.send_json(message)
        except:
            pass

    async def authenticate(self, ws, token):
        """Authenticate user using session token, with reconnection support"""
        user_info, success = await self.db.authenticate_token(token)
        self.ws_avatar[ws] = user_info.get("profile_pic") if user_info else None
        if not success:
            await ws.send_json({"type": "auth_error", "message": "Invalid or expired session. Please login again."})
            return False
        
        username = user_info["username"]

        # ---------- RECONNECTION LOGIC ----------
        # Cancel any pending cleanup for this user
        if username in self.pending_disconnect:
            self.pending_disconnect[username].cancel()
            del self.pending_disconnect[username]
            del self.disconnect_time[username]

        # Check if this user already has an active game but the old WebSocket died
        old_ws = self.active_connections.get(username)
        if old_ws and old_ws != ws:
            game = self.ws_to_game.get(old_ws)
            if game:
                # Determine which side this user is on
                colour = self.ws_color.get(old_ws)
                if colour == 'white':
                    game.white = ws
                else:
                    game.black = ws
                self.ws_to_game[ws] = game
                self.ws_color[ws] = colour
                self.ws_username[ws] = username
                self.ws_token[ws] = token
                self.active_connections[username] = ws
                self.ws_avatar[ws] = user_info.get("profile_pic")
                # Remove old ws mappings
                self.ws_to_game.pop(old_ws, None)
                self.ws_color.pop(old_ws, None)
                self.ws_username.pop(old_ws, None)
                self.ws_token.pop(old_ws, None)
                # Send current game state immediately
                await ws.send_json(game.state_packet())
                await ws.send_json({"type": "info", "message": "Reconnected to your game."})
                return True
        # ---------------------------------------

        # Check for duplicate active session (only if no game reconnection happened)
        if username in self.active_connections:
            existing_ws = self.active_connections[username]
            if existing_ws and not existing_ws.closed:
                await ws.send_json({"type": "auth_error", "message": "Username already logged in from another session."})
                return False
        
        # Store connection
        self.active_connections[username] = ws
        self.ws_username[ws] = username
        self.ws_token[ws] = token
        
        await ws.send_json({
            "type": "auth_success",
            "message": f"Welcome back {username}!",
            "user": user_info
        })
        return True

    async def add_to_queue(self, ws, username):
        # Prevent spam clicking
        for (q_ws, _) in self.waiting:
            if q_ws == ws:
                await ws.send_json({
                    "type": "info",
                    "message": "Already searching..."
                })
                return

        self.waiting.append((ws, username))

        # Send queue info
        await ws.send_json({
            "type": "info",
            "message": f"Searching... Players in queue: {len(self.waiting)}"
        })

        # Match players
        if len(self.waiting) >= 2:
            p1_ws, p1_name = self.waiting.pop(0)
            p2_ws, p2_name = self.waiting.pop(0)

            white, black = (p1_ws, p2_ws) if random.choice([True, False]) else (p2_ws, p1_ws)
            white_name, black_name = (p1_name, p2_name) if white == p1_ws else (p2_name, p1_name)

            game_id = self.next_game_id
            self.next_game_id += 1

            game = ChessGame(game_id, white, black, white_name, black_name)

            self.games[game_id] = game
            self.ws_to_game[white] = game
            self.ws_to_game[black] = game
            self.ws_color[white] = 'white'
            self.ws_color[black] = 'black'

            # Notify both players
            white_avatar = self.ws_avatar.get(white)
            black_avatar = self.ws_avatar.get(black)

            await white.send_json({
                "type": "match_start",
                "game_id": game_id,
                "color": "white",
                "fen": game.board.fen(),
                "opponent": black_name,
                "opponentAvatar": black_avatar
            })

            await black.send_json({
                "type": "match_start",
                "game_id": game_id,
                "color": "black",
                "fen": game.board.fen(),
                "opponent": white_name,
                "opponentAvatar": white_avatar
            })

            # Send initial state
            await white.send_json(game.state_packet())
            await black.send_json(game.state_packet())

    def remove_from_queue(self, ws):
        for i, (q_ws, _) in enumerate(self.waiting):
            if q_ws == ws:
                self.waiting.pop(i)
                return True
        return False

    async def _remove_user_from_game(self, username, ws):
        """Permanently remove a user from the game after grace period expires."""
        # Find the game that involved this user
        game = None
        for g in self.games.values():
            if g.white_username == username or g.black_username == username:
                game = g
                break
        if not game:
            return

        # Notify opponent that this player disconnected permanently
        opponent_ws = game.white if game.white_username != username else game.black
        if opponent_ws and not opponent_ws.closed:
            await self._safe_send_external(opponent_ws, {
                "type": "error",
                "message": f"{username} disconnected and did not return. Game over."
            })

        # Remove game completely
        for sock in (game.white, game.black):
            if sock:
                self.ws_to_game.pop(sock, None)
                self.ws_color.pop(sock, None)
        self.games.pop(game.id, None)

    async def cleanup_connection(self, ws):
        """Remove connection from all structures, with a 15-second grace period for game reconnection."""
        username = self.ws_username.get(ws)
        if not username:
            return

        # Remove from queue if present
        self.remove_from_queue(ws)

        # If the user is in a game, start a 15-second timer before permanent removal
        game = self.ws_to_game.get(ws)
        if game:
            async def delayed_cleanup():
                await asyncio.sleep(15)
                # If still pending (user did not reconnect), remove permanently
                if username in self.pending_disconnect:
                    del self.pending_disconnect[username]
                    del self.disconnect_time[username]
                    await self._remove_user_from_game(username, ws)

            task = asyncio.create_task(delayed_cleanup())
            self.pending_disconnect[username] = task
            self.disconnect_time[username] = asyncio.get_event_loop().time()

        # Remove from active connections (so new login is allowed, but game state remains)
        if self.active_connections.get(username) == ws:
            del self.active_connections[username]

        # Clean up other maps
        self.ws_username.pop(ws, None)
        self.ws_token.pop(ws, None)
        self.ws_avatar.pop(ws, None)

        # Note: ws_to_game and ws_color are NOT removed immediately to allow reconnection.
        # They will be cleaned up by the delayed task if reconnection never happens.

    async def handle_message(self, ws, data):
        msg_type = data.get("type")
        
        # ===== AUTHENTICATION (must be first) =====
        if ws not in self.ws_username:
            if msg_type == "auth":
                token = data.get("token", "")
                await self.authenticate(ws, token)
            else:
                await ws.send_json({"type": "auth_error", "message": "Please authenticate first."})
            return
        
        username = self.ws_username[ws]
        
        # ===== WAITING STATE =====
        if any(q_ws == ws for q_ws, _ in self.waiting):
            if msg_type == "cancel_match":
                if self.remove_from_queue(ws):
                    await ws.send_json({
                        "type": "info",
                        "message": "Matchmaking cancelled."
                    })
                else:
                    await ws.send_json({
                        "type": "error",
                        "message": "Not in queue."
                    })
            else:
                await ws.send_json({
                    "type": "error",
                    "message": "Still searching. Cancel first."
                })
            return

        # ===== IN GAME =====
        game = self.ws_to_game.get(ws)
        if game:
            colour = self.ws_color[ws]

            if msg_type == "move":
                move = data.get("move", "")
                success, err = game.apply_move(move, colour)
                if not success:
                    await ws.send_json({"type": "error", "message": err})

            elif msg_type == "chat":
                text = data.get("text", "")[:200]
                game.broadcast({
                    "type": "chat",
                    "from": colour,
                    "text": text
                })

            else:
                await ws.send_json({
                    "type": "error",
                    "message": "Invalid action in game."
                })

        # ===== IDLE (authenticated but not in game/queue) =====
        else:
            if msg_type == "find_match":
                await self.add_to_queue(ws, username)
            else:
                await ws.send_json({
                    "type": "error",
                    "message": "Send 'find_match' to start searching."
                })


# ========================= HTTP HANDLERS =========================
matchmaker = Matchmaker()

async def register_handler(request):
    try:
        data = await request.json()
        username = data.get('username', '').strip()
        password = data.get('password', '')
        success, user_info, token, error = await matchmaker.db.register_user(username, password)
        if success:
            return web.json_response({
                "success": True,
                "user": user_info,
                "token": token
            })
        else:
            return web.json_response({"success": False, "message": error}, status=400)
    except Exception as e:
        return web.json_response({"success": False, "message": str(e)}, status=500)

async def login_handler(request):
    try:
        data = await request.json()
        username = data.get('username', '').strip()
        password = data.get('password', '')
        success, user_info, token, error = await matchmaker.db.login_user(username, password)
        if success:
            return web.json_response({
                "success": True,
                "user": user_info,
                "token": token
            })
        else:
            return web.json_response({"success": False, "message": error}, status=401)
    except Exception as e:
        return web.json_response({"success": False, "message": str(e)}, status=500)

async def token_login_handler(request):
    try:
        data = await request.json()
        token = data.get('token', '')
        user_info, success = await matchmaker.db.authenticate_token(token)
        if success:
            return web.json_response({
                "success": True,
                "user": user_info,
                "token": token
            })
        else:
            return web.json_response({"success": False, "message": "Invalid token"}, status=401)
    except Exception as e:
        return web.json_response({"success": False, "message": str(e)}, status=500)

async def upload_avatar_handler(request):
    # Check Authorization header
    auth_header = request.headers.get('Authorization', '')
    token = None
    if auth_header.startswith('Bearer '):
        token = auth_header[7:]
    if not token:
        return web.json_response({"success": False, "message": "Missing token"}, status=401)
    
    user_info, valid = await matchmaker.db.authenticate_token(token)
    if not valid:
        return web.json_response({"success": False, "message": "Invalid token"}, status=401)
    
    # Process file upload
    reader = await request.multipart()
    field = await reader.next()
    if field.name != 'avatar':
        return web.json_response({"success": False, "message": "Expected field 'avatar'"}, status=400)
    
    content = await field.read()
    # Size limit removed – allow any size (up to the global client_max_size, set to 50 MB)
    # Convert to base64 data URL
    mime_type = field.headers.get('Content-Type', 'image/png')
    if mime_type not in ['image/png', 'image/jpeg', 'image/jpg']:
        mime_type = 'image/png'
    b64_data = f"data:{mime_type};base64," + base64.b64encode(content).decode('utf-8')
    
    await matchmaker.db.update_profile_pic(user_info['username'], b64_data)
    
    return web.json_response({
        "success": True,
        "profile_pic": b64_data
    })

async def websocket_handler(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)

    # ----- HEARTBEAT PING/PONG to keep connection alive -----
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
    # --------------------------------------------------------

    try:
        async for msg in ws:
            if msg.type == WSMsgType.TEXT:
                try:
                    data = json.loads(msg.data)
                except:
                    await ws.send_json({"type": "error", "message": "Invalid JSON"})
                    continue

                await matchmaker.handle_message(ws, data)

    finally:
        heartbeat_task.cancel()
        await matchmaker.cleanup_connection(ws)

    return ws

async def index_handler(request):
    with open("index.html", "r", encoding="utf-8") as f:
        return web.Response(text=f.read(), content_type='text/html')


# ========================= SERVER SETUP =========================
# Increase maximum request body size to 50 MB (allows large avatar uploads)
app = web.Application(client_max_size=50 * 1024 * 1024)

# Add static assets route
assets_path = Path(__file__).parent / 'assets'
if assets_path.exists():
    app.router.add_static('/assets', str(assets_path))

# API routes
app.router.add_post('/api/register', register_handler)
app.router.add_post('/api/login', login_handler)
app.router.add_post('/api/token_login', token_login_handler)
app.router.add_post('/api/upload_avatar', upload_avatar_handler)

# WebSocket and main page
app.router.add_get('/', index_handler)
app.router.add_get('/ws', websocket_handler)


# ========================= RUN =========================
if __name__ == '__main__':
    PORT = int(os.environ.get("PORT", 5050))

    print("\n" + "="*50)
    print("♞ CHESS MATCHMAKING SERVER READY")
    print("="*50)
    print(f"Open: http://localhost:{PORT}")
    print("Users register/login with username + password (plain text)")
    print("Auto-login via session token (stored in browser)")
    print("Profile pictures supported (upload in menu) – size limit removed (max 50 MB)")
    print("Click 'Find Match' to play")
    print("Cancel supported ✔")
    print("Heartbeat enabled – connections stay alive")
    print("15-second reconnection grace period")
    print("Data stored in chess_users.json (no SQLite)")
    print("="*50 + "\n")

    web.run_app(app, host='0.0.0.0', port=PORT)
