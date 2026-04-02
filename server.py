"""
Chess Server — Two‑Player Local Game
=====================================
Run this script, then open http://localhost:5050 in TWO browser windows.
The first player gets white, the second black. No extra setup needed.
"""

import asyncio
import json
import sys
import chess
from aiohttp import web
from pathlib import Path
import os

# Fix for Windows: use selector event loop to avoid proactor shutdown errors
if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())



class GameRoom:
    """Manages a single two‑player chess game."""
    def __init__(self):
        self.board = chess.Board()
        self.white = None   # WebSocket for white player
        self.black = None   # WebSocket for black player
        self.game_over = False

    @property
    def full(self):
        return self.white is not None and self.black is not None

    def add_player(self, ws):
        """Add a player, returns assigned colour or None if full."""
        if self.white is None:
            self.white = ws
            return "white"
        if self.black is None:
            self.black = ws
            return "black"
        return None

    def remove_player(self, ws):
        """Remove a player, reset game if anyone left."""
        if self.white == ws:
            self.white = None
        elif self.black == ws:
            self.black = None
        else:
            return

        # If there is still an opponent, close their connection
        other = self.white or self.black
        if other and not other.closed:
            try:
                # send a goodbye message then close the connection
                asyncio.create_task(other.send_json({
                    "type": "error",
                    "message": "Opponent disconnected. Refresh to start a new game."
                }))
                asyncio.create_task(other.close())
            except Exception:
                # ignore any errors when the socket is already closed
                pass

        # Reset the room for a fresh game
        self.board = chess.Board()
        self.white = None
        self.black = None
        self.game_over = False

    def other_player(self, ws):
        """Return the opponent's WebSocket, if any."""
        if self.white == ws:
            return self.black
        if self.black == ws:
            return self.white
        return None

    def broadcast(self, message, exclude=None):
        """Send a JSON message to both players (optional exclusion)."""
        for sock in (self.white, self.black):
            if sock and sock != exclude and not sock.closed:
                asyncio.create_task(sock.send_json(message))

    def state_packet(self, extra=None):
        """Build the current game state for clients."""
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
        return pkt

    def apply_move(self, move_uci, colour):
        """Try to make a move. Returns (success, error_message)."""
        if self.game_over:
            return False, "Game is already over."
        if colour != ("white" if self.board.turn == chess.WHITE else "black"):
            return False, "Not your turn."

        try:
            move = chess.Move.from_uci(move_uci)
            if move not in self.board.legal_moves:
                return False, "Illegal move."
            self.board.push(move)
        except Exception:
            return False, f"Invalid move: {move_uci}"

        # Check for game end
        if self.board.is_game_over():
            self.game_over = True
            result = self._get_result()
            self.broadcast(self.state_packet({"result": result}))
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
            return "Draw — 75‑move rule."
        if b.is_fivefold_repetition():
            return "Draw — Fivefold repetition."
        return "Game over."


# Global single game room
room = GameRoom()


async def websocket_handler(request):
    """Handle WebSocket connections."""
    ws = web.WebSocketResponse()
    await ws.prepare(request)

    # Assign colour (or reject if full)
    colour = room.add_player(ws)
    if colour is None:
        await ws.send_json({"type": "error", "message": "Game is full. Only two players allowed."})
        await ws.close()
        return ws

    # Welcome message
    await ws.send_json({
        "type": "welcome",
        "color": colour,
        "fen": room.board.fen(),
    })

    # If both players now present, start the game
    if room.full:
        room.broadcast(room.state_packet({
            "message": "Both players connected — White moves first.",
            "ready": True,
        }))

    # Message loop
    try:
        async for msg in ws:
            if msg.type == web.WSMsgType.TEXT:
                try:
                    data = json.loads(msg.data)
                except json.JSONDecodeError:
                    await ws.send_json({"type": "error", "message": "Invalid JSON."})
                    continue

                if data.get("type") == "move":
                    if not room.full:
                        await ws.send_json({"type": "error", "message": "Waiting for opponent."})
                        continue
                    success, err = room.apply_move(data.get("move", ""), colour)
                    if not success:
                        await ws.send_json({"type": "error", "message": err})
                elif data.get("type") == "chat":
                    text = data.get("text", "")[:200]
                    room.broadcast({
                        "type": "chat",
                        "from": colour,
                        "text": text,
                    })
                else:
                    await ws.send_json({"type": "error", "message": "Unknown message type."})
            elif msg.type == web.WSMsgType.ERROR:
                break
    finally:
        # Clean up on disconnect
        room.remove_player(ws)
    return ws


async def index_handler(request):
    """Serve the chess client page."""
    with open("index.html", "r", encoding="utf-8") as f:
        html = f.read()
    return web.Response(text=html, content_type='text/html')


app = web.Application()
assets_path = Path(__file__).parent / 'assets'
if assets_path.exists():
    app.router.add_static('/assets', str(assets_path))
    print("Serving static files from 'assets/'")
else:
    print("Warning: 'assets' folder not found – piece images will be missing.")
app.router.add_get('/', index_handler)
app.router.add_get('/ws', websocket_handler)

if __name__ == '__main__':
    WS_PORT = int(os.environ.get("PORT", 5050))
    print("\n" + "="*50)
    print("♞ CHESS SERVER READY")
    print("="*50)
    print(f"Open two browser windows at: http://localhost:{WS_PORT}")
    print("The first visitor gets white, the second gets black.")
    print("Only two players can play at the same time.")
    print("Press Ctrl+C to stop the server.")
    print("="*50 + "\n")
    
    
    web.run_app(app, host='0.0.0.0', port=WS_PORT)
    
    # https://cad1-2402-3a80-4354-4d3a-c08a-b5bf-26e4-47a7.ngrok-free.app/