# Chess Server (Node.js)

A pure JavaScript/Node.js port of the original Python (aiohttp + python-chess) chess
matchmaking server. Same HTTP API, same WebSocket protocol, same `index.html` — only
the backend runtime changed, so the existing front-end works unmodified.

## Stack
- **express** — HTTP routes (`/api/*`, static `/assets`, `/`)
- **ws** — WebSocket server (`/ws`)
- **chess.js** — move legality, check/checkmate/stalemate detection (replaces `python-chess`)
- **multer** — multipart avatar uploads

## Run it

```bash
npm install
npm start
# or: PORT=5050 node server.js
```

Then open http://localhost:5050

## Files
- `server.js` — HTTP routes + WebSocket wiring (equivalent of the old handlers + app setup)
- `matchmaker.js` — queueing, auth/session/reconnect logic, gem settlement (equivalent of `Matchmaker`)
- `chessGame.js` — per-game state, move timers, timeout/resign/checkmate handling (equivalent of `ChessGame`)
- `db.js` — JSON-file user database with an async mutex, matching the original's atomic-write-and-lock behavior (equivalent of `JSONUserDatabase`)

## Data
User accounts/gems/stats persist to `assets/database/chess_users.json`, written atomically
(temp file + rename) exactly like the Python version.

## Notes on the port
- All REST endpoints (`/api/register`, `/api/login`, `/api/token_login`, `/api/get_gems`,
  `/api/upload_avatar`, `/api/change_password`, `/api/delete_account`) behave identically,
  including status codes (401/409/400/500) and JSON shapes.
- The WebSocket message protocol (`auth`, `find_match`, `move`, `chat`, `resign`,
  `cancel_match`, `force_logout_confirm`, `ping`/`pong`) is unchanged.
- Moves are sent/received as UCI strings (e.g. `e2e4`, `e7e8q`), matching `chess.js`'s
  `.lan` move field, so no front-end changes were needed.
- Timers (60s per move, 3 missed moves = timeout loss), the 30s reconnect grace window,
  and the 60s "force logout" confirmation window all use the same durations as the original.
