'use strict';

const crypto = require('crypto');
const { JSONUserDatabase } = require('./db');
const { ChessGame, safeSend } = require('./chessGame');

const RECONNECT_GRACE_MS = 30 * 1000;
const FORCE_LOGOUT_TIMEOUT_MS = 60 * 1000;

class Matchmaker {
  constructor() {
    this.waiting = []; // [{ ws, username, stake }]
    this.games = new Map(); // gameId -> ChessGame
    this.wsToGame = new Map();
    this.wsColor = new Map();
    this.wsUsername = new Map();
    this.wsToken = new Map();
    this.nextGameId = 1;
    this.db = new JSONUserDatabase();
    this.activeConnections = new Map(); // username -> ws
    this.wsAvatar = new Map();
    this.pendingDisconnect = new Map(); // username -> timeout handle
    this.disconnectTime = new Map();
    this.pendingConfirmations = new Map(); // confId -> { username, token, userInfo, resolve }
  }

  removeGame(gameId) {
    const game = this.games.get(gameId);
    if (game) {
      this.games.delete(gameId);
      for (const ws of [game.white, game.black]) {
        if (ws) {
          this.wsToGame.delete(ws);
          this.wsColor.delete(ws);
        }
      }
    }
  }

  async _forceDisconnectUser(ws, username, reason = 'logged in elsewhere') {
    this.removeFromQueue(ws);
    const pending = this.pendingDisconnect.get(username);
    if (pending) {
      clearTimeout(pending);
      this.pendingDisconnect.delete(username);
      this.disconnectTime.delete(username);
    }
    const game = this.wsToGame.get(ws);
    if (game) {
      const opponentWs = game.whiteUsername !== username ? game.white : game.black;
      if (opponentWs && opponentWs.readyState === opponentWs.OPEN) {
        safeSend(opponentWs, { type: 'error', message: `${username} disconnected (${reason}). Game over.` });
      }
      for (const sock of [game.white, game.black]) {
        if (sock) {
          this.wsToGame.delete(sock);
          this.wsColor.delete(sock);
        }
      }
      this.games.delete(game.id);
    }
    if (this.activeConnections.get(username) === ws) {
      this.activeConnections.delete(username);
    }
    this.wsUsername.delete(ws);
    this.wsToken.delete(ws);
    this.wsAvatar.delete(ws);
    this.wsToGame.delete(ws);
    this.wsColor.delete(ws);
    if (ws.readyState === ws.OPEN) {
      try {
        ws.close(4001, reason);
      } catch (e) {
        // ignore
      }
    }
  }

  async requestConfirmationOrReplace(ws, username, token, userInfo) {
    const oldWs = this.activeConnections.get(username);
    if (oldWs && oldWs !== ws && oldWs.readyState === oldWs.OPEN) {
      const confId = crypto.randomBytes(16).toString('base64url');
      const confirmedPromise = new Promise((resolve) => {
        this.pendingConfirmations.set(confId, { username, token, userInfo, resolve });
      });
      safeSend(oldWs, {
        type: 'force_logout_request',
        confirmation_id: confId,
        message: 'Another device is trying to log into your account. Allow it?',
      });

      let confirmed;
      try {
        confirmed = await Promise.race([
          confirmedPromise,
          new Promise((resolve) => setTimeout(() => resolve(false), FORCE_LOGOUT_TIMEOUT_MS)),
        ]);
      } catch (e) {
        confirmed = false;
      }
      this.pendingConfirmations.delete(confId);
      if (!confirmed) {
        return [false, null, null, 'Login rejected by active session.'];
      }
    }
    return [true, token, userInfo, null];
  }

  async replaceSession(username, newToken, userInfo) {
    await this.db._lock.withLock(async () => {
      const user = this.db.users[username];
      if (user) {
        const oldToken = user.session_token;
        if (oldToken) delete this.db.tokenToUsername[oldToken];
        user.session_token = newToken;
        user.last_login = new Date().toISOString();
        this.db.tokenToUsername[newToken] = username;
        await this.db._save();
      }
    });
  }

  async authenticate(ws, token) {
    if (ws.readyState !== ws.OPEN) return false;

    const [userInfo, success] = await this.db.authenticateToken(token);
    this.wsAvatar.set(ws, userInfo ? userInfo.profile_pic : null);
    if (!success) {
      safeSend(ws, { type: 'auth_error', message: 'Invalid or expired session. Please login again.' });
      return false;
    }

    const username = userInfo.username;

    // --- Check for pending disconnect and resume game ---
    if (this.pendingDisconnect.has(username)) {
      const handle = this.pendingDisconnect.get(username);
      clearTimeout(handle);
      this.pendingDisconnect.delete(username);
      this.disconnectTime.delete(username);

      let game = null;
      for (const g of this.games.values()) {
        if (g.whiteUsername === username || g.blackUsername === username) {
          game = g;
          break;
        }
      }

      if (game && game.gameOver) {
        this.removeGame(game.id);
        // fall through to normal auth
      } else if (game) {
        const color = game.whiteUsername === username ? 'white' : 'black';
        const oldWs = color === 'white' ? game.white : game.black;
        if (oldWs) {
          this.wsToGame.delete(oldWs);
          this.wsColor.delete(oldWs);
        }
        if (color === 'white') game.white = ws;
        else game.black = ws;
        this.wsToGame.set(ws, game);
        this.wsColor.set(ws, color);

        this.activeConnections.set(username, ws);
        this.wsUsername.set(ws, username);
        this.wsToken.set(ws, token);

        const opponentName = color === 'white' ? game.blackUsername : game.whiteUsername;
        const opponentAvatar = this.wsAvatar.get(color === 'black' ? game.white : game.black);
        safeSend(ws, {
          type: 'resume_game',
          color,
          fen: game.board.fen(),
          opponent: opponentName,
          opponentAvatar,
          game_id: game.id,
          your_gems: await this.db.getUserGems(username),
        });
        safeSend(ws, game.statePacket());
        const stats = await this.db.getUserStats(username);
        safeSend(ws, { type: 'user_stats', stats });
        return true;
      }
      // else (no game found) - fall through to normal auth
    }

    // --- Existing check for already logged in elsewhere ---
    const oldWs = this.activeConnections.get(username);
    if (oldWs && oldWs !== ws && oldWs.readyState === oldWs.OPEN) {
      safeSend(ws, { type: 'auth_error', message: 'Account already logged in on another device or tab.' });
      return false;
    }

    // Normal authentication (no pending game)
    this.activeConnections.set(username, ws);
    this.wsUsername.set(ws, username);
    this.wsToken.set(ws, token);

    const [awarded, newBalance] = await this.db.awardDailyGems(username);
    safeSend(ws, { type: 'auth_success', message: `Welcome back ${username}!`, user: userInfo });
    safeSend(ws, { type: 'gem_balance', balance: newBalance });
    const stats = await this.db.getUserStats(username);
    safeSend(ws, { type: 'user_stats', stats });
    return true;
  }

  async deleteAccount(username) {
    for (let i = 0; i < this.waiting.length; i++) {
      if (this.waiting[i].username === username) {
        this.waiting.splice(i, 1);
        break;
      }
    }

    let game = null;
    for (const g of this.games.values()) {
      if (g.whiteUsername === username || g.blackUsername === username) {
        game = g;
        break;
      }
    }
    if (game) {
      await game.forceResignByUsername(username);
    }

    const ws = this.activeConnections.get(username);
    if (ws) {
      this.activeConnections.delete(username);
      this.wsUsername.delete(ws);
      this.wsToken.delete(ws);
      this.wsAvatar.delete(ws);
      if (ws.readyState === ws.OPEN) {
        try {
          ws.close(4002, 'Account deleted');
        } catch (e) {
          // ignore
        }
      }
    }

    const pending = this.pendingDisconnect.get(username);
    if (pending) {
      clearTimeout(pending);
      this.pendingDisconnect.delete(username);
      this.disconnectTime.delete(username);
    }

    return this.db.deleteUser(username);
  }

  removeFromQueue(ws) {
    for (let i = 0; i < this.waiting.length; i++) {
      if (this.waiting[i].ws === ws) {
        this.waiting.splice(i, 1);
        return true;
      }
    }
    return false;
  }

  async addToQueue(ws, username, stake) {
    for (const entry of this.waiting) {
      if (entry.ws === ws) {
        safeSend(ws, { type: 'info', message: 'Already searching...' });
        return;
      }
    }

    const gems = await this.db.getUserGems(username);
    if (gems < stake) {
      safeSend(ws, { type: 'error', message: `❌ You need ${stake} gems to play this stake. You have ${gems} gems.` });
      return;
    }

    this.waiting.push({ ws, username, stake });
    safeSend(ws, { type: 'info', message: `Searching for ${stake} gems stake... Players in queue: ${this.waiting.length}` });

    let matched = null;
    let matchedIndex = -1;
    for (let i = 0; i < this.waiting.length; i++) {
      const entry = this.waiting[i];
      if (entry.ws === ws) continue;
      if (entry.stake === stake) {
        matched = entry;
        matchedIndex = i;
        break;
      }
    }

    if (matched) {
      this.waiting.splice(matchedIndex, 1);
      this.removeFromQueue(ws);

      const p2Ws = matched.ws;
      const p2Name = matched.username;

      let newWhiteGems;
      let newBlackGems;
      let cancelled = false;
      await this.db._lock.withLock(async () => {
        const whiteGems = (this.db.users[username] || {}).gems || 0;
        const blackGems = (this.db.users[p2Name] || {}).gems || 0;
        if (whiteGems < stake || blackGems < stake) {
          cancelled = true;
          return;
        }
        this.db.users[username].gems -= stake;
        this.db.users[p2Name].gems -= stake;
        await this.db._save();
        newWhiteGems = this.db.users[username].gems;
        newBlackGems = this.db.users[p2Name].gems;
      });

      if (cancelled) {
        safeSend(ws, { type: 'error', message: 'Match cancelled: insufficient gems.' });
        safeSend(p2Ws, { type: 'error', message: 'Match cancelled: insufficient gems.' });
        return;
      }

      const flip = Math.random() < 0.5;
      const whiteWs = flip ? ws : p2Ws;
      const blackWs = flip ? p2Ws : ws;
      const whiteName = whiteWs === ws ? username : p2Name;
      const blackName = whiteWs === ws ? p2Name : username;

      const gameId = this.nextGameId;
      this.nextGameId += 1;
      const game = new ChessGame(gameId, whiteWs, blackWs, whiteName, blackName, stake, this);
      this.games.set(gameId, game);
      this.wsToGame.set(whiteWs, game);
      this.wsToGame.set(blackWs, game);
      this.wsColor.set(whiteWs, 'white');
      this.wsColor.set(blackWs, 'black');
      const whiteAvatar = this.wsAvatar.get(whiteWs);
      const blackAvatar = this.wsAvatar.get(blackWs);

      const whiteGemsAfter = whiteWs === ws ? newWhiteGems : newBlackGems;
      const blackGemsAfter = blackWs === ws ? newWhiteGems : newBlackGems;

      safeSend(whiteWs, {
        type: 'match_start', game_id: gameId, color: 'white',
        fen: game.board.fen(), opponent: blackName, opponentAvatar: blackAvatar,
        your_gems: whiteGemsAfter,
      });
      safeSend(blackWs, {
        type: 'match_start', game_id: gameId, color: 'black',
        fen: game.board.fen(), opponent: whiteName, opponentAvatar: whiteAvatar,
        your_gems: blackGemsAfter,
      });
      safeSend(whiteWs, game.statePacket());
      safeSend(blackWs, game.statePacket());
    }
  }

  async settleGameGems(game) {
    const white = game.whiteUsername;
    const black = game.blackUsername;
    const stake = game.stake;
    const result = game.result;

    if (!game.gameOver) return;

    function getWinnerLoser(res) {
      if (res.includes('wins by')) {
        const winnerStr = res.split(' wins by')[0].trim();
        if (winnerStr === white || winnerStr === 'White') return [white, black];
        if (winnerStr === black || winnerStr === 'Black') return [black, white];
      }
      return [null, null];
    }

    const [winner, loser] = getWinnerLoser(result);

    if (winner !== null) {
      await this.db.updateGameStats(winner, true);
      await this.db.updateGameStats(loser, false);
      await this.db.addGems(winner, 2 * stake);
    } else if (result.includes('Draw') || result.includes('Stalemate') || result.includes('Insufficient material')) {
      await this.db.updateGameStats(white, false);
      await this.db.updateGameStats(black, false);
      await this.db.addGems(white, stake);
      await this.db.addGems(black, stake);
    } else {
      console.log(`Unrecognized game result: ${result}`);
      await this.db.updateGameStats(white, false);
      await this.db.updateGameStats(black, false);
      await this.db.addGems(white, stake);
      await this.db.addGems(black, stake);
    }

    const newWhite = await this.db.getUserGems(white);
    const newBlack = await this.db.getUserGems(black);
    const statsWhite = await this.db.getUserStats(white);
    const statsBlack = await this.db.getUserStats(black);

    for (const [ws, bal, stats] of [[game.white, newWhite, statsWhite], [game.black, newBlack, statsBlack]]) {
      if (ws && ws.readyState === ws.OPEN) {
        safeSend(ws, { type: 'gem_balance', balance: bal });
        safeSend(ws, { type: 'user_stats', stats });
      }
    }
  }

  async _removeUserFromGame(username, ws) {
    let game = null;
    for (const g of this.games.values()) {
      if (g.whiteUsername === username || g.blackUsername === username) {
        game = g;
        break;
      }
    }
    if (!game) return;
    if (!game.gameOver) {
      const opponentWs = game.whiteUsername !== username ? game.white : game.black;
      if (opponentWs && opponentWs.readyState === opponentWs.OPEN) {
        safeSend(opponentWs, { type: 'error', message: `${username} disconnected and did not return. Game over.` });
      }
    }
    for (const sock of [game.white, game.black]) {
      if (sock) {
        this.wsToGame.delete(sock);
        this.wsColor.delete(sock);
      }
    }
    this.games.delete(game.id);
  }

  async cleanupConnection(ws) {
    const username = this.wsUsername.get(ws);
    if (!username) return;
    this.removeFromQueue(ws);
    const game = this.wsToGame.get(ws);
    if (game) {
      if (game.gameOver) {
        await this._removeUserFromGame(username, ws);
        if (this.activeConnections.get(username) === ws) {
          this.activeConnections.delete(username);
        }
        this.wsUsername.delete(ws);
        this.wsToken.delete(ws);
        this.wsAvatar.delete(ws);
        return;
      }

      const handle = setTimeout(async () => {
        if (this.pendingDisconnect.has(username)) {
          this.pendingDisconnect.delete(username);
          this.disconnectTime.delete(username);

          const g = this.wsToGame.get(ws);
          if (g && !g.gameOver) {
            let winner;
            if (g.whiteUsername === username) {
              winner = 'Black';
            } else {
              winner = 'White';
            }
            g.gameOver = true;
            g.result = `${winner} wins by opponent disconnection`;
            g.broadcast(g.statePacket({ result: g.result }));
            g._cancelAllTimers();
            await this.settleGameGems(g);
            this.removeGame(g.id);
          }
          await this._removeUserFromGame(username, ws);
        }
      }, RECONNECT_GRACE_MS);

      this.pendingDisconnect.set(username, handle);
      this.disconnectTime.set(username, Date.now());
    }
    if (this.activeConnections.get(username) === ws) {
      this.activeConnections.delete(username);
    }
    this.wsUsername.delete(ws);
    this.wsToken.delete(ws);
    this.wsAvatar.delete(ws);
  }

  async handleMessage(ws, data) {
    const msgType = data.type;

    if (msgType === 'ping') {
      safeSend(ws, { type: 'pong' });
      return;
    }

    if (!this.wsUsername.has(ws)) {
      if (msgType === 'auth') {
        const token = data.token || '';
        await this.authenticate(ws, token);
      } else {
        safeSend(ws, { type: 'auth_error', message: 'Please authenticate first.' });
      }
      return;
    }

    const username = this.wsUsername.get(ws);

    if (msgType === 'force_logout_confirm') {
      const confId = data.confirmation_id;
      const confirm = !!data.confirm;
      const entry = this.pendingConfirmations.get(confId);
      if (entry) {
        entry.resolve(confirm);
        if (confirm) {
          safeSend(ws, { type: 'info', message: 'Confirmation received. Logging out...' });
          await this._forceDisconnectUser(ws, username, 'logged in elsewhere');
        } else {
          safeSend(ws, { type: 'info', message: 'Login rejected.' });
        }
      } else {
        safeSend(ws, { type: 'error', message: 'Invalid confirmation ID' });
      }
      return;
    }

    for (const entry of this.waiting) {
      if (entry.ws === ws) {
        if (msgType === 'cancel_match') {
          if (this.removeFromQueue(ws)) {
            safeSend(ws, { type: 'info', message: 'Matchmaking cancelled.' });
          } else {
            safeSend(ws, { type: 'error', message: 'Not in queue.' });
          }
        } else {
          safeSend(ws, { type: 'error', message: 'Still searching. Cancel first.' });
        }
        return;
      }
    }

    const game = this.wsToGame.get(ws);
    if (game) {
      const colour = this.wsColor.get(ws);
      if (msgType === 'move') {
        const move = data.move || '';
        const [success, err] = await game.applyMove(move, colour);
        if (!success) safeSend(ws, { type: 'error', message: err });
      } else if (msgType === 'chat') {
        const text = (data.text || '').slice(0, 200);
        game.broadcast({ type: 'chat', from: colour, text });
      } else if (msgType === 'resign') {
        const [success, err] = await game.resign(ws);
        if (!success) safeSend(ws, { type: 'error', message: err });
      } else {
        safeSend(ws, { type: 'error', message: 'Invalid action in game.' });
      }
    } else {
      if (msgType === 'find_match') {
        const stake = data.stake;
        if (!stake || !Number.isInteger(stake) || stake <= 0) {
          safeSend(ws, { type: 'error', message: 'Invalid stake amount.' });
          return;
        }
        await this.addToQueue(ws, username, stake);
      } else if (msgType === 'get_gems') {
        const gems = await this.db.getUserGems(username);
        safeSend(ws, { type: 'gem_balance', balance: gems });
      } else {
        safeSend(ws, { type: 'error', message: "Send 'find_match' with stake to start searching." });
      }
    }
  }
}

module.exports = { Matchmaker };
