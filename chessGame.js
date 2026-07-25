'use strict';

const { Chess } = require('chess.js');

const TIMEOUT_SECONDS = 60;

function safeSend(ws, message) {
  try {
    if (ws && ws.readyState === ws.OPEN) {
      ws.send(JSON.stringify(message));
    }
  } catch (e) {
    // ignore
  }
}

class ChessGame {
  constructor(gameId, whiteWs, blackWs, whiteUsername, blackUsername, stake, matchmaker) {
    this.id = gameId;
    this.white = whiteWs;
    this.black = blackWs;
    this.whiteUsername = whiteUsername;
    this.blackUsername = blackUsername;
    this.stake = stake;
    this.board = new Chess();
    this.gameOver = false;
    this.result = null;
    this.matchmaker = matchmaker;

    this.missedWhite = 0;
    this.missedBlack = 0;

    this.whiteTimerHandle = null;
    this.blackTimerHandle = null;

    this._startTimer('white');
  }

  async resign(ws) {
    if (this.gameOver) return [false, 'Game already finished.'];

    let winner;
    if (ws === this.white) {
      winner = this.blackUsername;
    } else if (ws === this.black) {
      winner = this.whiteUsername;
    } else {
      return [false, 'Unknown player'];
    }

    this.gameOver = true;
    this.result = `${winner} wins by resignation`;
    this._cancelAllTimers();

    this.broadcast(this.statePacket({ result: this.result }));

    await this.matchmaker.settleGameGems(this);
    this.matchmaker.removeGame(this.id);
    return [true, null];
  }

  async forceResignByUsername(username) {
    if (this.gameOver) return [false, 'Game already finished.'];
    let winner;
    if (username === this.whiteUsername) {
      winner = this.blackUsername;
    } else if (username === this.blackUsername) {
      winner = this.whiteUsername;
    } else {
      return [false, 'Username not in this game'];
    }

    this.gameOver = true;
    this.result = `${winner} wins (opponent account deleted)`;
    this._cancelAllTimers();
    this.broadcast(this.statePacket({ result: this.result }));
    await this.matchmaker.settleGameGems(this);
    this.matchmaker.removeGame(this.id);
    return [true, winner];
  }

  opponent(ws) {
    return ws === this.white ? this.black : this.white;
  }

  opponentUsername(ws) {
    return ws === this.white ? this.blackUsername : this.whiteUsername;
  }

  broadcast(message, exclude = null) {
    for (const sock of [this.white, this.black]) {
      if (sock && sock !== exclude) {
        safeSend(sock, message);
      }
    }
  }

  statePacket(extra = null) {
    const pkt = {
      type: 'state',
      fen: this.board.fen(),
      turn: this.board.turn() === 'w' ? 'white' : 'black',
      legal_moves: this.board.moves({ verbose: true }).map((m) => m.lan),
      in_check: this.board.inCheck(),
      game_over: this.gameOver,
    };
    if (extra) Object.assign(pkt, extra);
    if (this.result) pkt.result = this.result;
    return pkt;
  }

  _startTimer(color) {
    this._cancelTimer(color);
    if (this.gameOver) return;

    const handle = setTimeout(() => {
      if (!this.gameOver) {
        this._onTimeout(color).catch((e) => console.error('Error in timeout handler:', e));
      }
    }, TIMEOUT_SECONDS * 1000);

    if (color === 'white') this.whiteTimerHandle = handle;
    else this.blackTimerHandle = handle;
  }

  _cancelTimer(color) {
    const handle = color === 'white' ? this.whiteTimerHandle : this.blackTimerHandle;
    if (handle) clearTimeout(handle);
    if (color === 'white') this.whiteTimerHandle = null;
    else this.blackTimerHandle = null;
  }

  _cancelAllTimers() {
    this._cancelTimer('white');
    this._cancelTimer('black');
  }

  async _onTimeout(color) {
    if (this.gameOver) return;

    let missedCount;
    let opponentColor;
    if (color === 'white') {
      this.missedWhite += 1;
      missedCount = this.missedWhite;
      opponentColor = 'black';
    } else {
      this.missedBlack += 1;
      missedCount = this.missedBlack;
      opponentColor = 'white';
    }

    this._cancelTimer(color);

    if (missedCount >= 3) {
      const winner = color === 'white' ? this.blackUsername : this.whiteUsername;
      const result = `${winner} wins by timeout (inactive player)`;

      this.gameOver = true;
      this.result = result;

      this.broadcast(this.statePacket({ result: this.result }));
      this._cancelAllTimers();

      await this.matchmaker.settleGameGems(this);
      this.matchmaker.removeGame(this.id);
      return;
    }

    // Not game over: flip the turn to the opponent (skip the missed player's turn)
    this._flipTurn();

    this._cancelTimer(opponentColor);
    this._startTimer(opponentColor);

    const capColor = color.charAt(0).toUpperCase() + color.slice(1);
    const capOpp = opponentColor.charAt(0).toUpperCase() + opponentColor.slice(1);
    const msg = `${capColor} missed the move. ${capOpp}'s turn.`;
    this.broadcast({ type: 'info', message: msg });
    this.broadcast(this.statePacket());
  }

  /** chess.js has no public "set turn" API, so rebuild the FEN with the turn field flipped. */
  _flipTurn() {
    const parts = this.board.fen().split(' ');
    parts[1] = parts[1] === 'w' ? 'b' : 'w';
    this.board.load(parts.join(' '));
  }

  async applyMove(moveUci, colour) {
    if (this.gameOver) return [false, 'Game already finished.'];
    const currentTurn = this.board.turn() === 'w' ? 'white' : 'black';
    if (colour !== currentTurn) return [false, 'Not your turn.'];

    const from = moveUci.slice(0, 2);
    const to = moveUci.slice(2, 4);
    const promotion = moveUci.length > 4 ? moveUci.slice(4, 5) : undefined;

    let moveResult;
    try {
      moveResult = this.board.move({ from, to, promotion });
    } catch (e) {
      moveResult = null;
    }

    if (!moveResult) {
      return [false, 'Illegal move.'];
    }

    this._cancelTimer(colour);
    if (colour === 'white') this.missedWhite = 0;
    else this.missedBlack = 0;

    if (this.board.isGameOver()) {
      this.gameOver = true;
      this.result = this._getResult();
      this._cancelAllTimers();
      this.broadcast(this.statePacket({ result: this.result }));
      await this.matchmaker.settleGameGems(this);
      this.matchmaker.removeGame(this.id);
    } else {
      const nextColor = this.board.turn() === 'w' ? 'white' : 'black';
      this._startTimer(nextColor);
      this.broadcast(this.statePacket());
    }

    return [true, null];
  }

  _getResult() {
    const b = this.board;
    if (b.isCheckmate()) {
      const winner = b.turn() === 'w' ? 'Black' : 'White';
      return `${winner} wins by checkmate!`;
    }
    if (b.isStalemate()) return 'Draw — Stalemate.';
    if (b.isInsufficientMaterial()) return 'Draw — Insufficient material.';
    if (b.isThreefoldRepetition()) return 'Draw — Fivefold repetition.';
    if (b.isDraw()) return 'Draw — 75-move rule.';
    return 'Game over.';
  }
}

module.exports = { ChessGame, safeSend };
