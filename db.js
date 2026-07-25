'use strict';

const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

/**
 * Simple async mutex, used the same way Python's asyncio.Lock() was used
 * in the original server (all DB mutations are serialized).
 */
class Mutex {
  constructor() {
    this._locked = false;
    this._queue = [];
  }
  lock() {
    return new Promise((resolve) => {
      if (!this._locked) {
        this._locked = true;
        resolve();
      } else {
        this._queue.push(resolve);
      }
    });
  }
  unlock() {
    if (this._queue.length > 0) {
      const next = this._queue.shift();
      next();
    } else {
      this._locked = false;
    }
  }
  async withLock(fn) {
    await this.lock();
    try {
      return await fn();
    } finally {
      this.unlock();
    }
  }
}

class JSONUserDatabase {
  constructor(jsonPath = path.join(__dirname, 'assets', 'database', 'chess_users.json')) {
    this.jsonPath = jsonPath;
    this._lock = new Mutex();
    this.users = {};
    this.tokenToUsername = {};
    this._nextId = 1;
    this._load();
  }

  _load() {
    if (fs.existsSync(this.jsonPath)) {
      try {
        const raw = fs.readFileSync(this.jsonPath, 'utf-8');
        const data = JSON.parse(raw);
        this.users = data.users || {};
        this.tokenToUsername = {};
        for (const [username, record] of Object.entries(this.users)) {
          if (record.gems === undefined) record.gems = 100;
          if (record.last_daily_award === undefined) record.last_daily_award = null;
          const token = record.session_token;
          if (token) this.tokenToUsername[token] = username;
          if (record.id !== undefined && record.id >= this._nextId) {
            this._nextId = record.id + 1;
          }
        }
      } catch (e) {
        this.users = {};
        this.tokenToUsername = {};
        this._nextId = 1;
      }
    } else {
      this.users = {};
      this.tokenToUsername = {};
      this._nextId = 1;
    }
  }

  async _save() {
    const data = { users: this.users, next_id: this._nextId };
    const dir = path.dirname(this.jsonPath);
    fs.mkdirSync(dir, { recursive: true });
    const tempPath = this.jsonPath + '.tmp';
    fs.writeFileSync(tempPath, JSON.stringify(data, null, 2), 'utf-8');
    fs.renameSync(tempPath, this.jsonPath);
  }

  _generateToken() {
    return crypto.randomBytes(32).toString('base64url');
  }

  async deleteUser(username) {
    return this._lock.withLock(async () => {
      const user = this.users[username];
      if (!user) return false;
      delete this.users[username];
      const token = user.session_token;
      if (token) delete this.tokenToUsername[token];
      await this._save();
      return true;
    });
  }

  async changePassword(username, currentPassword, newPassword) {
    return this._lock.withLock(async () => {
      const user = this.users[username];
      if (!user) return [false, 'User not found'];
      if (user.password !== currentPassword) return [false, 'Current password is incorrect'];
      if (newPassword.length < 4) return [false, 'New password must be at least 4 characters'];
      user.password = newPassword;
      await this._save();
      return [true, 'Password updated successfully'];
    });
  }

  async getUserStats(username) {
    return this._lock.withLock(async () => {
      const user = this.users[username];
      if (!user) return { games_played: 0, games_won: 0, win_percentage: 0 };
      const played = user.games_played || 0;
      const won = user.games_won || 0;
      const winPercentage = played > 0 ? (won / played) * 100 : 0;
      return {
        games_played: played,
        games_won: won,
        win_percentage: Math.round(winPercentage * 10) / 10,
      };
    });
  }

  async registerUser(username, password) {
    if (!/^[a-zA-Z0-9_]{3,20}$/.test(username)) {
      return [false, null, null, 'Invalid username format.'];
    }
    if (password.length < 4) {
      return [false, null, null, 'Password must be at least 4 characters.'];
    }
    return this._lock.withLock(async () => {
      if (this.users[username]) {
        return [false, null, null, 'Username already exists.'];
      }
      const token = this._generateToken();
      const now = new Date().toISOString();
      const userId = this._nextId;
      this._nextId += 1;
      const userRecord = {
        id: userId,
        username,
        password,
        profile_pic: null,
        session_token: token,
        created_at: now,
        last_login: now,
        games_played: 0,
        games_won: 0,
        gems: 100,
        last_daily_award: null,
      };
      this.users[username] = userRecord;
      this.tokenToUsername[token] = username;
      await this._save();
      const userInfo = {
        id: userId,
        username,
        profile_pic: null,
        created_at: now,
        last_login: now,
      };
      return [true, userInfo, token, null];
    });
  }

  async loginUser(username, password) {
    return this._lock.withLock(async () => {
      const user = this.users[username];
      if (!user) return [false, null, null, 'User not found.'];
      if (user.password !== password) return [false, null, null, 'Invalid password.'];
      const newToken = this._generateToken();
      const oldToken = user.session_token;
      if (oldToken) delete this.tokenToUsername[oldToken];
      user.session_token = newToken;
      const now = new Date().toISOString();
      user.last_login = now;
      this.tokenToUsername[newToken] = username;
      await this._save();
      const userInfo = {
        id: user.id,
        username: user.username,
        profile_pic: user.profile_pic,
        created_at: user.created_at,
        last_login: now,
      };
      return [true, userInfo, newToken, null];
    });
  }

  async authenticateToken(token) {
    if (!token) return [null, false];
    return this._lock.withLock(async () => {
      const username = this.tokenToUsername[token];
      if (!username) return [null, false];
      const user = this.users[username];
      if (!user || user.session_token !== token) return [null, false];
      const userInfo = {
        id: user.id,
        username: user.username,
        profile_pic: user.profile_pic,
        created_at: user.created_at,
        last_login: user.last_login,
      };
      return [userInfo, true];
    });
  }

  async updateProfilePic(username, base64Data) {
    return this._lock.withLock(async () => {
      const user = this.users[username];
      if (user) {
        user.profile_pic = base64Data;
        await this._save();
      }
    });
  }

  async updateGameStats(username, won = false) {
    return this._lock.withLock(async () => {
      const user = this.users[username];
      if (user) {
        user.games_played = (user.games_played || 0) + 1;
        if (won) user.games_won = (user.games_won || 0) + 1;
        await this._save();
      }
    });
  }

  async getUserGems(username) {
    return this._lock.withLock(async () => {
      const user = this.users[username];
      return user ? (user.gems || 0) : 0;
    });
  }

  async deductGems(username, amount) {
    return this._lock.withLock(async () => {
      const user = this.users[username];
      if (!user || (user.gems || 0) < amount) return false;
      user.gems = (user.gems || 0) - amount;
      await this._save();
      return true;
    });
  }

  async addGems(username, amount) {
    return this._lock.withLock(async () => {
      const user = this.users[username];
      if (user) {
        user.gems = (user.gems || 0) + amount;
        await this._save();
      }
    });
  }

  async awardDailyGems(username) {
    return this._lock.withLock(async () => {
      const user = this.users[username];
      if (!user) return [false, 0];
      const today = new Date();
      const todayStr = today.toISOString().slice(0, 10);
      const lastAward = user.last_daily_award;
      if (lastAward) {
        const lastDateStr = lastAward.slice(0, 10);
        if (lastDateStr >= todayStr) {
          return [false, user.gems || 0];
        }
      }
      user.gems = (user.gems || 0) + 50;
      user.last_daily_award = new Date().toISOString();
      await this._save();
      return [true, user.gems];
    });
  }
}

module.exports = { JSONUserDatabase, Mutex };
