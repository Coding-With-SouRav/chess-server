'use strict';

const path = require('path');
const fs = require('fs');
const express = require('express');
const multer = require('multer');
const { WebSocketServer } = require('ws');
const { Matchmaker } = require('./matchmaker');

const app = express();
app.use(express.json({ limit: '50mb' }));

const matchmaker = new Matchmaker();

// Serve /assets as static files (equivalent of app.router.add_static('/assets', ...))
const assetsPath = path.join(__dirname, 'assets');
if (fs.existsSync(assetsPath)) {
  app.use('/assets', express.static(assetsPath));
}

// ========================= HTTP HANDLERS =========================

function getBearerToken(req) {
  const authHeader = req.headers['authorization'] || '';
  if (authHeader.startsWith('Bearer ')) {
    return authHeader.slice(7);
  }
  return null;
}

app.post('/api/register', async (req, res) => {
  try {
    const username = (req.body.username || '').trim();
    const password = req.body.password || '';
    const [success, userInfo, token, error] = await matchmaker.db.registerUser(username, password);
    if (success) {
      res.json({ success: true, user: userInfo, token });
    } else {
      res.status(400).json({ success: false, message: error });
    }
  } catch (e) {
    res.status(500).json({ success: false, message: e.message });
  }
});

app.post('/api/login', async (req, res) => {
  try {
    const username = (req.body.username || '').trim();
    const password = req.body.password || '';
    const [success, userInfo, token, error] = await matchmaker.db.loginUser(username, password);
    if (!success) {
      return res.status(401).json({ success: false, message: error });
    }

    const oldWs = matchmaker.activeConnections.get(username);
    if (oldWs && oldWs.readyState === oldWs.OPEN) {
      return res.status(409).json({
        success: false,
        message: 'Already logged in on another device. Please logout from there first.',
      });
    }

    res.json({ success: true, user: userInfo, token });
  } catch (e) {
    res.status(500).json({ success: false, message: e.message });
  }
});

app.post('/api/token_login', async (req, res) => {
  try {
    const token = req.body.token || '';
    const [userInfo, success] = await matchmaker.db.authenticateToken(token);
    if (!success) {
      return res.status(401).json({ success: false, message: 'Invalid token' });
    }

    const username = userInfo.username;
    const oldWs = matchmaker.activeConnections.get(username);
    if (oldWs && oldWs.readyState === oldWs.OPEN) {
      return res.status(409).json({
        success: false,
        message: 'Already logged in on another device. Please logout from there first.',
      });
    }

    res.json({ success: true, user: userInfo, token });
  } catch (e) {
    res.status(500).json({ success: false, message: e.message });
  }
});

app.get('/api/get_gems', async (req, res) => {
  const token = getBearerToken(req);
  if (!token) {
    return res.status(401).json({ success: false, message: 'Missing token' });
  }
  const [userInfo, valid] = await matchmaker.db.authenticateToken(token);
  if (!valid) {
    return res.status(401).json({ success: false, message: 'Invalid token' });
  }
  const gems = await matchmaker.db.getUserGems(userInfo.username);
  res.json({ balance: gems });
});

const upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 50 * 1024 * 1024 } });

app.post('/api/upload_avatar', upload.single('avatar'), async (req, res) => {
  const token = getBearerToken(req);
  if (!token) {
    return res.status(401).json({ success: false, message: 'Missing token' });
  }
  const [userInfo, valid] = await matchmaker.db.authenticateToken(token);
  if (!valid) {
    return res.status(401).json({ success: false, message: 'Invalid token' });
  }
  if (!req.file) {
    return res.status(400).json({ success: false, message: "Expected field 'avatar'" });
  }
  let mimeType = req.file.mimetype || 'image/png';
  if (!['image/png', 'image/jpeg', 'image/jpg'].includes(mimeType)) {
    mimeType = 'image/png';
  }
  const b64Data = `data:${mimeType};base64,` + req.file.buffer.toString('base64');
  await matchmaker.db.updateProfilePic(userInfo.username, b64Data);
  res.json({ success: true, profile_pic: b64Data });
});

app.post('/api/change_password', async (req, res) => {
  const token = getBearerToken(req);
  if (!token) {
    return res.status(401).json({ success: false, message: 'Missing token' });
  }
  const [userInfo, valid] = await matchmaker.db.authenticateToken(token);
  if (!valid) {
    return res.status(401).json({ success: false, message: 'Invalid token' });
  }

  try {
    const currentPassword = req.body.currentPassword || '';
    const newPassword = req.body.newPassword || '';
    const confirmPassword = req.body.confirmPassword || '';

    if (newPassword !== confirmPassword) {
      return res.status(400).json({ success: false, message: 'New passwords do not match' });
    }

    const [success, message] = await matchmaker.db.changePassword(userInfo.username, currentPassword, newPassword);
    if (success) {
      res.json({ success: true, message });
    } else {
      res.status(400).json({ success: false, message });
    }
  } catch (e) {
    res.status(500).json({ success: false, message: e.message });
  }
});

app.post('/api/delete_account', async (req, res) => {
  const token = getBearerToken(req);
  if (!token) {
    return res.status(401).json({ success: false, message: 'Missing token' });
  }
  const [userInfo, valid] = await matchmaker.db.authenticateToken(token);
  if (!valid) {
    return res.status(401).json({ success: false, message: 'Invalid token' });
  }

  const username = userInfo.username;
  const success = await matchmaker.deleteAccount(username);
  if (success) {
    res.json({ success: true, message: 'Account permanently deleted.' });
  } else {
    res.status(500).json({ success: false, message: 'Account deletion failed.' });
  }
});

app.get('/', (req, res) => {
  res.type('html').send(fs.readFileSync(path.join(__dirname, 'index.html'), 'utf-8'));
});

// ========================= SERVER SETUP =========================
const PORT = parseInt(process.env.PORT || '5050', 10);
const server = app.listen(PORT, '0.0.0.0', () => {
  console.log(`Open: http://localhost:${PORT}`);
});

const wss = new WebSocketServer({ server, path: '/ws' });

wss.on('connection', (ws) => {
  // Heartbeat: ping every 30s, terminate if the client doesn't pong back.
  ws.isAlive = true;
  ws.on('pong', () => {
    ws.isAlive = true;
  });
  const heartbeatInterval = setInterval(() => {
    if (ws.isAlive === false) {
      clearInterval(heartbeatInterval);
      return ws.terminate();
    }
    ws.isAlive = false;
    try {
      ws.ping();
    } catch (e) {
      // ignore
    }
  }, 30 * 1000);

  ws.on('message', async (raw) => {
    let data;
    try {
      data = JSON.parse(raw.toString());
    } catch (e) {
      if (ws.readyState === ws.OPEN) {
        ws.send(JSON.stringify({ type: 'error', message: 'Invalid JSON' }));
      }
      return;
    }
    try {
      await matchmaker.handleMessage(ws, data);
    } catch (e) {
      console.log(`Error in handle_message: ${e.message}`);
    }
  });

  ws.on('close', async () => {
    clearInterval(heartbeatInterval);
    await matchmaker.cleanupConnection(ws);
  });

  ws.on('error', () => {
    // 'close' will also fire; nothing extra to do here.
  });
});
