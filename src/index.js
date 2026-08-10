const http = require('http');
const express = require('express');
const { WebSocketServer, WebSocket } = require('ws');
const path = require('path');
const db = require('./db');
const createAdminRouter = require('./admin');

// --- Config ---
const BACKEND_WS_URL = process.env.BACKEND_WS_URL || 'ws://localhost:8008';
const BACKEND_HTTP_URL = process.env.BACKEND_HTTP_URL || 'http://localhost:8008';
const PORT = parseInt(process.env.PORT || '8080', 10);
const ADMIN_TOKEN = process.env.ADMIN_TOKEN || '';
const DATA_DIR = process.env.DATA_DIR || './data';

const backendUrl = new URL(BACKEND_HTTP_URL);

// --- Init DB ---
db.init(DATA_DIR);

// --- Express (admin dashboard + API) ---
const app = express();
app.use(express.static(path.join(__dirname, '..', 'public')));

// Admin auth
app.use('/api', (req, res, next) => {
  if (ADMIN_TOKEN && req.headers.authorization !== `Bearer ${ADMIN_TOKEN}`) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  next();
});
app.use('/api', createAdminRouter());

// --- HTTP server ---
const server = http.createServer((req, res) => {
  const accept = req.headers.accept || '';

  // NIP-11 relay info doc: proxy to backend
  if (accept.includes('nostr+json')) {
    proxyHttp(req, res);
    return;
  }

  // Everything else: let express handle it (dashboard, API)
  app(req, res);
});

// --- WebSocket proxy ---
const wss = new WebSocketServer({ noServer: true });

server.on('upgrade', (request, socket, head) => {
  // All WebSocket upgrades go through the proxy
  wss.handleUpgrade(request, socket, head, (ws) => {
    wss.emit('connection', ws, request);
  });
});

function getClientIp(req) {
  const xff = req.headers['x-forwarded-for'];
  if (xff) {
    return xff.split(',')[0].trim();
  }
  return req.socket.remoteAddress || 'unknown';
}

// --- HTTP proxy (NIP-11 + any other HTTP) ---
function proxyHttp(req, res) {
  const proxyReq = http.request(
    {
      hostname: backendUrl.hostname,
      port: backendUrl.port,
      path: req.url,
      method: req.method,
      headers: { ...req.headers, host: backendUrl.host },
    },
    (proxyRes) => {
      res.writeHead(proxyRes.statusCode, proxyRes.headers);
      proxyRes.pipe(res);
    }
  );

  proxyReq.on('error', (err) => {
    console.error('HTTP proxy error:', err.message);
    if (!res.headersSent) {
      res.writeHead(502);
      res.end('Bad Gateway');
    }
  });

  req.pipe(proxyReq);
}

// --- WebSocket proxy logic ---
wss.on('connection', (clientWs, req) => {
  const ip = getClientIp(req);
  const userAgent = req.headers['user-agent'] || '';
  const connId = db.logConnection(ip, userAgent);

  console.log(`[+] #${connId} ${ip} connected`);

  // Open backend connection
  const backendWs = new WebSocket(BACKEND_WS_URL);
  const messageQueue = [];

  backendWs.on('open', () => {
    // Flush messages that arrived before backend was ready
    while (messageQueue.length > 0) {
      const { data, isBinary } = messageQueue.shift();
      backendWs.send(data, { binary: isBinary });
    }
  });

  // Client → Backend: intercept and log
  clientWs.on('message', (data, isBinary) => {
    if (!isBinary) {
      try {
        const msg = JSON.parse(data.toString());

        if (msg[0] === 'EVENT' && msg[1]) {
          const event = msg[1];
          db.logPublishedEvent(connId, ip, {
            event_id: event.id,
            pubkey: event.pubkey,
            kind: event.kind,
            created_at: event.created_at,
            tags: JSON.stringify(event.tags || []),
            content_len: event.content ? event.content.length : 0,
          });
        } else if (msg[0] === 'AUTH' && msg[1]) {
          // NIP-42 auth response
          const event = msg[1];
          db.logPublishedEvent(connId, ip, {
            event_id: event.id,
            pubkey: event.pubkey,
            kind: event.kind,
            created_at: event.created_at,
            tags: JSON.stringify(event.tags || []),
            content_len: event.content ? event.content.length : 0,
          });
        } else if (msg[0] === 'REQ' && msg.length >= 3) {
          const subId = msg[1];
          const filters = msg.slice(2);
          db.logSubscription(connId, ip, subId, JSON.stringify(filters));
        } else if (msg[0] === 'CLOSE' && msg[1]) {
          db.logSubscriptionClose(connId, ip, msg[1]);
        }
      } catch {
        // Not valid JSON — just forward
      }
    }

    // Forward to backend
    if (backendWs.readyState === WebSocket.OPEN) {
      backendWs.send(data, { binary: isBinary });
    } else if (backendWs.readyState === WebSocket.CONNECTING) {
      messageQueue.push({ data, isBinary });
    }
  });

  // Backend → Client: transparent passthrough
  backendWs.on('message', (data, isBinary) => {
    if (clientWs.readyState === WebSocket.OPEN) {
      clientWs.send(data, { binary: isBinary });
    }
  });

  // --- Cleanup ---
  const cleanup = () => {
    db.logDisconnection(connId);
    console.log(`[-] #${connId} ${ip} disconnected`);
    if (backendWs.readyState === WebSocket.OPEN || backendWs.readyState === WebSocket.CONNECTING) {
      backendWs.close();
    }
  };

  clientWs.on('close', cleanup);
  clientWs.on('error', cleanup);

  backendWs.on('close', () => {
    if (clientWs.readyState === WebSocket.OPEN) {
      clientWs.close();
    }
  });

  backendWs.on('error', (err) => {
    console.error(`[!] Backend error for #${connId}:`, err.message);
    if (clientWs.readyState === WebSocket.OPEN) {
      clientWs.close();
    }
  });
});

// --- Start ---
server.listen(PORT, () => {
  console.log(`🍯 Honey listening on :${PORT}`);
  console.log(`   Backend WS:   ${BACKEND_WS_URL}`);
  console.log(`   Backend HTTP: ${BACKEND_HTTP_URL}`);
  console.log(`   Dashboard:    http://localhost:${PORT}/`);
});
