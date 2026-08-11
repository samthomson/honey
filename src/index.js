const http = require('http');
const express = require('express');
const { WebSocketServer, WebSocket } = require('ws');
const path = require('path');
const db = require('./db');
const createAdminRouter = require('./admin');

// --- Config ---
// Single BACKEND_HOST:port — we derive ws:// and http:// from it
const BACKEND_HOST = process.env.BACKEND_HOST || 'localhost:8008';
const BACKEND_SCHEME = process.env.BACKEND_SCHEME || 'wss'; // wss for TLS, ws for plain
const BACKEND_WS_URL = `${BACKEND_SCHEME}://${BACKEND_HOST}`;
const BACKEND_HTTP_URL = `http${BACKEND_SCHEME === 'wss' ? 's' : ''}://${BACKEND_HOST}`;
const PORT = parseInt(process.env.PORT || '8080', 10);
const ADMIN_TOKEN = process.env.ADMIN_TOKEN || '';
const DATA_DIR = process.env.DATA_DIR || './data';
const DEBUG = process.env.DEBUG === '1' || process.env.DEBUG === 'true';

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

// Cloudflare IPv4/IPv6 ranges (for detecting when socket IP is a CDN edge)
const CF_RANGES = [
  // IPv4
  '173.245.48.0/20', '103.21.244.0/22', '103.22.200.0/22', '103.31.4.0/22',
  '141.101.64.0/18', '108.162.192.0/18', '190.93.240.0/20', '188.114.96.0/20',
  '197.234.240.0/22', '198.41.128.0/17', '162.158.0.0/15', '104.16.0.0/13',
  '104.24.0.0/14', '172.64.0.0/13', '131.0.72.0/22',
];

function ipToInt(ip) {
  const parts = ip.split('.').map(Number);
  if (parts.length !== 4 || parts.some(p => isNaN(p))) return null;
  return (parts[0] << 24) + (parts[1] << 16) + (parts[2] << 8) + parts[3];
}

function cidrMatch(ip, cidr) {
  const [range, bits] = cidr.split('/');
  const ipInt = ipToInt(ip);
  const rangeInt = ipToInt(range);
  if (ipInt === null || rangeInt === null) return false;
  const mask = bits === '0' ? 0 : (0xFFFFFFFF << (32 - parseInt(bits))) >>> 0;
  return (ipInt & mask) === (rangeInt & mask);
}

function isCloudflareIp(ip) {
  // Strip IPv6-mapped IPv4
  const clean = ip.replace(/^::ffff:/, '');
  return CF_RANGES.some(cidr => cidrMatch(clean, cidr));
}

function getClientIp(req) {
  const socketIp = (req.socket.remoteAddress || '').replace(/^::ffff:/, '');

  if (DEBUG) {
    const relevantHeaders = {};
    for (const [k, v] of Object.entries(req.headers)) {
      if (k.includes('ip') || k.includes('forward') || k.includes('real') || k.includes('proxy')) {
        relevantHeaders[k] = v;
      }
    }
    console.log(`[debug] IP detection | socket: ${socketIp} | headers: ${JSON.stringify(relevantHeaders)}`);
  }

  // 1. Cloudflare's CF-Connecting-IP — always set when proxied through CF
  const cfIp = req.headers['cf-connecting-ip'];
  if (cfIp) {
    return cfIp.trim();
  }

  // 2. True-Client-IP (some CDNs)
  const trueClientIp = req.headers['true-client-ip'];
  if (trueClientIp) {
    return trueClientIp.trim();
  }

  // 3. X-Forwarded-For chain — walk backwards to find first non-CDN IP
  const xff = req.headers['x-forwarded-for'];
  if (xff) {
    const chain = xff.split(',').map(s => s.trim());
    // If socket IP is Cloudflare, the XFF chain has the real client
    // Walk from rightmost (closest to us) leftward, skipping CDN IPs
    for (let i = chain.length - 1; i >= 0; i--) {
      if (!isCloudflareIp(chain[i])) {
        return chain[i];
      }
    }
    // All entries are CDN — return leftmost (best guess)
    return chain[0];
  }

  return socketIp || 'unknown';
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
