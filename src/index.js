const http = require('http');
const https = require('https');
const express = require('express');
const { WebSocketServer, WebSocket } = require('ws');
const path = require('path');
const db = require('./db');
const createAdminRouter = require('./admin');
const es = require('./es');
const sync = require('./sync');

// --- Config ---
const BACKEND_HOST = process.env.BACKEND_HOST || 'localhost:8008';
const BACKEND_SCHEME = process.env.BACKEND_SCHEME || 'wss';
const BACKEND_WS_URL = `${BACKEND_SCHEME}://${BACKEND_HOST}`;
const BACKEND_HTTP_URL = `http${BACKEND_SCHEME === 'wss' ? 's' : ''}://${BACKEND_HOST}`;
const PORT = parseInt(process.env.PORT || '8080', 10);
const ADMIN_TOKEN = process.env.ADMIN_TOKEN || '';
const DATA_DIR = process.env.DATA_DIR || './data';
const DEBUG = process.env.DEBUG === '1' || process.env.DEBUG === 'true';

const backendUrl = new URL(BACKEND_HTTP_URL);

// --- Init DB ---
db.init(DATA_DIR);
db.startQueueFlusher();

// --- Init Elasticsearch (required, blocking) ---
const ES_URL = process.env.ES_URL;
if (!ES_URL) {
  console.error('[es] ES_URL not set. Required. Exiting.');
  process.exit(1);
}

async function startup() {
  await es.init(ES_URL); // Throws on failure — no fallback
  sync.setDb(db);
  sync.startSyncWorker();

  // --- Express (admin dashboard + API) ---
const app = express();
app.use(express.static(path.join(__dirname, '..', 'public')));

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
  if (accept.includes('nostr+json')) {
    proxyHttp(req, res);
    return;
  }
  app(req, res);
});

// --- WebSocket proxy ---
const wss = new WebSocketServer({ noServer: true });

server.on('upgrade', (request, socket, head) => {
  wss.handleUpgrade(request, socket, head, (ws) => {
    wss.emit('connection', ws, request);
  });
});

// --- Cloudflare IP detection (shared) ---
const { isCloudflareIp } = require('./cf');

function getClientIp(req) {
  const socketIp = (req.socket.remoteAddress || '').replace(/^::ffff:/, '');

  if (DEBUG) {
    const relevantHeaders = {};
    for (const [k, v] of Object.entries(req.headers)) {
      if (k.includes('ip') || k.includes('forward') || k.includes('real') || k.includes('proxy') || k.includes('connecting')) {
        relevantHeaders[k] = v;
      }
    }
    console.log(`[debug] IP detection | socket: ${socketIp} | headers: ${JSON.stringify(relevantHeaders)}`);
  }

  // 1. Cloudflare CF-Connecting-IP
  const cfIp = req.headers['cf-connecting-ip'];
  if (cfIp) return cfIp.trim();

  // 2. True-Client-IP
  const trueClientIp = req.headers['true-client-ip'];
  if (trueClientIp) return trueClientIp.trim();

  // 3. X-Forwarded-For — walk backwards, skip CDN IPs
  const xff = req.headers['x-forwarded-for'];
  if (xff) {
    const chain = xff.split(',').map(s => s.trim()).filter(Boolean);
    for (let i = chain.length - 1; i >= 0; i--) {
      if (!isCloudflareIp(chain[i])) {
        return chain[i];
      }
    }
    if (chain.length > 0) return chain[0];
  }

  return socketIp || 'unknown';
}

// --- HTTP proxy ---
function proxyHttp(req, res) {
  const isTls = backendUrl.protocol === 'https:';
  const transport = isTls ? https : http;
  const proxyReq = transport.request(
    {
      hostname: backendUrl.hostname,
      port: backendUrl.port || (isTls ? 443 : 80),
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

  const backendWs = new WebSocket(BACKEND_WS_URL);
  const messageQueue = [];

  backendWs.on('open', () => {
    while (messageQueue.length > 0) {
      const { data, isBinary } = messageQueue.shift();
      backendWs.send(data, { binary: isBinary });
    }
  });

  clientWs.on('message', (data, isBinary) => {
    // Forward to backend FIRST — user sees zero DB latency
    if (backendWs.readyState === WebSocket.OPEN) {
      backendWs.send(data, { binary: isBinary });
    } else if (backendWs.readyState === WebSocket.CONNECTING) {
      messageQueue.push({ data, isBinary });
    }

    // Then log asynchronously (queued, flushed in batch)
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
            content: event.content || '',
            content_len: event.content ? event.content.length : 0,
          });
          // Queue kind:0 profile for caching
          if (event.kind === 0 && event.pubkey) {
            try { db.cacheProfile(event.pubkey, JSON.parse(event.content)); } catch {}
          }
        } else if (msg[0] === 'AUTH' && msg[1]) {
          const event = msg[1];
          db.logPublishedEvent(connId, ip, {
            event_id: event.id,
            pubkey: event.pubkey,
            kind: event.kind,
            created_at: event.created_at,
            tags: JSON.stringify(event.tags || []),
            content: event.content || '',
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
        // Not valid JSON
      }
    }
  });

  backendWs.on('message', (data, isBinary) => {
    if (clientWs.readyState === WebSocket.OPEN) {
      clientWs.send(data, { binary: isBinary });
    }
  });

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
    if (clientWs.readyState === WebSocket.OPEN) clientWs.close();
  });

  backendWs.on('error', (err) => {
    console.error(`[!] Backend error for #${connId}:`, err.message);
    if (clientWs.readyState === WebSocket.OPEN) clientWs.close();
  });
});

// --- Background workers ---

// Geocode all uncached IPs on startup, then every 10 minutes
async function geoWorker() {
  async function run() {
    try {
      const ips = db.getAllUniqueIps();
      if (ips.length) await db.geocodeIps(ips);
    } catch (err) { console.error('[geo] Worker error:', err.message); }
  }
  // Initial run after 5s
  setTimeout(run, 5000);
  // Then every 10 min
  setInterval(run, 10 * 60 * 1000);
}

// Fetch missing profiles from the backend relay every 5 minutes
async function profileWorker() {
  async function run() {
    try {
      const pubkeys = db.getAllPubkeys();
      const stale = db.getStaleProfiles(pubkeys);
      if (stale.length > 0) {
        console.log(`[profiles] Fetching ${stale.length} missing profiles...`);
        await db.fetchProfilesFromRelay(stale.slice(0, 100), BACKEND_WS_URL);
      }
    } catch (err) { console.error('[profiles] Worker error:', err.message); }
  }
  // Initial run after 10s
  setTimeout(run, 10000);
  // Then every 5 min
  setInterval(run, 5 * 60 * 1000);
}

// --- Start ---
app.set('backendWsUrl', BACKEND_WS_URL);
app.set('backend host', BACKEND_HOST);
app.set('backend scheme', BACKEND_SCHEME);

server.listen(PORT, () => {
    console.log(`🍯 Honey listening on :${PORT}`);
    console.log(`   Backend WS:   ${BACKEND_WS_URL}`);
    console.log(`   Backend HTTP: ${BACKEND_HTTP_URL}`);
    console.log(`   Dashboard:    http://localhost:${PORT}/`);
    geoWorker();
    profileWorker();
  });
}

startup().catch(err => {
  console.error('[startup] Fatal:', err.message);
  process.exit(1);
});
