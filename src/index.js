const http = require('http');
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

// --- Cloudflare IP detection ---
const CF_V4_RANGES = [
  '173.245.48.0/20', '103.21.244.0/22', '103.22.200.0/22', '103.31.4.0/22',
  '141.101.64.0/18', '108.162.192.0/18', '190.93.240.0/20', '188.114.96.0/20',
  '197.234.240.0/22', '198.41.128.0/17', '162.158.0.0/15', '104.16.0.0/13',
  '104.24.0.0/14', '172.64.0.0/13', '131.0.72.0/22',
];

const CF_V6_RANGES = [
  '2400:cb00::/32', '2606:4700::/32', '2803:f800::/32',
  '2405:b500::/32', '2405:8100::/32', '2a06:98c0::/29',
  '2c0f:f248::/32',
];

function ipv4ToInt(ip) {
  const parts = ip.split('.').map(Number);
  if (parts.length !== 4 || parts.some(p => isNaN(p))) return null;
  return ((parts[0] << 24) + (parts[1] << 16) + (parts[2] << 8) + parts[3]) >>> 0;
}

function ipv6ToBigInt(ip) {
  // Handle :: shorthand expansion
  const halves = ip.split('::');
  let left = halves[0] ? halves[0].split(':') : [];
  let right = halves.length > 1 && halves[1] ? halves[1].split(':') : [];
  const missing = 8 - left.length - right.length;
  if (missing < 0) return null;
  const full = [...left, ...Array(missing).fill('0'), ...right];
  if (full.length !== 8) return null;
  const hex = full.map(g => g.padStart(4, '0')).join('');
  return BigInt('0x' + hex);
}

function isCloudflareIp(ip) {
  const clean = ip.replace(/^::ffff:/, '');

  // IPv4
  if (clean.includes('.')) {
    const ipInt = ipv4ToInt(clean);
    if (ipInt === null) return false;
    return CF_V4_RANGES.some(cidr => {
      const [range, bits] = cidr.split('/');
      const rangeInt = ipv4ToInt(range);
      const mask = bits === '0' ? 0 : (0xFFFFFFFF << (32 - parseInt(bits))) >>> 0;
      return (ipInt & mask) === (rangeInt & mask);
    });
  }

  // IPv6
  if (clean.includes(':')) {
    const ipBig = ipv6ToBigInt(clean);
    if (ipBig === null) return false;
    return CF_V6_RANGES.some(cidr => {
      const [range, bits] = cidr.split('/');
      const rangeBig = ipv6ToBigInt(range);
      const bitCount = parseInt(bits);
      const mask = (1n << 128n) - (1n << BigInt(128 - bitCount));
      return (ipBig & mask) === (rangeBig & mask);
    });
  }

  return false;
}

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
