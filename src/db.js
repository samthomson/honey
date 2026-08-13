const { DatabaseSync } = require('node:sqlite');
const fs = require('fs');
const path = require('path');

let db;

function init(dataDir) {
  if (!fs.existsSync(dataDir)) fs.mkdirSync(dataDir, { recursive: true });

  db = new DatabaseSync(path.join(dataDir, 'honey.db'));
  db.exec('PRAGMA journal_mode = WAL');
  db.exec('PRAGMA busy_timeout = 5000');

  db.exec(`
    CREATE TABLE IF NOT EXISTS connections (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      ip TEXT NOT NULL,
      user_agent TEXT,
      connected_at INTEGER NOT NULL,
      disconnected_at INTEGER,
      pubkey TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_conn_ip ON connections(ip);
    CREATE INDEX IF NOT EXISTS idx_conn_at ON connections(connected_at);

    CREATE TABLE IF NOT EXISTS published_events (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      connection_id INTEGER,
      ip TEXT NOT NULL,
      event_id TEXT,
      pubkey TEXT,
      kind INTEGER,
      created_at INTEGER,
      tags TEXT,
      content TEXT,
      content_len INTEGER,
      logged_at INTEGER NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_pe_ip ON published_events(ip);
    CREATE INDEX IF NOT EXISTS idx_pe_kind ON published_events(kind);
    CREATE INDEX IF NOT EXISTS idx_pe_pubkey ON published_events(pubkey);
    CREATE INDEX IF NOT EXISTS idx_pe_at ON published_events(logged_at);

    CREATE TABLE IF NOT EXISTS subscriptions (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      connection_id INTEGER,
      ip TEXT NOT NULL,
      subscription_id TEXT NOT NULL,
      filters TEXT NOT NULL,
      logged_at INTEGER NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_sub_ip ON subscriptions(ip);
    CREATE INDEX IF NOT EXISTS idx_sub_at ON subscriptions(logged_at);
    CREATE INDEX IF NOT EXISTS idx_sub_conn ON subscriptions(connection_id);

    CREATE TABLE IF NOT EXISTS subscription_closes (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      connection_id INTEGER,
      ip TEXT NOT NULL,
      subscription_id TEXT NOT NULL,
      logged_at INTEGER NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_subclose_conn ON subscription_closes(connection_id);
    CREATE INDEX IF NOT EXISTS idx_subclose_ip ON subscription_closes(ip);

    CREATE TABLE IF NOT EXISTS ip_geo (
      ip TEXT PRIMARY KEY,
      country TEXT,
      country_code TEXT,
      region TEXT,
      city TEXT,
      lat REAL,
      lon REAL,
      isp TEXT,
      org TEXT,
      "as" TEXT,
      proxy INTEGER DEFAULT 0,
      hosting INTEGER DEFAULT 0,
      geocoded_at INTEGER NOT NULL
    );

    CREATE TABLE IF NOT EXISTS profiles (
      pubkey TEXT PRIMARY KEY,
      name TEXT,
      display_name TEXT,
      picture TEXT,
      about TEXT,
      nip05 TEXT,
      website TEXT,
      lud16 TEXT,
      raw_json TEXT,
      fetched_at INTEGER NOT NULL
    );
  `);

  // --- Migrations ---
  try { db.exec('ALTER TABLE connections ADD COLUMN pubkey TEXT'); } catch {}
  try { db.exec('CREATE INDEX IF NOT EXISTS idx_conn_pubkey ON connections(pubkey)'); } catch {}
  // Add content column to existing tables (migration)
  try { db.exec('ALTER TABLE published_events ADD COLUMN content TEXT'); } catch {}
}

// ─── Write Queue (batched async writes) ───
// Hot-path logging functions push to an in-memory queue.
// A background timer flushes everything in a single transaction every ~1s.
// This keeps the event loop unblocked for WebSocket forwarding.

const writeQueue = [];
let flushTimer = null;
const FLUSH_INTERVAL_MS = parseInt(process.env.LOG_FLUSH_INTERVAL_MS || '1000', 10);

// Pre-compiled statements (prepared once, reused across flushes)
let stmtInsertConnection, stmtUpdateDisconnection, stmtUpdateConnPubkey,
    stmtInsertEvent, stmtInsertSubscription, stmtInsertSubClose;

function prepareStatements() {
  stmtInsertConnection = db.prepare('INSERT INTO connections (ip, user_agent, connected_at) VALUES (?, ?, ?)');
  stmtUpdateDisconnection = db.prepare('UPDATE connections SET disconnected_at = ? WHERE id = ?');
  stmtUpdateConnPubkey = db.prepare('UPDATE connections SET pubkey = ? WHERE id = ? AND (pubkey IS NULL OR pubkey != ?)');
  stmtInsertEvent = db.prepare('INSERT INTO published_events (connection_id, ip, event_id, pubkey, kind, created_at, tags, content, content_len, logged_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)');
  stmtInsertSubscription = db.prepare('INSERT INTO subscriptions (connection_id, ip, subscription_id, filters, logged_at) VALUES (?, ?, ?, ?, ?)');
  stmtInsertSubClose = db.prepare('INSERT INTO subscription_closes (connection_id, ip, subscription_id, logged_at) VALUES (?, ?, ?, ?)');
}

// Map temp IDs → real SQLite row IDs, resolved during flush
const tempIdMap = new Map();
let nextTempId = -1;

function flushWriteQueue() {
  if (writeQueue.length === 0) return;
  const batch = writeQueue.splice(0, writeQueue.length);
  try {
    db.exec('BEGIN');
    for (const op of batch) {
      switch (op.type) {
        case 'connection':
          const realId = stmtInsertConnection.run(op.ip, op.ua, op.ts).lastInsertRowid;
          tempIdMap.set(op.tempId, realId);
          break;
        case 'disconnection':
          stmtUpdateDisconnection.run(op.ts, tempIdMap.get(op.connId) || op.connId);
          break;
        case 'pubkey':
          stmtUpdateConnPubkey.run(op.pubkey, tempIdMap.get(op.connId) || op.connId, op.pubkey);
          break;
        case 'event':
          stmtInsertEvent.run(tempIdMap.get(op.connId) || op.connId, op.ip, op.eventId, op.pubkey, op.kind, op.createdAt, op.tags, op.content, op.contentLen, op.ts);
          if (op.pubkey) stmtUpdateConnPubkey.run(op.pubkey, tempIdMap.get(op.connId) || op.connId, op.pubkey);
          break;
        case 'subscription':
          stmtInsertSubscription.run(tempIdMap.get(op.connId) || op.connId, op.ip, op.subId, op.filters, op.ts);
          break;
        case 'sub_close':
          stmtInsertSubClose.run(tempIdMap.get(op.connId) || op.connId, op.ip, op.subId, op.ts);
          break;
      }
    }
    db.exec('COMMIT');
  } catch (err) {
    try { db.exec('ROLLBACK'); } catch {}
    console.error('[db] Batch flush error:', err.message);
    // Re-queue the items we failed to write
    writeQueue.unshift(...batch);
  }
}

function startQueueFlusher() {
  if (flushTimer) return;
  prepareStatements();
  flushTimer = setInterval(flushWriteQueue, FLUSH_INTERVAL_MS);
  // Flush on process exit (best effort)
  process.on('beforeExit', flushWriteQueue);
}

// ─── Connection logging (fully async) ───
// Returns a temp ID immediately (no DB access). The real SQLite ID is resolved
// during flush when the connection INSERT runs in the same batch as its events.

function logConnection(ip, userAgent) {
  const tempId = nextTempId--;
  writeQueue.push({ type: 'connection', tempId, ip, ua: userAgent, ts: Math.floor(Date.now() / 1000) });
  return tempId;
}

function logDisconnection(connId) {
  writeQueue.push({ type: 'disconnection', connId, ts: Math.floor(Date.now() / 1000) });
}

function updateConnectionPubkey(connId, pubkey) {
  if (!pubkey) return;
  writeQueue.push({ type: 'pubkey', connId, pubkey });
}

function logPublishedEvent(connId, ip, event) {
  writeQueue.push({
    type: 'event', connId, ip,
    eventId: event.event_id, pubkey: event.pubkey, kind: event.kind,
    createdAt: event.created_at, tags: event.tags,
    content: event.content || '', contentLen: event.content_len,
    ts: Math.floor(Date.now() / 1000),
  });
}

function logSubscription(connId, ip, subId, filters) {
  writeQueue.push({ type: 'subscription', connId, ip, subId, filters, ts: Math.floor(Date.now() / 1000) });
}

function logSubscriptionClose(connId, ip, subId) {
  writeQueue.push({ type: 'sub_close', connId, ip, subId, ts: Math.floor(Date.now() / 1000) });
}

// ─── Profiles ───

function cacheProfile(pubkey, metadata) {
  db.prepare(`
    INSERT OR REPLACE INTO profiles (pubkey, name, display_name, picture, about, nip05, website, lud16, raw_json, fetched_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `).run(
    pubkey,
    metadata.name || null,
    metadata.display_name || null,
    metadata.picture || null,
    metadata.about || null,
    metadata.nip05 || null,
    metadata.website || null,
    metadata.lud16 || null,
    JSON.stringify(metadata),
    Math.floor(Date.now() / 1000)
  );
}

function getProfile(pubkey) {
  return db.prepare('SELECT * FROM profiles WHERE pubkey = ?').get(pubkey);
}

function getProfiles(pubkeys) {
  if (!pubkeys.length) return [];
  const placeholders = pubkeys.map(() => '?').join(',');
  return db.prepare(`SELECT * FROM profiles WHERE pubkey IN (${placeholders})`).all(...pubkeys);
}

function getStaleProfiles(pubkeys) {
  // Return pubkeys that have no cached profile
  if (!pubkeys.length) return [];
  const placeholders = pubkeys.map(() => '?').join(',');
  const rows = db.prepare(`SELECT pubkey FROM profiles WHERE pubkey IN (${placeholders})`).all(...pubkeys);
  const cached = new Set(rows.map(r => r.pubkey));
  return [...new Set(pubkeys)].filter(pk => !cached.has(pk));
}

async function fetchProfilesFromRelay(pubkeys, relayUrl) {
  const { WebSocket } = require('ws');
  return new Promise((resolve) => {
    const ws = new WebSocket(relayUrl);
    const results = {};
    let pending = new Set(pubkeys);
    let settled = false;

    const timeout = setTimeout(() => {
      if (!settled) { settled = true; try { ws.close(); } catch {} resolve(results); }
    }, 5000);

    ws.on('open', () => {
      ws.send(JSON.stringify(['REQ', 'profiles-' + Date.now(), { kinds: [0], authors: pubkeys }]));
    });

    ws.on('message', (data) => {
      try {
        const msg = JSON.parse(data.toString());
        if (msg[0] === 'EVENT' && msg[2]?.pubkey && msg[2]?.kind === 0) {
          try {
            const meta = JSON.parse(msg[2].content);
            cacheProfile(msg[2].pubkey, meta);
            results[msg[2].pubkey] = meta;
          } catch {}
          pending.delete(msg[2].pubkey);
          if (pending.size === 0 && !settled) {
            settled = true;
            clearTimeout(timeout);
            ws.close();
            resolve(results);
          }
        }
        if (msg[0] === 'EOSE' && pending.size > 0 && !settled) {
          // Give 1 more second for trailing events
          setTimeout(() => {
            if (!settled) { settled = true; clearTimeout(timeout); ws.close(); resolve(results); }
          }, 1000);
        }
      } catch {}
    });

    ws.on('error', () => {
      if (!settled) { settled = true; clearTimeout(timeout); resolve(results); }
    });
  });
}

// ─── Geo ───
// Server-side geocoding using ip-api.com (primary) with proper error handling.
// All results cached in ip_geo table. Called as background job, never blocks API responses.

function getUncachedIps(ips) {
  if (!ips.length) return [];
  const placeholders = ips.map(() => '?').join(',');
  const rows = db.prepare(`SELECT ip FROM ip_geo WHERE ip IN (${placeholders})`).all(...ips);
  const cached = new Set(rows.map(r => r.ip));
  return [...new Set(ips)].filter(ip => !cached.has(ip));
}

function cacheGeo(entry) {
  db.prepare(`
    INSERT OR REPLACE INTO ip_geo (ip, country, country_code, region, city, lat, lon, isp, org, "as", proxy, hosting, geocoded_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `).run(
    entry.ip, entry.country, entry.countryCode, entry.region, entry.city,
    entry.lat, entry.lon, entry.isp, entry.org, entry.as,
    entry.proxy ? 1 : 0, entry.hosting ? 1 : 0,
    Math.floor(Date.now() / 1000)
  );
}

let geocodingInProgress = false;

async function geocodeIps(ips) {
  if (geocodingInProgress) { console.log('[geo] Already running, skip'); return; }
  const uncached = getUncachedIps(ips);
  if (!uncached.length) return;

  geocodingInProgress = true;
  console.log(`[geo] Geocoding ${uncached.length} IPs...`);

  try {
    // Provider 1: ipwho.is — HTTPS, free, no key, reliable from containers
    let remaining = [...uncached];
    let cached = 0;

    for (const ip of remaining.slice()) {
      try {
        const r = await fetch(`https://ipwho.is/${encodeURIComponent(ip)}`, { signal: AbortSignal.timeout(8000) });
        const j = await r.json();
        if (j.success !== false && j.latitude) {
          cacheGeo({
            ip, country: j.country, countryCode: j.country_code,
            region: j.region, city: j.city, lat: j.latitude, lon: j.longitude,
            isp: j.connection?.isp, org: j.connection?.org,
            as: j.connection?.asn ? `AS${j.connection.asn}` : null,
            proxy: !!(j.security?.proxy || j.security?.tor),
            hosting: !!(j.type === 'ipv4' && j.connection?.org && /host|server|datacenter|cloud|vps|aws|google|azure|digitalocean/i.test(j.connection?.org || '')),
          });
          cached++;
          remaining = remaining.filter(x => x !== ip);
        }
      } catch (e) {
        console.error(`[geo] ipwho.is failed for ${ip}: ${e.message}`);
      }
    }
    console.log(`[geo] ipwho.is: ${cached}/${uncached.length} cached`);

    // Provider 2: ip-api.com batch (HTTP fallback for any remaining)
    if (remaining.length > 0) {
      console.log(`[geo] Trying ip-api.com for ${remaining.length} remaining...`);
      for (let i = 0; i < remaining.length; i += 100) {
        const batch = remaining.slice(i, i + 100);
        try {
          const res = await fetch('http://ip-api.com/batch?fields=status,country,countryCode,region,city,lat,lon,isp,org,as,proxy,hosting,query', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(batch.map(ip => ({ query: ip }))),
            signal: AbortSignal.timeout(10000),
          });
          if (!res.ok) throw new Error(`HTTP ${res.status}`);
          const results = await res.json();
          let bc = 0;
          for (const r of results) {
            if (r.status === 'success') {
              cacheGeo({ ip: r.query, country: r.country, countryCode: r.countryCode, region: r.region, city: r.city, lat: r.lat, lon: r.lon, isp: r.isp, org: r.org, as: r.as, proxy: r.proxy, hosting: r.hosting });
              bc++;
            }
          }
          console.log(`[geo] ip-api.com batch: ${bc}/${batch.length}`);
        } catch (err) {
          console.error(`[geo] ip-api.com failed: ${err.message}`);
        }
      }
    }

    const finalUncached = getUncachedIps(uncached);
    console.log(`[geo] Done. ${uncached.length - finalUncached.length}/${uncached.length} cached.`);
  } catch (err) {
    console.error(`[geo] Fatal error: ${err.message}`);
  } finally {
    geocodingInProgress = false;
  }
}

function getGeoForIp(ip) {
  return db.prepare('SELECT * FROM ip_geo WHERE ip = ?').get(ip);
}

function getAllGeo() {
  // Pre-aggregate to avoid 3 correlated subqueries per geo row
  return db.prepare(`
    WITH conn_counts AS (
      SELECT ip, COUNT(DISTINCT id) as connections, COUNT(DISTINCT pubkey) as pubkeys
      FROM connections GROUP BY ip
    ),
    event_counts AS (
      SELECT ip, COUNT(DISTINCT id) as events FROM published_events GROUP BY ip
    )
    SELECT g.*,
      COALESCE(c.connections, 0) as connections,
      COALESCE(e.events, 0) as events,
      COALESCE(c.pubkeys, 0) as pubkeys
    FROM ip_geo g
    LEFT JOIN conn_counts c ON c.ip = g.ip
    LEFT JOIN event_counts e ON e.ip = g.ip
    WHERE g.lat IS NOT NULL ORDER BY connections DESC
  `).all();
}

function getGeoForPubkey(pubkey) {
  return db.prepare(`
    SELECT g.*,
      (SELECT COUNT(DISTINCT c.id) FROM connections c WHERE c.ip = g.ip AND c.pubkey = ?) as connections,
      (SELECT COUNT(DISTINCT pe.id) FROM published_events pe WHERE pe.ip = g.ip AND pe.pubkey = ?) as events
    FROM ip_geo g WHERE g.lat IS NOT NULL AND g.ip IN (
      SELECT DISTINCT ip FROM connections WHERE pubkey = ?
      UNION SELECT DISTINCT ip FROM published_events WHERE pubkey = ?
    ) ORDER BY connections DESC
  `).all(pubkey, pubkey, pubkey, pubkey);
}

function getGeoStats() {
  const cached = db.prepare('SELECT COUNT(*) as c FROM ip_geo WHERE lat IS NOT NULL').get().c;
  const total = db.prepare('SELECT COUNT(DISTINCT ip) as c FROM connections').get().c;
  return { cached, total, uncached: Math.max(0, total - cached) };
}

function getGeoStatsForPubkey(pubkey) {
  const ips = getUniqueIpsForPubkey(pubkey);
  if (!ips.length) return { cached: 0, total: 0, uncached: 0 };
  const placeholders = ips.map(() => '?').join(',');
  const cached = db.prepare(`SELECT COUNT(*) as c FROM ip_geo WHERE lat IS NOT NULL AND ip IN (${placeholders})`).get(...ips).c;
  return { cached, total: ips.length, uncached: Math.max(0, ips.length - cached) };
}

function getAllUniqueIps() {
  return db.prepare('SELECT DISTINCT ip FROM connections').all().map(r => r.ip);
}

function getUniqueIpsForPubkey(pubkey) {
  return db.prepare(`
    SELECT DISTINCT ip FROM (
      SELECT ip FROM connections WHERE pubkey = ?
      UNION SELECT ip FROM published_events WHERE pubkey = ?
    )
  `).all(pubkey, pubkey).map(r => r.ip);
}

// ─── Query functions ───

// In-memory stats cache (30s TTL) — avoids 6 full-table scans on every dashboard load
let _statsCache = null;
let _statsCacheAt = 0;
const STATS_TTL_MS = 30000;

function getStats() {
  const now = Date.now();
  if (_statsCache && (now - _statsCacheAt) < STATS_TTL_MS) return _statsCache;
  _statsCache = {
    totalConnections: db.prepare('SELECT COUNT(*) as c FROM connections').get().c,
    uniqueIps: db.prepare('SELECT COUNT(DISTINCT ip) as c FROM connections').get().c,
    totalEvents: db.prepare('SELECT COUNT(*) as c FROM published_events').get().c,
    totalSubscriptions: db.prepare('SELECT COUNT(*) as c FROM subscriptions').get().c,
    activeConnections: db.prepare('SELECT COUNT(*) as c FROM connections WHERE disconnected_at IS NULL').get().c,
    uniquePubkeys: db.prepare('SELECT COUNT(DISTINCT pubkey) as c FROM published_events WHERE pubkey IS NOT NULL').get().c,
  };
  _statsCacheAt = now;
  return _statsCache;
}

function getConnections(limit, offset) {
  return db.prepare('SELECT * FROM connections ORDER BY connected_at DESC LIMIT ? OFFSET ?').all(limit, offset);
}

function getEvents(limit, offset) {
  return db.prepare(`
    SELECT pe.*, g.country, g.country_code, g.city, g.lat, g.lon
    FROM published_events pe
    LEFT JOIN ip_geo g ON pe.ip = g.ip
    ORDER BY pe.logged_at DESC LIMIT ? OFFSET ?
  `).all(limit, offset);
}

function getSubscriptions(limit, offset) {
  return db.prepare('SELECT s.*, c.pubkey FROM subscriptions s LEFT JOIN connections c ON s.connection_id = c.id ORDER BY s.logged_at DESC LIMIT ? OFFSET ?').all(limit, offset);
}

function getTopIps(limit) {
  // Pre-aggregate each table separately, then join — avoids O(n) correlated subqueries
  return db.prepare(`
    WITH conn_counts AS (
      SELECT ip, COUNT(DISTINCT id) as connections FROM connections GROUP BY ip
    ),
    event_counts AS (
      SELECT ip, COUNT(*) as events FROM published_events GROUP BY ip
    ),
    sub_counts AS (
      SELECT ip, COUNT(*) as subscriptions FROM subscriptions GROUP BY ip
    )
    SELECT c.ip, c.connections,
      COALESCE(e.events, 0) as events,
      COALESCE(s.subscriptions, 0) as subscriptions
    FROM conn_counts c
    LEFT JOIN event_counts e ON e.ip = c.ip
    LEFT JOIN sub_counts s ON s.ip = c.ip
    ORDER BY c.connections DESC LIMIT ?
  `).all(limit);
}

function getActivity() {
  const weekAgo = Math.floor(Date.now() / 1000) - 7 * 24 * 60 * 60;
  return {
    connections: db.prepare(`SELECT strftime('%Y-%m-%dT%H:00:00', connected_at, 'unixepoch') as t, COUNT(*) as c FROM connections WHERE connected_at >= ? GROUP BY t ORDER BY t`).all(weekAgo),
    events: db.prepare(`SELECT strftime('%Y-%m-%dT%H:00:00', logged_at, 'unixepoch') as t, COUNT(*) as c FROM published_events WHERE logged_at >= ? GROUP BY t ORDER BY t`).all(weekAgo),
  };
}

// ─── Pubkey queries ───

function getPubkeys(limit, offset, filter) {
  if (filter === 'readers') {
    return db.prepare(`
      SELECT NULL as pubkey, c.ip,
        COUNT(DISTINCT c.id) as connections,
        COUNT(DISTINCT s.id) as sub_count, 0 as event_count,
        MIN(c.connected_at) as first_seen, MAX(c.connected_at) as last_seen
      FROM connections c LEFT JOIN subscriptions s ON s.connection_id = c.id
      WHERE c.pubkey IS NULL AND EXISTS (SELECT 1 FROM subscriptions WHERE connection_id = c.id)
      GROUP BY c.ip ORDER BY last_seen DESC LIMIT ? OFFSET ?
    `).all(limit, offset);
  }
  // Pre-aggregate each table separately to avoid fan-out explosion from multi-table JOIN
  return db.prepare(`
    WITH pe_agg AS (
      SELECT pubkey,
        COUNT(DISTINCT id) as event_count,
        COUNT(DISTINCT ip) as event_ips,
        MIN(logged_at) as first_seen, MAX(logged_at) as last_seen
      FROM published_events WHERE pubkey IS NOT NULL GROUP BY pubkey
    ),
    conn_agg AS (
      SELECT pubkey, COUNT(DISTINCT id) as connections
      FROM connections WHERE pubkey IS NOT NULL GROUP BY pubkey
    ),
    sub_agg AS (
      SELECT c.pubkey, COUNT(DISTINCT s.id) as sub_count
      FROM subscriptions s JOIN connections c ON s.connection_id = c.id
      WHERE c.pubkey IS NOT NULL GROUP BY c.pubkey
    )
    SELECT pe.pubkey, pe.event_count,
      COALESCE(s.sub_count, 0) as sub_count,
      pe.event_ips,
      COALESCE(c.connections, 0) as connections,
      pe.first_seen, pe.last_seen
    FROM pe_agg pe
    LEFT JOIN conn_agg c ON c.pubkey = pe.pubkey
    LEFT JOIN sub_agg s ON s.pubkey = pe.pubkey
    ORDER BY pe.last_seen DESC LIMIT ? OFFSET ?
  `).all(limit, offset);
}

function getReaderStats() {
  return {
    totalReaders: db.prepare(`SELECT COUNT(DISTINCT c.ip) as c FROM connections c WHERE c.pubkey IS NULL AND EXISTS (SELECT 1 FROM subscriptions WHERE connection_id = c.id)`).get().c,
    totalPublishers: db.prepare('SELECT COUNT(DISTINCT pubkey) as c FROM published_events WHERE pubkey IS NOT NULL').get().c,
  };
}

function getPubkeyDetail(pubkey) {
  const summary = db.prepare(`
    SELECT pubkey, COUNT(DISTINCT id) as event_count,
      MIN(logged_at) as first_seen, MAX(logged_at) as last_seen
    FROM published_events WHERE pubkey = ? GROUP BY pubkey
  `).get(pubkey);
  if (!summary) return null;

  const connections = db.prepare('SELECT COUNT(DISTINCT id) as c FROM connections WHERE pubkey = ?').get(pubkey).c;
  const subscriptions = db.prepare(`SELECT COUNT(DISTINCT s.id) as c FROM subscriptions s JOIN connections c ON s.connection_id = c.id WHERE c.pubkey = ?`).get(pubkey).c;
  const ips = db.prepare(`SELECT DISTINCT ip FROM (SELECT ip FROM published_events WHERE pubkey = ? UNION SELECT ip FROM connections WHERE pubkey = ?)`).all(pubkey, pubkey).map(r => r.ip);
  const kinds = db.prepare(`SELECT kind, COUNT(*) as count FROM published_events WHERE pubkey = ? GROUP BY kind ORDER BY count DESC`).all(pubkey);
  const profile = getProfile(pubkey);

  return { ...summary, ips_used: ips.length, connections, subscriptions, ips, kinds, profile };
}

function getPubkeyEvents(pubkey, limit, offset) {
  return db.prepare(`
    SELECT pe.*, g.country, g.country_code, g.city, g.lat, g.lon
    FROM published_events pe
    LEFT JOIN ip_geo g ON pe.ip = g.ip
    WHERE pe.pubkey = ? ORDER BY pe.logged_at DESC LIMIT ? OFFSET ?
  `).all(pubkey, limit, offset);
}

function getPubkeySubscriptions(pubkey, limit, offset) {
  return db.prepare(`SELECT s.*, c.ip FROM subscriptions s JOIN connections c ON s.connection_id = c.id WHERE c.pubkey = ? ORDER BY s.logged_at DESC LIMIT ? OFFSET ?`).all(pubkey, limit, offset);
}

function getPubkeyIps(pubkey) {
  return db.prepare(`SELECT ip, COUNT(DISTINCT id) as connections, MIN(connected_at) as first_seen, MAX(connected_at) as last_seen FROM connections WHERE pubkey = ? GROUP BY ip ORDER BY last_seen DESC`).all(pubkey);
}

// ─── IP Detail (for map popup click-through) ───

function getIpDetail(ip) {
  const geo = db.prepare('SELECT * FROM ip_geo WHERE ip = ?').get(ip);
  const connections = db.prepare(`
    SELECT c.*, p.name, p.display_name, p.picture
    FROM connections c LEFT JOIN profiles p ON c.pubkey = p.pubkey
    WHERE c.ip = ? ORDER BY c.connected_at DESC LIMIT 50
  `).all(ip);
  const events = db.prepare(`
    SELECT pe.*, p.name, p.display_name
    FROM published_events pe LEFT JOIN profiles p ON pe.pubkey = p.pubkey
    WHERE pe.ip = ? ORDER BY pe.logged_at DESC LIMIT 50
  `).all(ip);
  const subscriptions = db.prepare(`
    SELECT s.*, c.pubkey FROM subscriptions s
    LEFT JOIN connections c ON s.connection_id = c.id
    WHERE s.ip = ? ORDER BY s.logged_at DESC LIMIT 50
  `).all(ip);
  const stats = {
    connections: db.prepare('SELECT COUNT(*) as c FROM connections WHERE ip = ?').get(ip).c,
    events: db.prepare('SELECT COUNT(*) as c FROM published_events WHERE ip = ?').get(ip).c,
    subscriptions: db.prepare('SELECT COUNT(*) as c FROM subscriptions WHERE ip = ?').get(ip).c,
    pubkeys: db.prepare('SELECT COUNT(DISTINCT pubkey) as c FROM connections WHERE ip = ? AND pubkey IS NOT NULL').get(ip).c,
    first_seen: db.prepare('SELECT MIN(connected_at) as c FROM connections WHERE ip = ?').get(ip).c,
    last_seen: db.prepare('SELECT MAX(connected_at) as c FROM connections WHERE ip = ?').get(ip).c,
  };
  return { ip, geo, stats, connections, events, subscriptions };
}

function getAllPubkeys() {
  return db.prepare('SELECT DISTINCT pubkey FROM published_events WHERE pubkey IS NOT NULL').all().map(r => r.pubkey);
}

module.exports = {
  init, startQueueFlusher, flushWriteQueue,
  logConnection, logDisconnection, updateConnectionPubkey,
  logPublishedEvent, logSubscription, logSubscriptionClose,
  getStats, getConnections, getEvents, getSubscriptions, getTopIps, getActivity,
  getPubkeys, getReaderStats, getPubkeyDetail, getPubkeyEvents, getPubkeySubscriptions, getPubkeyIps, getIpDetail,
  // Geo
  geocodeIps, getGeoForIp, getAllGeo, getGeoForPubkey, getGeoStats, getGeoStatsForPubkey, getAllUniqueIps, getUniqueIpsForPubkey,
  // Profiles
  cacheProfile, getProfile, getProfiles, getStaleProfiles, fetchProfilesFromRelay,
  getAllPubkeys,
};
