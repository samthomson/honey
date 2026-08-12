const { DatabaseSync } = require('node:sqlite');
const fs = require('fs');
const path = require('path');

let db;

function init(dataDir) {
  if (!fs.existsSync(dataDir)) {
    fs.mkdirSync(dataDir, { recursive: true });
  }

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
  `);

  // --- Migrations ---
  try { db.exec('ALTER TABLE connections ADD COLUMN pubkey TEXT'); } catch {}
  try { db.exec('CREATE INDEX IF NOT EXISTS idx_conn_pubkey ON connections(pubkey)'); } catch {}
}

function logConnection(ip, userAgent) {
  const stmt = db.prepare('INSERT INTO connections (ip, user_agent, connected_at) VALUES (?, ?, ?)');
  return stmt.run(ip, userAgent, Math.floor(Date.now() / 1000)).lastInsertRowid;
}

function logDisconnection(connId) {
  db.prepare('UPDATE connections SET disconnected_at = ? WHERE id = ?')
    .run(Math.floor(Date.now() / 1000), connId);
}

function updateConnectionPubkey(connId, pubkey) {
  if (!pubkey) return;
  db.prepare('UPDATE connections SET pubkey = ? WHERE id = ? AND (pubkey IS NULL OR pubkey != ?)')
    .run(pubkey, connId, pubkey);
}

function logPublishedEvent(connId, ip, event) {
  db.prepare(
    'INSERT INTO published_events (connection_id, ip, event_id, pubkey, kind, created_at, tags, content_len, logged_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)'
  ).run(
    connId, ip, event.event_id, event.pubkey, event.kind, event.created_at,
    event.tags, event.content_len, Math.floor(Date.now() / 1000)
  );

  if (event.pubkey) {
    updateConnectionPubkey(connId, event.pubkey);
  }
}

function logSubscription(connId, ip, subId, filters) {
  db.prepare(
    'INSERT INTO subscriptions (connection_id, ip, subscription_id, filters, logged_at) VALUES (?, ?, ?, ?, ?)'
  ).run(connId, ip, subId, filters, Math.floor(Date.now() / 1000));
}

function logSubscriptionClose(connId, ip, subId) {
  db.prepare(
    'INSERT INTO subscription_closes (connection_id, ip, subscription_id, logged_at) VALUES (?, ?, ?, ?)'
  ).run(connId, ip, subId, Math.floor(Date.now() / 1000));
}

// --- Geo ---

function getUncachedIps(ips) {
  if (!ips.length) return [];
  const placeholders = ips.map(() => '?').join(',');
  const rows = db.prepare(`SELECT ip FROM ip_geo WHERE ip IN (${placeholders})`).all(...ips);
  const cached = new Set(rows.map(r => r.ip));
  return ips.filter(ip => !cached.has(ip));
}

function cacheGeo(entry) {
  db.prepare(`
    INSERT OR REPLACE INTO ip_geo (ip, country, country_code, region, city, lat, lon, isp, org, as, proxy, hosting, geocoded_at)
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
  if (geocodingInProgress) { console.log('[geo] Already in progress, skipping'); return; }
  const uncached = getUncachedIps(ips);
  if (uncached.length === 0) return;

  geocodingInProgress = true;
  console.log(`[geo] Geocoding ${uncached.length} uncached IPs...`);

  try {
    // Strategy 1: ip-api.com batch (HTTP, fast, up to 100 per request)
    let batchWorked = false;
    for (let i = 0; i < uncached.length; i += 100) {
      const batch = uncached.slice(i, i + 100);
      try {
        const res = await fetch('http://ip-api.com/batch?fields=status,country,countryCode,region,city,lat,lon,isp,org,as,proxy,hosting,query', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(batch.map(ip => ({ query: ip }))),
          signal: AbortSignal.timeout(10000),
        });
        const results = await res.json();
        let cached = 0;
        for (const r of results) {
          if (r.status === 'success') {
            cacheGeo({
              ip: r.query, country: r.country, countryCode: r.countryCode,
              region: r.region, city: r.city, lat: r.lat, lon: r.lon,
              isp: r.isp, org: r.org, as: r.as,
              proxy: r.proxy, hosting: r.hosting,
            });
            cached++;
          }
        }
        console.log(`[geo] ip-api.com batch: ${cached}/${batch.length} cached`);
        batchWorked = true;
      } catch (err) {
        console.error(`[geo] ip-api.com batch failed: ${err.message}`);
      }
    }
    if (batchWorked) {
      const remaining = getUncachedIps(uncached);
      if (remaining.length === 0) { console.log('[geo] All cached via batch'); return; }
    }

    // Strategy 2: ipwho.is (HTTPS, free, no key, individual)
    const remaining2 = getUncachedIps(uncached);
    if (remaining2.length > 0) {
      console.log(`[geo] Trying ipwho.is for ${remaining2.length} IPs...`);
      let cached2 = 0;
      for (const ip of remaining2) {
        try {
          const res = await fetch(`https://ipwho.is/${encodeURIComponent(ip)}`, {
            signal: AbortSignal.timeout(8000),
          });
          const r = await res.json();
          if (r.success !== false && r.latitude) {
            cacheGeo({
              ip,
              country: r.country, countryCode: r.country_code,
              region: r.region, city: r.city,
              lat: r.latitude, lon: r.longitude,
              isp: r.connection?.isp || r.connection?.org,
              org: r.connection?.org,
              as: r.connection?.asn ? `AS${r.connection.asn}` : null,
              proxy: !!(r.security?.proxy || r.security?.tor),
              hosting: false,
            });
            cached2++;
          }
        } catch {}
      }
      console.log(`[geo] ipwho.is: ${cached2}/${remaining2.length} cached`);
    }

    // Strategy 3: ipapi.co (HTTPS, 1 req/sec rate limit, last resort)
    const remaining3 = getUncachedIps(uncached);
    if (remaining3.length > 0) {
      console.log(`[geo] Trying ipapi.co for ${remaining3.length} IPs...`);
      for (const ip of remaining3) {
        try {
          const res = await fetch(`https://ipapi.co/${encodeURIComponent(ip)}/json/`, {
            signal: AbortSignal.timeout(8000),
          });
          const r = await res.json();
          if (r.latitude && r.longitude) {
            cacheGeo({
              ip, country: r.country_name, countryCode: r.country_code,
              region: r.region, city: r.city,
              lat: r.latitude, lon: r.longitude,
              isp: r.org || r.asn, org: r.org, as: r.asn,
              proxy: false, hosting: false,
            });
          }
          await new Promise(resolve => setTimeout(resolve, 1100));
        } catch (err) {
          console.error(`[geo] ipapi.co failed for ${ip}: ${err.message}`);
        }
      }
    }

    const finalUncached = getUncachedIps(uncached);
    console.log(`[geo] Done. ${uncached.length - finalUncached.length}/${uncached.length} cached.`);
  } finally {
    geocodingInProgress = false;
  }
}

function getGeoForIp(ip) {
  return db.prepare('SELECT * FROM ip_geo WHERE ip = ?').get(ip);
}

function getGeoForIps(ips) {
  if (!ips.length) return [];
  const placeholders = ips.map(() => '?').join(',');
  return db.prepare(`SELECT * FROM ip_geo WHERE ip IN (${placeholders})`).all(...ips);
}

function getAllGeo() {
  return db.prepare(`
    SELECT g.*,
      (SELECT COUNT(DISTINCT c.id) FROM connections c WHERE c.ip = g.ip) as connections,
      (SELECT COUNT(DISTINCT pe.id) FROM published_events pe WHERE pe.ip = g.ip) as events,
      (SELECT COUNT(DISTINCT c.pubkey) FROM connections c WHERE c.ip = g.ip AND c.pubkey IS NOT NULL) as pubkeys
    FROM ip_geo g
    WHERE g.lat IS NOT NULL
    ORDER BY connections DESC
  `).all();
}

function getGeoForPubkey(pubkey) {
  return db.prepare(`
    SELECT g.*,
      (SELECT COUNT(DISTINCT c.id) FROM connections c WHERE c.ip = g.ip AND c.pubkey = ?) as connections,
      (SELECT COUNT(DISTINCT pe.id) FROM published_events pe WHERE pe.ip = g.ip AND pe.pubkey = ?) as events
    FROM ip_geo g
    WHERE g.lat IS NOT NULL AND g.ip IN (
      SELECT DISTINCT ip FROM connections WHERE pubkey = ?
      UNION
      SELECT DISTINCT ip FROM published_events WHERE pubkey = ?
    )
    ORDER BY connections DESC
  `).all(pubkey, pubkey, pubkey, pubkey);
}

// --- Query functions ---

function getStats() {
  return {
    totalConnections: db.prepare('SELECT COUNT(*) as c FROM connections').get().c,
    uniqueIps: db.prepare('SELECT COUNT(DISTINCT ip) as c FROM connections').get().c,
    totalEvents: db.prepare('SELECT COUNT(*) as c FROM published_events').get().c,
    totalSubscriptions: db.prepare('SELECT COUNT(*) as c FROM subscriptions').get().c,
    activeConnections: db.prepare('SELECT COUNT(*) as c FROM connections WHERE disconnected_at IS NULL').get().c,
    uniquePubkeys: db.prepare('SELECT COUNT(DISTINCT pubkey) as c FROM published_events WHERE pubkey IS NOT NULL').get().c,
  };
}

function getConnections(limit, offset) {
  return db.prepare('SELECT * FROM connections ORDER BY connected_at DESC LIMIT ? OFFSET ?').all(limit, offset);
}

function getEvents(limit, offset) {
  return db.prepare(`
    SELECT pe.*, c.pubkey as conn_pubkey
    FROM published_events pe
    LEFT JOIN connections c ON pe.connection_id = c.id
    ORDER BY pe.logged_at DESC LIMIT ? OFFSET ?
  `).all(limit, offset);
}

function getSubscriptions(limit, offset) {
  return db.prepare(`
    SELECT s.*, c.pubkey
    FROM subscriptions s
    LEFT JOIN connections c ON s.connection_id = c.id
    ORDER BY s.logged_at DESC LIMIT ? OFFSET ?
  `).all(limit, offset);
}

function getTopIps(limit) {
  return db.prepare(`
    SELECT ip,
      COUNT(DISTINCT c.id) as connections,
      (SELECT COUNT(*) FROM published_events WHERE ip = c.ip) as events,
      (SELECT COUNT(*) FROM subscriptions WHERE ip = c.ip) as subscriptions
    FROM connections c
    GROUP BY ip
    ORDER BY connections DESC
    LIMIT ?
  `).all(limit);
}

function getActivity() {
  const now = Math.floor(Date.now() / 1000);
  const weekAgo = now - 7 * 24 * 60 * 60;

  const connections = db.prepare(`
    SELECT strftime('%Y-%m-%dT%H:00:00', connected_at, 'unixepoch') as t, COUNT(*) as c
    FROM connections WHERE connected_at >= ?
    GROUP BY t ORDER BY t
  `).all(weekAgo);

  const events = db.prepare(`
    SELECT strftime('%Y-%m-%dT%H:00:00', logged_at, 'unixepoch') as t, COUNT(*) as c
    FROM published_events WHERE logged_at >= ?
    GROUP BY t ORDER BY t
  `).all(weekAgo);

  return { connections, events };
}

// --- Pubkey queries ---

function getPubkeys(limit, offset, filter) {
  // filter: 'all' | 'publishers' | 'readers'
  if (filter === 'readers') {
    // Anonymous readers: connections with subscriptions but no published events and no pubkey
    return db.prepare(`
      SELECT
        NULL as pubkey,
        c.ip,
        COUNT(DISTINCT c.id) as connections,
        COUNT(DISTINCT s.id) as sub_count,
        0 as event_count,
        MIN(c.connected_at) as first_seen,
        MAX(c.connected_at) as last_seen
      FROM connections c
      LEFT JOIN subscriptions s ON s.connection_id = c.id
      WHERE c.pubkey IS NULL
        AND EXISTS (SELECT 1 FROM subscriptions WHERE connection_id = c.id)
      GROUP BY c.ip
      ORDER BY last_seen DESC
      LIMIT ? OFFSET ?
    `).all(limit, offset);
  }

  // publishers (default): all from published_events
  return db.prepare(`
    SELECT
      pe.pubkey,
      COUNT(DISTINCT pe.id) as event_count,
      COUNT(DISTINCT s.id) as sub_count,
      COUNT(DISTINCT pe.ip) as event_ips,
      COUNT(DISTINCT c.id) as connections,
      MIN(pe.logged_at) as first_seen,
      MAX(pe.logged_at) as last_seen
    FROM published_events pe
    LEFT JOIN connections c ON c.pubkey = pe.pubkey
    LEFT JOIN subscriptions s ON s.connection_id = c.id
    WHERE pe.pubkey IS NOT NULL
    GROUP BY pe.pubkey
    ORDER BY last_seen DESC
    LIMIT ? OFFSET ?
  `).all(limit, offset);
}

function getReaderStats() {
  const totalReaders = db.prepare(`
    SELECT COUNT(DISTINCT c.ip) as c
    FROM connections c
    WHERE c.pubkey IS NULL
      AND EXISTS (SELECT 1 FROM subscriptions WHERE connection_id = c.id)
  `).get().c;

  const totalPublishers = db.prepare('SELECT COUNT(DISTINCT pubkey) as c FROM published_events WHERE pubkey IS NOT NULL').get().c;

  return { totalReaders, totalPublishers };
}

function getPubkeyDetail(pubkey) {
  const summary = db.prepare(`
    SELECT
      pubkey,
      COUNT(DISTINCT id) as event_count,
      COUNT(DISTINCT ip) as ips_used,
      MIN(logged_at) as first_seen,
      MAX(logged_at) as last_seen
    FROM published_events
    WHERE pubkey = ?
    GROUP BY pubkey
  `).get(pubkey);

  if (!summary) return null;

  const connections = db.prepare('SELECT COUNT(DISTINCT id) as c FROM connections WHERE pubkey = ?').get(pubkey).c;

  const subscriptions = db.prepare(`
    SELECT COUNT(DISTINCT s.id) as c
    FROM subscriptions s
    JOIN connections c ON s.connection_id = c.id
    WHERE c.pubkey = ?
  `).get(pubkey).c;

  const ips = db.prepare(`
    SELECT DISTINCT ip FROM (
      SELECT ip FROM published_events WHERE pubkey = ?
      UNION
      SELECT ip FROM connections WHERE pubkey = ?
    )
  `).all(pubkey, pubkey);

  const kinds = db.prepare(`
    SELECT kind, COUNT(*) as count
    FROM published_events
    WHERE pubkey = ?
    GROUP BY kind ORDER BY count DESC
  `).all(pubkey);

  return { ...summary, connections, subscriptions, ips: ips.map(r => r.ip), kinds };
}

function getPubkeyEvents(pubkey, limit, offset) {
  return db.prepare('SELECT * FROM published_events WHERE pubkey = ? ORDER BY logged_at DESC LIMIT ? OFFSET ?').all(pubkey, limit, offset);
}

function getPubkeySubscriptions(pubkey, limit, offset) {
  return db.prepare(`
    SELECT s.*, c.ip, c.pubkey
    FROM subscriptions s
    JOIN connections c ON s.connection_id = c.id
    WHERE c.pubkey = ?
    ORDER BY s.logged_at DESC LIMIT ? OFFSET ?
  `).all(pubkey, limit, offset);
}

function getPubkeyIps(pubkey) {
  return db.prepare(`
    SELECT ip, COUNT(DISTINCT id) as connections, MIN(connected_at) as first_seen, MAX(connected_at) as last_seen
    FROM connections
    WHERE pubkey = ?
    GROUP BY ip ORDER BY last_seen DESC
  `).all(pubkey);
}

function getGeoStats() {
  const cached = db.prepare('SELECT COUNT(*) as c FROM ip_geo WHERE lat IS NOT NULL').get().c;
  const total = db.prepare('SELECT COUNT(DISTINCT ip) as c FROM connections').get().c;
  return { cached, total, uncached: Math.max(0, total - cached) };
}

function getAllUniqueIps() {
  return db.prepare('SELECT DISTINCT ip FROM connections').all().map(r => r.ip);
}

function getUniqueIpsForPubkey(pubkey) {
  const rows = db.prepare(`
    SELECT DISTINCT ip FROM (
      SELECT ip FROM connections WHERE pubkey = ?
      UNION
      SELECT ip FROM published_events WHERE pubkey = ?
    )
  `).all(pubkey, pubkey);
  return rows.map(r => r.ip);
}

module.exports = {
  init,
  logConnection,
  logDisconnection,
  updateConnectionPubkey,
  logPublishedEvent,
  logSubscription,
  logSubscriptionClose,
  getStats,
  getConnections,
  getEvents,
  getSubscriptions,
  getTopIps,
  getActivity,
  getPubkeys,
  getReaderStats,
  getPubkeyDetail,
  getPubkeyEvents,
  getPubkeySubscriptions,
  getPubkeyIps,
  // Geo
  geocodeIps,
  getGeoForIp,
  getGeoForIps,
  getAllGeo,
  getGeoForPubkey,
  getGeoStats,
  getAllUniqueIps,
  getUniqueIpsForPubkey,
};
