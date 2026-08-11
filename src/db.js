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
  `);

  // --- Migrations (additive, safe to re-run) ---
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

  // Associate pubkey to the connection
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

// --- Query functions for admin API ---

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

function getPubkeys(limit, offset) {
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

  const connections = db.prepare(`
    SELECT COUNT(DISTINCT id) as c FROM connections WHERE pubkey = ?
  `).get(pubkey).c;

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
  return db.prepare(`
    SELECT * FROM published_events
    WHERE pubkey = ?
    ORDER BY logged_at DESC LIMIT ? OFFSET ?
  `).all(pubkey, limit, offset);
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
  getPubkeyDetail,
  getPubkeyEvents,
  getPubkeySubscriptions,
  getPubkeyIps,
};
