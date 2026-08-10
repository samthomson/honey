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
      disconnected_at INTEGER
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

    CREATE TABLE IF NOT EXISTS subscription_closes (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      connection_id INTEGER,
      ip TEXT NOT NULL,
      subscription_id TEXT NOT NULL,
      logged_at INTEGER NOT NULL
    );
  `);
}

function logConnection(ip, userAgent) {
  const stmt = db.prepare('INSERT INTO connections (ip, user_agent, connected_at) VALUES (?, ?, ?)');
  return stmt.run(ip, userAgent, Math.floor(Date.now() / 1000)).lastInsertRowid;
}

function logDisconnection(connId) {
  db.prepare('UPDATE connections SET disconnected_at = ? WHERE id = ?')
    .run(Math.floor(Date.now() / 1000), connId);
}

function logPublishedEvent(connId, ip, event) {
  db.prepare(
    'INSERT INTO published_events (connection_id, ip, event_id, pubkey, kind, created_at, tags, content_len, logged_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)'
  ).run(
    connId, ip, event.event_id, event.pubkey, event.kind, event.created_at,
    event.tags, event.content_len, Math.floor(Date.now() / 1000)
  );
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
  };
}

function getConnections(limit, offset) {
  return db.prepare('SELECT * FROM connections ORDER BY connected_at DESC LIMIT ? OFFSET ?').all(limit, offset);
}

function getEvents(limit, offset) {
  return db.prepare('SELECT * FROM published_events ORDER BY logged_at DESC LIMIT ? OFFSET ?').all(limit, offset);
}

function getSubscriptions(limit, offset) {
  return db.prepare('SELECT * FROM subscriptions ORDER BY logged_at DESC LIMIT ? OFFSET ?').all(limit, offset);
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

module.exports = {
  init,
  logConnection,
  logDisconnection,
  logPublishedEvent,
  logSubscription,
  logSubscriptionClose,
  getStats,
  getConnections,
  getEvents,
  getSubscriptions,
  getTopIps,
  getActivity,
};
