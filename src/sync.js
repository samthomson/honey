const { getClient, INDICES } = require('./es');

// Watermarks: track last synced row ID per table
// Stored in a simple JSON file so it survives restarts
const fs = require('fs');
const path = require('path');

const WATERMARK_FILE = process.env.WATERMARK_FILE || path.join(process.env.DATA_DIR || './data', '.es-watermark.json');
const SYNC_INTERVAL_MS = parseInt(process.env.SYNC_INTERVAL_MS || '60000', 10);
const BATCH_SIZE = 500;

function loadWatermarks() {
  try {
    return JSON.parse(fs.readFileSync(WATERMARK_FILE, 'utf8'));
  } catch {
    return { connections: 0, events: 0, subscriptions: 0, geo: 0 };
  }
}

function saveWatermarks(wm) {
  try {
    fs.writeFileSync(WATERMARK_FILE, JSON.stringify(wm));
  } catch (err) {
    console.error('[sync] Failed to save watermarks:', err.message);
  }
}

let db = null;
let timer = null;

function setDb(dbModule) {
  db = dbModule;
}

// Read new rows from a SQLite table since last watermark
function getNewRows(table, idCol, lastId, limit) {
  // Access the internal db handle — db module exposes getDb for us
  return db.getRawDb().prepare(
    `SELECT * FROM ${table} WHERE ${idCol} > ? ORDER BY ${idCol} ASC LIMIT ?`
  ).all(lastId, limit);
}

function getNewGeoRows(lastGeocodedAt) {
  // Sync geo rows that were updated since last sync
  return db.getRawDb().prepare(
    'SELECT * FROM ip_geo WHERE geocoded_at > ? ORDER BY geocoded_at ASC LIMIT ?'
  ).all(lastGeocodedAt, BATCH_SIZE);
}

async function syncConnections(es, wm) {
  let total = 0;
  while (true) {
    const rows = getNewRows('connections', 'id', wm.connections, BATCH_SIZE);
    if (!rows.length) break;

    const body = [];
    for (const r of rows) {
      body.push({ index: { _index: INDICES.connections, _id: r.id } });
      body.push({
        ip: r.ip,
        user_agent: r.user_agent,
        connected_at: r.connected_at,
        disconnected_at: r.disconnected_at,
        pubkey: r.pubkey || null,
        duration: r.disconnected_at ? (r.disconnected_at - r.connected_at) : null,
      });
    }

    const result = await es.bulk({ refresh: false, body });
    if (result.errors) {
      const errs = result.items.filter(i => i.error).slice(0, 3);
      console.error('[sync] ES bulk errors (connections):', errs);
    }

    wm.connections = rows[rows.length - 1].id;
    total += rows.length;

    if (rows.length < BATCH_SIZE) break;
  }
  return total;
}

async function syncEvents(es, wm) {
  let total = 0;
  while (true) {
    const rows = getNewRows('published_events', 'id', wm.events, BATCH_SIZE);
    if (!rows.length) break;

    const body = [];
    for (const r of rows) {
      body.push({ index: { _index: INDICES.events, _id: r.id } });
      body.push({
        connection_id: r.connection_id,
        ip: r.ip,
        event_id: r.event_id,
        pubkey: r.pubkey || null,
        kind: r.kind,
        created_at: r.created_at,
        tags: r.tags,
        content: r.content || '',
        content_len: r.content_len || 0,
        logged_at: r.logged_at,
      });
    }

    const result = await es.bulk({ refresh: false, body });
    if (result.errors) {
      const errs = result.items.filter(i => i.error).slice(0, 3);
      console.error('[sync] ES bulk errors (events):', errs);
    }

    wm.events = rows[rows.length - 1].id;
    total += rows.length;

    if (rows.length < BATCH_SIZE) break;
  }
  return total;
}

async function syncSubscriptions(es, wm) {
  let total = 0;
  while (true) {
    const rows = getNewRows('subscriptions', 'id', wm.subscriptions, BATCH_SIZE);
    if (!rows.length) break;

    // Enrich with pubkey from connections
    const body = [];
    for (const r of rows) {
      // Look up pubkey from connections cache
      let pubkey = null;
      if (r.connection_id) {
        const conn = db.getRawDb().prepare('SELECT pubkey FROM connections WHERE id = ?').get(r.connection_id);
        pubkey = conn?.pubkey || null;
      }

      body.push({ index: { _index: INDICES.subscriptions, _id: r.id } });
      body.push({
        connection_id: r.connection_id,
        ip: r.ip,
        subscription_id: r.subscription_id,
        filters: r.filters,
        logged_at: r.logged_at,
        pubkey,
      });
    }

    const result = await es.bulk({ refresh: false, body });
    if (result.errors) {
      const errs = result.items.filter(i => i.error).slice(0, 3);
      console.error('[sync] ES bulk errors (subscriptions):', errs);
    }

    wm.subscriptions = rows[rows.length - 1].id;
    total += rows.length;

    if (rows.length < BATCH_SIZE) break;
  }
  return total;
}

async function syncGeo(es, wm) {
  let total = 0;
  while (true) {
    const rows = getNewGeoRows(wm.geo || 0);
    if (!rows.length) break;

    const body = [];
    for (const r of rows) {
      body.push({ index: { _index: INDICES.geo, _id: r.ip } });
      body.push({
        ip: r.ip,
        country: r.country,
        country_code: r.country_code,
        region: r.region,
        city: r.city,
        location: (r.lat && r.lon) ? { lat: r.lat, lon: r.lon } : null,
        isp: r.isp,
        org: r.org,
        as: r.as,
        proxy: !!r.proxy,
        hosting: !!r.hosting,
        geocoded_at: r.geocoded_at,
      });
    }

    const result = await es.bulk({ refresh: true, body });
    if (result.errors) {
      const errs = result.items.filter(i => i.error).slice(0, 3);
      console.error('[sync] ES bulk errors (geo):', errs);
    }

    wm.geo = rows[rows.length - 1].geocoded_at;
    total += rows.length;

    if (rows.length < BATCH_SIZE) break;
  }
  return total;
}

async function runSync() {
  const es = getClient();
  if (!es) { console.error('[sync] ES client not initialized'); return; }

  const wm = loadWatermarks();
  const start = Date.now();

  try {
    const [conns, events, subs, geo] = await Promise.all([
      syncConnections(es, wm),
      syncEvents(es, wm),
      syncSubscriptions(es, wm),
      syncGeo(es, wm),
    ]);

    saveWatermarks(wm);

    const total = conns + events + subs + geo;
    const ms = Date.now() - start;
    if (total > 0) {
      console.log(`[sync] Synced ${total} docs in ${ms}ms (conns:${conns} ev:${events} subs:${subs} geo:${geo})`);
    }
  } catch (err) {
    console.error('[sync] Error:', err.message);
  }
}

function startSyncWorker() {
  if (timer) return;
  // Initial sync after 10s (give ES time to init)
  setTimeout(() => {
    runSync().then(() => {
      timer = setInterval(runSync, SYNC_INTERVAL_MS);
      console.log(`[sync] Worker started, interval: ${SYNC_INTERVAL_MS}ms`);
    }).catch(err => {
      console.error('[sync] Initial sync failed:', err.message);
      // Retry starting worker after 30s
      setTimeout(startSyncWorker, 30000);
    });
  }, 10000);
}

// One-time full reindex (for backfilling existing data)
async function fullReindex() {
  const es = getClient();
  if (!es) throw new Error('ES client not initialized');

  console.log('[sync] Starting full reindex...');
  const wm = { connections: 0, events: 0, subscriptions: 0, geo: 0 };
  const start = Date.now();

  const [conns, events, subs, geo] = await Promise.all([
    syncConnections(es, wm),
    syncEvents(es, wm),
    syncSubscriptions(es, wm),
    syncGeo(es, wm),
  ]);

  saveWatermarks(wm);
  console.log(`[sync] Full reindex complete: ${conns + events + subs + geo} docs in ${Date.now() - start}ms`);
}

module.exports = { setDb, startSyncWorker, runSync, fullReindex };
