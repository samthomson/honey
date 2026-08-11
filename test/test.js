const { WebSocketServer, WebSocket } = require('ws');
const http = require('http');
const { DatabaseSync } = require('node:sqlite');
const fs = require('fs');
const path = require('path');

// --- Test config ---
const FAKE_RELAY_PORT = 9700;
const HONEY_PORT = 9800;
const DATA_DIR = path.join(__dirname, 'test-data');

// Clean test data dir
if (fs.existsSync(DATA_DIR)) {
  fs.rmSync(DATA_DIR, { recursive: true });
}

let passed = 0;
let failed = 0;
const failures = [];

function assert(condition, label) {
  if (condition) {
    passed++;
  } else {
    failed++;
    failures.push(label);
    console.error(`  ❌ ${label}`);
  }
}

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

let relayReceived = [];
let relayConnections = 0;

// --- Fake relay (HTTP + WS on same port) ---
const relayHttpServer = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'application/nostr+json' });
  res.end(JSON.stringify({ name: 'fake-relay', supported_nips: [1, 11] }));
});

const fakeRelay = new WebSocketServer({ server: relayHttpServer });
relayHttpServer.listen(FAKE_RELAY_PORT);

fakeRelay.on('connection', (ws) => {
  relayConnections++;
  ws.on('message', (data) => {
    relayReceived.push(data.toString());
    const msg = JSON.parse(data.toString());

    if (msg[0] === 'REQ') {
      const subId = msg[1];
      const event = {
        kind: 1, content: 'test event from relay',
        created_at: Math.floor(Date.now() / 1000), tags: [],
        pubkey: 'relaypubkey', id: 'relayeventid', sig: 'relaysig',
      };
      ws.send(JSON.stringify(['EVENT', subId, event]));
      ws.send(JSON.stringify(['EOSE', subId]));
    }

    if (msg[0] === 'EVENT') {
      ws.send(JSON.stringify(['OK', msg[1].id, true, '']));
    }
  });
});

// --- Start Honey ---
process.env.BACKEND_HOST = `localhost:${FAKE_RELAY_PORT}`;
process.env.BACKEND_SCHEME = 'ws';
process.env.PORT = String(HONEY_PORT);
process.env.DATA_DIR = DATA_DIR;

const db = require('../src/db');
db.init(DATA_DIR);
require('../src/index.js');

function connectClient() {
  return new WebSocket(`ws://localhost:${HONEY_PORT}`);
}

function httpGet(url, headers = {}) {
  return new Promise((resolve, reject) => {
    http.get(url, { headers }, (res) => {
      let body = '';
      res.on('data', (chunk) => body += chunk);
      res.on('end', () => resolve({ status: res.statusCode, headers: res.headers, body }));
    }).on('error', reject);
  });
}

async function run() {
  console.log('Running Honey tests...\n');
  await sleep(500);

  // === TEST 1: Connection passthrough ===
  console.log('Test 1: Connection passthrough');
  relayReceived = []; relayConnections = 0;
  const client1 = connectClient();
  await new Promise(r => client1.on('open', r));
  await sleep(300);
  assert(relayConnections === 1, 'Fake relay received connection');

  // === TEST 2: EVENT passthrough + logging ===
  console.log('Test 2: EVENT passthrough + logging');
  const testEvent = {
    id: 'test-event-id-123', pubkey: 'test-pubkey-abc',
    kind: 1, created_at: Math.floor(Date.now() / 1000),
    tags: [['t', 'test']], content: 'hello world', sig: 'test-sig',
  };
  client1.send(JSON.stringify(['EVENT', testEvent]));
  await sleep(300);

  assert(relayReceived.some(m => m.includes('test-event-id-123')), 'EVENT forwarded to backend relay');

  const testDb = new DatabaseSync(path.join(DATA_DIR, 'honey.db'));
  const events = testDb.prepare('SELECT * FROM published_events WHERE event_id = ?').all('test-event-id-123');
  assert(events.length === 1, 'EVENT logged in SQLite');
  assert(events[0]?.pubkey === 'test-pubkey-abc', 'EVENT pubkey logged correctly');
  assert(events[0]?.kind === 1, 'EVENT kind logged correctly');
  assert(events[0]?.content_len === 11, 'EVENT content_len correct');

  // === TEST 2b: Pubkey associated to connection ===
  console.log('Test 2b: Pubkey associated to connection');
  const connWithPubkey = testDb.prepare('SELECT pubkey FROM connections WHERE id = 1').get();
  assert(connWithPubkey?.pubkey === 'test-pubkey-abc', 'Connection pubkey updated after EVENT');

  // === TEST 3: REQ passthrough + logging ===
  console.log('Test 3: REQ passthrough + logging');
  let clientMessages = [];
  client1.on('message', (data) => { clientMessages.push(data.toString()); });
  relayReceived = [];
  client1.send(JSON.stringify(['REQ', 'sub1', { kinds: [1], limit: 10 }]));
  await sleep(300);

  assert(relayReceived.some(m => m.includes('"sub1"')), 'REQ forwarded to backend relay');
  const subs = testDb.prepare('SELECT * FROM subscriptions WHERE subscription_id = ?').all('sub1');
  assert(subs.length === 1, 'REQ logged in SQLite');
  assert(subs[0]?.filters.includes('"kinds":[1]'), 'REQ filters logged correctly');

  // === TEST 4: Response passthrough ===
  console.log('Test 4: Response passthrough from relay');
  await sleep(300);
  assert(clientMessages.some(m => m.includes('EOSE')), 'EOSE forwarded from relay to client');
  assert(clientMessages.some(m => m.includes('relayeventid')), 'EVENT response forwarded from relay to client');

  // === TEST 5: CLOSE logging ===
  console.log('Test 5: CLOSE logging');
  client1.send(JSON.stringify(['CLOSE', 'sub1']));
  await sleep(300);
  const closes = testDb.prepare('SELECT * FROM subscription_closes WHERE subscription_id = ?').all('sub1');
  assert(closes.length === 1, 'CLOSE logged in SQLite');

  // === TEST 6: Disconnection logging ===
  console.log('Test 6: Disconnection logging');
  client1.close();
  await sleep(300);
  const conns = testDb.prepare('SELECT * FROM connections').all();
  assert(conns.length >= 1, 'Connection logged');
  assert(conns[0]?.disconnected_at !== null, 'Disconnection logged');

  // === TEST 7: NIP-11 HTTP proxy ===
  console.log('Test 7: NIP-11 HTTP proxy');
  const nip11Res = await httpGet(`http://localhost:${HONEY_PORT}/`, { Accept: 'application/nostr+json' });
  assert(nip11Res.status === 200 || nip11Res.status === 502, 'NIP-11 request handled (200 or 502)');

  // === TEST 8: Dashboard ===
  console.log('Test 8: Dashboard + API');
  const dashboardRes = await httpGet(`http://localhost:${HONEY_PORT}/`);
  assert(dashboardRes.status === 200, 'Dashboard HTML served');

  // === TEST 9: API /stats (includes uniquePubkeys) ===
  const statsRes = await httpGet(`http://localhost:${HONEY_PORT}/api/stats`);
  assert(statsRes.status === 200, 'API /stats returns 200');
  let statsBody;
  try { statsBody = JSON.parse(statsRes.body); } catch {}
  assert(statsBody && typeof statsBody.totalConnections === 'number', 'API /stats has totalConnections');
  assert(statsBody && typeof statsBody.totalEvents === 'number', 'API /stats has totalEvents');
  assert(statsBody && typeof statsBody.totalSubscriptions === 'number', 'API /stats has totalSubscriptions');
  assert(statsBody && typeof statsBody.uniquePubkeys === 'number', 'API /stats has uniquePubkeys');
  assert(statsBody && statsBody.uniquePubkeys >= 1, 'API /stats uniquePubkeys >= 1');

  // === TEST 10-13: API endpoints ===
  const connRes = await httpGet(`http://localhost:${HONEY_PORT}/api/connections`);
  assert(connRes.status === 200 && Array.isArray(JSON.parse(connRes.body)), 'API /connections returns array');

  const eventsRes = await httpGet(`http://localhost:${HONEY_PORT}/api/events`);
  assert(eventsRes.status === 200 && Array.isArray(JSON.parse(eventsRes.body)), 'API /events returns array');

  const topIpsRes = await httpGet(`http://localhost:${HONEY_PORT}/api/top-ips`);
  assert(topIpsRes.status === 200 && Array.isArray(JSON.parse(topIpsRes.body)), 'API /top-ips returns array');

  const activityRes = await httpGet(`http://localhost:${HONEY_PORT}/api/activity`);
  const activityBody = JSON.parse(activityRes.body);
  assert(activityRes.status === 200, 'API /activity returns 200');
  assert(Array.isArray(activityBody.connections), 'API /activity has connections array');
  assert(Array.isArray(activityBody.events), 'API /activity has events array');

  // === TEST 14: Multiple concurrent clients ===
  console.log('Test 14: Multiple concurrent clients');
  relayConnections = 0;
  const c1 = connectClient(); const c2 = connectClient(); const c3 = connectClient();
  await Promise.all([
    new Promise(r => c1.on('open', r)),
    new Promise(r => c2.on('open', r)),
    new Promise(r => c3.on('open', r)),
  ]);
  await sleep(300);
  assert(relayConnections === 3, 'All 3 clients connected to relay');
  c1.close(); c2.close(); c3.close();
  await sleep(200);

  // === TEST 15: AUTH message logging ===
  console.log('Test 15: AUTH message logging');
  const authClient = connectClient();
  await new Promise(r => authClient.on('open', r));
  const authEvent = {
    id: 'auth-event-id', pubkey: 'auth-pubkey', kind: 22242,
    created_at: Math.floor(Date.now() / 1000),
    tags: [['relay', 'ws://localhost']], content: '', sig: 'auth-sig',
  };
  authClient.send(JSON.stringify(['AUTH', authEvent]));
  await sleep(300);

  const authEvents = testDb.prepare('SELECT * FROM published_events WHERE kind = 22242').all();
  assert(authEvents.length >= 1, 'AUTH event (kind 22242) logged');
  authClient.close();
  await sleep(200);

  // === TEST 16: Pubkey API endpoints ===
  console.log('Test 16: Pubkey API endpoints');

  // /api/pubkeys list
  const pubkeysRes = await httpGet(`http://localhost:${HONEY_PORT}/api/pubkeys`);
  assert(pubkeysRes.status === 200, 'API /pubkeys returns 200');
  const pubkeysList = JSON.parse(pubkeysRes.body);
  assert(Array.isArray(pubkeysList), 'API /pubkeys returns array');
  assert(pubkeysList.some(p => p.pubkey === 'test-pubkey-abc'), 'API /pubkeys includes test-pubkey-abc');

  // /api/pubkeys/:pubkey detail
  const pubkeyDetailRes = await httpGet(`http://localhost:${HONEY_PORT}/api/pubkeys/test-pubkey-abc`);
  assert(pubkeyDetailRes.status === 200, 'API /pubkeys/:pubkey returns 200');
  const pubkeyDetail = JSON.parse(pubkeyDetailRes.body);
  assert(pubkeyDetail.pubkey === 'test-pubkey-abc', 'Pubkey detail has correct pubkey');
  assert(pubkeyDetail.event_count >= 1, 'Pubkey detail has event_count >= 1');
  assert(Array.isArray(pubkeyDetail.ips), 'Pubkey detail has ips array');
  assert(Array.isArray(pubkeyDetail.kinds), 'Pubkey detail has kinds array');

  // /api/pubkeys/:pubkey/events
  const pubkeyEventsRes = await httpGet(`http://localhost:${HONEY_PORT}/api/pubkeys/test-pubkey-abc/events`);
  assert(pubkeyEventsRes.status === 200, 'API /pubkeys/:pubkey/events returns 200');
  const pubkeyEvents = JSON.parse(pubkeyEventsRes.body);
  assert(Array.isArray(pubkeyEvents), 'Pubkey events returns array');
  assert(pubkeyEvents.some(e => e.event_id === 'test-event-id-123'), 'Pubkey events includes test event');

  // /api/pubkeys/:pubkey/subscriptions
  const pubkeySubsRes = await httpGet(`http://localhost:${HONEY_PORT}/api/pubkeys/test-pubkey-abc/subscriptions`);
  assert(pubkeySubsRes.status === 200, 'API /pubkeys/:pubkey/subscriptions returns 200');
  const pubkeySubs = JSON.parse(pubkeySubsRes.body);
  assert(Array.isArray(pubkeySubs), 'Pubkey subscriptions returns array');
  assert(pubkeySubs.some(s => s.subscription_id === 'sub1'), 'Pubkey subscriptions includes sub1 (via connection)');

  // /api/pubkeys/:pubkey/ips
  const pubkeyIpsRes = await httpGet(`http://localhost:${HONEY_PORT}/api/pubkeys/test-pubkey-abc/ips`);
  assert(pubkeyIpsRes.status === 200, 'API /pubkeys/:pubkey/ips returns 200');
  const pubkeyIps = JSON.parse(pubkeyIpsRes.body);
  assert(Array.isArray(pubkeyIps), 'Pubkey IPs returns array');

  // 404 for unknown pubkey
  const unknownPkRes = await httpGet(`http://localhost:${HONEY_PORT}/api/pubkeys/nonexistent-pubkey`);
  assert(unknownPkRes.status === 404, 'API /pubkeys/:unknown returns 404');

  // === DONE ===
  console.log('\n--- Results ---');
  console.log(`Passed: ${passed}`);
  console.log(`Failed: ${failed}`);
  if (failures.length > 0) {
    console.log('\nFailures:');
    failures.forEach(f => console.log(`  - ${f}`));
  }

  fakeRelay.close();
  process.exit(failed > 0 ? 1 : 0);
}

run().catch(err => {
  console.error('Test runner crashed:', err);
  process.exit(1);
});
