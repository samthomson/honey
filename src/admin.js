const express = require('express');
const db = require('./db');
const esQueries = require('./es-queries');

function createAdminRouter() {
  const router = express.Router();

  // Wrap async ES handlers
  const wrap = (fn) => (req, res) => {
    fn(req, res).catch(err => {
      console.error('[api] Error:', err.message);
      res.status(500).json({ error: err.message });
    });
  };

  // ─── Stats ───
  router.get('/stats', wrap(async (req, res) => res.json(await esQueries.getStats())));
  router.get('/reader-stats', wrap(async (req, res) => res.json(await esQueries.getReaderStats())));
  router.get('/activity', wrap(async (req, res) => res.json(await esQueries.getActivity())));
  router.get('/top-ips', wrap(async (req, res) => {
    const limit = Math.min(parseInt(req.query.limit) || 20, 100);
    res.json(await esQueries.getTopIps(limit));
  }));

  // ─── Connections / Events / Subscriptions ───
  router.get('/connections', wrap(async (req, res) => {
    const limit = Math.min(parseInt(req.query.limit) || 50, 200);
    const offset = parseInt(req.query.offset) || 0;
    res.json(await esQueries.getConnections(limit, offset));
  }));

  router.get('/events', wrap(async (req, res) => {
    const limit = Math.min(parseInt(req.query.limit) || 50, 200);
    const offset = parseInt(req.query.offset) || 0;
    res.json(await esQueries.getEvents(limit, offset));
  }));

  router.get('/subscriptions', wrap(async (req, res) => {
    const limit = Math.min(parseInt(req.query.limit) || 50, 200);
    const offset = parseInt(req.query.offset) || 0;
    res.json(await esQueries.getSubscriptions(limit, offset));
  }));

  // ─── Pubkeys ───
  router.get('/pubkeys', wrap(async (req, res) => {
    const limit = Math.min(parseInt(req.query.limit) || 50, 200);
    const offset = parseInt(req.query.offset) || 0;
    const filter = req.query.filter || 'publishers';
    const rows = await esQueries.getPubkeys(limit, offset, filter);

    // Attach cached profiles (always from SQLite — profiles are small/fast)
    const pubkeys = rows.filter(r => r.pubkey).map(r => r.pubkey);
    if (pubkeys.length) {
      const profiles = db.getProfiles(pubkeys);
      const map = Object.fromEntries(profiles.map(p => [p.pubkey, p]));
      for (const r of rows) { if (r.pubkey && map[r.pubkey]) r.profile = map[r.pubkey]; }
      // Background-fetch missing profiles
      const stale = db.getStaleProfiles(pubkeys);
      if (stale.length > 0) {
        const relayUrl = req.app.get('backendWsUrl') || 'wss://relay.example.com';
        db.fetchProfilesFromRelay(stale, relayUrl).catch(() => {});
      }
    }
    res.json(rows);
  }));

  router.get('/pubkeys/:pubkey', wrap(async (req, res) => {
    const detail = await esQueries.getPubkeyDetail(req.params.pubkey);
    if (!detail) return res.status(404).json({ error: 'Not found' });

    // Attach profile from SQLite
    detail.profile = db.getProfile(req.params.pubkey);
    if (!detail.profile) {
      const relayUrl = req.app.get('backendWsUrl') || 'wss://relay.example.com';
      db.fetchProfilesFromRelay([req.params.pubkey], relayUrl).catch(() => {});
    }

    res.json(detail);
  }));

  router.get('/pubkeys/:pubkey/events', wrap(async (req, res) => {
    const limit = Math.min(parseInt(req.query.limit) || 20, 200);
    const offset = parseInt(req.query.offset) || 0;
    res.json(await esQueries.getPubkeyEvents(req.params.pubkey, limit, offset));
  }));

  router.get('/pubkeys/:pubkey/subscriptions', wrap(async (req, res) => {
    const limit = Math.min(parseInt(req.query.limit) || 50, 200);
    const offset = parseInt(req.query.offset) || 0;
    res.json(await esQueries.getPubkeySubscriptions(req.params.pubkey, limit, offset));
  }));

  router.get('/pubkeys/:pubkey/ips', wrap(async (req, res) => res.json(await esQueries.getPubkeyIps(req.params.pubkey))));

  // ─── IP Detail ───
  router.get('/ips/:ip/detail', wrap(async (req, res) => {
    const detail = await esQueries.getIpDetail(req.params.ip);
    if (!detail) return res.status(404).json({ error: 'Not found' });
    res.json(detail);
  }));

  // ─── Profiles ───
  router.get('/profiles/:pubkey', (req, res) => {
    const profile = db.getProfile(req.params.pubkey);
    if (!profile) return res.status(404).json({ error: 'Not found' });
    res.json(profile);
  });

  router.post('/profiles/:pubkey/fetch', async (req, res) => {
    try {
      const relayUrl = `ws${req.app.get('backend scheme') === 'wss' ? 'ss' : 's'}://${req.app.get('backend host')}`;
      await db.fetchProfilesFromRelay([req.params.pubkey], relayUrl);
      res.json(db.getProfile(req.params.pubkey) || { error: 'No profile found' });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // ─── Geo ───
  router.get('/geo/all', wrap(async (req, res) => res.json(await esQueries.getAllGeo())));
  router.get('/geo/pubkey/:pubkey', wrap(async (req, res) => res.json(await esQueries.getGeoForPubkey(req.params.pubkey))));
  router.get('/geo/status', wrap(async (req, res) => {
    const pubkey = req.query.pubkey;
    const stats = await (pubkey ? esQueries.getGeoStatsForPubkey(pubkey) : esQueries.getGeoStats());
    // Trigger background geocoding if uncached IPs exist (always SQLite — it's the source of truth for geocoding)
    if (stats.uncached > 0) {
      const ips = pubkey ? db.getUniqueIpsForPubkey(pubkey) : db.getAllUniqueIps();
      db.geocodeIps(ips).catch(() => {});
    }
    res.json(stats);
  }));

  // ─── Sync control ───
  router.post('/sync', wrap(async (req, res) => {
    const sync = require('./sync');
    await sync.runSync();
    res.json({ ok: true });
  }));

  router.post('/reindex', wrap(async (req, res) => {
    const sync = require('./sync');
    await sync.fullReindex();
    res.json({ ok: true });
  }));

  return router;
}

module.exports = createAdminRouter;
