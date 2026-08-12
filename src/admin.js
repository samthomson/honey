const express = require('express');
const db = require('./db');

function createAdminRouter() {
  const router = express.Router();

  // ─── Stats ───
  router.get('/stats', (req, res) => res.json(db.getStats()));
  router.get('/reader-stats', (req, res) => res.json(db.getReaderStats()));
  router.get('/activity', (req, res) => res.json(db.getActivity()));
  router.get('/top-ips', (req, res) => {
    const limit = Math.min(parseInt(req.query.limit) || 20, 100);
    res.json(db.getTopIps(limit));
  });

  // ─── Connections / Events / Subscriptions ───
  router.get('/connections', (req, res) => {
    const limit = Math.min(parseInt(req.query.limit) || 50, 200);
    const offset = parseInt(req.query.offset) || 0;
    res.json(db.getConnections(limit, offset));
  });

  router.get('/events', (req, res) => {
    const limit = Math.min(parseInt(req.query.limit) || 50, 200);
    const offset = parseInt(req.query.offset) || 0;
    res.json(db.getEvents(limit, offset));
  });

  router.get('/subscriptions', (req, res) => {
    const limit = Math.min(parseInt(req.query.limit) || 50, 200);
    const offset = parseInt(req.query.offset) || 0;
    res.json(db.getSubscriptions(limit, offset));
  });

  // ─── Pubkeys ───
  router.get('/pubkeys', (req, res) => {
    const limit = Math.min(parseInt(req.query.limit) || 50, 200);
    const offset = parseInt(req.query.offset) || 0;
    const filter = req.query.filter || 'publishers';
    const rows = db.getPubkeys(limit, offset, filter);
    // Attach cached profiles
    const pubkeys = rows.filter(r => r.pubkey).map(r => r.pubkey);
    if (pubkeys.length) {
      const profiles = db.getProfiles(pubkeys);
      const map = Object.fromEntries(profiles.map(p => [p.pubkey, p]));
      for (const r of rows) { if (r.pubkey && map[r.pubkey]) r.profile = map[r.pubkey]; }
    }
    res.json(rows);
  });

  router.get('/pubkeys/:pubkey', (req, res) => {
    const detail = db.getPubkeyDetail(req.params.pubkey);
    if (!detail) return res.status(404).json({ error: 'Not found' });
    res.json(detail);
  });

  router.get('/pubkeys/:pubkey/events', (req, res) => {
    const limit = Math.min(parseInt(req.query.limit) || 20, 200);
    const offset = parseInt(req.query.offset) || 0;
    res.json(db.getPubkeyEvents(req.params.pubkey, limit, offset));
  });

  router.get('/pubkeys/:pubkey/subscriptions', (req, res) => {
    const limit = Math.min(parseInt(req.query.limit) || 50, 200);
    const offset = parseInt(req.query.offset) || 0;
    res.json(db.getPubkeySubscriptions(req.params.pubkey, limit, offset));
  });

  router.get('/pubkeys/:pubkey/ips', (req, res) => res.json(db.getPubkeyIps(req.params.pubkey)));

  // ─── Profiles ───
  router.get('/profiles/:pubkey', (req, res) => {
    const profile = db.getProfile(req.params.pubkey);
    if (!profile) return res.status(404).json({ error: 'Not found' });
    res.json(profile);
  });

  // Trigger profile fetch from relay (non-blocking)
  router.post('/profiles/:pubkey/fetch', async (req, res) => {
    try {
      const relayUrl = `ws${req.app.get('backend scheme') === 'wss' ? 'ss' : 's'}://${req.app.get('backend host')}`;
      await db.fetchProfilesFromRelay([req.params.pubkey], relayUrl);
      res.json(db.getProfile(req.params.pubkey) || { error: 'No profile found' });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // ─── Geo (all read-only, returns cached data instantly) ───
  router.get('/geo/all', (req, res) => res.json(db.getAllGeo()));
  router.get('/geo/pubkey/:pubkey', (req, res) => res.json(db.getGeoForPubkey(req.params.pubkey)));
  router.get('/geo/status', (req, res) => res.json(db.getGeoStats()));

  return router;
}

module.exports = createAdminRouter;
