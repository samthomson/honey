const express = require('express');
const db = require('./db');

function createAdminRouter() {
  const router = express.Router();

  router.get('/stats', (req, res) => {
    res.json(db.getStats());
  });

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

  router.get('/top-ips', (req, res) => {
    const limit = Math.min(parseInt(req.query.limit) || 20, 100);
    res.json(db.getTopIps(limit));
  });

  router.get('/activity', (req, res) => {
    res.json(db.getActivity());
  });

  // --- Pubkey endpoints ---

  router.get('/pubkeys', (req, res) => {
    const limit = Math.min(parseInt(req.query.limit) || 50, 200);
    const offset = parseInt(req.query.offset) || 0;
    res.json(db.getPubkeys(limit, offset));
  });

  router.get('/pubkeys/:pubkey', (req, res) => {
    const detail = db.getPubkeyDetail(req.params.pubkey);
    if (!detail) return res.status(404).json({ error: 'Pubkey not found' });
    res.json(detail);
  });

  router.get('/pubkeys/:pubkey/events', (req, res) => {
    const limit = Math.min(parseInt(req.query.limit) || 50, 200);
    const offset = parseInt(req.query.offset) || 0;
    res.json(db.getPubkeyEvents(req.params.pubkey, limit, offset));
  });

  router.get('/pubkeys/:pubkey/subscriptions', (req, res) => {
    const limit = Math.min(parseInt(req.query.limit) || 50, 200);
    const offset = parseInt(req.query.offset) || 0;
    res.json(db.getPubkeySubscriptions(req.params.pubkey, limit, offset));
  });

  router.get('/pubkeys/:pubkey/ips', (req, res) => {
    res.json(db.getPubkeyIps(req.params.pubkey));
  });

  return router;
}

module.exports = createAdminRouter;
