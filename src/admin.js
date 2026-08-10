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

  return router;
}

module.exports = createAdminRouter;
