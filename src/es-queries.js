const { getClient, INDICES } = require('./es');

// All the heavy dashboard queries rewritten for Elasticsearch

async function getStats() {
  const es = getClient();
  const [connCount, eventCount, subCount, activeCount, uniqueIps, uniquePubkeys] = await Promise.all([
    es.count({ index: INDICES.connections }),
    es.count({ index: INDICES.events }),
    es.count({ index: INDICES.subscriptions }),
    es.count({ index: INDICES.connections, body: { query: { bool: { must_not: { exists: { field: 'disconnected_at' } } } } } }),
    es.search({ index: INDICES.connections, size: 0, body: { aggs: { unique: { cardinality: { field: 'ip' } } } } }),
    es.search({ index: INDICES.events, size: 0, body: { query: { exists: { field: 'pubkey' } }, aggs: { unique: { cardinality: { field: 'pubkey' } } } } }),
  ]);

  return {
    totalConnections: connCount.count,
    uniqueIps: uniqueIps.aggregations.unique.value,
    totalEvents: eventCount.count,
    totalSubscriptions: subCount.count,
    activeConnections: activeCount.count,
    uniquePubkeys: uniquePubkeys.aggregations.unique.value,
  };
}

async function getReaderStats() {
  const es = getClient();
  const [readers, publishers] = await Promise.all([
    es.search({
      index: INDICES.connections, size: 0,
      body: {
        query: { bool: { must_not: { exists: { field: 'pubkey' } } } },
        aggs: { unique: { cardinality: { field: 'ip' } } },
      },
    }),
    es.search({
      index: INDICES.events, size: 0,
      body: { query: { exists: { field: 'pubkey' } }, aggs: { unique: { cardinality: { field: 'pubkey' } } } },
    }),
  ]);
  return {
    totalReaders: readers.aggregations.unique.value,
    totalPublishers: publishers.aggregations.unique.value,
  };
}

async function getTopIps(limit = 20) {
  const es = getClient();
  // Get top IPs by connection count, then enrich with event/sub counts
  const topByConns = await es.search({
    index: INDICES.connections, size: 0,
    body: {
      aggs: {
        top_ips: {
          terms: { field: 'ip', size: limit },
          aggs: {
            pubkeys: { cardinality: { field: 'pubkey' } },
          },
        },
      },
    },
  });

  const ips = topByConns.aggregations.top_ips.buckets;
  if (!ips.length) return [];

  // Batch: get event counts and sub counts for these IPs
  const ipList = ips.map(b => b.key);
  const [eventAgg, subAgg] = await Promise.all([
    es.search({
      index: INDICES.events, size: 0,
      body: {
        query: { terms: { ip: ipList } },
        aggs: { by_ip: { terms: { field: 'ip', size: limit } } },
      },
    }),
    es.search({
      index: INDICES.subscriptions, size: 0,
      body: {
        query: { terms: { ip: ipList } },
        aggs: { by_ip: { terms: { field: 'ip', size: limit } } },
      },
    }),
  ]);

  const eventMap = Object.fromEntries(eventAgg.aggregations.by_ip.buckets.map(b => [b.key, b.doc_count]));
  const subMap = Object.fromEntries(subAgg.aggregations.by_ip.buckets.map(b => [b.key, b.doc_count]));

  return ips.map(b => ({
    ip: b.key,
    connections: b.doc_count,
    events: eventMap[b.key] || 0,
    subscriptions: subMap[b.key] || 0,
    pubkeys: b.pubkeys.value,
  }));
}

async function getActivity() {
  const es = getClient();
  const weekAgo = Math.floor(Date.now() / 1000) - 7 * 24 * 60 * 60;

  const [connActivity, eventActivity] = await Promise.all([
    es.search({
      index: INDICES.connections, size: 0,
      body: {
        query: { range: { connected_at: { gte: weekAgo } } },
        aggs: {
          hourly: {
            date_histogram: { field: 'connected_at', calendar_interval: '1h', format: 'yyyy-MM-dd\'T\'HH:mm:ss' },
          },
        },
      },
    }),
    es.search({
      index: INDICES.events, size: 0,
      body: {
        query: { range: { logged_at: { gte: weekAgo } } },
        aggs: {
          hourly: {
            date_histogram: { field: 'logged_at', fixed_interval: '1h', format: 'yyyy-MM-dd\'T\'HH:mm:ss' },
          },
        },
      },
    }),
  ]);

  return {
    connections: connActivity.aggregations.hourly.buckets.map(b => ({ t: b.key_as_string, c: b.doc_count })),
    events: eventActivity.aggregations.hourly.buckets.map(b => ({ t: b.key_as_string, c: b.doc_count })),
  };
}

async function getConnections(limit, offset) {
  const es = getClient();
  const result = await es.search({
    index: INDICES.connections,
    size: limit, from: offset,
    body: { sort: [{ connected_at: { order: 'desc' } }] },
  });
  return result.hits.hits.map(h => ({ _id: h._id, ...h._source }));
}

async function getEvents(limit, offset) {
  const es = getClient();
  const result = await es.search({
    index: INDICES.events,
    size: limit, from: offset,
    body: {
      sort: [{ logged_at: { order: 'desc' } }],
      // Join geo data for display
    },
  });

  const hits = result.hits.hits.map(h => ({ id: parseInt(h._id), ...h._source }));

  // Enrich with geo
  if (hits.length) {
    const ips = [...new Set(hits.map(h => h.ip))];
    const geoResult = await es.search({
      index: INDICES.geo, size: ips.length,
      body: { query: { terms: { ip: ips } } },
    });
    const geoMap = Object.fromEntries(geoResult.hits.hits.map(h => [h._source.ip, h._source]));
    for (const h of hits) {
      const g = geoMap[h.ip];
      if (g) {
        h.country = g.country;
        h.country_code = g.country_code;
        h.city = g.city;
        if (g.location) { h.lat = g.location.lat; h.lon = g.location.lon; }
      }
    }
  }

  return hits;
}

async function getSubscriptions(limit, offset) {
  const es = getClient();
  const result = await es.search({
    index: INDICES.subscriptions,
    size: limit, from: offset,
    body: { sort: [{ logged_at: { order: 'desc' } }] },
  });
  return result.hits.hits.map(h => ({ ...h._source }));
}

async function getPubkeys(limit, offset, filter) {
  const es = getClient();

  if (filter === 'readers') {
    // IPs that have subscriptions but no pubkey in connections
    const result = await es.search({
      index: INDICES.connections, size: 0,
      body: {
        query: { bool: { must_not: { exists: { field: 'pubkey' } } } },
        aggs: {
          by_ip: {
            terms: { field: 'ip', size: limit, order: { last_seen: 'desc' } },
            aggs: {
              last_seen: { max: { field: 'connected_at' } },
              first_seen: { min: { field: 'connected_at' } },
              sub_count: {
                filter: { exists: { field: 'subscription_id' } },
              },
            },
          },
        },
      },
    });
    // This approach needs a join with subscriptions — let's do it differently
    // Get reader IPs from connections (no pubkey), then count their subs
    const readerConns = await es.search({
      index: INDICES.connections,
      size: limit, from: offset,
      body: {
        query: { bool: { must_not: { exists: { field: 'pubkey' } } } },
        sort: [{ connected_at: { order: 'desc' } }],
        aggs: {
          by_ip: {
            composite: {
              size: limit,
              sources: [{ ip: { terms: { field: 'ip' } } }],
            },
            aggs: {
              connections: { value_count: { field: 'ip' } },
              first_seen: { min: { field: 'connected_at' } },
              last_seen: { max: { field: 'connected_at' } },
            },
          },
        },
      },
    });

    const buckets = readerConns.aggregations.by_ip.buckets;
    if (!buckets.length) return [];

    // Get sub counts for these IPs
    const ipList = buckets.map(b => b.key.ip);
    const subAgg = await es.search({
      index: INDICES.subscriptions, size: 0,
      body: {
        query: { terms: { ip: ipList } },
        aggs: { by_ip: { terms: { field: 'ip', size: limit } } },
      },
    });
    const subMap = Object.fromEntries(subAgg.aggregations.by_ip.buckets.map(b => [b.key, b.doc_count]));

    return buckets.map(b => ({
      pubkey: null,
      ip: b.key.ip,
      connections: b.connections.value,
      sub_count: subMap[b.key.ip] || 0,
      event_count: 0,
      first_seen: b.first_seen.value,
      last_seen: b.last_seen.value,
    }));
  }

  // Publishers: aggregate by pubkey from events
  const result = await es.search({
    index: INDICES.events, size: 0,
    body: {
      query: { exists: { field: 'pubkey' } },
      aggs: {
        by_pubkey: {
          terms: { field: 'pubkey', size: limit, order: { last_seen: 'desc' } },
          aggs: {
            event_count: { value_count: { field: 'pubkey' } },
            event_ips: { cardinality: { field: 'ip' } },
            first_seen: { min: { field: 'logged_at' } },
            last_seen: { max: { field: 'logged_at' } },
          },
        },
      },
    },
  });

  const buckets = result.aggregations.by_pubkey.buckets;
  if (!buckets.length) return [];

  // Enrich with connection counts and sub counts
  const pubkeyList = buckets.map(b => b.key);
  const [connAgg, subAgg] = await Promise.all([
    es.search({
      index: INDICES.connections, size: 0,
      body: {
        query: { terms: { pubkey: pubkeyList } },
        aggs: { by_pubkey: { terms: { field: 'pubkey', size: limit } } },
      },
    }),
    es.search({
      index: INDICES.subscriptions, size: 0,
      body: {
        query: { terms: { pubkey: pubkeyList } },
        aggs: { by_pubkey: { terms: { field: 'pubkey', size: limit } } },
      },
    }),
  ]);

  const connMap = Object.fromEntries(connAgg.aggregations.by_pubkey.buckets.map(b => [b.key, b.doc_count]));
  const subMap = Object.fromEntries(subAgg.aggregations.by_pubkey.buckets.map(b => [b.key, b.doc_count]));

  return buckets.map(b => ({
    pubkey: b.key,
    event_count: b.doc_count,
    sub_count: subMap[b.key] || 0,
    event_ips: b.event_ips.value,
    connections: connMap[b.key] || 0,
    first_seen: b.first_seen.value,
    last_seen: b.last_seen.value,
  }));
}

async function getPubkeyDetail(pubkey) {
  const es = getClient();

  const [eventAgg, connAgg, subAgg, kindsAgg, ipAgg] = await Promise.all([
    es.search({
      index: INDICES.events, size: 0,
      body: {
        query: { term: { pubkey } },
        aggs: {
          count: { value_count: { field: 'pubkey' } },
          first_seen: { min: { field: 'logged_at' } },
          last_seen: { max: { field: 'logged_at' } },
        },
      },
    }),
    es.count({ index: INDICES.connections, body: { query: { term: { pubkey } } } }),
    es.count({ index: INDICES.subscriptions, body: { query: { term: { pubkey } } } }),
    es.search({
      index: INDICES.events, size: 0,
      body: {
        query: { term: { pubkey } },
        aggs: { kinds: { terms: { field: 'kind', size: 20, order: { _count: 'desc' } } } },
      },
    }),
    es.search({
      index: INDICES.events, size: 0,
      body: {
        query: { term: { pubkey } },
        aggs: { ips: { terms: { field: 'ip', size: 50 } } },
      },
    }),
  ]);

  const summary = eventAgg.aggregations;
  if (!summary.count.value) return null;

  // Also check connections for IPs
  const connIpAgg = await es.search({
    index: INDICES.connections, size: 0,
    body: {
      query: { term: { pubkey } },
      aggs: { ips: { terms: { field: 'ip', size: 50 } } },
    },
  });

  const eventIps = ipAgg.aggregations.ips.buckets.map(b => b.key);
  const connIps = connIpAgg.aggregations.ips.buckets.map(b => b.key);
  const allIps = [...new Set([...eventIps, ...connIps])];

  return {
    pubkey,
    event_count: summary.count.value,
    first_seen: summary.first_seen.value,
    last_seen: summary.last_seen.value,
    connections: connAgg.count,
    subscriptions: subAgg.count,
    ips_used: allIps.length,
    ips: allIps,
    kinds: kindsAgg.aggregations.kinds.buckets.map(b => ({ kind: b.key, count: b.doc_count })),
  };
}

async function getPubkeyEvents(pubkey, limit, offset) {
  const es = getClient();
  const result = await es.search({
    index: INDICES.events,
    size: limit, from: offset,
    body: {
      query: { term: { pubkey } },
      sort: [{ logged_at: { order: 'desc' } }],
    },
  });
  return result.hits.hits.map(h => ({ id: parseInt(h._id), ...h._source }));
}

async function getPubkeySubscriptions(pubkey, limit, offset) {
  const es = getClient();
  const result = await es.search({
    index: INDICES.subscriptions,
    size: limit, from: offset,
    body: {
      query: { term: { pubkey } },
      sort: [{ logged_at: { order: 'desc' } }],
    },
  });
  return result.hits.hits.map(h => ({ ...h._source }));
}

async function getPubkeyIps(pubkey) {
  const es = getClient();
  const result = await es.search({
    index: INDICES.connections, size: 0,
    body: {
      query: { term: { pubkey } },
      aggs: {
        by_ip: {
          terms: { field: 'ip', size: 50, order: { last_seen: 'desc' } },
          aggs: {
            connections: { value_count: { field: 'ip' } },
            first_seen: { min: { field: 'connected_at' } },
            last_seen: { max: { field: 'connected_at' } },
          },
        },
      },
    },
  });
  return result.aggregations.by_ip.buckets.map(b => ({
    ip: b.key,
    connections: b.doc_count,
    first_seen: b.first_seen.value,
    last_seen: b.last_seen.value,
  }));
}

async function getAllGeo() {
  const es = getClient();
  // Get all geo points, enrich with connection/event counts
  const geoResult = await es.search({
    index: INDICES.geo, size: 10000,
    body: {
      query: { exists: { field: 'location' } },
    },
  });

  if (!geoResult.hits.hits.length) return [];

  const ips = geoResult.hits.hits.map(h => h._source.ip);

  // Batch aggregation for connection and event counts per IP
  const [connAgg, eventAgg, pubkeyAgg] = await Promise.all([
    es.search({
      index: INDICES.connections, size: 0,
      body: {
        query: { terms: { ip: ips } },
        aggs: { by_ip: { terms: { field: 'ip', size: ips.length }, aggs: { pubkeys: { cardinality: { field: 'pubkey' } } } } },
      },
    }),
    es.search({
      index: INDICES.events, size: 0,
      body: { query: { terms: { ip: ips } }, aggs: { by_ip: { terms: { field: 'ip', size: ips.length } } } },
    }),
    null,
  ]);

  const connMap = Object.fromEntries(connAgg.aggregations.by_ip.buckets.map(b => [b.key, b]));
  const eventMap = Object.fromEntries(eventAgg.aggregations.by_ip.buckets.map(b => [b.key, b.doc_count]));

  return geoResult.hits.hits.map(h => {
    const g = h._source;
    const c = connMap[g.ip];
    return {
      ...g,
      lat: g.location?.lat,
      lon: g.location?.lon,
      connections: c?.doc_count || 0,
      events: eventMap[g.ip] || 0,
      pubkeys: c?.pubkeys?.value || 0,
    };
  }).sort((a, b) => b.connections - a.connections);
}

async function getGeoForPubkey(pubkey) {
  const es = getClient();

  // Get IPs for this pubkey
  const [eventIps, connIps] = await Promise.all([
    es.search({
      index: INDICES.events, size: 0,
      body: { query: { term: { pubkey } }, aggs: { ips: { terms: { field: 'ip', size: 50 } } } },
    }),
    es.search({
      index: INDICES.connections, size: 0,
      body: { query: { term: { pubkey } }, aggs: { ips: { terms: { field: 'ip', size: 50 } } } },
    }),
  ]);

  const ips = [...new Set([
    ...eventIps.aggregations.ips.buckets.map(b => b.key),
    ...connIps.aggregations.ips.buckets.map(b => b.key),
  ])];

  if (!ips.length) return [];

  const geoResult = await es.search({
    index: INDICES.geo, size: ips.length,
    body: { query: { terms: { ip: ips } } },
  });

  const [connAgg, eventAgg] = await Promise.all([
    es.search({
      index: INDICES.connections, size: 0,
      body: { query: { bool: { must: [{ term: { pubkey } }, { terms: { ip: ips } }] } }, aggs: { by_ip: { terms: { field: 'ip', size: ips.length } } } },
    }),
    es.search({
      index: INDICES.events, size: 0,
      body: { query: { bool: { must: [{ term: { pubkey } }, { terms: { ip: ips } }] } }, aggs: { by_ip: { terms: { field: 'ip', size: ips.length } } } },
    }),
  ]);

  const connMap = Object.fromEntries(connAgg.aggregations.by_ip.buckets.map(b => [b.key, b.doc_count]));
  const eventMap = Object.fromEntries(eventAgg.aggregations.by_ip.buckets.map(b => [b.key, b.doc_count]));

  return geoResult.hits.hits.map(h => {
    const g = h._source;
    return {
      ...g,
      lat: g.location?.lat,
      lon: g.location?.lon,
      connections: connMap[g.ip] || 0,
      events: eventMap[g.ip] || 0,
    };
  }).sort((a, b) => b.connections - a.connections);
}

async function getGeoStats() {
  const es = getClient();
  const [geoCount, connIpsAgg] = await Promise.all([
    es.count({ index: INDICES.geo }),
    es.search({
      index: INDICES.connections, size: 0,
      body: { aggs: { unique: { cardinality: { field: 'ip' } } } },
    }),
  ]);

  // For uncached, we need to know which connection IPs aren't in geo index
  // Simplified: use cardinality comparison
  return {
    cached: geoCount.count,
    total: connIpsAgg.aggregations.unique.value,
    uncached: Math.max(0, connIpsAgg.aggregations.unique.value - geoCount.count),
  };
}

async function getGeoStatsForPubkey(pubkey) {
  const es = getClient();
  const [eventIps, connIps] = await Promise.all([
    es.search({
      index: INDICES.events, size: 0,
      body: { query: { term: { pubkey } }, aggs: { ips: { cardinality: { field: 'ip' } } } },
    }),
    es.search({
      index: INDICES.connections, size: 0,
      body: { query: { term: { pubkey } }, aggs: { ips: { cardinality: { field: 'ip' } } } },
    }),
  ]);

  const total = Math.max(eventIps.aggregations.ips.value, connIps.aggregations.ips.value);
  if (!total) return { cached: 0, total: 0, uncached: 0 };

  // Get actual IPs to check which are geocoded
  const ipAgg = await es.search({
    index: INDICES.events, size: 0,
    body: {
      query: { term: { pubkey } },
      aggs: { ips: { terms: { field: 'ip', size: 100 } } },
    },
  });
  const ips = ipAgg.aggregations.ips.buckets.map(b => b.key);

  if (!ips.length) return { cached: 0, total: 0, uncached: 0 };

  const geoResult = await es.search({
    index: INDICES.geo, size: ips.length,
    body: { query: { terms: { ip: ips } } },
  });

  return {
    cached: geoResult.hits.hits.length,
    total: ips.length,
    uncached: Math.max(0, ips.length - geoResult.hits.hits.length),
  };
}

async function getIpDetail(ip) {
  const es = getClient();

  const [geoResult, connResult, eventResult, subResult, statsAgg] = await Promise.all([
    es.search({ index: INDICES.geo, size: 1, body: { query: { term: { ip } } } }),
    es.search({
      index: INDICES.connections, size: 50,
      body: { query: { term: { ip } }, sort: [{ connected_at: { order: 'desc' } }] },
    }),
    es.search({
      index: INDICES.events, size: 50,
      body: { query: { term: { ip } }, sort: [{ logged_at: { order: 'desc' } }] },
    }),
    es.search({
      index: INDICES.subscriptions, size: 50,
      body: { query: { term: { ip } }, sort: [{ logged_at: { order: 'desc' } }] },
    }),
    es.search({
      index: INDICES.connections, size: 0,
      body: {
        query: { term: { ip } },
        aggs: {
          connections: { value_count: { field: 'ip' } },
          pubkeys: { cardinality: { field: 'pubkey' } },
          first_seen: { min: { field: 'connected_at' } },
          last_seen: { max: { field: 'connected_at' } },
        },
      },
    }),
  ]);

  const eventCount = await es.count({ index: INDICES.events, body: { query: { term: { ip } } } });
  const subCount = await es.count({ index: INDICES.subscriptions, body: { query: { term: { ip } } } });

  const a = statsAgg.aggregations;
  return {
    ip,
    geo: geoResult.hits.hits[0]?._source || null,
    stats: {
      connections: a.connections.value,
      events: eventCount.count,
      subscriptions: subCount.count,
      pubkeys: a.pubkeys.value,
      first_seen: a.first_seen?.value || null,
      last_seen: a.last_seen?.value || null,
    },
    connections: connResult.hits.hits.map(h => ({ id: parseInt(h._id), ...h._source })),
    events: eventResult.hits.hits.map(h => ({ id: parseInt(h._id), ...h._source })),
    subscriptions: subResult.hits.hits.map(h => ({ ...h._source })),
  };
}

module.exports = {
  getStats, getReaderStats, getTopIps, getActivity,
  getConnections, getEvents, getSubscriptions,
  getPubkeys, getPubkeyDetail, getPubkeyEvents, getPubkeySubscriptions, getPubkeyIps,
  getAllGeo, getGeoForPubkey, getGeoStats, getGeoStatsForPubkey,
  getIpDetail,
};
