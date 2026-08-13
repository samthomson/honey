const { Client } = require('@elastic/elasticsearch');

let client = null;

const INDICES = {
  connections: 'honey-connections',
  events: 'honey-events',
  subscriptions: 'honey-subscriptions',
  geo: 'honey-geo',
};

const INDEX_SETTINGS = {
  connections: {
    mappings: {
      properties: {
        ip: { type: 'keyword' },
        user_agent: { type: 'text', index: false },
        connected_at: { type: 'date', format: 'epoch_second' },
        disconnected_at: { type: 'date', format: 'epoch_second' },
        pubkey: { type: 'keyword' },
        duration: { type: 'integer' },
      },
    },
  },
  events: {
    mappings: {
      properties: {
        connection_id: { type: 'integer' },
        ip: { type: 'keyword' },
        event_id: { type: 'keyword' },
        pubkey: { type: 'keyword' },
        kind: { type: 'integer' },
        created_at: { type: 'date', format: 'epoch_second' },
        tags: { type: 'text', index: false },
        content: { type: 'text' },
        content_len: { type: 'integer' },
        logged_at: { type: 'date', format: 'epoch_second' },
      },
    },
  },
  subscriptions: {
    mappings: {
      properties: {
        connection_id: { type: 'integer' },
        ip: { type: 'keyword' },
        subscription_id: { type: 'keyword' },
        filters: { type: 'text', index: false },
        logged_at: { type: 'date', format: 'epoch_second' },
        pubkey: { type: 'keyword' },
      },
    },
  },
  geo: {
    mappings: {
      properties: {
        ip: { type: 'keyword' },
        country: { type: 'keyword' },
        country_code: { type: 'keyword' },
        region: { type: 'text' },
        city: { type: 'keyword' },
        location: { type: 'geo_point' },
        isp: { type: 'text' },
        org: { type: 'text' },
        as: { type: 'keyword' },
        proxy: { type: 'boolean' },
        hosting: { type: 'boolean' },
        geocoded_at: { type: 'date', format: 'epoch_second' },
      },
    },
  },
};

async function init(url) {
  const esUrl = url || process.env.ES_URL || 'http://localhost:9200';
  const tempClient = new Client({ node: esUrl, requestTimeout: 30000 });

  // Wait for ES to be ready
  let retries = 15;
  while (retries-- > 0) {
    try {
      const health = await tempClient.info();
      console.log(`[es] Connected to Elasticsearch: ${health.version.number}`);
      client = tempClient; // Only set client after confirmed connection
      break;
    } catch (err) {
      if (retries === 0) {
        console.error('[es] Could not connect after retries:', err.message);
        throw err;
      }
      await new Promise(r => setTimeout(r, 2000));
    }
  }

  // Create indices if they don't exist
  for (const [key, indexName] of Object.entries(INDICES)) {
    try {
      const exists = await client.indices.exists({ index: indexName });
      if (!exists) {
        await client.indices.create({ index: indexName, body: INDEX_SETTINGS[key] });
        console.log(`[es] Created index: ${indexName}`);
      }
    } catch (err) {
      console.error(`[es] Error creating index ${indexName}:`, err.message);
    }
  }

  return client;
}

function getClient() {
  return client;
}

module.exports = { init, getClient, INDICES };
