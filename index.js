const _ = require('lodash');
const logger = require('pelias-logger').get('openstreetmap');
const { Client } = require('@elastic/elasticsearch');
const importPipeline = require('./stream/importPipeline');

const stationHashes = {};
var hashCount = 0;

// OSM import is 2nd step after GTFS  stop/station import
// read all existing stations from ES into a hashtable for deduping

async function createDeduper() {

  const client = new Client({ node: 'http://localhost:9200' });

  function addHash(hit) {
    const doc = hit._source;
    const name = doc.name.default;
    const postal = doc.parent.postalcode;

    if(name && postal) {
      const pos = doc.center_point;
      const hash = name + postal + 'station';
      if (!stationHashes[hash]) {
        stationHashes[hash] = [];
      }
      stationHashes[hash].push({'popularity': doc.popularity, 'lat': pos.lat, 'lon': pos.lon});
      hashCount++;
    }
  }

  const responseQueue = [];

  logger.info( 'Reading existing stations for deduping');
  const response = await client.search({
    index: 'pelias',
    scroll: '30s',
    size: 10000,
    body: {
      'query': {
        'term': {
          'layer': {
            'value': 'station',
            'boost': 1.0
          }
        }
      }
    }
  });
  responseQueue.push(response.body);

  while (responseQueue.length) {
    const body = responseQueue.shift();
    body.hits.hits.forEach(addHash);

    // check to see if we have collected all docs
    if (!body.hits.hits.length) {
      logger.info('Extracted ' + hashCount + ' existing stations for deduping');
      break;
    }
    // get the next response if there are more items
    const next = await client.scroll({
      scroll_id: body._scroll_id,
      scroll: '30s'
    });
    responseQueue.push(next.body);
  }
}

createDeduper().then(() => {
  logger.info( 'Starting OSM import');
  importPipeline.import(stationHashes);
});
