const through = require('through2');
const logger = require( 'pelias-logger' ).get( 'openstreetmap' );
const addresses = {};

var dedupedAddresses = 0;

function dedupe( doc ){
  if(doc.getLayer() === 'address' && doc.parent.postalcode) {
    var hash = doc.getAddress('street') + doc.getAddress('number') + doc.parent.postalcode;
    var pop  = doc.getPopularity();
    if (!addresses[hash] || pop > addresses[hash]) {
      // let more popular duplicates pass through, because we do not
      // want to prefer a building center over an exact entrance location
      addresses[hash] = pop;
    } else {
      dedupedAddresses++;
      return false;
    }
  }
  return true;
}

module.exports = function() {
  return through.obj(function( record, enc, next ) {
    if (dedupe(record)) {
      this.push(record);
    }
    next();
  }, function(next) {
    logger.info('Deduped addresses: ' + dedupedAddresses);
    next();
  });
};
