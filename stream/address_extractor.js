/**
  The address extractor is responsible for cloning documents where a valid address
  data exists.

  The hasValidAddress() function examines the address property which was populated
  earier in the pipeline by the osm.tag.mapper stream and therefore MUST come after
  that stream in the pipeline or it will fail to find any address information.

  There are a few different outcomes for this stream depending on the data contained
  in each individual document, the result can be, 0, 1 or 2 documents being emitted.

  In the case where the document contains BOTH a valid house number & street name we
  consider this record to be an address in it's own right and we clone that record,
  duplicating the data across to the new doc instance while adjusting it's id and type.
**/

var through = require('through2');
var isObject = require('is-object');
var extend = require('extend');
var peliasLogger = require( 'pelias-logger' ).get( 'openstreetmap' );
var Document = require('pelias-model').Document;
var geolib = require( 'geolib' );
var config = require('pelias-config').generate().api;
var highways = require('../config/features').highways;
var venuefilters = require('../config/features').venue_filters;
var addrfilters = require('../config/features').address_filters;
var NAME_SCHEMA = require('../schema/name_osm');
var popularityById = require('../config/popularity');

// ranking by place tag values. Default popularity is 10
var placePopularity = {
  district: 30,
  city: 30,
  town: 20,
  municipality: 20,
  county: 15,
  village: 8,
  suburb: 8,
  hamlet: 7,
  square: 6,
  neighbourhood: 5,
  allotments: 5,
  quarter: 4,
  city_block: 3,
  locality: 2,
  isolated_dwelling: 2,
  farm: 2,
  island: 2,
  plot: 1,
  islet: 1
};

// Popularity coefficients
var popularityCoeff = {
  amenity: { toilets: 0.2 }  // reduce toilet popularity to 20 % only
};

function hasValidAddress( doc ){
  if( !isObject( doc ) ){ return false; }
  if( !isObject( doc.address_parts ) ){ return false; }
  if( 'string' !== typeof doc.address_parts.number ){ return false; }
  if( 'string' !== typeof doc.address_parts.street ){ return false; }
  if( !doc.address_parts.number.length ){ return false; }
  if( !doc.address_parts.street.length ){ return false; }
  return true;
}

var languages;
if (Array.isArray(config.languages) && config.languages.length>0) {
  languages = config.languages;
}

function hasValidName( doc ){
  if(languages) {
    for(var lang in languages) {
      if (doc.getName(languages[lang])) {
        return true;
      }
    }
  } else {
    return !!doc.getName('default') ;
  }
  return false;
}

function isStreet( tags ){
  var hwtype = tags.highway;

  if (!hwtype) {
    return false;
  }
  return (highways.indexOf(hwtype) !== -1);
}

function applyFilters( tags, filters ){
  if (filters) {
    for (let i=0; i<filters.length; i++) {
      const filter = filters[i];
      var match = true;
      for(var f in filter) {
        if (tags[f] !== filter[f])  {
          match = false;
          break;
        }
      }
      if (match) {
        return false;
      }
    }
  }
  return true; // doc OK, it passes filtering
}

var houseNameValidator = new RegExp('[a-zA-Z]{3,}');

var minorBuildings = ['barn', 'cabin', 'shed', 'garage', 'hut', 'carbage_shed'];

function getHouseName( doc ){
  if( !isObject( doc ) ){ return null; }
  if( !isObject( doc.address_parts ) ){ return null; }
  if( 'string' !== typeof doc.address_parts.name ){ return null; }
  if( !houseNameValidator.test(doc.address_parts.name) ){ return null; }

  return doc.address_parts.name;
}

var houses = {};

function dedupeHouse(name, doc) {
  var centroid = doc.getCentroid();
  if (!houses[name]) {
    houses[name] = [centroid];
  } else {
    for (var i in houses[name]) {
      var c2 =  houses[name][i];
      var p1 = { longitude: centroid.lon, latitude: centroid.lat };
      var p2 = { longitude: c2.lon, latitude: c2.lat };
      if(geolib.getDistance(p1, p2) < 1000) { // m
        return true;
      }
    }
    houses[name].push(centroid);
  }
  return false;
}

module.exports = function(){

  var stream = through.obj( function( doc, enc, next ) {
    var isNamedPoi = hasValidName( doc );
    var isAddress = hasValidAddress( doc );
    var houseName = getHouseName( doc );
    var tags = doc.getMeta('tags');
    var addressNames = {}; // for deduping
    var popularity = 10;
    var id = doc.getSourceId();

    if (popularityById[id]) {
      // use configured popularity to favor or avoid items
      popularity=popularityById[id];
    } else if(
      (tags.building && minorBuildings.indexOf(tags.building) !== -1) ||
      tags.waterway || tags.landuse // myllypuro puro is not as important as the venue
    ) {
      popularity=5;
    } else if(tags.place) {
      // fallback to default popularity 10
      popularity = placePopularity[tags.place] || 10;
    }

    for(var f in popularityCoeff) {
      const val = tags[f];
      if (val) {
        const coeff = popularityCoeff[f][val];
        if (coeff) {
          popularity = Math.ceil(coeff*popularity);
        }
      }
    }

    // create a new record for street addresses
    if(isAddress && applyFilters(tags, addrfilters)) {
      var record;
      var apop = popularity;

      // boost popularity of explicit address points at entrances and gates
      if (tags.barrier === 'gate') {
        apop = 13;
      } else if (tags.entrance === 'main' || tags._centroidType === 'mainEntrance') {
        apop = 12;
      } else if (tags.entrance === 'yes' || tags._centroidType === 'entrance') {
        apop = 11;
      }

      // accept semi-colon delimited house numbers
      // ref: https://github.com/pelias/openstreetmap/issues/21
      var streetnumbers = doc.address_parts.number.split(';').map(Function.prototype.call, String.prototype.trim);

      var unit = null;

      if (tags['addr:unit']) {
        unit = ' ' + tags['addr:unit'].toUpperCase();
      }

      streetnumbers.forEach( function( streetno, i ) {
        let uno = unit ? streetno + unit : streetno; // add unit if available
        try {
          var newid = [id];
          if( i > 0 ){
            newid.push( uno );
          }
          var name = doc.address_parts.street + ' ' +  uno;
          // copy data to new document
          record = new Document( 'openstreetmap', 'address', newid.join(':') )
            .setName( 'default', name )
            .setCentroid( doc.getCentroid() );
          addressNames[name] = true;

          setProperties( record, doc, uno );
        }

        catch( e ){
          peliasLogger.error( 'address_extractor error' );
          peliasLogger.error( e.stack );
          peliasLogger.error( JSON.stringify( doc, null, 2 ) );
        }

        if( record !== undefined ){
          // copy meta data (but maintain the id & type assigned above)
          record._meta = extend( true, {}, doc._meta, { id: record.getId() } );

          record.setPopularity(apop);

          // multilang/altname support for addresses
          for( var tag in tags ) {
            var suffix = getStreetSuffix(tag);
            if (suffix && suffix !== 'default' && tags[tag] !== doc.address_parts.street) {
              record.setName(suffix, tags[tag] + ' ' + uno);
            }
          }
          var namefi = record.getName('fi');
          var namesv = record.getName('sv');
          var namedef = record.getName('default');
          if (namefi && namesv && namedef && namesv === namedef && namefi !== namedef) {
            record.setName('default', namefi);
          }
          this.push( record );
        }
        else {
          peliasLogger.error( '[address_extractor] failed to push address downstream' );
        }
      }, this);
    }

    // create a new record for buildings. Try to avoid duplicates
    if( houseName && doc.getName('default') !== houseName && !dedupeHouse(houseName, doc)) {
      var record2;

      try {
        var newid = id + ':B';

        // copy data to new document
        record2 = new Document( 'openstreetmap', 'venue', newid )
          .setName( 'default', houseName )
          .setCentroid( doc.getCentroid() );

        setProperties( record2, doc );
        record2.setPopularity(7); // set lower priority than regular venues
      }

      catch( e ){
        peliasLogger.error( 'address_extractor error' );
        peliasLogger.error( e.stack );
        peliasLogger.error( JSON.stringify( doc, null, 2 ) );
      }

      if( record2 !== undefined ){
        // copy meta data (but maintain the id & type assigned above)
        record2._meta = extend( true, {}, doc._meta, { id: record2.getId() } );
        this.push( record2 );
      }
      else {
        peliasLogger.error( '[address_extractor] failed to push housename downstream' );
      }
    }

    // forward doc downstream if it's a POI in its own right
    // note: this MUST be below the address push()
    if( isNamedPoi && !addressNames[doc.getName('default')] && applyFilters(tags, venuefilters)) {
      if (tags.public_transport === 'station' || tags.amenity === 'bus_station') {
        doc.setLayer('station');
        if (tags.usage !== 'tourism') {
          popularity = 1000000; // same as in gtfs stations
        }
      } else if (isStreet(tags)) {
        doc.setLayer('street');
        popularity = 11; // higher as address, to ensure that plain streetname fits into search
        doc.setId(doc.getId().replace('venue','street'));
      }
      doc.setPopularity(popularity);
      this.push( doc );
    }
    return next();

  });

  // catch stream errors
  stream.on( 'error', peliasLogger.error.bind( peliasLogger, __filename ) );

  return stream;
};

// properties to map from the osm record to the pelias doc
var addrProps = [ 'name', 'street', 'zip' ];

// call document setters and ignore non-fatal errors
function setProperties( record, doc, number ){
  addrProps.forEach( function ( prop ){
    try {
      const val = doc.getAddress( prop );
      if (val) {
        record.setAddress( prop, val );
        if (prop === 'zip') {
          record.addParent( 'postalcode', val, '?' );
        }
      }
    } catch ( ex ) {}
  });
  if (number) {
    record.setAddress( 'number', number);
  }
}


function getStreetSuffix( tag ){
  if( tag.length < 6 || tag.substr(0,12) !== 'addr:street:' ){
    return false;
  }
  // normalize suffix
  var suffix = tag.substr(12).toLowerCase();
  if( suffix in NAME_SCHEMA ){
    // Map to pelias world
    suffix = NAME_SCHEMA[suffix];
  }
  if (languages && languages.indexOf(suffix) === -1) { // not interested in this name version
    return false;
  }
  return suffix;
}

// export for testing
module.exports.hasValidAddress = hasValidAddress;
