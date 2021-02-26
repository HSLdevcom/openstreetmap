
/**
  The tag mapper is responsible for mapping OSM tag information in to the
  document model, using a variety of different schemas found in /schema/*.
**/

var through = require('through2'),
    _ = require('lodash'),
    merge = require('merge'),
    peliasLogger = require( 'pelias-logger' ).get( 'openstreetmap' );

var LOCALIZED_NAME_KEYS = require('../config/localized_name_keys');
var NAME_SCHEMA = require('../schema/name_osm');
var prefixes = Object.keys(NAME_SCHEMA).map(name => name + ':');

var ADDRESS_SCHEMA = merge( true, false,
  require('../schema/address_tiger'),
  require('../schema/address_osm'),
  require('../schema/address_naptan'),
  require('../schema/address_karlsruhe')
);

var config = require('pelias-config').generate();
var api = config.api;

var languages;
if (Array.isArray(api.languages) && api.languages.length>0) {
  languages = api.languages;
}


module.exports = function(){

  var stream = through.obj( function( doc, enc, next ) {

    try {

      // skip records with no tags
      var tags = doc.getMeta('tags');
      if( !tags ){
        return next( null, doc );
      }

      var names = {};
      var aliases = {};
      var allNames = {}; // deduping array

      var storeName = (key, value, forceAlias) => {
        var val1 = trim(value);
        if( !val1 ) {
            return;
        }
        var splitNames = val1.split(';');
        for(var name of splitNames) {
          allNames[name] = true;
          if(names[key] || forceAlias) { // slot already used
            if (!aliases[key]) {
              aliases[key] = [];
            }
            aliases[key].push(name);
          } else {
            names[key] = name;
          }
        }
      };

      // Unfortunately we need to iterate over every tag,
      // so we only do the iteration once to save CPU.
      for( var tag in tags ){
        if( tag in NAME_SCHEMA ){
          // Map name data from our name mapping schema
          storeName(NAME_SCHEMA[tag], tags[tag]);
        } else if( tag in ADDRESS_SCHEMA ){
          var val3 = trim( tags[tag] );
          if( val3 ){
            doc.setAddress( ADDRESS_SCHEMA[tag], val3 );
          }
        } else {
          // Map localized names which begin with '???name:'
          var parts = getNameParts( tag );

          if (!parts) {
            continue;
          }
          var prefix = parts[0];
          var suffix = parts[1];
          // set only languages we wish to support
          if( suffix && (!languages || languages.indexOf(suffix) !== -1)) {
            if (prefix === 'name:') {
              storeName(suffix, tags[tag]);
            } else { // push to language aliases, to prefer the actual name:xx tag
              storeName(suffix, tags[tag], true);
            }
          }
        }
      }
      // push secondary name suggestions to actual name slots if they are free
      for(var key in aliases) {
        for(var ali of aliases[key]) {
          if (!names[key]) {
            names[key] = ali;
          }
        }
      }

      // process names
      var defaultName = names['default'];

      if (!defaultName && languages) { // api likes default name
        for(var lang of languages) {
          defaultName = names[lang];
          if (defaultName) { // use first supported name version as default
            break;
          }
        }
      }
      if (defaultName) {
        doc.setName('default', defaultName);
      }
      var namefi = names.fi;
      var namesv = names.sv;
      var namedef = names.default;
      if (namefi && namesv && namedef && namesv === namedef && namefi !== namedef) {
	// hit to name.default does not understand that it is in swedish, so put fi as default
	names.default = namefi;
      }

      for(var prop in names) {
        doc.setName( prop, names[prop] );
      }
      for(var akey in aliases) {
        for(var alias of aliases[akey]) {
          if (names[akey] !== alias) {
            doc.setNameAlias(key, alias);
          }
        }
      }

      // Import airport codes as aliases
      if( tags.hasOwnProperty('aerodrome') || tags.hasOwnProperty('aeroway') ){
        if( tags.hasOwnProperty('iata') ){
          var iata = trim( tags.iata );
          if( iata ){
            doc.setNameAlias( 'default', iata );
            doc.setNameAlias( 'default', `${iata} Airport` );
            doc.setPopularity(10000);
          }
        }
      }
    }

    catch( e ){
      peliasLogger.error( 'tag_mapper error' );
      peliasLogger.error( e.stack );
      peliasLogger.error( JSON.stringify( doc, null, 2 ) );
    }

    return next( null, doc );

  });

  // catch stream errors
  stream.on( 'error', peliasLogger.error.bind( peliasLogger, __filename ) );

  return stream;
};

// Clean string of leading/trailing junk chars
function trim( str ){
  return _.trim( str, '#$%^*<>-=_{};:",./?\t\n\' ' );
}

// extract name prefix and suffix, eg 'name:EN' returns ['name', 'en']
// if not valid, return null

function getNameParts( tag ) {
  for(var prefix of prefixes) {
    if (tag.startsWith(prefix)) {
      // normalized suffix
      var suffix = tag.substr(prefix.length).toLowerCase();
      // check the suffix is in the localized key list
      if (suffix.length > 0 &&  LOCALIZED_NAME_KEYS.indexOf(suffix) !== -1) {
        return [prefix, suffix];
      }
    }
  }
  return null;
}
