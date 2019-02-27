
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
      var aliases = [];
      var allNames = {}; // deduping array

      var storeName = (key, value) => {
        var val1 = trim(value);
        if( !val1 ) {
            return;
        }
        var splitNames = val1.split(';');
        for(var name of splitNames) {
          if(allNames[name]) {
            continue;
          }
          allNames[name] = true;
          if(names[key]) { // slot already used
            aliases.push(name);
          } else {
            names[key] = name;
          }
        }
      };

      // Unfortunately we need to iterate over every tag,
      // so we only do the iteration once to save CPU.
      for( var tag in tags ){

        // Map localized names which begin with 'name:'
        // @ref: http://wiki.openstreetmap.org/wiki/Namespace#Language_code_suffix
        var suffix = getNameSuffix( tag );
        // set only languages we wish to support
        if( suffix && (!languages || languages.indexOf(suffix) !== -1)) {
          storeName(suffix, tags[tag]);
        }
        // Map name data from our name mapping schema
        else if( tag in NAME_SCHEMA ){
          storeName(NAME_SCHEMA[tag], tags[tag]);
        }
        // Map address data from our address mapping schema
        else if( tag in ADDRESS_SCHEMA ){
          var val3 = trim( tags[tag] );
          if( val3 ){
            doc.setAddress( ADDRESS_SCHEMA[tag], val3 );
          }
        }
      }

      // process names
      var defaultName = names['default'] || aliases[0];

      if (!defaultName && languages) { // api likes default name
        for(var lang of languages) {
          defaultName = names[lang];
          if (defaultName) { // use first supported name version as default
            break;
          }
        }
      }
      if (defaultName) {
        doc.setName( 'default', defaultName);
      }
      for(var prop in names) {
        if ( names[prop] !== defaultName) {
          // don't set duplicates. A missing language defaults to name.default.
          doc.setName( prop, names[prop] );
        }
      }
      for(const ali of aliases) {
        if(ali !== defaultName) {
          doc.setNameAlias( 'default', ali );
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

// extract name suffix, eg for 'name:EN' return 'en'
// if not valid, return false.
function getNameSuffix( tag ){

  if( tag.length < 6 || tag.substr(0,5) !== 'name:' ){
    return false;
  }

  // normalize suffix
  var suffix = tag.substr(5).toLowerCase();

  // check the suffix is in the localized key list
  if( LOCALIZED_NAME_KEYS.indexOf(suffix) === -1 ){
    return false;
  }

  return suffix;
}
