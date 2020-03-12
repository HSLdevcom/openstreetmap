/**
 default list of tags to extract from the pbf file when running
 imports. @see: https://github.com/hldevcom/pbf2json for more info.
**/

var name_schema = require('../schema/name_osm');
var names = Object.keys(name_schema);
var name_expression = names.join('|'); // for name regex
var highways = ['motorway','trunk','primary','secondary','tertiary','unclassified','track',
                'service','residential','pedestrian','footway','living_street','cycleway','road'];
var highway_expression = highways.join(';'); // for name regex

// address tags imported
var tags = [
  'addr:housenumber§addr:street'
];

/* tags corresponding to venues. Syntax:
   - top level array items are combined with OR operation: item OR item2 OR item3 ...
   - each item can contain multiple AND conditions denoted by '§'. item = condition1 AND condition2 AND ...
   - each condition can contain multiple tag names, separated by exclamation mark '!'. Tag names are alternatives (OR operation)
   - each tag can have an optional value requirement, which is denoted by tilde '~'.
   - tag value definition can consist of several alternatives, separated by semicolon ';'
   - tag name and value can also be regular expressions. This is denoted by a hashtag '#' before the expression
   - this rule set means that we cannot use regex special chars or delimiters '!,;#§~' in the required tag names/values.
*/



var venue_tags = [
  'addr:housename',
  'amenity~library;fire_station;university;bus_station;hospital;police;townhall', // import these also without name
  'place!amenity!building!shop!office!cuisine!sport!natural!tourism!leisure!' +
  'historic!man_made!landuse!waterway!aerialway!craft!military!' +
  'aeroway~terminal;aerodrome;helipad;airstrip;heliport;areodrome;spaceport;landing_strip;airfield;airport' +
  '!highway~' + highway_expression +
  '§#(' + name_expression + ')(:(fi|sv|en))?'
];

names = names.join(',');
highways = highways.join(',');

module.exports = {tags, venue_tags, names, highways};
