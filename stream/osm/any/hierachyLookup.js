
var through = require('through2'),
    buildHierachy = require('./buildHierachy');

function hierachyLookup( backends, fallbackBackend ){

  var stream = through.obj( function( item, enc, done ) {

    var reply = function(){
      this.push( item ); // Forward record down the pipe
      return done(); // ACK and take next record from the inbound stream
    }.bind(this);

    // Skip lookup for nodes without a name
    if( !item.name || !item.name.default ){
      return reply();
    }

    // Skip lookup if record already has geo info
    if( item.admin0 && item.admin1 && item.admin2 ){
      return reply();
    }

    buildHierachy( backends, item.center_point, function( error, result ){

      // An error occurred
      // @todo: this should never happen
      if( error ){
        console.error( 'hierachyLookup error:', error );
        return reply();
      }

      else if( !result ){
        console.error( 'hierachyLookup returned 0 results' );
        return reply();
      }

      // Copy admin data to the osm record
      else {
        if( result.admin0 ){ item.admin0 = result.admin0; }
        if( result.admin1 ){ item.admin1 = result.admin1; }

        if( result.neighborhood ){ item.admin2 = result.neighborhood; }
        else if( result.locality ){ item.admin2 = result.locality; }
        else if( result.local_admin ){ item.admin2 = result.local_admin; }
        else if( result.admin2 ){ item.admin2 = result.admin2; }

        // fallback to geonames hierachy
        if( !item.admin0 || !item.admin1 || !item.admin2 ){
          fallbackBackend.findAdminHeirachy( item.center_point, null, function ( error, resp ) {
            if( Array.isArray( resp ) && resp.length ){
              if( !item.admin0 && resp[0].admin0 ){ item.admin0 = resp[0].admin0; }
              if( !item.admin1 && resp[0].admin1 ){ item.admin1 = resp[0].admin1; }
              if( !item.admin2 && resp[0].admin2 ){ item.admin2 = resp[0].admin2; }
            }
            return reply();
          });
        }

        else return reply();
      }

    }.bind(this));

  });

  // catch stream errors
  stream.on( 'error', console.error.bind( console, __filename ) );

  return stream;
}

module.exports = hierachyLookup;