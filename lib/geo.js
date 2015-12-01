// Everything is approximate to the meter
//TODO coordinates should be a sequence, not a datum
function ReqlGeometry(type, coordinates, query) {
  if (type === 'Point') {
    ReqlGeometry.validateCoordinates(coordinates, query);
  }
  else if (type === 'Polygon') {
    //TODO validate
  }
  this.$reql_type$ = 'GEOMETRY';
  this.type = type;
  this.coordinates = coordinates; // longitude, latitude
}
module.exports = ReqlGeometry;

var geolib = require('geolib');
var Error = require(__dirname+"/error.js");
var util = require(__dirname+"/utils.js");


ReqlGeometry.buildFromDatum = function(value, query) {
  return new ReqlGeometry(value.type, value.coordinates, query);
};
ReqlGeometry.prototype.toDatum = function() {
  return {
    $reql_type$: this.$reql_type$,
    type: this.type,
    coordinates: util.toDatum(this.coordinates)
  };
};

ReqlGeometry.prototype.distance = function(to, options) {
  options = options || {};
  if ((options.geoSystem !== undefined) && (options.geoSystem !== 'WGS84')) {
    throw new Error.ReqlRuntimeError("GeoSystem not supported");
  }

  if (to.type !== 'Point') {
    //TODO Implement
    util.notAvailable('You can compute the distance to a point only in Reqlite (for now)', /* ?? query */ {});
  }
  var distance = geolib.getDistance(
    { latitude: this.coordinates[1], longitude: this.coordinates[0] },
    { latitude: to.coordinates[1], longitude: to.coordinates[0] },
    0.00001 // accuracy
  );

  var unit = 'km';
  if (options.unit !== undefined) {
    unit = options.unit;
  }

  return ReqlGeometry.convert(distance, 'm', unit);
};


ReqlGeometry.prototype.includes = function(geometry, query) {
  if (this.type !== 'Polygon') {
    throw new Error.ReqlRuntimeError('Expected geometry of type `Polygon` but found `'+this.type+'`', query.frames);
  }
  else if (this.type === 'Polygon') {
    if (geometry.type === 'Point') {
      return geometry.intersects(this, query);
    }
    else if (geometry.type === 'Polygon') {
      //TODO Handle polygon with holes
      for(var i=0; i<geometry.coordinates[0].length; i++) {
        if (!(new ReqlGeometry('Point', geometry.coordinates[0][i]).intersects(this))) {
          return false;
        }
        /*
        //TODO Handle polygons with holes
        for(var p=1; i<this.coordinates.length; p++) {
          if ((new ReqlGeometry('Point', geometry.coordinates[p][i]).intersects(this))) {
            return false
          }
        }
        */
      }
      return true;
    }
    else if (geometry.type === 'LineString') {
      //TODO See section 11 and 13 of http://arxiv.org/abs/1102.1215
      throw new Error.ReqlRuntimeError('intersects with LineString is not supported yet', query.frames);
    }
  }
  else if (geometry.type === 'LineString') {
    throw new Error.ReqlRuntimeError('intersects with LineString is not supported yet', query.frames);
  }
};

ReqlGeometry.prototype.intersects = function(geometry, query) {
  //TODO Handle polygon with holes?
  if (this.type === 'Point') {
    if (geometry.type === 'Point') {
      return util.eq(this.coordinates, geometry.coordinates);
    }
    else if (geometry.type === 'Polygon') {
      return geolib.isPointInside(this.toGeolib(), geometry.toGeolib());
    }
    else if (geometry.type === 'LineString') {
      // It seems that because of rounding errors, a point cannot intersect
      // a line except if it's actually of the extremities of the line
      for(var i=0; i<geometry.coordinates.length; i++) {
        if (util.eq(this.coordinates, geometry.coordinates[i])) {
          return true;
        }
      }
      return false;
    }
  }
  else if (this.type === 'Polygon') {
    if (geometry.type === 'Point') {
      return geometry.intersects(this, query);
    }
    else if (geometry.type === 'Polygon') {
      if (ReqlGeometry.polygonsIntersect(this, geometry) || ReqlGeometry.polygonsIntersect(geometry, this)) {
        return true;
      }
      return false;
    }
    else if (geometry.type === 'LineString') {
      //TODO See section 11 and 13 of http://arxiv.org/abs/1102.1215
      throw new Error.ReqlRuntimeError('intersects with LineString is not supported yet', query.frames);
    }
  }
  else if (geometry.type === 'LineString') {
    throw new Error.ReqlRuntimeError('intersects with LineString is not supported yet', query.frames);
  }

};

ReqlGeometry.polygonsIntersect = function(polygon1, polygon2) {
  // One point is inside the other polygon
  for(var p=0; p<polygon1.coordinates.length; p++) {
    var intersects = false;
    for(var i=0; i<polygon1.coordinates[p].length; i++) {
      if (new ReqlGeometry('Point', polygon1.coordinates[p][i]).intersects(polygon2)) {
        intersects = true;
      }
    }
    if ((p === 0) && (intersects === false)) {
      return false;
    }
    else if ((p>0) && (intersects === true)) {
      return false;
    }
  }
  return true;
};

ReqlGeometry.prototype.toGeolib = function(query) {
  if (this.type === 'Point') {
    return {
      longitude: this.coordinates[0],
      latitude: this.coordinates[1]
    };
  }
  else if (this.type === 'Polygon') {
    var result = [];
    for(var i=0; i<this.coordinates[0].length; i++) {
      result.push({
        longitude: this.coordinates[0][i][0],
        latitude: this.coordinates[0][i][1]
      });
    }
    return result;
  }
};
ReqlGeometry.prototype.toGeojson = function(query) {
  return {
    type: this.type,
    coordinates: this.coordinates
  };
};

ReqlGeometry.geojson = function(geojson, query) {
  var coordinates;
  util.assertType(geojson, "OBJECT", query);
  util.assertAttributes(geojson, ['type', 'coordinates'], query);

  if (geojson.type === 'Point') {
    util.assertType(geojson.coordinates, "ARRAY", query);
    coordinates = geojson.coordinates;
    util.assertCoordinatesLength(coordinates, query);
    return new ReqlGeometry(geojson.type, coordinates, query);
  }
  else if (geojson.type === 'LineString') {
    util.assertType(geojson.coordinates, "ARRAY", query);
    coordinates = geojson.coordinates;
    if (coordinates.length < 2) {
      throw new Error.ReqlRuntimeError('GeoJSON LineString must have at least two positions', query.frames);
    }
    for(var i=0; i<coordinates.length; i++) {
      util.assertType(coordinates.get(i), "ARRAY", query);
      if (coordinates.get(i).length < 2) {
        throw new Error.ReqlRuntimeError('Too few coordinates.  Need at least longitude and latitude', query.frames);
      }
      else if (coordinates.get(i).length > 2) {
        throw new Error.ReqlRuntimeError('A third altitude coordinate in GeoJSON positions was found, but is not supported', query.frames);
      }
      util.assertType(coordinates.get(i).get(0), "NUMBER", this);
      util.assertType(coordinates.get(i).get(1), "NUMBER", this);
    }
    return new ReqlGeometry(geojson.type, geojson.coordinates.toDatum(), query);
  }
  else if (geojson.type === 'Polygon') {
    util.assertType(geojson.coordinates, "ARRAY", query);
    coordinates = geojson.coordinates;
    // RethinkDB accepts an empty array here...
    if (util.isSequence(coordinates) && (coordinates.length > 0) && (coordinates.get(0).length < 4)) {
      throw new Error.ReqlRuntimeError('GeoJSON LinearRing must have at least four positions', query.frames);
    }

    for(var i=0; i<coordinates.length; i++) {
      util.assertType(coordinates.get(i), "ARRAY", query);
      for(var j=0; j<coordinates.get(i).length; j++) {
        util.assertType(coordinates.get(i).get(j), "ARRAY", query);
        if (coordinates.get(i).get(j).length < 2) {
          throw new Error.ReqlRuntimeError('Too few coordinates.  Need at least longitude and latitude', query.frames);
        }
        else if (coordinates.get(i).get(j).length > 2) {
          throw new Error.ReqlRuntimeError('A third altitude coordinate in GeoJSON positions was found, but is not supported', query.frames);
        }

        util.assertType(coordinates.get(i).get(j).get(0), "NUMBER", query);
        util.assertType(coordinates.get(i).get(j).get(1), "NUMBER", query);
      }
    }

    if (util.isSequence(coordinates) && (coordinates.length > 0)
        && !util.eq(coordinates.get(0).get(0), coordinates.get(0).get(coordinates.get(0).length-1))) {
      throw new Error.ReqlRuntimeError('First and last vertex of GeoJSON LinearRing must be identical', query.frames);
    }
    return new ReqlGeometry(geojson.type, coordinates, query);
  }
  else if ((geojson.type === 'MultiPoint')
      || (geojson.type === 'MultiLineString')
      || (geojson.type === 'MultiPolygon')) {
    throw new Error.ReqlRuntimeError('GeoJSON type `'+geojson.type+'` is not supported', query.frames);
  }
  else {
    throw new Error.ReqlRuntimeError('Unrecognized GeoJSON type `'+geojson.type+'`', query.frames);
  }
};

ReqlGeometry.factors = {
  m: 1,
  km: 1000,
  mi: 1609.344,
  nm: 1852,
  ft: 0.3048
};

ReqlGeometry.convert = function(distance, from, to) {
  //TODO assert valid units
  var factor = ReqlGeometry.factors[from]/ReqlGeometry.factors[to];
  return distance*factor;
};

ReqlGeometry.validateCoordinates = function(coordinates, query) {
  if ((coordinates[0] < -180) || (coordinates[0] > 180)) {
    throw new Error.ReqlRuntimeError('Longitude must be between -180 and 180.  Got '+coordinates[0], query.frames);
  }
  if ((coordinates[1] < -90) || (coordinates[1] > 90)) {
    throw new Error.ReqlRuntimeError('Latitude must be between -90 and 90.  Got '+coordinates[1], query.frames);
  }
};
