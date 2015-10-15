var config = require('./../config.js');

var r = require('rethinkdb');
var assert = require('assert');

var connections = {};
var TEST_DB = 'reqlitetest';
var TEST_TABLE = 'reqlitetestgeo';

var compare = require('./util.js').generateCompare(connections);

describe('geo.js', function(){
  before(function(done) {
    setTimeout(function() { // Delay for nodemon to restart the server
      r.connect(config.rethinkdb).bind({}).then(function(conn) {
        connections.rethinkdb = conn;
        return r.connect(config.reqlite);
      }).then(function(conn) {
        connections.reqlite = conn;
        this.query = r.dbCreate(TEST_DB);
        return this.query.run(connections.rethinkdb);
      }).catch(function() { // ignore errors
      }).finally(function() {
        return this.query.run(connections.reqlite);
      }).catch(function() { // ignore errors
      }).finally(function() {
        this.query = r.db(TEST_DB).tableDrop(TEST_TABLE);
        return this.query.run(connections.rethinkdb);
      }).catch(function() { // ignore errors
      }).finally(function() {
        return this.query.run(connections.reqlite);
      }).catch(function() { // ignore errors
      }).finally(function() {
        this.query = r.db(TEST_DB).tableCreate(TEST_TABLE);
        return this.query.run(connections.rethinkdb);
      }).catch(function() { // ignore errors
      }).finally(function() {
        return this.query.run(connections.reqlite);
      }).catch(function() { // ignore errors
      }).finally(function() {
        this.query = r.db(TEST_DB).table(TEST_TABLE).insert([
          {id: 1, location: r.point(10, 10)},
          {id: 2, location: r.point(10, 11)},
          {id: 3, location: r.point(11, 11)},
          {id: 4, location: r.point(11, 10)},
        ]);
        return this.query.run(connections.rethinkdb);
      }).catch(function() { // ignore errors
      }).finally(function() {
        return this.query.run(connections.reqlite);
      }).catch(function() { // ignore errors
      }).finally(function() {
        this.query = r.db(TEST_DB).table(TEST_TABLE).indexCreate('location', {geo: true});
        return this.query.run(connections.rethinkdb);
      }).catch(function() { // ignore errors
      }).finally(function() {
        return this.query.run(connections.reqlite);
      }).catch(function() { // ignore errors
      }).finally(function() {
        this.query = r.db(TEST_DB).table(TEST_TABLE).indexWait();
        return this.query.run(connections.rethinkdb);
      }).catch(function(e) { // ignore errors
      }).finally(function() {
        return this.query.run(connections.reqlite);
      }).catch(function() { // ignore errors
      }).finally(function() {
        done();
      });
    }, 500);
  });

  it('point - 1', function(done) {
    var query = r.point(0, 0);
    compare(query, done);
  });

  it('point - 2', function(done) {
    var query = r.point(1, 2);
    compare(query, done);
  });

  it('point - 3', function(done) {
    var query = r.point(1, 2000);
    compare(query, done);
  });

  it('point - 4', function(done) {
    var query = r.point(1, 90);
    compare(query, done);
  });

  it('point - 5', function(done) {
    var query = r.point(1000, 1);
    compare(query, done);
  });

  it('point - 6', function(done) {
    var query = r.point(1000, 1000);
    compare(query, done);
  });

  it('point - 8', function(done) {
    var query = r.point('foo', 1000);
    compare(query, done);
  });

  it('point - 9', function(done) {
    var query = r.point(20, 'foo');
    compare(query, done);
  });

  it('point - 10', function(done) {
    var query = r.point(20, 20, 20, 20);
    compare(query, done);
  });

  it('distance - 1', function(done) {
    var point1 = r.point(-122,37);
    var point2 = r.point(-117,32);
    var query = r.distance(point1, point2, {unit: 'km'});
    compare(query, done, function(result) {
      return Math.floor(result*1000);
    });
  });

  it('distance - 2', function(done) {
    var point1 = r.point(-2,57);
    var point2 = r.point(30,-80);
    var query = r.distance(point1, point2, {unit: 'km'});
    compare(query, done, function(result) {
      return Math.floor(result*1000);
    });
  });

  it('distance - 3', function(done) {
    var point1 = r.point(-122,37);
    var point2 = r.point(-117,32);
    var query = r.distance(point1, point2, {unit: 'm'});
    compare(query, done, function(result) {
      return Math.floor(result);
    });
  });

  it('distance - 4', function(done) {
    var point1 = r.point(-122,37);
    var point2 = r.point(-117,32);
    var query = r.distance(r.args([point1, point1, point2]), {unit: 'km'});
    compare(query, done);
  });

  it('distance - 5', function(done) {
    var point1 = r.point(-122,37);
    var point2 = r.point(-117,32);
    var query = r.distance('point1', point2, {unit: 'km'});
    compare(query, done);
  });

  it('distance - 6', function(done) {
    var point1 = r.point(-122,37);
    var point2 = r.point(-117,32);
    var query = r.distance(point1, 'point2', {unit: 'km'});
    compare(query, done);
  });

  it('circle - 1', function(done) {
    var query = r.circle([-122, 37], 1000000);
    compare(query, done, function(result) {
      var roundedResults = [result.type, result.coordinates.length];
      for(var i=0; i<result.coordinates[0].length; i++) {
        roundedResults.push(Math.floor(result.coordinates[0][i][0]*1000));
        roundedResults.push(Math.floor(result.coordinates[0][i][1]*1000));
      }
      return roundedResults;
    });
  });

  it('circle - 2', function(done) {
    var query = r.circle(r.args([-122, 37], 1000000), 'bar');
    compare(query, done, function(error) {
      return /^Expected 1 argument but found 2/.test(error);
    });
  });

  it('circle - 3', function(done) {
    var query = r.circle([-122, 37], 'foo');
    compare(query, done);
  });

  it('circle - 4', function(done) {
    var query = r.circle({foo: -122, bar: 37, buzz: 37}, 200);
    compare(query, done);
  });

  it('circle - 5', function(done) {
    var query = r.circle([-122, 37, 37], 200);
    compare(query, done);
  });

  it('circle - 6', function(done) {
    var query = r.circle(['-122', 37], 200);
    compare(query, done);
  });

  it('line - 1', function(done) {
    var query = r.line(
      [-122.423246,37.779388],
      [-122.423246,37.329898],
      [-121.886420,37.329898],
      [-121.886420,37.779388]
    );
    compare(query, done);
  });

  it('line - 2', function(done) {
    var query = r.line(
      [-122.423246,37.779388]
    );
    compare(query, done);
  });

  it('line - 3', function(done) {
    var query = r.line(
      [-122.423246,37.779388],
      ['-122.423246',37.329898],
      [-121.886420,37.779388]
    );
    compare(query, done);
  });

  it('line - 4', function(done) {
    var query = r.line(
      [-122.423246,37.779388],
      [-122.423246,37.329898, 2],
      [-121.886420,37.779388]
    );
    compare(query, done);
  });

  it('fill - 1', function(done) {
    var query = r.line(
      [-122.423246,37.779388],
      [-122.423246,37.329898],
      [-121.886420,37.329898],
      [-121.886420,37.779388]
    ).fill();
    compare(query, done);
  });

  it('fill - 2', function(done) {
    var query = r.line(
      [-122.423246,37.779388],
      [-122.423246,37.329898],
      [-121.886420,37.329898],
      [-121.886420,37.779388],
      [-122.423246,37.779388]
    ).fill();
    compare(query, done);
  });

  it('fill - 3', function(done) {
    var query = r.line(
      [-122.423246,37.779388],
      [-122.423246,37.329898],
      [-121.886420,37.329898],
      [-121.886420,37.779388],
      [-122.423246,37.779388]
    ).fill('foo');
    compare(query, done);
  });

  it('fill - 4', function(done) {
    var query = r.line(
      [-122.423246,37.779388],
      ['-122.423246',37.329898],
      [-121.886420,37.329898],
      [-121.886420,37.779388],
      [-122.423246,37.779388]
    ).fill();
    compare(query, done);
  });

  it('fill - 5', function(done) {
    var query = r.line(
      [-122.423246,37.779388]
    ).fill();
    compare(query, done);
  });

  it('polygon - 1', function(done) {
    var query = r.polygon(
      [-122.423246,37.779388],
      [-122.423246,37.329898],
      [-121.886420,37.329898],
      [-121.886420,37.779388]
    );
    compare(query, done);
  });

  it('polygon - 2', function(done) {
    var query = r.polygon(
      [-122.423246,37.779388],
      [-122.423246,37.329898],
      [-121.886420,37.329898],
      [-121.886420,37.779388],
      [-122.423246,37.779388]
    );
    compare(query, done);
  });

  it('polygonSub - 1', function(done) {
    var outerPolygon = r.polygon(
        [-122.4,37.7],
        [-122.4,37.3],
        [-121.8,37.3],
        [-121.8,37.7]
    );
    var innerPolygon = r.polygon(
        [-122.3,37.4],
        [-122.3,37.6],
        [-122.0,37.6],
        [-122.0,37.4]
    );
    var query = outerPolygon.polygonSub(innerPolygon);
    compare(query, done);
  });

  it('polygonSub - 2', function(done) {
    var outerPolygon = r.polygon(
        [-122.4,37.7],
        [-122.4,37.3],
        [-121.8,37.3],
        [-121.8,37.7]
    );
    var innerPolygon = r.polygon(
        [-122.3,37.4],
        [-122.3,37.6],
        [-122.0,37.6],
        [-122.0,37.4]
    );
    var otherPolygon = r.polygon(
        [-125.3,32.4],
        [-121.3,35.6],
        [-120.0,32.6],
        [-123.0,36.4]
    );
    var query = outerPolygon.polygonSub(innerPolygon).polygonSub(otherPolygon);
    compare(query, done);
  });

  it('polygonSub - 3', function(done) {
    var outerPolygon = r.polygon(
        [-122.4,37.7],
        [-122.4,37.3],
        [-121.8,37.3],
        [-121.8,37.7]
    );
    var innerPolygon = r.line(
        [-122.3,37.4],
        [-122.3,37.6],
        [-122.0,37.6],
        [-122.0,37.4]
    );

    var query = outerPolygon.polygonSub(innerPolygon);
    compare(query, done);
  });

  it('polygonSub - 4', function(done) {
    var outerPolygon = r.line(
        [-122.4,37.7],
        [-122.4,37.3],
        [-121.8,37.3],
        [-121.8,37.7]
    );
    var innerPolygon = r.polygon(
        [-122.3,37.4],
        [-122.3,37.6],
        [-122.0,37.6],
        [-122.0,37.4]
    );

    var query = outerPolygon.polygonSub(innerPolygon);
    compare(query, done);
  });

  it('geojson - 1', function(done) {
    var query = r.geojson({
      'type': 'Point',
      'coordinates': [ -122.423246, 37.779388 ]
    });
    compare(query, done);
  });

  it('geojson - 2', function(done) {
    var query = r.geojson({
      'type': 'LineString',
      'coordinates': [[ -122, 37 ], [ -120, 34 ]]
    });
    compare(query, done);
  });

  it('geojson - 3', function(done) {
    var query = r.geojson({
      'type': 'LineString',
      'coordinates': [[ -122, 37 ], [ -120, 34 ], [-100, 45]]
    });
    compare(query, done);
  });

  it('geojson - 4', function(done) {
    var query = r.geojson({
      'type': 'Polygon',
      'coordinates': [ [ [100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0], [100.0, 0.0] ] ]
    });
    compare(query, done);
  });

  it('geojson - 5', function(done) {
    var query = r.geojson({
      'type': 'Polygon',
      'coordinates': [ [ [102, 0], [101, 1], [100, 2], [105, 50] ] ]
    });
    compare(query, done);
  });

  it('geojson - 6', function(done) {
    var query = r.geojson({
      'type': 'Unknowntype',
      'coordinates': [ -122.423246, 37.779388 ]
    });
    compare(query, done);
  });

  it('geojson - 7', function(done) {
    var query = r.geojson({
      'type': 'Point',
      'coordinates': []
    });
    compare(query, done);
  });
  it('geojson - 8', function(done) {
    var query = r.geojson({
      'type': 'LineString',
      'coordinates': []
    });
    compare(query, done);
  });
  it('geojson - 9', function(done) {
    var query = r.geojson({
      'type': 'Polygon',
      'coordinates': []
    });
    compare(query, done);
  });

  it('geojson - 10', function(done) {
    var query = r.geojson({
      'type': 'Point',
      'coordinates': 'foo'
    });
    compare(query, done);
  });

  it('geojson - 11', function(done) {
    var query = r.geojson({
      'type': 'LineString',
      'coordinates': 'foo'
    });
    compare(query, done);
  });

  it('geojson - 12', function(done) {
    var query = r.geojson({
      'type': 'Polygon',
      'coordinates': 'foo'
    });
    compare(query, done);
  });

  // Geojson types that can't be converted in Reql
  it('geojson - 13', function(done) {
    var query = r.geojson({
      'type': 'MultiPoint',
      'coordinates': [[ -122.423246, 37.779388 ], [ -122.423246, 37.779388 ]]
    });
    compare(query, done);
  });

  it('geojson - 14', function(done) {
    var query = r.geojson({
      'type': 'MultiLineString',
      'coordinates': [[[ -122, 37 ], [ -120, 34 ]], [[ -42, 19 ], [ -56, 66 ]]]
    });
    compare(query, done);
  });

  it('geojson - 15', function(done) {
    var query = r.geojson({
      'type': 'MultiPolygon',
      'coordinates': [[[[ -122, 37 ], [ -120, 34 ]], [[ -42, 19 ], [ -56, 66 ]]], [[[ -122, 37 ], [ -120, 34 ]], [[ -42, 19 ], [ -56, 66 ]]]]
    });
    compare(query, done);
  });

  it('geojson - 16', function(done) {
    var query = r.geojson('foo');
    compare(query, done);
  });

  it('geojson - 17', function(done) {
    var query = r.geojson({
      'type': 'Polygon',
      'coordinates': [[-122.4,37.7], [-122.4,37.3], [-121.8,37.3], [-121.8,37.7]]
    });
    compare(query, done);
  });

  it('geojson - 17', function(done) {
    var query = r.geojson({
      'type': 'Polygon',
      'coordinates': [[-122.4,37.7], [-122.4,37.3], [-121.8,37.3], [-121.8,37.7]]
    });
    compare(query, done);
  });

  it('geojson - 18', function(done) {
    var query = r.geojson();
    compare(query, done);
  });

  it('geojson - 19', function(done) {
    var query = r.geojson('foo');
    compare(query, done);
  });

  it('geojson - 20', function(done) {
    var query = r.geojson({});
    compare(query, done, function(error) {
      var message = error.split(':')[0];
      assert(message.length > 0);
      return message;
    });
  });

  it('geojson - 21', function(done) {
    var query = r.geojson({type: 'foo'});
    compare(query, done, function(error) {
      var message = error.split(':')[0];
      assert(message.length > 0);
      return message;
    });
  });

  it('geojson - 22', function(done) {
    var query = r.geojson({type: 'foo', coordinates: 'bar'});
    compare(query, done, function(error) {
      var message = error.split(':')[0];
      assert(message.length > 0);
      return message;
    });
  });

  it('geojson - 23', function(done) {
    var query = r.geojson({type: 'Point', coordinates: 'bar'});
    compare(query, done, function(error) {
      var message = error.split(':')[0];
      assert(message.length > 0);
      return message;
    });
  });

  it('geojson - 24', function(done) {
    var query = r.geojson({type: 'Point', coordinates: []});
    compare(query, done, function(error) {
      var message = error.split(':')[0];
      assert(message.length > 0);
      return message;
    });
  });

  it('geojson - 25', function(done) {
    var query = r.geojson({type: 'Point', coordinates: [1,2,3,4,5]});
    compare(query, done, function(error) {
      var message = error.split(':')[0];
      assert(message.length > 0);
      return message;
    });
  });

  it('geojson - 26', function(done) {
    var query = r.geojson({
      'type': 'LineString',
      'coordinates': [[ -122, 37, 2], [ -120, 34 ], [-100, 45]]
    });
    compare(query, done);
  });

  it('geojson - 27', function(done) {
    var query = r.geojson({
      'type': 'LineString',
      'coordinates': [[ -122], [ -120, 34 ], [-100, 45]]
    });
    compare(query, done);
  });

  it('geojson - 28', function(done) {
    var query = r.geojson({
      'type': 'LineString',
      'coordinates': [[ -122, 32], 'foo', [ -120, 34 ], [-100, 45]]
    });
    compare(query, done);
  });

  it('geojson - 29', function(done) {
    var query = r.geojson({
      'type': 'LineString',
      'coordinates': [[ '-122', 37], [ -120, 34 ], [-100, 45]]
    });
    compare(query, done);
  });

  it('geojson - 30', function(done) {
    var query = r.geojson({
      'type': 'Polygon',
      'coordinates': ['[-122.4,37.7]', [-122.4,37.3], [-121.8,37.3], [-121.8,37.7]]
    });
    compare(query, done);
  });

  it('geojson - 31', function(done) {
    var query = r.geojson({
      'type': 'Polygon',
      'coordinates': [ [ [100.0, 0.0], ['101.0', 0.0], [101.0, 1.0], [100.0, 1.0], [100.0, 0.0] ] ]
    });
    compare(query, done);
  });

  it('geojson - 32', function(done) {
    var query = r.geojson({
      'type': 'Polygon',
      'coordinates': [ [ [100.0, 0.0], [101.0, 0.0, 2], [101.0, 1.0], [100.0, 1.0], [100.0, 0.0] ] ]
    });
    compare(query, done);
  });

  it('geojson - 33', function(done) {
    var query = r.geojson({
      'type': 'Polygon',
      'coordinates': [ [ [100.0, 0.0], [101.0], [101.0, 1.0], [100.0, 1.0], [100.0, 0.0] ] ]
    });
    compare(query, done);
  });

  it('geojson - 34', function(done) {
    var query = r.geojson({
      'type': 'Polygon',
      'coordinates': [ [ [100.0, 0.0], '[101.0, 0.0]', [101.0, 1.0], [100.0, 1.0], [100.0, 0.0] ] ]
    });
    compare(query, done);
  });

  it('toGeojson - 1', function(done) {
    var query = r.geojson({
      'type': 'Point',
      'coordinates': [ -122.423246, 37.779388 ]
    }).toGeojson();
    compare(query, done);
  });

  it('toGeojson - 2', function(done) {
    var query = r.geojson({
      'type': 'LineString',
      'coordinates': [[ -122, 37 ], [ -120, 34 ]]
    }).toGeojson();
    compare(query, done);
  });

  it('toGeojson - 3', function(done) {
    var query = r.geojson({
      'type': 'LineString',
      'coordinates': [[ -122, 37 ], [ -120, 34 ], [-100, 45]]
    }).toGeojson();
    compare(query, done);
  });

  it('toGeojson - 4', function(done) {
    var query = r.geojson({
      'type': 'Polygon',
      'coordinates': [ [ [100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0], [100.0, 0.0] ] ]
    }).toGeojson();
    compare(query, done);
  });

  it('toGeojson - 5', function(done) {
    var query = r.geojson({
      'type': 'Point',
      'coordinates': [ -122.423246, 37.779388 ]
    }).toGeojson('bar');
    compare(query, done);
  });


  it('toGeojson - 5', function(done) {
    var outerPolygon = r.polygon(
        [-122.4,37.7],
        [-122.4,37.3],
        [-121.8,37.3],
        [-121.8,37.7]
    );
    var innerPolygon = r.polygon(
        [-122.3,37.4],
        [-122.3,37.6],
        [-122.0,37.6],
        [-122.0,37.4]
    );
    var query = outerPolygon.polygonSub(innerPolygon).toGeojson();
    compare(query, done);
  });

  it('intersects - 1', function(done) {
    var query = r.point(100, 20).intersects(r.point(30, 40));
    compare(query, done);
  });

  it('intersects - 2', function(done) {
    var query = r.point(100, 20).intersects(r.point(100, 20));
    compare(query, done);
  });

  it('intersects - 3', function(done) {
    var query = r.point(100, 20).intersects(r.polygon(
      [-122,37],
      [-122,38],
      [-121,38],
      [-121,37]
    ));
    compare(query, done);
  });
  it('intersects - 4', function(done) {
    var query = r.point(-121.5, 37.5).intersects(r.polygon(
      [-122,37],
      [-122,38],
      [-121,38],
      [-121,37]
    ));
    compare(query, done);
  });

  it('intersects - 5', function(done) {
    var query = r.point(20, 30).intersects(r.line(
      r.point(10, 30),
      r.point(30, 30)
    ));
    compare(query, done);
  });

  it('intersects - 6', function(done) {
    var query = r.point(10, 30).intersects(r.line(
      r.point(10, 30),
      r.point(30, 30)
    ));
    compare(query, done);
  });

  it('intersects - 7', function(done) {
    var query = r.point(30, 30).intersects(r.line(
      r.point(10, 30),
      r.point(30, 30)
    ));
    compare(query, done);
  });

  it('intersects - 8', function(done) {
    var query = r.point(10, 30).intersects(r.circle(
      r.point(30, 30),
      100
    ));
    compare(query, done);
  });

  it('intersects - 9', function(done) {
    var query = r.point(10, 30).intersects(r.circle(
      r.point(10.3, 30),
      100000
    ));
    compare(query, done);
  });

  it('intersects - 10', function(done) {
    var query = r.polygon(
      [-122,37],
      [-122,38],
      [-121,38],
      [-121,37]
    ).intersects(r.polygon(
      [-131,40],
      [-131,41],
      [-130,41],
      [-130,40]
    ));
    compare(query, done);
  });

  it('intersects - 11', function(done) {
    var query = r.polygon(
      [-131,40],
      [-131,41],
      [-130,41],
      [-121.5,37.5]
  ).intersects(r.polygon(
      [-122,37],
      [-122,38],
      [-121,38],
      [-121,37]
    ));
    compare(query, done);
  });

  it('intersects - 12', function(done) {
    var query = r.expr([
      r.polygon(
        [-131,40],
        [-131,41],
        [-130,41],
        [-121.5,37.5]
      ),
      r.polygon(
        [-131,40],
        [-131,41],
        [-130,41],
        [-130,40]
      )
    ]).intersects(r.polygon(
      [-122,37],
      [-122,38],
      [-121,38],
      [-121,37]
    ));
    compare(query, done);
  });

  it('intersects - 13', function(done) {
    var query = r.expr([
        'foo'
    ]).intersects(r.polygon(
        [-122,37],
        [-122,38],
        [-121,38],
        [-121,37]
      ));
    compare(query, done);
  });

  it('intersects - 14', function(done) {
    var query = r.expr('foo').intersects(r.polygon(
        [-122,37],
        [-122,38],
        [-121,38],
        [-121,37]
      ));
    compare(query, done);
  });

  it('intersects - 14', function(done) {
    var query = r.polygon(
        [-122,37],
        [-122,38],
        [-121,38],
        [-121,37]
    ).intersects('foo');
    compare(query, done);
  });

  it('intersects - 15', function(done) {
    var query = r.point(100, 20).intersects(r.point(30, 40), r.point(30, 40));
    compare(query, done);
  });

  it('intersects - 16', function(done) {
    var query = r.point(100, 20).intersects('foo');
    compare(query, done);
  });

  it('intersects - 17', function(done) {
    var query = r.expr(100).intersects(r.point(30, 40));
    compare(query, done);
  });

  it('includes - 1', function(done) {
    var query = r.point(100, 20).includes(r.point(30, 40));
    compare(query, done);
  });

  it('includes - 2', function(done) {
    var query = r.polygon(
      [-122,37],
      [-122,38],
      [-121,38],
      [-121,37]
    ).includes(r.point(-121.5, 37.5));
    compare(query, done);
  });


  it('includes - 3', function(done) {
    var query = r.polygon(
      [-122,37],
      [-122,38],
      [-121,38],
      [-121,37]
    ).includes(r.polygon(
      [-131,40],
      [-131,41],
      [-130,41],
      [-130,40]
    ));
    compare(query, done);
  });

  it('includes - 4', function(done) {
    var query = r.polygon(
      [-133,40],
      [-133,44],
      [-130,44],
      [-130,40]
  ).includes(r.polygon(
      [-132,41],
      [-132,42],
      [-131,42],
      [-131,41]
    ));
    compare(query, done);
  });

  it('includes - 5', function(done) {
    var query = r.expr([
      r.polygon(
        [-133,40],
        [-133,44],
        [-130,44],
        [-130,40]
      ),
      r.polygon(
        [-122,37],
        [-122,38],
        [-121,38],
        [-121,37]
      )
  ]).includes(r.polygon(
      [-132,41],
      [-132,42],
      [-131,42],
      [-131,41]
    ));
    compare(query, done);
  });

  it('includes - 6', function(done) {
    var query = r.polygon(
        [20,40],
        [20,80],
        [40,80],
        [40,40]
      ).polygonSub(r.polygon(
        [30,55],
        [30,60],
        [35,60],
        [35,55]
    )).includes(r.polygon(
      [25,41],
      [25,42],
      [26,42],
      [26,41]
    ));
    compare(query, done);
  });

  it('includes - 7', function(done) {
    var query = r.expr('foo').includes(r.polygon(
        [-122,37],
        [-122,38],
        [-121,38],
        [-121,37]
      ));
    compare(query, done);
  });

  it('includes - 8', function(done) {
    var query = r.expr([
        'foo'
    ]).includes(r.polygon(
        [-122,37],
        [-122,38],
        [-121,38],
        [-121,37]
      ));
    compare(query, done);
  });

  it('includes - 9', function(done) {
    var query = r.polygon(
        [-122,37],
        [-122,38],
        [-121,38],
        [-121,37]
    ).includes('foo');
    compare(query, done);
  });

  it('includes - 10', function(done) {
    var query = r.point(100, 20).includes(r.point(30, 40), r.point(30, 40));
    compare(query, done);
  });

  it('includes - 11', function(done) {
    var query = r.expr(100).includes(r.point(30, 40));
    compare(query, done);
  });

  it('includes - 12', function(done) {
    var query = r.polygon(
        [-122,37],
        [-122,38],
        [-121,38],
        [-121,37]
    ).includes('foo');
    compare(query, done);
  });

  it('getIntersecting - 1', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).getIntersecting(r.circle([10, 10], 100), {index: 'location'}).orderBy('id');
    compare(query, done);
  });

  it('getIntersecting - 2', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).getIntersecting(r.circle([10, 10], 1000000), {index: 'location'}).orderBy('id');
    compare(query, done);
  });

  it('getIntersecting - 3', function(done) {
    var query = r.expr('foo').getIntersecting(r.circle([10, 10], 100), {index: 'location'}).orderBy('id');
    compare(query, done);
  });

  it('getIntersecting - 4', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).getIntersecting(r.args([r.circle([10, 10], 100), r.circle([10, 10], 100)]), {index: 'location'}).orderBy('id');
    compare(query, done, function(error) {
      return /^Expected 2 argument but found 3/.test(error);
    });
  });

  it('getIntersecting - 5', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).getIntersecting('foo', {index: 'location'}).orderBy('id');
    compare(query, done);
  });

  it('getNearest - 1', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).getNearest(r.point(10, 10), {index: 'location'}).orderBy('id');
    compare(query, done);
  });

  it('getNearest - 2', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).getNearest(r.point(10, 10), {index: 'location', maxDist: 120000}).orderBy(r.row);
    compare(query, done, function(result) {
      for(var i=0; i<result.length; i++) {
        result[i].dist = Math.floor(result[i].dist);
      }
      return result;
    });
  });

  it('getNearest - 3', function(done) {
    var query = r.expr('foo').getNearest(r.point(10, 10), {index: 'location'}).orderBy('id');
    compare(query, done);
  });

  it('getNearest - 1', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).getNearest(r.args([10, 10]), {index: 'location'}).orderBy('id');
    compare(query, done, function(error) {
      return /^Expected 2 arguments but found 3/.test(error);
    });
  });

  it('getNearest - 1', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).getNearest('bar', {index: 'location'}).orderBy('id');
    compare(query, done);
  });

  /*
  //TODO
  it('polygonSub - 3', function(done) {
    var outerPolygon = r.polygon(
        [-122.4,37.7],
        [-122.4,37.3],
        [-121.8,37.3],
        [-121.8,37.7]
    );
    var innerPolygon = r.polygon(
        [-122.3,37.4],
        [-122.3,37.6],
        [-122.0,37.6],
        [-122.0,37.4]
    );
    var otherPolygon = r.polygon(
        [-122.30,37.5],
        [-122.30,37.51],
        [-122.31,37.51],
        [-122.31,37.5]
    );
    var query = outerPolygon.polygonSub(innerPolygon.polygonSub(otherPolygon));
    compare(query, done);
  });
  */

});
