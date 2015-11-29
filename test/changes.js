var config = require('./../config.js');

//var r = require('rethinkdb');
//TODO Revert to the official driver once they bix #4240 and #4240
var r = require('rethinkdbdash')({
  pool: false,
  cursor: true
});
var assert = require('assert');

var connections = {};
var mainConnection;
var TEST_DB = 'reqlitetest';
var TEST_TABLE = 'reqlitetestchanges';

var compare = require('./util.js').generateCompare(connections);

describe('changes.js', function(){
  //NOTE: The order of the tests matters! Append only!
  before(function(done) {
    setTimeout(function() { // Delay for nodemon to restart the server
      r.connect(config.rethinkdb).bind({}).then(function(conn) {
        connections.rethinkdb = conn;
        // Use the rethinkdb connection to validate the tests
        mainConnection = conn;
        return r.connect(config.reqlite);
      }).then(function(conn) {
        connections.reqlite = conn;
        // Comment the next line to run the tests on RethinkDB
        // They should all pass...
        mainConnection = conn;
        this.query = r.dbCreate(TEST_DB);
        return this.query.run(connections.reqlite);
      }).catch(function() { // ignore errors
      }).finally(function() {
        return this.query.run(connections.rethinkdb);
      }).catch(function() { // ignore errors
      }).finally(function() {
        this.query = r.db(TEST_DB).tableDrop(TEST_TABLE);
        return this.query.run(connections.reqlite);
      }).catch(function() { // ignore errors
      }).finally(function() {
        return this.query.run(connections.rethinkdb);
      }).catch(function() { // ignore errors
      }).finally(function() {
        this.query = r.db(TEST_DB).tableCreate(TEST_TABLE);
        return this.query.run(connections.reqlite);
      }).catch(function() { // ignore errors
      }).finally(function() {
        return this.query.run(connections.rethinkdb);
      }).catch(function() { // ignore errors
      }).finally(function() {
        this.query = r.db(TEST_DB).table(TEST_TABLE).insert([
            {id: 1, foo: 10, redundant: 100, optional: 1000},
            {id: 2, foo: 20, redundant: 200, optional: 2000},
            {id: 3, foo: 30, redundant: 100, optional: 3000},
            {id: 4, foo: 40, redundant: 200},
            {id: 5, foo: 50, redundant: 100, optional: 5000}
        ]);
        return this.query.run(connections.reqlite);
      }).catch(function() { // ignore errors
      }).finally(function() {
        return this.query.run(connections.rethinkdb);
      }).catch(function() { // ignore errors
      }).finally(function() {
        this.query = r.db(TEST_DB).table(TEST_TABLE).indexCreate('foo');
        return this.query.run(mainConnection);
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

  it('changes - 1', function(done) {
    var query = r.expr(1).changes();
    compare(query, done);
  });

  it('changes - 2', function(done) {
    var doc = {id: 6, foo: 60, redundant: 200};
    var query = r.db(TEST_DB).table(TEST_TABLE).changes();
    query.run(mainConnection).then(function(feed) {
      feed.next().then(function(change) {
        assert.deepEqual(change, {new_val: doc, old_val: null});
        return feed.close();
      }).then(function() {
        done();
      }).catch(done);

      r.db(TEST_DB).table(TEST_TABLE).insert(doc).run(mainConnection).then(function(result) {
      }).catch(done);
    }).catch(done);
  });

  it('changes - 3', function(done) {
    var doc = {id: 6, foo: 1, bar: 'buzz'};
    var query = r.db(TEST_DB).table(TEST_TABLE).changes();
    query.run(mainConnection).then(function(feed) {
      feed.next().then(function(change) {
        assert.deepEqual(change, {new_val: doc, old_val: {id: 6, foo: 60, redundant: 200}});
        return feed.close();
      }).then(function() {
        done();
      }).catch(done);

      r.db(TEST_DB).table(TEST_TABLE).insert(doc, {conflict: 'replace'}).run(mainConnection).then(function(result) {
      }).catch(done);
    }).catch(done);
  });

  it('changes - 4', function(done) {
    var doc = {id: 6, foo: 60, redundant: 200};
    var query = r.db(TEST_DB).table(TEST_TABLE).changes();
    query.run(mainConnection).then(function(feed) {
      feed.next().then(function(change) {
        assert.deepEqual(change, {new_val: {id: 6, foo: 60, redundant: 200, bar: 'buzz'}, old_val: {id: 6, foo: 1, bar: 'buzz'}});
        return feed.close();
      }).then(function() {
        done();
      }).catch(done);

      r.db(TEST_DB).table(TEST_TABLE).insert(doc, {conflict: 'update'}).run(mainConnection).then(function(result) {
      }).catch(done);
    }).catch(done);
  });

  it('changes - 5', function(done) {
    var doc = {id: 6, foo: 60, redundant: 200};
    var query = r.db(TEST_DB).table(TEST_TABLE).changes();
    query.run(mainConnection).then(function(feed) {
      feed.next().then(function(change) {
        assert.deepEqual(change, {
          new_val: {id: 6, foo: 60, redundant: 200, bar: 'buzz', hello: 'world'},
          old_val: {id: 6, foo: 60, redundant: 200, bar: 'buzz'}
        });
        return feed.close();
      }).then(function() {
        done();
      }).catch(done);

      r.db(TEST_DB).table(TEST_TABLE).get(6).update({hello: 'world'}).run(mainConnection).then(function(result) {
      }).catch(done);
    }).catch(done);
  });

  it('changes - 6', function(done) {
    var doc = {id: 6, foo: 60, redundant: 200};
    var query = r.db(TEST_DB).table(TEST_TABLE).changes();
    query.run(mainConnection).then(function(feed) {
      feed.next().then(function(change) {
        assert.deepEqual(change, {
          new_val: {id: 6, foo: 60, redundant: 600},
          old_val: {id: 6, foo: 60, redundant: 200, bar: 'buzz', hello: 'world'},
        });
        return feed.close();
      }).then(function() {
        done();
      }).catch(done);

      r.db(TEST_DB).table(TEST_TABLE).get(6).replace({id: 6, foo: 60, redundant: 600}).run(mainConnection).then(function(result) {
      }).catch(done);
    }).catch(done);
  });

  it('changes - 7', function(done) {
    var doc = {id: 6, foo: 60, redundant: 200};
    var query = r.db(TEST_DB).table(TEST_TABLE).changes();
    query.run(mainConnection).then(function(feed) {
      feed.next().then(function(change) {
        assert.deepEqual(change, {
          new_val: null,
          old_val: {id: 6, foo: 60, redundant: 600}
        });
        return feed.close();
      }).then(function() {
        done();
      }).catch(done);

      r.db(TEST_DB).table(TEST_TABLE).get(6).delete().run(mainConnection).then(function(result) {
      }).catch(done);
    }).catch(done);
  });

  it('changes - 8', function(done) {
    var doc = {id: 6, foo: 60, redundant: 200};
    var query = r.db(TEST_DB).table(TEST_TABLE).get(6).changes();
    query.run(mainConnection).then(function(feed) {
      feed.next().then(function(change) {
        assert.deepEqual(change, {
          new_val: {id: 6, foo: 60, redundant: 200},
          old_val: null
        });
        return feed.close();
      }).then(function() {
        done();
      }).catch(done);

      r.db(TEST_DB).table(TEST_TABLE).insert(doc).run(mainConnection).then(function(result) {
      }).catch(done);
    }).catch(done);
  });

  it('changes - 9', function(done) {
    var doc = {id: 6, foo: 60, redundant: 200};
    var query = r.db(TEST_DB).table(TEST_TABLE).get(6).changes();
    query.run(mainConnection).then(function(feed) {
      feed.next().then(function(change) {
        assert.deepEqual(change, {
          new_val: {id: 6, foo: 61, redundant: 200},
          old_val: {id: 6, foo: 60, redundant: 200}
        });
        return feed.close();
      }).then(function() {
        done();
      }).catch(done);

      r.db(TEST_DB).table(TEST_TABLE).get(6).update({foo: 61}).run(mainConnection).then(function(result) {
      }).catch(done);
    }).catch(done);
  });

  it('changes - 10', function(done) {
    var doc = {id: 6, foo: 60, redundant: 200};
    var query = r.db(TEST_DB).table(TEST_TABLE).get(6).changes();
    query.run(mainConnection).then(function(feed) {
      feed.next().then(function(change) {
        assert.deepEqual(change, {
          new_val: null,
          old_val: {id: 6, foo: 61, redundant: 200}
        });
        return feed.close();
      }).then(function() {
        done();
      }).catch(done);

      r.db(TEST_DB).table(TEST_TABLE).get(6).delete().run(mainConnection).then(function(result) {
      }).catch(done);
    }).catch(done);
  });


  it('changes - 11', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).between(2, 4, {index: 'id'}).changes();
    query.run(mainConnection).then(function(feed) {
      feed.next().then(function(change) {
        assert.deepEqual(change, {
          new_val: {id: 2, foo: 21, redundant: 200, optional: 2000},
          old_val: {id: 2, foo: 20, redundant: 200, optional: 2000}
        });
        return feed.next();
      }).then(function(change) {
        assert.deepEqual(change, {new_val: {id: 3.5}, old_val: null});
      }).then(function(change) {
        return feed.close();
      }).then(function() {
        return r.db(TEST_DB).table(TEST_TABLE).getAll(0, 3.5, 6).delete().run(mainConnection);
      }).then(function() {
        done();
      }).catch(done);

      // In range update
      r.db(TEST_DB).table(TEST_TABLE).get(2).update({foo: 21}).run(mainConnection).then(function(result) {
        // Out of range
        return r.db(TEST_DB).table(TEST_TABLE).insert({id: 0}).run(mainConnection);
      }).then(function() {
        // Out of range
        return r.db(TEST_DB).table(TEST_TABLE).insert({id: 6}).run(mainConnection);
      }).then(function() {
        // In range insert
        return r.db(TEST_DB).table(TEST_TABLE).insert({id: 3.5}).run(mainConnection);
      }).catch(done);
    }).catch(done);
  });

  it('changes - 12', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).orderBy({index: 'id'}).changes();
    query.run(mainConnection).then(function(feed) {
      feed.next().then(function(change) {
        assert.deepEqual(change, {
          new_val: {id: 2, foo: 22, redundant: 200, optional: 2000},
          old_val: {id: 2, foo: 21, redundant: 200, optional: 2000}
        });
        return feed.next();
      }).then(function(change) {
        assert.deepEqual(change, {new_val: {id: 6}, old_val: null});
        return feed.next();
      }).then(function(change) {
        assert.deepEqual(change, {new_val: {id: 0}, old_val: null});
        return feed.next();
      }).then(function(change) {
        assert.deepEqual(change, {new_val: {id: 3.5}, old_val: null});
        return feed.next();
      }).then(function(change) {
        assert.deepEqual(change, {old_val: {id: 3.5}, new_val: null});
        return feed.close();
      }).then(function() {
        return r.db(TEST_DB).table(TEST_TABLE).getAll(0, 3.5, 6).delete().run(mainConnection);
      }).then(function() {
        done();
      }).catch(done);

      r.db(TEST_DB).table(TEST_TABLE).get(2).update({foo: 22}).run(mainConnection).then(function(result) {
        return r.db(TEST_DB).table(TEST_TABLE).insert({id: 6}).run(mainConnection);
      }).then(function() {
        return r.db(TEST_DB).table(TEST_TABLE).insert({id: 0}).run(mainConnection);
      }).then(function() {
        return r.db(TEST_DB).table(TEST_TABLE).insert({id: 3.5}).run(mainConnection);
      }).then(function() {
        return r.db(TEST_DB).table(TEST_TABLE).get(3.5).delete().run(mainConnection);
      }).catch(done);
    }).catch(done);
  });

  it('changes - 13', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).orderBy({index: 'id'}).limit(3).changes({
      includeInitial: true
    });
    query.run(mainConnection).then(function(feed) {
      feed.next().then(function(change) {
        assert.deepEqual(change, {new_val: {id: 1, foo: 10, redundant: 100, optional: 1000}});
        return feed.next();
      }).then(function(change) {
        assert.deepEqual(change, {new_val: {id: 2, foo: 22, redundant: 200, optional: 2000}});
        return feed.next();
      }).then(function(change) {
        assert.deepEqual(change, {new_val: {id: 3, foo: 30, redundant: 100, optional: 3000}});
        return feed.next();
      }).then(function(change) {
        assert.deepEqual(change, {
          new_val: {id: 2, foo: 23, redundant: 200, optional: 2000},
          old_val: {id: 2, foo: 22, redundant: 200, optional: 2000}
        });
        return feed.next();
      }).then(function(change) {
        assert.deepEqual(change, {new_val: {id: 0}, old_val: {id: 3, foo: 30, redundant: 100, optional: 3000}});
        return feed.next();
      }).then(function(change) {
        assert.deepEqual(change, {old_val: {id: 0}, new_val: {id: 3, foo: 30, redundant: 100, optional: 3000}});
        return feed.close();
      }).then(function() {
        return r.db(TEST_DB).table(TEST_TABLE).getAll(0, 3.5, 6).delete().run(mainConnection);
      }).then(function() {
        done();
      }).catch(done);

      r.db(TEST_DB).table(TEST_TABLE).get(2).update({foo: 23}).run(mainConnection)
      r.db(TEST_DB).table(TEST_TABLE).get(2).update({foo: 23}).run(mainConnection).then(function(result) {
        return r.db(TEST_DB).table(TEST_TABLE).insert({id: 6}).run(mainConnection);
      }).then(function() {
        return r.db(TEST_DB).table(TEST_TABLE).insert({id: 0}).run(mainConnection);
      }).then(function() {
        return r.db(TEST_DB).table(TEST_TABLE).insert({id: 3.5}).run(mainConnection);
      }).then(function() {
        return r.db(TEST_DB).table(TEST_TABLE).get(0).delete().run(mainConnection);
      }).then(function() {
        return r.db(TEST_DB).table(TEST_TABLE).get(3.5).delete().run(mainConnection);
      }).catch(done);
    }).catch(done);
  });

  it('changes - 14', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).orderBy({index: 'id'}).limit(8).changes({
      includeInitial: true
    });
    query.run(mainConnection).then(function(feed) {
      feed.next().then(function(change) {
        assert.deepEqual(change, {new_val: {id: 1, foo: 10, redundant: 100, optional: 1000}});
        return feed.next();
      }).then(function(change) {
        assert.deepEqual(change, {new_val: {id: 2, foo: 23, redundant: 200, optional: 2000}});
        return feed.next();
      }).then(function(change) {
        assert.deepEqual(change, {new_val: {id: 3, foo: 30, redundant: 100, optional: 3000}});
        return feed.next();
      }).then(function(change) {
        assert.deepEqual(change, {new_val: {id: 4, foo: 40, redundant: 200}});
        return feed.next();
      }).then(function(change) {
        assert.deepEqual(change, {new_val: {id: 5, foo: 50, redundant: 100, optional: 5000}});
        return feed.next();
      }).then(function(change) {
        assert.deepEqual(change, {
          new_val: {id: 2, foo: 24, redundant: 200, optional: 2000},
          old_val: {id: 2, foo: 23, redundant: 200, optional: 2000}
        });
        return feed.next();
      }).then(function(change) {
        assert.deepEqual(change, {new_val: {id: 6}, old_val: null});
        return feed.next();
      }).then(function(change) {
        assert.deepEqual(change, {new_val: {id: 0}, old_val: null});
        return feed.next();
      }).then(function(change) {
        assert.deepEqual(change, {new_val: {id: -1}, old_val: null});
        return feed.next();
      }).then(function(change) {
        assert.deepEqual(change, {new_val: {id: -2}, old_val: {id: 6}});
        return feed.next();
      }).then(function(change) {
        assert.deepEqual(change, {new_val: {id: -3}, old_val: {id: 5, foo: 50, redundant: 100, optional: 5000}});
        return feed.next();
      }).then(function(change) {
        assert.deepEqual(change, {new_val: {id: 3.5}, old_val: {id: 4, foo: 40, redundant: 200}});
        return feed.next();
      }).then(function(change) {
        assert.deepEqual(change, {old_val: {id: 0}, new_val: {id: 4, foo: 40, redundant: 200}});
        return feed.next();
      }).then(function(change) {
        assert.deepEqual(change, {old_val: {id: 3.5}, new_val: {id: 5, foo: 50, redundant: 100, optional: 5000}});
        return feed.close();
      }).then(function() {
        return r.db(TEST_DB).table(TEST_TABLE).getAll(0, 3.5, 6, -1, -2, -3).delete().run(mainConnection);
      }).then(function() {
        done();
      }).catch(done);

      r.db(TEST_DB).table(TEST_TABLE).get(2).update({foo: 24}).run(mainConnection).then(function(result) {
        return r.db(TEST_DB).table(TEST_TABLE).insert({id: 6}).run(mainConnection);
      }).then(function() {
        return r.db(TEST_DB).table(TEST_TABLE).insert({id: 0}).run(mainConnection);
      }).then(function() {
        return r.db(TEST_DB).table(TEST_TABLE).insert({id: -1}).run(mainConnection);
      }).then(function() {
        return r.db(TEST_DB).table(TEST_TABLE).insert({id: -2}).run(mainConnection);
      }).then(function() {
        return r.db(TEST_DB).table(TEST_TABLE).insert({id: -3}).run(mainConnection);
      }).then(function() {
        return r.db(TEST_DB).table(TEST_TABLE).insert({id: 3.5}).run(mainConnection);
      }).then(function() {
        return r.db(TEST_DB).table(TEST_TABLE).get(0).delete().run(mainConnection);
      }).then(function() {
        return r.db(TEST_DB).table(TEST_TABLE).get(3.5).delete().run(mainConnection);
      }).catch(done);
    }).catch(done);
  });

  // Base for other tests
  it('changes - 15', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).changes();
    query.run(mainConnection).then(function(feed) {
      feed.next().then(function(change) {
        assert.deepEqual(change, {new_val: {id: 0}, old_val: null});
        return feed.next();
      }).then(function(change) {
        assert.deepEqual(change, {new_val: {id: 0, foo: 'bar'}, old_val: {id: 0}});
        return feed.next();
      }).then(function(change) {
        assert.deepEqual(change, {new_val: null, old_val: {id: 0, foo: 'bar'}});
        return feed.close();
      }).then(function() {
        done();
      }).catch(done);

      r.db(TEST_DB).table(TEST_TABLE).insert({id: 0}).run(mainConnection).then(function() {
        return r.db(TEST_DB).table(TEST_TABLE).get(0).update({foo: 'bar'}).run(mainConnection);
      }).then(function() {
        return r.db(TEST_DB).table(TEST_TABLE).get(0).delete().run(mainConnection);
      }).catch(done);
    }).catch(done);
  });

  it('changes - 16', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).changes().sum();
    compare(query, done);
  });

  it('changes - 17', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).changes().avg();
    compare(query, done);
  });

  it('changes - 18', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).changes().min();
    compare(query, done);
  });

  it('changes - 19', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).changes().max();
    compare(query, done);
  });

  it('changes - 20', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).changes().group('id');
    compare(query, done);
  });

  it('changes - 21', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).changes().insertAt(2, 1);
    compare(query, done);
  });

  it('changes - 22', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).changes().spliceAt(2, 1);
    compare(query, done);
  });

  it('changes - 23', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).changes().changeAt(2, 1);
    compare(query, done);
  });

  it('changes - 24', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).changes().deleteAt(2, 1);
    compare(query, done);
  });

  it('changes - 25', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).changes().distinct();
    compare(query, done);
  });

  it('changes - 26', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).changes().reduce(function(left, right) {
      return left.add(right);
    });
    compare(query, done);
  });

  it('changes - 27', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).changes().setInsert([1,2,3]);
    compare(query, done);
  });

  it('changes - 28', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).changes().contains(1);
    compare(query, done);
  });

  it('changes - 29', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).changes().setIntersection([1,2,3]);
    compare(query, done);
  });

  it('changes - 30', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).changes().setDifference([1,2,3]);
    compare(query, done);
  });

  it('changes - 31', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).changes().setUnion([1,2,3]);
    compare(query, done);
  });

  it('changes - 32', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).changes().sample(2);
    compare(query, done);
  });

  it('changes - 33', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).changes().count();
    compare(query, done);
  });

  it('changes - 34', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).changes().nth(2);
    compare(query, done);
  });

  it('changes - 35', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).changes().orderBy('id');
    compare(query, done);
  });

  it('changes - 36', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).changes().filter({new_val: {id: 0}});
    query.run(mainConnection).then(function(feed) {
      feed.next().then(function(change) {
        assert.deepEqual(change, {new_val: {id: 0}, old_val: null});
        return feed.next();
      }).then(function(change) {
        assert.deepEqual(change, {new_val: {id: 0, foo: 'bar'}, old_val: {id: 0}});
        return feed.next();
      }).then(function(change) {
        assert.deepEqual(change, {new_val: {id: 0}, old_val: null});
        return feed.close();
      }).catch(done);

      r.db(TEST_DB).table(TEST_TABLE).insert({id: 0}).run(mainConnection).then(function() {
        return r.db(TEST_DB).table(TEST_TABLE).get(0).update({foo: 'bar'}).run(mainConnection);
      }).then(function() {
        return r.db(TEST_DB).table(TEST_TABLE).get(0).delete().run(mainConnection);
      }).then(function() {
        return r.db(TEST_DB).table(TEST_TABLE).insert({id: 0}).run(mainConnection);
      }).then(function() {
        return r.db(TEST_DB).table(TEST_TABLE).get(0).delete().run(mainConnection);
      }).then(function() {
        done();
      }).catch(done);
    }).catch(done);
  });

  it('changes - 37', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).changes().difference([{new_val: {id: 0, foo: 'bar'}, old_val: {id: 0}}]);
    query.run(mainConnection).then(function(feed) {
      feed.next().then(function(change) {
        assert.deepEqual(change, {new_val: {id: 0}, old_val: null});
        return feed.next();
      }).then(function(change) {
        assert.deepEqual(change, {new_val: null, old_val: {id: 0, foo: 'bar'}});
        return feed.close();
      }).then(function() {
        done();
      }).catch(done);

      r.db(TEST_DB).table(TEST_TABLE).insert({id: 0}).run(mainConnection).then(function() {
        return r.db(TEST_DB).table(TEST_TABLE).get(0).update({foo: 'bar'}).run(mainConnection);
      }).then(function() {
        return r.db(TEST_DB).table(TEST_TABLE).get(0).delete().run(mainConnection);
      }).catch(done);
    }).catch(done);
  });

  it('changes - 38', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).changes().zip();
    query.run(mainConnection).then(function(feed) {
      feed.next().then(function(result) {
        done(new Error('Unexpected result'));
      }).error(function(error) {
        assert.equal(error.message.split(':')[0], 'ZIP can only be called on the result of a join in');
        return r.db(TEST_DB).table(TEST_TABLE).get(0).delete().run(mainConnection);
      }).then(function(result) {
        done();
      });

      return r.db(TEST_DB).table(TEST_TABLE).insert({id: 0}).run(mainConnection);
    }).catch(done);
  });


  it('changes - 39', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).changes().skip(2);
    query.run(mainConnection).then(function(feed) {
      feed.next().then(function(result) {
        assert.deepEqual(result, {new_val: {id: 12}, old_val: null});
        return feed.close();
      }).then(function() {
        return r.db(TEST_DB).table(TEST_TABLE).getAll(10, 11, 12).delete().run(mainConnection);
      }).then(function() {
        done();
      });

      return r.db(TEST_DB).table(TEST_TABLE).insert({id: 10}).run(mainConnection);
    }).then(function() {
      return r.db(TEST_DB).table(TEST_TABLE).insert({id: 11}).run(mainConnection);
    }).then(function() {
      return r.db(TEST_DB).table(TEST_TABLE).insert({id: 12}).run(mainConnection);
    }).catch(done);
  });

  it('changes - 40', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).changes().limit(2);
    query.run(mainConnection).then(function(feed) {
      feed.next().then(function(result) {
        assert.deepEqual(result, {new_val: {id: 10}, old_val: null});
        return feed.next();
      }).then(function(result) {
        assert.deepEqual(result, {new_val: {id: 11}, old_val: null});
        return feed.next();
      }).error(function(error) {
        assert(error.message, 'No more rows in the Feed.');

        r.db(TEST_DB).table(TEST_TABLE).getAll(10, 11, 12).delete().run(mainConnection).then(function(result) {
          done();
        }).catch(done);
      });

      return r.db(TEST_DB).table(TEST_TABLE).insert({id: 10}).run(mainConnection);
    }).then(function() {
      return r.db(TEST_DB).table(TEST_TABLE).insert({id: 11}).run(mainConnection);
    }).then(function() {
      return r.db(TEST_DB).table(TEST_TABLE).insert({id: 12}).run(mainConnection);
    }).catch(done);
  });

  it('changes - 41', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).changes().slice(1, 2);
    var isDone = false;
    query.run(mainConnection).then(function(feed) {
      feed.next().then(function(result) {
        assert.deepEqual(result, {new_val: {id: 11}, old_val: null});
        return feed.next();
      }).error(function(error) {
        assert(error.message, 'No more rows in the Feed.');
        r.db(TEST_DB).table(TEST_TABLE).getAll(10, 11, 12).delete().run(mainConnection).then(function() {
          if (isDone) {
            done();
          }
          isDone = true;
        }).catch(done);
      }).catch(done);

      return r.db(TEST_DB).table(TEST_TABLE).insert({id: 10}).run(mainConnection);
    }).then(function() {
      return r.db(TEST_DB).table(TEST_TABLE).insert({id: 11}).run(mainConnection);
    }).then(function() {
      return r.db(TEST_DB).table(TEST_TABLE).insert({id: 12}).run(mainConnection);
    }).then(function() {
      if (isDone) {
        r.db(TEST_DB).table(TEST_TABLE).getAll(10, 11, 12).delete().run(mainConnection).then(function() {
          done();
        });
      }
      isDone = true;
    }).catch(done);
  });

  it('changes - 42', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).changes().slice(1, 2, {leftBound: 'open'});
    query.run(mainConnection).then(function(feed) {
      feed.next().then(function(result) {
        done(new Error('Unexpected error'));
      }).error(function(error) {
        assert(error.message, 'No more rows in the Feed.');
        done();
      }).catch(done);
    }).catch(done);
  });

  it('changes - 43', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).changes().offsetsOf({new_val: {id: 10}, old_val: null});
    query.run(mainConnection).then(function(feed) {
      feed.next().then(function(result) {
        assert.deepEqual(result, 0);
        return feed.next();
      }).then(function(result) {
        assert.deepEqual(result, 4);
        return feed.close();
      }).then(function() {
        return r.db(TEST_DB).table(TEST_TABLE).getAll(10, 11, 12).delete().run(mainConnection);
      }).then(function() {
        done();
      }).catch(done);

      return r.db(TEST_DB).table(TEST_TABLE).insert({id: 10}).run(mainConnection);
    }).then(function() {
      return r.db(TEST_DB).table(TEST_TABLE).insert({id: 11}).run(mainConnection);
    }).then(function() {
      return r.db(TEST_DB).table(TEST_TABLE).insert({id: 12}).run(mainConnection);
    }).then(function() {
      return r.db(TEST_DB).table(TEST_TABLE).get(10).delete().run(mainConnection);
    }).then(function() {
      return r.db(TEST_DB).table(TEST_TABLE).insert({id: 10}).run(mainConnection);
    }).catch(done);
  });

  it('changes - 44', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).changes().concatMap(function(doc) {
      return [doc, doc];
    });
    query.run(mainConnection).then(function(feed) {
      feed.next().then(function(result) {
        assert.deepEqual(result, {new_val: {id: 10}, old_val: null});
        return feed.next();
      }).then(function(result) {
        assert.deepEqual(result, {new_val: {id: 10}, old_val: null});
        return feed.next();
      }).then(function(result) {
        assert.deepEqual(result, {new_val: {id: 11}, old_val: null});
        return feed.next();
      }).then(function(result) {
        assert.deepEqual(result, {new_val: {id: 11}, old_val: null});
        return feed.close();
      }).then(function() {
        return r.db(TEST_DB).table(TEST_TABLE).getAll(10, 11).delete().run(mainConnection);
      }).then(function() {
        done();
      }).catch(done);

      r.db(TEST_DB).table(TEST_TABLE).insert({id: 10}).run(mainConnection).then(function() {
        return r.db(TEST_DB).table(TEST_TABLE).insert({id: 11}).run(mainConnection);
      });
    }).catch(done);
  });

  it('changes - 45', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).changes().hasFields('new_val');
    query.run(mainConnection).then(function(feed) {
      feed.next().then(function(result) {
        assert.deepEqual(result, {new_val: {id: 10}, old_val: null});
        return feed.next();
      }).then(function(result) {
        assert.deepEqual(result, {new_val: {id: 11}, old_val: null});
        return feed.close();
      }).then(function() {
        return r.db(TEST_DB).table(TEST_TABLE).getAll(10, 11).delete().run(mainConnection);
      }).then(function() {
        done();
      }).catch(done);

    }).then(function() {
      return r.db(TEST_DB).table(TEST_TABLE).insert({id: 10}).run(mainConnection);
    }).then(function() {
      return r.db(TEST_DB).table(TEST_TABLE).get(10).delete().run(mainConnection);
    }).then(function() {
      return r.db(TEST_DB).table(TEST_TABLE).insert({id: 11}).run(mainConnection);
    }).catch(done);
  });

  it('changes - 46', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).changes().merge({foo: 'bar'});
    query.run(mainConnection).then(function(feed) {
      feed.next().then(function(result) {
        assert.deepEqual(result, {foo: 'bar', new_val: {id: 10}, old_val: null});
        return feed.next();
      }).then(function(result) {
        assert.deepEqual(result, {foo: 'bar', new_val: {id: 11}, old_val: null});
        return feed.close();
      }).then(function() {
        return r.db(TEST_DB).table(TEST_TABLE).getAll(10, 11).delete().run(mainConnection);
      }).then(function() {
        done();
      }).catch(done);

    }).then(function() {
      return r.db(TEST_DB).table(TEST_TABLE).insert({id: 10}).run(mainConnection);
    }).then(function() {
      return r.db(TEST_DB).table(TEST_TABLE).insert({id: 11}).run(mainConnection);
    }).catch(done);
  });

  it('changes - 47', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).changes().pluck('new_val');
    query.run(mainConnection).then(function(feed) {
      feed.next().then(function(result) {
        assert.deepEqual(result, {new_val: {id: 10}});
        return feed.next();
      }).then(function(result) {
        assert.deepEqual(result, {new_val: null});
        return feed.next();
      }).then(function(result) {
        assert.deepEqual(result, {new_val: {id: 11}});
        return feed.close();
      }).then(function() {
        return r.db(TEST_DB).table(TEST_TABLE).getAll(10, 11).delete().run(mainConnection);
      }).then(function() {
        done();
      }).catch(done);

    }).then(function() {
      return r.db(TEST_DB).table(TEST_TABLE).insert({id: 10}).run(mainConnection);
    }).then(function() {
      return r.db(TEST_DB).table(TEST_TABLE).get(10).delete().run(mainConnection);
    }).then(function() {
      return r.db(TEST_DB).table(TEST_TABLE).insert({id: 11}).run(mainConnection);
    }).catch(done);
  });

  it('changes - 48', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).changes().without('old_val');
    query.run(mainConnection).then(function(feed) {
      feed.next().then(function(result) {
        assert.deepEqual(result, {new_val: {id: 10}});
        return feed.next();
      }).then(function(result) {
        assert.deepEqual(result, {new_val: null});
        return feed.next();
      }).then(function(result) {
        assert.deepEqual(result, {new_val: {id: 11}});
        return feed.close();
      }).then(function() {
        return r.db(TEST_DB).table(TEST_TABLE).getAll(10, 11).delete().run(mainConnection);
      }).then(function() {
        done();
      }).catch(done);

    }).then(function() {
      return r.db(TEST_DB).table(TEST_TABLE).insert({id: 10}).run(mainConnection);
    }).then(function() {
      return r.db(TEST_DB).table(TEST_TABLE).get(10).delete().run(mainConnection);
    }).then(function() {
      return r.db(TEST_DB).table(TEST_TABLE).insert({id: 11}).run(mainConnection);
    }).catch(done);
  });

  it('changes - 49', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).changes().withFields('new_val');
    query.run(mainConnection).then(function(feed) {
      feed.next().then(function(result) {
        assert.deepEqual(result, {new_val: {id: 10}});
        return feed.next();
      }).then(function(result) {
        assert.deepEqual(result, {new_val: {id: 11}});
        return feed.close();
      }).then(function() {
        return r.db(TEST_DB).table(TEST_TABLE).getAll(10, 11).delete().run(mainConnection);
      }).then(function() {
        done();
      }).catch(done);

    }).then(function() {
      return r.db(TEST_DB).table(TEST_TABLE).insert({id: 10}).run(mainConnection);
    }).then(function() {
      return r.db(TEST_DB).table(TEST_TABLE).get(10).delete().run(mainConnection);
    }).then(function() {
      return r.db(TEST_DB).table(TEST_TABLE).insert({id: 11}).run(mainConnection);
    }).catch(done);
  });

  it('changes - 50', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).changes().getField('new_val');
    query.run(mainConnection).then(function(feed) {
      feed.next().then(function(result) {
        assert.deepEqual(result, {id: 10});
        return feed.next();
      }).then(function(result) {
        assert.deepEqual(result, null);
        return feed.next();
      }).then(function(result) {
        assert.deepEqual(result, {id: 11});
        return feed.close();
      }).then(function() {
        return r.db(TEST_DB).table(TEST_TABLE).getAll(10, 11).delete().run(mainConnection);
      }).then(function() {
        done();
      }).catch(done);

    }).then(function() {
      return r.db(TEST_DB).table(TEST_TABLE).insert({id: 10}).run(mainConnection);
    }).then(function() {
      return r.db(TEST_DB).table(TEST_TABLE).get(10).delete().run(mainConnection);
    }).then(function() {
      return r.db(TEST_DB).table(TEST_TABLE).insert({id: 11}).run(mainConnection);
    }).catch(done);
  });

  it('changes - 51', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).changes()
      .getField('new_val').getField('location')
      .intersects(r.circle(r.point(30, 20), 1000));

    query.run(mainConnection).then(function(feed) {
      feed.next().then(function(result) {
        assert.deepEqual(result, {$reql_type$: "GEOMETRY", coordinates: [30, 20], type: 'Point'});
        return feed.next();
      }).then(function(result) {
        assert.deepEqual(result, {$reql_type$: "GEOMETRY", coordinates: [30.0001, 20.0001], type: 'Point'});
        return feed.close();
      }).then(function() {
        return r.db(TEST_DB).table(TEST_TABLE).getAll(10, 11, 12).delete().run(mainConnection);
      }).then(function() {
        done();
      }).catch(done);

    }).then(function() {
      return r.db(TEST_DB).table(TEST_TABLE).insert({id: 10, location: r.point(30, 20)}).run(mainConnection);
    }).then(function() {
      return r.db(TEST_DB).table(TEST_TABLE).insert({id: 11, location: r.point(0, 0)}).run(mainConnection);
    }).then(function() {
      return r.db(TEST_DB).table(TEST_TABLE).insert({id: 12, location: r.point(30.0001, 20.0001)}).run(mainConnection);
    }).catch(done);
  });

  it('changes - 52', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).changes()
      .getField('new_val').getField('location')
      .includes(r.point(30, 20));

    query.run(mainConnection).then(function(feed) {
      feed.next().then(function(result) {
        assert.equal(result.$reql_type$, "GEOMETRY");
        assert.deepEqual(result.coordinates[0][0][0], 30); // Just checking that we have the appropriate document
        return feed.next();
      }).then(function(result) {
        assert.equal(result.$reql_type$, "GEOMETRY");
        assert.deepEqual(result.coordinates[0][0][0], 30.0001);
        return feed.close();
      }).then(function() {
        return r.db(TEST_DB).table(TEST_TABLE).getAll(10, 11).delete().run(mainConnection);
      }).then(function() {
        done();
      }).catch(done);

    }).then(function() {
      return r.db(TEST_DB).table(TEST_TABLE).insert({id: 10, location: r.circle(r.point(30, 20), 1000)}).run(mainConnection);
    }).then(function() {
      return r.db(TEST_DB).table(TEST_TABLE).insert({id: 11, location: r.circle(r.point(0, 0), 1000)}).run(mainConnection);
    }).then(function() {
      return r.db(TEST_DB).table(TEST_TABLE).insert({id: 12, location: r.circle(r.point(30.0001, 20.0001), 1000)}).run(mainConnection);
    }).catch(done);
  });

  it('changes - 53', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).changes()('new_val');
    query.run(mainConnection).then(function(feed) {
      feed.next().then(function(result) {
        assert.deepEqual(result, {id: 10});
        return feed.next();
      }).then(function(result) {
        assert.deepEqual(result, null);
        return feed.next();
      }).then(function(result) {
        assert.deepEqual(result, {id: 11});
        return feed.close();
      }).then(function() {
        return r.db(TEST_DB).table(TEST_TABLE).getAll(10, 11).delete().run(mainConnection);
      }).then(function() {
        done();
      }).catch(done);

    }).then(function() {
      return r.db(TEST_DB).table(TEST_TABLE).insert({id: 10}).run(mainConnection);
    }).then(function() {
      return r.db(TEST_DB).table(TEST_TABLE).get(10).delete().run(mainConnection);
    }).then(function() {
      return r.db(TEST_DB).table(TEST_TABLE).insert({id: 11}).run(mainConnection);
    }).catch(done);
  });

  it('changes - 54', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).changes().forEach(function(change) {
      return {};
    });
    compare(query, done);
  });

  it('changes - 55', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).changes().innerJoin(
      [{id: 10, foo: 'bar'}, {id: 11, foo: 'buzz'}],
      function(change, right) {
      return change('new_val')('id').eq(right('id')).default(false);
    });
    query.run(mainConnection).then(function(feed) {
      feed.next().then(function(result) {
        assert.deepEqual(result, {left: {new_val: {id: 10}, old_val: null}, right: {id: 10, foo: 'bar'}});
        return feed.next();
      }).then(function(result) {
        assert.deepEqual(result, {left: {new_val: {id: 11}, old_val: null}, right: {id: 11, foo: 'buzz'}});
        return feed.close();
      }).then(function() {
        return r.db(TEST_DB).table(TEST_TABLE).getAll(10, 11).delete().run(mainConnection);
      }).then(function() {
        done();
      }).catch(done);

    }).then(function() {
      return r.db(TEST_DB).table(TEST_TABLE).insert({id: 10}).run(mainConnection);
    }).then(function() {
      return r.db(TEST_DB).table(TEST_TABLE).insert({id: 11}).run(mainConnection);
    }).catch(done);
  });

  it('changes - 56', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).changes().outerJoin(
      [{id: 10, foo: 'bar'}, {id: 11, foo: 'buzz'}],
      function(change, right) {
      return change('new_val')('id').eq(right('id')).default(false);
    });
    query.run(mainConnection).then(function(feed) {
      feed.next().then(function(result) {
        assert.deepEqual(result, {left: {new_val: {id: 10}, old_val: null}, right: {id: 10, foo: 'bar'}});
        return feed.next();
      }).then(function(result) {
        assert.deepEqual(result, {left: {new_val: {id: 11}, old_val: null}, right: {id: 11, foo: 'buzz'}});
        return feed.close();
      }).then(function() {
        return r.db(TEST_DB).table(TEST_TABLE).getAll(10, 11).delete().run(mainConnection);
      }).then(function() {
        done();
      }).catch(done);

    }).then(function() {
      return r.db(TEST_DB).table(TEST_TABLE).insert({id: 10}).run(mainConnection);
    }).then(function() {
      return r.db(TEST_DB).table(TEST_TABLE).insert({id: 11}).run(mainConnection);
    }).catch(done);
  });

  it('changes - 57', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).changes().eqJoin(
        r.row('new_val')('id'),
        r.db(TEST_DB).table(TEST_TABLE)
    );
    query.run(mainConnection).then(function(feed) {
      feed.next().then(function(result) {
        assert.deepEqual(result, {left: {new_val: {id: 10}, old_val: null}, right: {id: 10}});
        return feed.next();
      }).then(function(result) {
        assert.deepEqual(result, {left: {new_val: {id: 11}, old_val: null}, right: {id: 11}});
        return feed.close();
      }).then(function() {
        return r.db(TEST_DB).table(TEST_TABLE).getAll(10, 11).delete().run(mainConnection);
      }).then(function() {
        done();
      }).catch(done);

    }).then(function() {
      return r.db(TEST_DB).table(TEST_TABLE).insert({id: 10}).run(mainConnection);
    }).then(function() {
      return r.db(TEST_DB).table(TEST_TABLE).insert({id: 11}).run(mainConnection);
    }).catch(done);
  });

  it('changes - 58', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).get(1).changes({
      includeInitial: true
    })
    query.run(mainConnection).then(function(feed) {
      feed.next().then(function(change) {
        assert.deepEqual(change, {
          new_val: {
            foo: 10,
            id: 1,
            optional: 1000,
            redundant: 100
          }
        })
        return feed.next();
      }).then(function(change) {
        assert.deepEqual(change, {
          new_val: {
            foo: 10,
            id: 1,
            newField: 'foo',
            optional: 1000,
            redundant: 100
          },
          old_val: {
            foo: 10,
            id: 1,
            optional: 1000,
            redundant: 100
          }
        })
        return feed.next();
      }).then(function(change) {
        assert.deepEqual(change, {
          new_val: {
            foo: 10,
            id: 1,
            newField: 'bar',
            optional: 1000,
            redundant: 100
          },
          old_val: {
            foo: 10,
            id: 1,
            newField: 'foo',
            optional: 1000,
            redundant: 100
          }
        })
        return feed.close();
      }).then(function() {
        done();
      }).catch(done);
      return null;
    }).then(function() {
      return r.db(TEST_DB).table(TEST_TABLE).get(1).update({newField: 'foo'}).run(mainConnection);
    }).then(function() {
      return r.db(TEST_DB).table(TEST_TABLE).get(1).update({newField: 'bar'}).run(mainConnection);
    }).catch(done);
  });

  it('changes - 59', function(done) {
    var doc = {id: 600, foo: 60, redundant: 200};
    var query = r.db(TEST_DB).table(TEST_TABLE).get(600).changes({
      includeInitial: true
    });
    query.run(mainConnection).then(function(feed) {
      feed.next().then(function(change) {
        assert.deepEqual(change, { new_val: null });
        return feed.next();
      }).then(function(change) {
        assert.deepEqual(change, {
          new_val: {id: 600, foo: 60, redundant: 200},
          old_val: null
        });
        return feed.close();
      }).then(function() {
        done();
      }).catch(done);

      r.db(TEST_DB).table(TEST_TABLE).insert(doc).run(mainConnection).then(function(result) {
      }).catch(done);
    }).catch(done);
  });


  it('changes - 60', function(done) {
    var doc = {id: 600, foo: 60, redundant: 200};
    var query = r.db(TEST_DB).table(TEST_TABLE).get(600).changes({
      includeInitial: true
    });
    query.run(mainConnection).then(function(feed) {
      feed.next().then(function(change) {
        assert.deepEqual(change, {new_val: {id: 600, foo: 60, redundant: 200}});
        return feed.next();
      }).then(function(change) {
        assert.deepEqual(change, {
          new_val: {id: 600, foo: 61, redundant: 200},
          old_val: {id: 600, foo: 60, redundant: 200}
        });
        return feed.close();
      }).then(function() {
        done();
      }).catch(done);

      r.db(TEST_DB).table(TEST_TABLE).get(600).update({foo: 61}).run(mainConnection).then(function(result) {
      }).catch(done);
    }).catch(done);
  });

  it('changes - 61', function(done) {
    var doc = {id: 600, foo: 60, redundant: 200};
    var query = r.db(TEST_DB).table(TEST_TABLE).get(600).changes({
      includeInitial: true
    });
    query.run(mainConnection).then(function(feed) {
      feed.next().then(function(change) {
        assert.deepEqual(change, {new_val: {id: 600, foo: 61, redundant: 200}});
        return feed.next();
      }).then(function(change) {
        assert.deepEqual(change, {
          new_val: null,
          old_val: {id: 600, foo: 61, redundant: 200}
        });
        return feed.close();
      }).then(function() {
        done();
      }).catch(done);

      r.db(TEST_DB).table(TEST_TABLE).get(600).delete().run(mainConnection).then(function(result) {
      }).catch(done);
    }).catch(done);
  });

  /*
  */
});
