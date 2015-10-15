var config = require('./../config.js');

var r = require('rethinkdb');
var assert = require('assert');

var connections = {};
var TEST_DB = 'reqlitetest';
var TEST_TABLE = 'reqlitetesttransformation';
var MISSING_FIELD = 'nonExistingField';
var MISSING_INDEX = 'nonExistingIndex';
var COMPLEX_OBJECT = {
  id: 1,
  foo: 2,
  buzz: {
    hello: [10, 11, 12],
    world: {
      nested: 100,
      field: 200
    },
    bonjour: [
      {monde: 1000, ciao: 4000},
      {monde: 2000, ciao: 5000},
      {monde: 3000, ciao: 6000},
    ]
  },
};

var compare = require('./util.js').generateCompare(connections);

describe('transformations.js', function(){
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
          {id: 1, foo: 20, bar: [100, 101, 102], optional: 1},
          {id: 2, foo: 10, bar: [200, 201, 202], optional: 2},
          {id: 3, foo: 10, bar: [300, 301, 302], optional: 2},
          {id: 4, foo: 10, bar: [400, 401, 402]},
        ]);
        return this.query.run(connections.rethinkdb);
      }).catch(function() { // ignore errors
      }).finally(function() {
        return this.query.run(connections.reqlite);
      }).catch(function() { // ignore errors
      }).finally(function() {
        this.query = r.db(TEST_DB).table(TEST_TABLE).indexCreate('foo');
        return this.query.run(connections.rethinkdb);
      }).catch(function() { // ignore errors
      }).finally(function() {
        return this.query.run(connections.reqlite);
      }).catch(function() { // ignore errors
      }).finally(function() {
        this.query = r.db(TEST_DB).table(TEST_TABLE).indexCreate('bar');
        return this.query.run(connections.rethinkdb);
      }).catch(function() { // ignore errors
      }).finally(function() {
        return this.query.run(connections.reqlite);
      }).catch(function() { // ignore errors
      }).finally(function() {
        this.query = r.db(TEST_DB).table(TEST_TABLE).indexCreate('barmulti', r.row('bar'), {multi: true});
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
    }, 700);
  });

  it('map - 1', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).map(function(doc) {
      return 1;
    });
    compare(query, done);
  });

  it('map - 2', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).map(function(doc) {
      return doc('id');
    }).orderBy(r.row);
    compare(query, done);
  });

  it('map - 3', function(done) {
    var query = r.expr([1,2,3,4,5]).map(function(value) {
      return value.mul(10);
    }).orderBy(r.row);
    compare(query, done);
  });

  it('map - 4', function(done) {
    var sequence1 = [100, 200, 300, 400];
    var sequence2 = [10, 20, 30, 40];
    var sequence3 = [1, 2, 3, 4];
    var query = r.map(sequence1, sequence2, sequence3, function (val1, val2, val3) {
        return val1.add(val2).add(val3);
    });
    compare(query, done);
  });

  it('map - 5', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).map(function(doc) {
      return doc.merge({bar: 'lol'});
    }).orderBy(r.row('id'));
    compare(query, done);
  });

  it('map - 6', function(done) {
    var query = r.expr([1, 2, 3]).map(function(x) { return [x, x.mul(2)]; });
    compare(query, done);
  });

  it('map - 7', function(done) {
    var query = r.expr('foo').map(function(x) { return [x, x.mul(2)]; });
    compare(query, done);
  });

  it('map - 8', function(done) {
    var query = r.map([1,2,3], 'foo', function(x) { return [x, x.mul(2)]; });
    compare(query, done);
  });

  it('map - 9', function(done) {
    var query = r.map([1,2,3], [1,2,3], function(x, y) {
      return r.db(TEST_DB).table(TEST_TABLE);
    });
    compare(query, done);
  });

  it('map - 10', function(done) {
    var query = r.map([1,2,3], [1,2,3], function(x) {
      return x;
    });
    compare(query, done);
  });

  it('map - 11', function(done) {
    var query = r.expr([1,2,3]).map(function(x, y) {
      return x;
    });
    compare(query, done);
  });

  it('map - 12', function(done) {
    var query = r.map(1);
    compare(query, done);
  });

  it('map - 13', function(done) {
    var query = r.map(1, 2);
    compare(query, done);
  });

  it('map - 14', function(done) {
    var query = r.map([1,2,3], [10, 20, 30], r.js('(function(x, y) { return x+y; })'));
    compare(query, done);
  });

  it('map - 15', function(done) {
    var query = r.map([1,2,3], [10, 0, 30], r.js('(function(x, y) { return x/y; })'));
    compare(query, done);
  });

  it('withFields - 1', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).withFields('id', 'foo').orderBy(r.row('id'));
    compare(query, done);
  });

  it('withFields - 2', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).withFields('id', 'foo', MISSING_FIELD).orderBy(r.row('id'));
    compare(query, done);
  });

  it('withFields - 3', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).withFields('id').orderBy(r.row('id'));
    compare(query, done);
  });

  it('withFields - 4', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).withFields();
    compare(query, done);
  });

  it('withFields - 5', function(done) {
    var query = r.expr('foo').withFields('id', 'foo').orderBy(r.row('id'));
    compare(query, done);
  });

  it('withFields - 6', function(done) {
    var query = r.expr([{id: 1, bar: 'foo'}]).withFields();
    compare(query, done);
  });

  it('withFields - 7', function(done) {
    var query = r.expr([COMPLEX_OBJECT]).withFields('buzz');
    compare(query, done);
  });

  it('withFields - 8', function(done) {
    var query = r.expr([COMPLEX_OBJECT]).withFields({'buzz': true});
    compare(query, done);
  });

  it('withFields - 9', function(done) {
    var query = r.expr([COMPLEX_OBJECT]).withFields({'buzz': {'missing': true}});
    compare(query, done);
  });

  it('withFields - 10', function(done) {
    var query = r.expr([COMPLEX_OBJECT]).withFields({'buzz': ['missing']});
    compare(query, done);
  });

  it('withFields - 11', function(done) {
    var query = r.expr([{foo: 'bar'}]).withFields(r.db(TEST_DB).table(TEST_TABLE));
    compare(query, done);
  });

  it('concatMap - 1', function(done) {
    var query = r.expr([1, 2, 3]).concatMap(function(x) { return [x, x.mul(2)]; });
    compare(query, done);
  });

  it('concatMap - 2', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).concatMap(function(doc) {
      return doc('bar');
    }).orderBy(r.row);
    compare(query, done);
  });

  it('concatMap - 3', function(done) {
    var query = r.expr([1, 2, 3]).concatMap(function(x) {
      return r.db(TEST_DB).table(TEST_TABLE);
    }).orderBy('id');
    compare(query, done);
  });

  it('concatMap - 4', function(done) {
    var query = r.expr([1, 2, 3]).concatMap(function(x) {
      return 'foo';
    });
    compare(query, done);
  });

  it('concatMap - 5', function(done) {
    var query = r.expr([1, 2, 3]).concatMap();
    compare(query, done);
  });

  it('concatMap - 6', function(done) {
    var query = r.expr([1, 2]).concatMap(function(x) {
      return r.db(TEST_DB).table(TEST_TABLE);
    }).orderBy(r.row);
    compare(query, done);
  });

  it('concatMap - 7', function(done) {
    var query = r.expr([1, 2]).concatMap([1,2,3]);
    compare(query, done, function(error) {
      var message = error.split(':')[0];
      assert(message.length > 0);
      return message;
    });

  });

  it('concatMap - 8', function(done) {
    var query = r.expr([1, 2]).concatMap(r.js('(function(row) { return [row] })'));
    compare(query, done);
  });

  it('concatMap - 9', function(done) {
    var query = r.expr([1, 2]).concatMap(r.js('(function(row) { return row })'));
    compare(query, done);
  });

  it('orderBy - 1', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).orderBy('id');
    compare(query, done);
  });

  it('orderBy - 2', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).orderBy(r.row('id'));
    compare(query, done);
  });

  it('orderBy - 3', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).orderBy(function(doc) {
      return doc('id');
    });
    compare(query, done);
  });

  it('orderBy - 4', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).orderBy('foo', 'id');
    compare(query, done);
  });

  it('orderBy - 5', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).orderBy('id', 'foo');
    compare(query, done);
  });

  it('orderBy - 6', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).orderBy(r.row('id'), 'foo');
    compare(query, done);
  });

  it('orderBy - 7', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).orderBy('foo', r.row('id'));
    compare(query, done);
  });

  it('orderBy - 8', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).orderBy(function(doc) {
      return doc('foo');
    }, r.row('id'));
    compare(query, done);
  });

  it('orderBy - 9', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).orderBy({index: 'id'});
    compare(query, done);
  });

  it('orderBy - 10', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).orderBy({index: 'foo'});
    compare(query, done, function(result) {
      return [result[0].foo, result[1].foo, result[2].foo,result[3].foo];
    });
  });

  it('orderBy - 11', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).orderBy({index: MISSING_INDEX});
    compare(query, done);
  });

  it('orderBy - 11', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).orderBy('id', {index: MISSING_INDEX});
    compare(query, done);
  });

  it('orderBy - 12', function(done) {
    var query = r.expr([
      {id: 1},
      {},
      {id: 3},
    ]).orderBy(r.row('id'));
    compare(query, done);
  });

  it('orderBy - 13', function(done) {
    var query = r.expr([ {id: 1}, {}, {id: 3}, ]).orderBy();
    compare(query, done);
  });

  it('orderBy - 14', function(done) {
    var query = r.expr('foo').orderBy('id');
    compare(query, done);
  });

  it('orderBy - 15', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).orderBy(1);
    compare(query, done);
  });

  it('orderBy - 16', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).orderBy(r.js('(function(row) { return row.id })'));
    compare(query, done);
  });

  it('orderBy - 17', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).orderBy({index: r.expr([1,2,3])});
    compare(query, done);
  });

  it('orderBy - 18', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).orderBy({index: r.expr(1)});
    compare(query, done);
  });

  it('orderBy - 19', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).orderBy(r.js('(function(row) { return NaN })'));
    compare(query, done);
  });

  it('orderBy - 20', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).coerceTo("ARRAY").orderBy('id');
    compare(query, done);
  });

  it('orderBy - 21', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).coerceTo("ARRAY").orderBy(r.row('id'));
    compare(query, done);
  });

  it('orderBy - 22', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).coerceTo("ARRAY").orderBy(function(doc) {
      return doc('id');
    });
    compare(query, done);
  });

  it('orderBy - 23', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).coerceTo("ARRAY").orderBy('foo', 'id');
    compare(query, done);
  });

  it('orderBy - 24', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).coerceTo("ARRAY").orderBy('id', 'foo');
    compare(query, done);
  });

  it('orderBy - 25', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).coerceTo("ARRAY").orderBy(r.row('id'), 'foo');
    compare(query, done);
  });

  it('orderBy - 26', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).coerceTo("ARRAY").orderBy('foo', r.row('id'));
    compare(query, done);
  });

  it('orderBy - 27', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).coerceTo("ARRAY").orderBy(function(doc) {
      return doc('foo');
    }, r.row('id'));
    compare(query, done);
  });

  it('orderBy - 28', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).coerceTo("ARRAY").orderBy({index: 'id'});
    compare(query, done);
  });

  it('orderBy - 29', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).coerceTo("ARRAY").orderBy({index: 'foo'});
    compare(query, done);
  });

  it('orderBy - 30', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).coerceTo("ARRAY").orderBy({index: MISSING_INDEX});
    compare(query, done);
  });

  it('orderBy - 31', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).coerceTo("ARRAY").orderBy('id', {index: MISSING_INDEX});
    compare(query, done);
  });

  it('orderBy - 32', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).coerceTo("ARRAY").orderBy(1);
    compare(query, done);
  });

  it('orderBy - 33', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).coerceTo("ARRAY").orderBy(r.js('(function(row) { return row.id })'));
    compare(query, done);
  });

  it('orderBy - 34', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).coerceTo("ARRAY").orderBy({index: r.expr([1,2,3])});
    compare(query, done);
  });

  it('orderBy - 35', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).coerceTo("ARRAY").orderBy({index: r.expr([1,2,3])});
    compare(query, done);
  });

  it('orderBy - 36', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).coerceTo("ARRAY").orderBy(r.js('(function(row) { return NaN })'));
    compare(query, done);
  });

  it('orderBy - 37', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).orderBy(r.desc('foo'), 'id');
    compare(query, done);
  });

  it('orderBy - 38', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).orderBy('id', r.desc('foo'));
    compare(query, done);
  });

  it('orderBy - 39', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).coerceTo("ARRAY").orderBy(r.desc('foo'), 'id');
    compare(query, done);
  });

  it('orderBy - 40', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).coerceTo("ARRAY").orderBy('id', r.desc('foo'));
    compare(query, done);
  });

  it('orderBy - 41', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).orderBy({index: r.desc('id')});
    compare(query, done);
  });

  it('orderBy - 42', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).orderBy({index: r.asc('id')});
    compare(query, done);
  });

  it('orderBy - 43', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).orderBy('id', {index: r.asc('foo')});
    compare(query, done);
  });

  it('orderBy - 44', function(done) {
    var query = r.expr([1,2,3]).orderBy({index: 'foo'});
    compare(query, done);
  });

  it('skip - 1', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).orderBy('id').skip(0);
    compare(query, done);
  });

  it('skip - 2', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).orderBy('id').skip(2);
    compare(query, done);
  });

  it('skip - 3', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).orderBy('id').skip(200);
    compare(query, done);
  });

  it('skip - 4', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).orderBy('id').skip(r.maxval);
    compare(query, done);
  });

  it('skip - 5', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).orderBy('id').skip(r.minval);
    compare(query, done);
  });

  it('skip - 6', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).orderBy('id').skip('foo');
    compare(query, done);
  });

  it('skip - 7', function(done) {
    var query = r.expr([1,2,3,4,5]).skip(2);
    compare(query, done);
  });

  it('skip - 8', function(done) {
    var query = r.expr([1,2,3,4,5]).skip(-1);
    compare(query, done);
  });

  it('skip - 9', function(done) {
    var query = r.expr([1,2,3,4,5]).skip(-100);
    compare(query, done);
  });

  it('skip - 10', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).orderBy('id').skip();
    compare(query, done);
  });

  it('skip - 11', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).orderBy('id').skip(1,2);
    compare(query, done);
  });

  it('skip - 12', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).orderBy('id').skip('foo');
    compare(query, done);
  });

  it('skip - 13', function(done) {
    var query = r.expr('foo').skip(2);
    compare(query, done);
  });

  it('skip - 13', function(done) {
    var query = r.binary(new Buffer('foobarbuzz')).skip(2);
    compare(query, done);
  });

  it('limit - 1', function(done) {
    var query = r.expr([1,2,3,4,5]).limit(2);
    compare(query, done);
  });

  it('limit - 2', function(done) {
    var query = r.expr([1,2,3,4,5]).limit(200);
    compare(query, done);
  });

  it('limit - 3', function(done) {
    var query = r.expr([1,2,3,4,5]).limit(-1);
    compare(query, done);
  });

  it('limit - 4', function(done) {
    var query = r.expr([1,2,3,4,5]).limit('foo');
    compare(query, done);
  });

  it('limit - 5', function(done) {
    var query = r.expr([1,2,3,4,5]).limit('foo');
    compare(query, done);
  });

  it('limit - 6', function(done) {
    var query = r.expr('foo').limit(2);
    compare(query, done);
  });

  it('limit - 7', function(done) {
    var query = r.expr([1,2,3]).limit(2, 'foo');
    compare(query, done);
  });

  it('limit - 8', function(done) {
    var query = r.expr([1,2,3]).limit(r.args([1,2,3,4]));
    compare(query, done);
  });

  it('limit - 9', function(done) {
    var query = r.binary(new Buffer('foobarbuzz')).limit(4);
    compare(query, done);
  });

  it('limit - 10', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).orderBy('id').limit(2);
    compare(query, done);
  });

  it('limit - 11', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).filter(true).orderBy('id').limit(2);
    compare(query, done);
  });

  it('limit - 12', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).filter(true).orderBy('id').limit(2);
    compare(query, done);
  });

  it('limit - 13', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).between(r.minval, r.maxval).orderBy('id').limit(2);
    compare(query, done);
  });

  it('limit - 14', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).between(r.minval, r.maxval).orderBy('id').skip(1).limit(2);
    compare(query, done);
  });

  it('slice - 1', function(done) {
    var query = r.expr([1,2,3,4,5]).slice(2);
    compare(query, done);
  });

  it('slice - 2', function(done) {
    var query = r.expr([1,2,3,4,5]).slice(2, {leftBound: 'closed'});
    compare(query, done);
  });

  it('slice - 3', function(done) {
    var query = r.expr([1,2,3,4,5]).slice(2, {leftBound: 'open'});
    compare(query, done);
  });

  it('slice - 4', function(done) {
    var query = r.expr([1,2,3,4,5]).slice(2, {leftBound: 'foo'});
    compare(query, done);
  });

  it('slice - 5', function(done) {
    var query = r.expr([1,2,3,4,5]).slice(2, 3);
    compare(query, done);
  });

  it('slice - 6', function(done) {
    var query = r.expr([1,2,3,4,5]).slice(2, 3, {rightBound: 'closed'});
    compare(query, done);
  });

  it('slice - 7', function(done) {
    var query = r.expr([1,2,3,4,5]).slice(2, 3, {rightBound: 'open'});
    compare(query, done);
  });

  it('slice - 8', function(done) {
    var query = r.expr([1,2,3,4,5]).slice(2, 3, {leftBound: 'open', rightBound: 'open'});
    compare(query, done);
  });

  it('slice - 9', function(done) {
    var query = r.expr([1,2,3,4,5]).slice(-1);
    compare(query, done);
  });

  it('slice - 10', function(done) {
    var query = r.expr([1,2,3,4,5]).slice(-100);
    compare(query, done);
  });

  it('slice - 11', function(done) {
    var query = r.expr([1,2,3,4,5]).slice(1, -1);
    compare(query, done);
  });

  it('slice - 12', function(done) {
    var query = r.expr([1,2,3,4,5]).slice(1, -100);
    compare(query, done);
  });

  it('slice - 13', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).orderBy('id').skip('foo').slice(2, 3);
    compare(query, done);
  });

  it('slice - 14', function(done) {
    var query = r.expr([1,2,3,4,5]).slice('foo');
    compare(query, done);
  });

  it('slice - 15', function(done) {
    var query = r.expr('foo').slice(2);
    compare(query, done);
  });

  it('slice - 16', function(done) {
    var query = r.expr([1,2,3]).slice(2, 'foo');
    compare(query, done);
  });

  it('slice - 17', function(done) {
    var query = r.expr([1,2,3]).slice(r.args([1,2,3,4]));
    compare(query, done);
  });

  it('nth - 1', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).orderBy('id').nth(2);
    compare(query, done);
  });

  it('nth - 2', function(done) {
    var query = r.expr([1,2,3,4,5]).nth(2);
    compare(query, done);
  });

  it('nth - 3', function(done) {
    var query = r.expr([1,2,3,4,5]).nth(-2);
    compare(query, done);
  });

  it('nth - 4', function(done) {
    var query = r.expr([1,2,3,4,5]).nth(200);
    compare(query, done);
  });

  it('nth - 5', function(done) {
    var query = r.expr([1,2,3,4,5]).nth(-200);
    compare(query, done);
  });

  it('nth - 6', function(done) {
    var query = r.expr([1,2,3,4,5]).nth();
    compare(query, done);
  });

  it('nth - 7', function(done) {
    var query = r.expr('foo').nth(2);
    compare(query, done);
  });

  it('nth - 8', function(done) {
    var query = r.expr({foo: 'bar'}).nth('foo');
    compare(query, done);
  });

  it('offsetsOf - 1', function(done) {
    var query = r.expr([1,2,3,4,5]).offsetsOf(2);
    compare(query, done);
  });

  it('offsetsOf - 2', function(done) {
    var query = r.expr([1,2,3,4,5]).offsetsOf(20);
    compare(query, done);
  });

  it('offsetsOf - 3', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).orderBy('id').map(function(doc) {
      return doc('id');
    }).offsetsOf(2);
    compare(query, done);
  });

  it('offsetsOf - 4', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).between(r.minval, r.maxval).orderBy('id')
      .offsetsOf({id: 3, foo: 10, bar: [300, 301, 302]});
    compare(query, done);
  });

  it('offsetsOf - 5', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).between(r.minval, r.maxval).orderBy('foo', 'id')
      .offsetsOf({id: 3, foo: 10, bar: [300, 301, 302]});
    compare(query, done);
  });

  it('offsetsOf - 6', function(done) {
    var query = r.expr('foo').offsetsOf(20);
    compare(query, done);
  });

  it('offsetsOf - 7', function(done) {
    var query = r.expr([1,2,3]).offsetsOf(r.db(TEST_DB).table(TEST_TABLE));
    compare(query, done);
  });

  it('offsetsOf - 8', function(done) {
    var query = r.expr([1,2,3]).offsetsOf(function(value) {
      return r.db(TEST_DB).table(TEST_TABLE);
    });
    compare(query, done);
  });

  it('offsetsOf - 9', function(done) {
    var query = r.expr([1,2,3]).offsetsOf(function(value) {
      return false;
    });
    compare(query, done);
  });

  it('offsetsOf - 10', function(done) {
    var query = r.expr([1,2,3,4,5]).offsetsOf();
    compare(query, done);
  });

  it('isEmpty - 1', function(done) {
    var query = r.expr([]).isEmpty();
    compare(query, done);
  });

  it('isEmpty - 2', function(done) {
    var query = r.expr([0]).isEmpty();
    compare(query, done);
  });

  it('isEmpty - 3', function(done) {
    var query = r.expr({}).isEmpty();
    compare(query, done);
  });

  it('isEmpty - 4', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).isEmpty();
    compare(query, done);
  });

  it('isEmpty - 5', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).limit(0).isEmpty();
    compare(query, done);
  });

  it('isEmpty - 6', function(done) {
    var query = r.expr('foo').isEmpty();
    compare(query, done);
  });

  it('isEmpty - 7', function(done) {
    var query = r.expr([]).isEmpty('foo');
    compare(query, done);
  });

  it('union - 1', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).orderBy('id').union(r.db(TEST_DB).table(TEST_TABLE).orderBy('id'));
    compare(query, done);
  });

  it('union - 2', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).orderBy('id').union([1,2,3,4]);
    compare(query, done);
  });

  it('union - 3', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).orderBy('id').union([]);
    compare(query, done);
  });

  it('union - 4', function(done) {
    var query = r.expr([1,2,3]).union([4,5,6]);
    compare(query, done);
  });

  it('union - 5', function(done) {
    var query = r.expr([1,2,3]).union([]);
    compare(query, done);
  });

  it('union - 6', function(done) {
    var query = r.expr([]).union([4,5,6]);
    compare(query, done);
  });

  it('union - 7', function(done) {
    var query = r.expr([]).union([]);
    compare(query, done);
  });

  it('union - 8', function(done) {
    var query = r.expr([]).union();
    compare(query, done);
  });

  it('union - 9', function(done) {
    var query = r.expr([]).union(1);
    compare(query, done);
  });

  it('union - 10', function(done) {
    var query = r.expr(1).union([]);
    compare(query, done);
  });

  it('sample - 1', function(done) {
    var query = r.expr([1,2,3,4,5]).sample(0);
    compare(query, done);
  });

  it('sample - 2', function(done) {
    var query = r.expr([1,2,3,4,5]).sample(2);
    compare(query, done, function(result) {
      return result.length;
    });
  });

  it('sample - 3', function(done) {
    var query = r.expr([1,2,3,4,5]).sample(5);
    compare(query, done, function(result) {
      return result.length;
    });
  });

  it('sample - 4', function(done) {
    var query = r.expr([1,2,3,4,5]).sample(20);
    compare(query, done, function(result) {
      return result.length;
    });
  });

  it('sample - 5', function(done) {
    var query = r.expr([1,2,3,4,5]).sample();
    compare(query, done);
  });

  it('sample - 6', function(done) {
    var query = r.expr('foo').sample(10);
    compare(query, done);
  });

  it('sample - 7', function(done) {
    var query = r.expr([1,2,3,4]).sample('lol');
    compare(query, done);
  });

  it('sample - 8', function(done) {
    var query = r.expr([1,2,3,4]).sample(-1);
    compare(query, done);
  });



  /*
  */
});
