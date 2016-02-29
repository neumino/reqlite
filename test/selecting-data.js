var config = require('./../config.js');

var r = require('rethinkdb');
var assert = require('assert');

var connections = {};
var TEST_DB = 'reqlitetest';
var TEST_TABLE = 'reqlitetestselectingdata';
var MISSING_ID = 'nonExistingId';
var MISSING_DB = 'databasethatdoesnotexist';
var MISSING_TABLE = 'tablethatdoesnotexist';

var compare = require('./util.js').generateCompare(connections);

describe('selecting-data.js', function(){
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
          {id: 1, foo: 10, bar: [100, 101, 102]},
          {id: 2, foo: 20, bar: [200, 201, 202]},
          {id: 3, foo: 30, bar: [300, 301, 302]},
          {id: 4, foo: 40, bar: [400, 401, 402]},
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
        this.query = r.db(TEST_DB).table(TEST_TABLE).indexCreate('foobar', function(doc) {
          return [doc('foo'), doc('bar')];
        });
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
        //TODO Add and test a geo index
        done();
      });
    }, 400);
  });

  it('db - 1', function(done) {
    var query = r.db(1).tableList();
    compare(query, done);
  });

  it('db - 2', function(done) {
    var query = r.db('foo', 'bar').tableList();
    compare(query, done);
  });

  it('db - 3', function(done) {
    var query = r.db('f~o').tableList();
    compare(query, done);
  });

  it('db - 4', function(done) {
    var query = r.db(MISSING_DB).tableList();
    compare(query, done);
  });

  it('db - 5', function(done) {
    var query = r.db('fo', 'oo').tableList();
    compare(query, done);
  });

  it('db - 6', function(done) {
    var query = r.db(TEST_DB);
    compare(query, done);
  });

  it('table - 1', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).orderBy('id');
    compare(query, done);
  });

  it('table - 2', function(done) {
    var query = r.db(TEST_DB).table(MISSING_TABLE);
    compare(query, done);
  });

  it('table - 3', function(done) {
    var query = r.db(TEST_DB).table('x~x');
    compare(query, done);
  });

  it('table - 4', function(done) {
    var query = r.db(TEST_DB).table(1);
    compare(query, done);
  });

  it('table - 5', function(done) {
    var query = r.table('x~x');
    compare(query, done);
  });

  it('table - 6', function(done) {
    var query = r.table(1);
    compare(query, done);
  });

  it('table - 7', function(done) {
    var query = r.expr([]).table(TEST_TABLE);
    compare(query, done);
  });

  it('table - 8', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).orderBy({index: 'foo'});
    compare(query, done);
  });

  it('table - 9', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).orderBy({index: r.desc('foo')});
    compare(query, done);
  });

  it('get - 1', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).get(1);
    compare(query, done);
  });

  it('get - 2', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).get(MISSING_ID);
    compare(query, done);
  });

  it('get - 3', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).get({});
    compare(query, done, function(error) {
      var message = error.split(':')[0];
      assert(message.length > 0);
      return message;
    });
  });

  it('get - 4', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).get(new Buffer('hello'));
    compare(query, done);
  });

  it('get - 5', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).get();
    compare(query, done);
  });

  it('get - 6', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).get(1,2,3);
    compare(query, done);
  });

  it('get - 7', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).get(r.db(TEST_DB).table(TEST_TABLE));
    compare(query, done);
  });

  it('get - 8', function(done) {
    var query = r.expr([]).get(1);
    compare(query, done);
  });

  it('getAll - 1', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).getAll(1);
    compare(query, done);
  });

  it('getAll - 2', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).getAll(1,2,3).orderBy('id');
    compare(query, done);
  });

  it('getAll - 3', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).getAll(MISSING_ID);
    compare(query, done);
  });

  it('getAll - 4', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).getAll(101, {index: 'foo'}).orderBy('id');
    compare(query, done);
  });

  it('getAll - 5', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).getAll([200, 201, 202], {index: 'bar'}).orderBy('id');
    compare(query, done);
  });

  it('getAll - 6', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).getAll(201, {index: 'barmulti'}).orderBy('id');
    compare(query, done);
  });

  it('getAll - 7', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).getAll();
    compare(query, done);
  });

  it('getAll - 8', function(done) {
    var query = r.expr('bar').getAll(1);
    compare(query, done);
  });

  it('between - 1', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).between(r.minval, r.maxval).orderBy('id');
    compare(query, done);
  });

  it('between - 2', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).between(r.minval, 3).orderBy('id');
    compare(query, done);
  });

  it('between - 3', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).between(2, r.maxval).orderBy('id');
    compare(query, done);
  });

  it('between - 4', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).between(2, 3).orderBy('id');
    compare(query, done);
  });

  it('between - 5', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).between(2, 3, {left_bound: 'closed', right_bound: 'closed'}).orderBy('id');
    compare(query, done);
  });

  it('between - 6', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).between(2, 3, {left_bound: 'open', right_bound: 'closed'}).orderBy('id');
    compare(query, done);
  });

  it('between - 7', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).between(2, 3, {left_bound: 'closed', right_bound: 'open'}).orderBy('id');
    compare(query, done);
  });

  it('between - 8', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).between(2, 3, {left_bound: 'open', right_bound: 'open'}).orderBy('id');
    compare(query, done);
  });

  it('between - 9', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).between(20, 30, {index: 'foo'}).orderBy('id');
    compare(query, done);
  });

  it('between - 10', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).between(20, 30, {left_bound: 'hello'});
    compare(query, done);
  });

  it('between - 11', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).between(20, 30, {rightBound: 'hello'});
    compare(query, done);
  });

  it('between - 12', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).between(r.db(TEST_DB).table(TEST_TABLE), 30, {left_bound: 'hello'});
    compare(query, done);
  });

  it('between - 13', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).between(r.minval, r.db(TEST_DB).table(TEST_TABLE), {left_bound: 'hello'});
    compare(query, done);
  });

  it('between - 14', function(done) {
    var query = r.expr([1,2,3]).between(r.minval, r.maxval);
    compare(query, done, function(error) {
      var message = error.split(':')[0];
      assert(message.length > 0);
      return message;
    });
  });

  it('between - 15', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).between(r.minval, r.maxval).between(r.minval, r.maxval);
    compare(query, done);
  });

  it('between - 16', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).between(r.minval, r.maxval).between(r.minval, r.maxval, {index: 'foo'});
    compare(query, done);
  });

  it('between - 17', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).between(r.minval, r.maxval).orderBy({index: 'id'});
    compare(query, done);
  });

  it('between - 18', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).between(r.minval, r.maxval).orderBy({index: 'foo'});
    compare(query, done);
  });

  it('between - 19', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).orderBy({index: 'id'}).between(r.minval, r.maxval);
    compare(query, done);
  });

  it('between - 20', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).orderBy({index: 'id'}).between(r.minval, r.maxval);
    compare(query, done);
  });

  it('between - 21', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).orderBy({index: 'foo'}).between(r.minval, r.maxval);
    compare(query, done);
  });

  it('between - 22', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).between(200, 300, {index: 'barmulti'});
    compare(query, done);
  });

  it('between - 23', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).orderBy({index: 'barmulti'}).between(200, 300, {index: 'barmulti'});
    compare(query, done);
  });

  it('between - 24', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).between(200, 300, {index: 'barmulti'});
    compare(query, done);
  });

  it('between - 25', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).between([20, r.minval], [20, r.maxval], {index: 'foobar'}).orderBy('id');
    compare(query, done);
  });

  it('between - 26', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).between(10, 30, {index: 'foo'}).orderBy({index: 'foo'});
    compare(query, done);
  });

  it('between - 27', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).between(10, 30, {index: 'foo'}).orderBy({index: r.desc('foo')});
    compare(query, done);
  });

  it('filter - 1', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).filter({id: 1}).orderBy('id');
    compare(query, done);
  });

  it('filter - 2', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).filter(true).orderBy('id');
    compare(query, done);
  });

  it('filter - 3', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).filter(r.row('id').eq(1)).orderBy('id');
    compare(query, done);
  });

  it('filter - 4', function(done) {
    var query = r.expr([{foo: {nested: "bar", hello: "world"}, buzz: "lol"}]).filter({foo: {nested: "bar"}});
    compare(query, done);
  });

  it('filter - 5', function(done) {
    var query = r.expr([{foo: {nested: ["bar", "baar"], hello: "world"}, buzz: "lol"}]).filter({foo: {nested: ["bar", "baar"]}});
    compare(query, done);
  });

  it('filter - 6', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).filter(function(doc) {
      return doc('id').eq(1);
    }).orderBy('id');
    compare(query, done);
  });

  it('filter - 7', function(done) {
    var query = r.expr([1,2,3,4,5]).filter(function(value) {
      return value.gt(2);
    }).orderBy(r.row);
    compare(query, done);
  });

  it('filter - 8', function(done) {
    var query = r.expr([1,2,3,4,5]).filter(r.args([1,2,3]));
    compare(query, done);
  });

  it('filter - 9', function(done) {
    var query = r.expr('foo').filter(r.row('id').eq(2));
    compare(query, done);
  });

  it('filter - 10', function(done) {
    var query = r.expr([1,2,3,4,5]).filter(r.js('(function(row) { return row > 2; })'));
    compare(query, done);
  });

  it('filter - 11', function(done) {
    var query = r.expr([1,2,3,4,5]).filter(r.js('(function(row) { return Infinity; })'));
    compare(query, done);
  });

  it('filter - 12', function(done) {
    var query = r.expr([1,2,3,4,5]).filter(r.js('(function(row) { return row.a.b; })'));
    compare(query, done);
  });

  it('filter - 13', function(done) {
    var query = r.expr([1,2,3,4,5]).filter(r.js('(function(row) { return undefined; })'));
    compare(query, done);
  });

  it('filter - 14', function(done) {
    var query = r.expr([1,2,3,4,5]).filter(r.js('(function(row) { return new RegExp("bar"); })'));
    compare(query, done);
  });

  it('filter - 15', function(done) {
    var query = r.expr([1,2,3,4,5]).filter(function(doc) {
      return 0;
    });
    compare(query, done);
  });


  /*
  // See rethinkdb/rethinkdb/issues/4189
  it('filter - 15', function(done) {
    var query = r.expr([1,2,3,4,5]).filter(function(x, y) {
      return x;
    });
    compare(query, done);
  });
  */

  /*
  // Crash the server, see rethinkdb/rethinkdb#4190
  it('filter - 15', function(done) {
    var query = r.expr([1,2,3,4,5]).filter(r.js('(function(row) { return function() {}; })'))
    compare(query, done);
  });
  */

  /*
  */
});
