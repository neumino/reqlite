var config = require('./../config.js');

// Use rethinkdbdash as rethinkdb doesn't properly reject errors
// See https://github.com/rethinkdb/rethinkdb/issues/5916
var r = require('rethinkdbdash')({pool: false});

var assert = require('assert');

var connections = {};
var TEST_DB = 'reqlitetest';
var TEST_TABLE = 'reqlitetestselectingdata';
var MISSING_ID = 'nonExistingId';
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
describe('connect', function(){
  it('should work', function(done) {
    r.connect(config.reqlite).then(function(connection) {
      return r.expr(1).run(connection);
    }).then(function(result) {
      assert.equal(1, result);
      done();
    }).catch(done);
  });
});

describe('new.js', function(){
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
          {id: 1, foo: 10, bar: [100, 101, 102], optional: 1},
          {id: 2, foo: 20, bar: [200, 201, 202], optional: 2},
          {id: 3, foo: 30, bar: [300, 301, 302], optional: 2},
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
        //TODO add and test a geo index
        done();
      });
    }, 300);
  });


  it('noreplyWait - 1', function(done) {
    connections.rethinkdb.noreplyWait().bind({}).then(function(result) {
      this.rethinkdbResult = result;
      return connections.reqlite.noreplyWait();
    }).then(function(result) {
      assert.deepEqual(this.rethinkdbResult, result);
      done();
    }).catch(done);
  });

  it('noreplyWait - 1', function(done) {
    connections.reqlite.server().then(function(result) {
      assert(result.name);
      assert(result.id);
      done();
    }).catch(done);
  });


  /*
  */
});

