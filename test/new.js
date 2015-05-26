var config = require(__dirname+'/../config.js');

var r = require('rethinkdb');
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
}

var compare = require(__dirname+'/util.js').generateCompare(connections);

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
        this.query = r.db(TEST_DB).tableDrop(TEST_TABLE)
        return this.query.run(connections.rethinkdb);
      }).catch(function() { // ignore errors
      }).finally(function() {
        return this.query.run(connections.reqlite);
      }).catch(function() { // ignore errors
      }).finally(function() {
        this.query = r.db(TEST_DB).tableCreate(TEST_TABLE)
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
        this.query = r.db(TEST_DB).table(TEST_TABLE).indexCreate('foo')
        return this.query.run(connections.rethinkdb);
      }).catch(function() { // ignore errors
      }).finally(function() {
        return this.query.run(connections.reqlite);
      }).catch(function() { // ignore errors
      }).finally(function() {
        this.query = r.db(TEST_DB).table(TEST_TABLE).indexCreate('bar')
        return this.query.run(connections.rethinkdb);
      }).catch(function() { // ignore errors
      }).finally(function() {
        return this.query.run(connections.reqlite);
      }).catch(function() { // ignore errors
      }).finally(function() {
        this.query = r.db(TEST_DB).table(TEST_TABLE).indexCreate('barmulti', r.row('bar'), {multi: true})
        return this.query.run(connections.rethinkdb);
      }).catch(function() { // ignore errors
      }).finally(function() {
        return this.query.run(connections.reqlite);
      }).catch(function() { // ignore errors
      }).finally(function() {
      //TODO add and test a geo index
        done();
      });
    }, 300)
  });


  //TODO Uncomment r.range - 5
  /*
  it('http - 1', function(done) {
    var query = r.http('http://httpbin.org/get');
    compare(query, done);
  })

  it('http - 2', function(done) {
    var query = r.expr([1,2,3, r.http('http://httpbin.org/get')]);
    compare(query, done);
  })

  it('http - 3', function(done) {
    var query = r.error(r.http('http://httpbin.org/'));
    compare(query, done);
  })

  it('http - 3', function(done) {
    var query = r.db(TEST_DB).table(r.http('http://httpbin.org/'));
    compare(query, done);
  })
  */

  it('eq - 1', function(done) {
    var query = r.expr([{id: 10}, {id: 22}, {id: 3}, {id: 1}, {id: 23}, {id: 4}, {id: 211}, {id: 3},{id: 5}]).orderBy('id');
    //compare(query, done);
    compare(query, done, function(e) { console.log(''); console.log(e); return e; });
  });

  /*
    compare(query, done, function(e) { console.log(''); console.log(e); return e; });
  */
});

