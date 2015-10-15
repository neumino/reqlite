var config = require('./../config.js');

var r = require('rethinkdb');
var assert = require('assert');

var connections = {};
var TEST_DB = 'reqlitetest';
var TEST_TABLE = 'reqlitetestdatum';

var compare = require('./util.js').generateCompare(connections);

describe('datum.js', function(){
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
        done();
      });
    }, 400);
  });

  it('Datum number', function(done) {
    var query = r.expr(1);
    compare(query, done);
  });

  it('Datum string', function(done) {
    var query = r.expr("Hello world!");
    compare(query, done);
  });

  it('Datum null', function(done) {
    var query = r.expr(null);
    compare(query, done);
  });

  it('Datum boolean - true', function(done) {
    var query = r.expr(true);
    compare(query, done);
  });

  it('Datum boolean - false', function(done) {
    var query = r.expr(false);
    compare(query, done);
  });

  it('Datum object', function(done) {
    var query = r.expr({});
    compare(query, done);
  });

  it('Datum object', function(done) {
    var query = r.expr({foo: "bar"});
    compare(query, done);
  });

  it('Datum binary', function(done) {
    var query = r.binary(new Buffer("hello world"));
    compare(query, done);
  });

  it('Datum stream', function(done) {
    var query = r.range().typeOf();
    compare(query, done);
  });

  it('Datum array - 1', function(done) {
    var query = r.expr([1,2,3,4]).typeOf();
    compare(query, done);
  });

  it('Datum array - 2', function(done) {
    var query = r.expr([1,2,[3,4,[5,6]]]);
    compare(query, done);
  });

  it('Datum array - 3', function(done) {
    var query = r.expr([1,2,3,4,5]);
    compare(query, done);
  });

  it('Datum array - 4', function(done) {
    var query = r.expr([r.db(TEST_DB).table(TEST_TABLE)]);
    compare(query, done);
  });

  it('Datum array - 5', function(done) {
    var query = r.expr([r.db(TEST_DB).table(TEST_TABLE).filter(true)]);
    compare(query, done);
  });

  it('Datum array - 6', function(done) {
    var query = r.expr([r.db(TEST_DB).table(TEST_TABLE).get(1)]);
    compare(query, done);
  });

  it('Datum table', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).typeOf();
    compare(query, done);
  });

  it('Datum table slice - 1', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).between(r.minval, r.maxval).typeOf();
    compare(query, done);
  });

  it('Datum table slice - 2', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).orderBy({index: 'id'}).typeOf();
    compare(query, done);
  });

  it('Datum selection<stream> - 1', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).orderBy('id', {index: 'id'}).typeOf();
    compare(query, done);
  });

  it('Datum selection<stream> - 2', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).limit(3).typeOf();
    compare(query, done);
  });

  it('Datum selection<stream> - 3', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).getAll(1, 2, 3, {index: 'id'}).typeOf();
    compare(query, done);
  });

  /*
  */
});
