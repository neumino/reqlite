var config = require('./../config.js');

var r = require('rethinkdb');
//var r = require('rethinkdbdash')({pool: false});

var connections = {};
var TEST_DB = 'reqlitetest';
var TEST_TABLE = 'reqlitetestmeta';
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

describe('meta.js', function(){
  before(function(done) {
    setTimeout(function() { // Delay for nodemon to restart the server
      r.connect(config.rethinkdb).bind({}).then(function(conn) {
        connections.rethinkdb = conn;
        return r.connect(config.reqlite);
      }).then(function(conn) {
        connections.reqlite = conn;
        this.query = r.dbCreate(TEST_DB);
        return this.query.run(connections.rethinkdb);
      }).catch(function(e) { // ignore errors
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
        done();
      });
    }, 300);
  });


  //Test playground


  it('cluster_config - 1', function(done) {
    var query = r.db('rethinkdb').table('cluster_config');
    compare(query, done);
  });

  it('cluster_config - 2', function(done) {
    var query = r.db('rethinkdb').table('cluster_config').insert({});
    compare(query, done, function(result) {
      delete result.generated_keys;
      return result;
    });
  });

  it('cluster_config - 3', function(done) {
    var query = r.db('rethinkdb').table('cluster_config').get('auth');
    compare(query, done);
  });

  /*
  */
});

