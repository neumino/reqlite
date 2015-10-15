var config = require('./../config.js');

var r = require('rethinkdb');
var assert = require('assert');
var util = require('./util.js');

var connections = {};
var TEST_DB = 'reqlitetestmanipdb';

var compare = require('./util.js').generateCompare(connections);

describe('manipulating-databases.js', function(){
  before(function(done) {
    setTimeout(function() { // Delay for nodemon to restart the server
      r.connect(config.rethinkdb).bind({}).then(function(conn) {
        connections.rethinkdb = conn;
        return r.connect(config.reqlite);
      }).then(function(conn) {
        connections.reqlite = conn;
        this.query = r.dbDrop(TEST_DB);
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

  it('dbCreate - 1', function(done) {
    var query = r.dbCreate(TEST_DB);
    compare(query, done, function(doc) {
      delete doc.config_changes[0].new_val.id;
      return doc;
    });
  });

  it('dbCreate - 2', function(done) {
    var query = r.dbCreate('foo', 'bar');
    compare(query, done);
  });

  it('dbCreate - 3', function(done) {
    var query = r.dbCreate('fo~o');
    compare(query, done);
  });

  it('dbCreate - 4', function(done) {
    var query = r.dbCreate(1);
    compare(query, done);
  });

  it('dbCreate - 5', function(done) {
    var query = r.dbCreate('foo_bar');
    compare(query, done, function(doc) {
      delete doc.config_changes[0].new_val.id;
      return doc;
    });
  });
  it('dbCreate - 5 - follow up', function(done) {
    var query = r.dbDrop('foo_bar');
    compare(query, done, function(result) {
      return result.dbs_dropped;
    });
  });

  it('dbCreate - 6', function(done) {
    var query = r.dbCreate(TEST_DB); // The database exists
    compare(query, done);
  });

  it('dbList', function(done) { // After dbCreate test and before dbDrop test
    var query = r.dbList();
    compare(query, done, function(dbs) {
      var contain = false;
      for(var i=0; i<dbs.length; i++) {
        if (dbs[i] === TEST_DB) {
          contain = true;
          break;
        }
      }
      return [Array.isArray(dbs), contain];
    });
  });
  it('dbList', function(done) {
    var query = r.dbList('foo', 'bar');
    compare(query, done);
  });


  it('dbDrop - 1', function(done) {
    var query = r.dbDrop(TEST_DB);
    compare(query, done, function(doc) {
      delete doc.config_changes[0].old_val.id;
      return doc;
    });
  });

  it('dbDrop - 2', function(done) {
    var query = r.dbDrop(TEST_DB);
    compare(query, done);
  });

  it('dbDrop - 3', function(done) {
    var query = r.dbDrop(1);
    compare(query, done);
  });

  it('dbDrop - 3', function(done) {
    var query = r.dbDrop('fo~o');
    compare(query, done);
  });


});
