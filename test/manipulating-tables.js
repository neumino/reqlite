var config = require('./../config.js');

var r = require('rethinkdb');
var assert = require('assert');
var util = require('./util.js');

var connections = {};
var TEST_DB = 'reqlitetest';
var TEST_DB2 = 'reqlitetest2';
var MISSING_DB = 'reqlitedatabasethatdoesntexist';
var TEST_TABLE = 'reqlitetesttable';
var TEST_INDEX = 'reqlitetestindex';
var TEST_INDEX2 = 'reqlitetestindex2';
var MISSING_INDEX = 'reqlitetestindexthatdoesnotexit';

var compare = require('./util.js').generateCompare(connections);

describe('manipulating-tables.js', function(){
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
        done();
      });
    }, 400);
  });


  it('tableCreate - 1', function(done) {
    var query = r.db(TEST_DB).tableCreate(TEST_TABLE);
    compare(query, done, function(doc) {
      delete doc.config_changes[0].new_val.id;
      delete doc.config_changes[0].new_val.shards;
      return doc;
    });
  });

  it('tableCreate - 2', function(done) {
    var query = r.db(TEST_DB).tableCreate(r.args(['foo', 'bar', 'buzz']));
    compare(query, done);
  });

  it('tableCreate - 3', function(done) {
    var query = r.db(TEST_DB).tableCreate(1);
    compare(query, done);
  });

  it('tableCreate - 4', function(done) {
    var query = r.db(TEST_DB).tableCreate(r.args());
    compare(query, done, function(error) {
      return /^Expected 1 argument but found 0/.test(error);
    });
  });

  it('tableCreate - 5', function(done) {
    var query = r.expr('foo').tableCreate('bar');
    compare(query, done);
  });

  it('tableCreate - 6 - pre', function(done) {
    var query = r.dbDrop(TEST_DB2);
    compare(query, done, function() {
      return true; // We just want to drop it
    });
  });
  it('tableCreate - 6', function(done) {
    var query = r.tableCreate(TEST_TABLE);
    compare(query, done);
  });

  it('tableCreate - 7', function(done) {
    var query = r.tableCreate(TEST_TABLE, {foo: 'bar'});
    compare(query, done);
  });

  it('tableCreate - 8', function(done) {
    var query = r.tableCreate(1, {foo: 'bar'});
    compare(query, done);
  });

  it('tableCreate - 9', function(done) {
    var query = r.tableCreate('foo_bar');
    compare(query, done, function(result) {
      return result.tables_created;
    });
  });

  it('tableCreate - 9 - follow up', function(done) {
    var query = r.tableDrop('foo_bar');
    compare(query, done, function(result) {
      return result.tables_dropped;
    });
  });

  it('tableList - 1', function(done) { // run after the tableCreate tests and before the tableDrop tests
    var query = r.db(TEST_DB).tableList();
    compare(query, done, function(tables) {
      var contain = false;
      for(var i=0; i<tables.length; i++) {
        if (tables[i] === TEST_TABLE) {
          contain = true;
          break;
        }
      }
      return [Array.isArray(tables), contain];
    });
  });

  it('tableList - 2', function(done) {
    var query = r.db(TEST_DB).tableList('foo');
    compare(query, done);
  });

  it('tableList - 3', function(done) {
    var query = r.tableList();
    compare(query, done, function(tables) {
      var contain = false;
      for(var i=0; i<tables.length; i++) {
        if (tables[i] === TEST_TABLE) {
          contain = true;
          break;
        }
      }
      return [Array.isArray(tables), contain];
    });
  });

  it('tableList - 4', function(done) {
    var query = r.db(MISSING_DB).tableList();
    compare(query, done);
  });

  it('sync - 1', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).sync();
    compare(query, done);
  });

  it('sync - 2', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).sync(1,2,3);
    compare(query, done);
  });

  it('sync - 3', function(done) {
    var query = r.expr('foo').sync();
    compare(query, done);
  });

  it('indexCreate - 1', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).indexCreate(TEST_INDEX);
    compare(query, done);
  });

  it('indexCreate - 2', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).indexCreate(1);
    compare(query, done);
  });

  it('indexCreate - 3', function(done) {
    var query = r.expr('foo').indexCreate(TEST_INDEX);
    compare(query, done);
  });

  it('indexCreate - 4', function(done) {
    var query = r.expr('foo').indexCreate(2);
    compare(query, done);
  });

  it('indexCreate - 5', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).indexCreate(r.args(['foo', 'bar', 'buzz']));
    compare(query, done);
  });

  it('indexCreate - 6', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).indexCreate('foo', {'bar': 1});
    compare(query, done);
  });

  it('indexCreate - 8', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).indexCreate('barmulti', r.row('bar'), {'multi': true});
    compare(query, done);
  });

  it('indexCreate - 9', function(done) {
    // Index already exists
    var query = r.db(TEST_DB).table(TEST_TABLE).indexCreate('barmulti', r.row('bar'), {'multi': true});
    compare(query, done);
  });

  it('indexList', function(done) { // run after the indexCreate test and before the indexDrop test
    var query = r.db(TEST_DB).table(TEST_TABLE).indexList().orderBy(r.row);
    compare(query, done);
  });

  it('indexDrop - 1', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).indexDrop(TEST_INDEX);
    compare(query, done);
  });

  it('indexDrop - 2', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).indexDrop(r.args(['foo', 'bar']));
    compare(query, done);
  });

  it('indexDrop - 3', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).indexDrop(MISSING_INDEX);
    compare(query, done);
  });

  it('indexDrop - 4', function(done) {
    var query = r.expr('foo').indexDrop(MISSING_INDEX);
    compare(query, done);
  });

  it('indexList - 1', function(done) { // run after the indexDrop test
    var query = r.db(TEST_DB).table(TEST_TABLE).indexList();
    compare(query, done);
  });

  it('indexList - 2', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).indexList('foo');
    compare(query, done);
  });

  it('indexList - 3', function(done) {
    var query = r.expr('foo').indexList('foo');
    compare(query, done);
  });

  it('indexRename - 1 - pre - 1', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).indexCreate(TEST_INDEX);
    compare(query, done);
  });

  it('indexRename - 1 - pre - 2', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).indexWait();
    compare(query, done, function() {/* Ignore result */});
  });

  it('indexRename - 1', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).indexRename(TEST_INDEX, TEST_INDEX2);
    compare(query, done);
  });

  it('indexRename - 2', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).indexRename(TEST_INDEX2, TEST_INDEX2);
    compare(query, done);
  });

  it('indexCreate - 3', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).indexCreate(TEST_INDEX);
    compare(query, done);
  });

  it('indexRename - 4', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).indexRename(TEST_INDEX, TEST_INDEX2);
    compare(query, done);
  });

  it('indexRename - 5', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).indexRename(TEST_INDEX, TEST_INDEX2, {nonValidOption: 2});
    compare(query, done);
  });

  it('indexRename - 6', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).indexRename(TEST_INDEX2, TEST_INDEX, {overwrite: true});
    compare(query, done);
  });

  it('indexRename - 7', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).indexRename(TEST_INDEX2, TEST_INDEX, {overwrite: true});
    compare(query, done);
  });

  it('indexRename - 8', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).indexRename(MISSING_INDEX, MISSING_INDEX);
    compare(query, done);
  });

  it('indexStatus - 1', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).indexStatus(TEST_INDEX);
    compare(query, done, function(indexes) {
      for(var i=0; i<indexes.length; i++) {
        delete indexes[i].function; //TODO
        // index could be still creating
        delete indexes[i].ready;
        delete indexes[i].blocks_processed;
        delete indexes[i].blocks_total;
        delete indexes[i].progress;
        delete indexes[i].query;
      }
      return indexes;
    });
  });

  it('indexStatus - 2', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).indexStatus();
    compare(query, done, function(indexes) {
      for(var i=0; i<indexes.length; i++) {
        delete indexes[i].function; //TODO
        // index could be still creating
        delete indexes[i].ready;
        delete indexes[i].blocks_processed;
        delete indexes[i].blocks_total;
        delete indexes[i].progress;
        delete indexes[i].query;
      }
      indexes.sort(function(a, b) {
        if (a.index > b.index) { return 1; }
        else if (a.index < b.index) { return -1; }
        else { return 0; }
      });
      return indexes;
    });
  });

  it('indexStatus - 3', function(done) {
    var query = r.expr('foo').indexStatus(TEST_INDEX);
    compare(query, done);
  });

  it('indexStatus - 4', function(done) {
    var query = r.expr('foo').indexStatus(2);
    compare(query, done);
  });

  it('indexStatus - 5', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).indexStatus('foo', 2);
    compare(query, done);
  });

  it('indexWait - 1', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).indexWait(TEST_INDEX);
    compare(query, done, function(indexes) {
      for(var i=0; i<indexes.length; i++) {
        delete indexes[i].function; //TODO
        delete indexes[i].progress;
        delete indexes[i].query;
      }
      return indexes;
    });
  });

  it('indexWait - 2', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).indexWait();
    compare(query, done, function(indexes) {
      for(var i=0; i<indexes.length; i++) {
        delete indexes[i].function; //TODO
        delete indexes[i].progress;
        delete indexes[i].query;
      }
      indexes.sort(function(a, b) {
        if (a.index > b.index) { return 1; }
        else if (a.index < b.index) { return -1; }
        else { return 0; }
      });
      return indexes;
    });
  });

  it('indexWait - 3', function(done) {
    var query = r.expr('foo').indexWait(TEST_INDEX);
    compare(query, done);
  });

  it('indexWait - 4', function(done) {
    var query = r.expr('foo').indexWait(2);
    compare(query, done);
  });

  it('indexWait - 5', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).indexWait('foo', 2);
    compare(query, done);
  });

  it('tableDrop - 1', function(done) {
    var query = r.db(TEST_DB).tableDrop(TEST_TABLE);
    compare(query, done, function(doc) {
      delete doc.config_changes[0].old_val.id;
      delete doc.config_changes[0].old_val.shards;
      return doc;
    });
  });

  it('tableDrop - 2', function(done) {
    var query = r.db(TEST_DB).tableDrop(r.args(['foo', 'bar', 'buzz']));
    compare(query, done);
  });

  it('tableDrop - 3', function(done) {
    var query = r.db(TEST_DB).tableDrop(1);
    compare(query, done);
  });

  it('tableDrop - 4', function(done) {
    var query = r.db(TEST_DB).tableDrop(r.args());
    compare(query, done, function(error) {
      return /^Expected 1 argument but found 0/.test(error);
    });
  });

  it('tableDrop - 5', function(done) {
    var query = r.expr('foo').tableDrop('bar');
    compare(query, done);
  });

  it('tableDrop - 6 - pre', function(done) {
    var query = r.dbDrop(TEST_DB2);
    compare(query, done, function() {
      return true; // We just want to drop it
    });
  });
  it('tableDrop - 6', function(done) {
    var query = r.tableDrop(TEST_TABLE);
    compare(query, done);
  });

  it('tableDrop - 7', function(done) {
    var query = r.tableDrop(TEST_TABLE, {foo: 'bar'});
    compare(query, done);
  });

  it('tableDrop - 8', function(done) {
    var query = r.tableDrop(1, {foo: 'bar'});
    compare(query, done);
  });

  /*
  */
});
