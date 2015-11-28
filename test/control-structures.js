var config = require('./../config.js');

var r = require('rethinkdb');
var assert = require('assert');

var connections = {};
var TEST_DB = 'reqlitetest';
var TEST_TABLE = 'reqlitetestcontrol';
var MISSING_ID = 'nonExistingId';
var MISSING_FIELD = 'nonExistingField';
var MISSING_INDEX = 'nonExistingIndex';

var compare = require('./util.js').generateCompare(connections);

describe('control-structures.js', function(){
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
          {id: 2, foo: 10, bar: [200, 201, 202], optional: 1},
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
        this.query = r.db(TEST_DB).table(TEST_TABLE).indexCreate('optional');
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

  it('args - 1', function(done) {
    var query = r.add(r.args([1,2,3,4,5,6]));
    compare(query, done);
  });

  it('args - 2', function(done) {
    var query = r.add(1, 2, r.args([1,2,3,4,5,6]));
    compare(query, done);
  });

  it('args - 3', function(done) {
    var query = r.add(r.args([1,2,3,4,5,6]), 4, 5);
    compare(query, done);
  });

  it('args - 4', function(done) {
    var query = r.add(1, 2, r.args([1,2,3,4,5,6]), 4, 5);
    compare(query, done);
  });

  it('args - 5', function(done) {
    var query = r.add(1, 2, r.args(), 4, 5);
    compare(query, done, function(error) {
      return /^Expected 1 argument but found 0/.test(error);
    });
  });

  it('args - 6', function(done) {
    var query = r.add(1, 2, r.args([]), 4, 5);
    compare(query, done);
  });

  it('args - 7', function(done) {
    var query = r.add(r.args('foo'));
    compare(query, done);
  });

  it('args - 8', function(done) {
    var query = r.add(r.args([1,2,], 'foo'));
    compare(query, done, function(error) {
      return /^Expected 1 argument but found 2/.test(error);
    });
  });

  it('args - 9', function(done) {
    var query = r.add(r.args(2), 'foo');
    compare(query, done);
  });

  it('args - 10', function(done) {
    var query = r.args([10, 20, 30]);
    compare(query, done);
  });

  it('args - 11', function(done) {
    var query = r.args([]);
    compare(query, done);
  });

  it('args - 12', function(done) {
    var query = r.args();
    compare(query, done);
  });


  it('binary - 1', function(done) {
    var query = r.binary(new Buffer('Hello world'));
    compare(query, done);
  });

  it('binary - 2', function(done) {
    var query = r.binary(new Buffer('Hello world')).count();
    compare(query, done);
  });

  it('binary - 3', function(done) {
    var query = r.binary(new Buffer('a')).count();
    compare(query, done);
  });

  it('do - 1', function(done) {
    var query = r.expr('hello').do(function(value) {
      return value.add('bar');
    });
    compare(query, done);
  });

  it('do - 2', function(done) {
    var query = r.do('foo', 'bar', function(left, right) {
      return left.add(right).add('buzz');
    });
    compare(query, done);
  });

  it('do - 3', function(done) {
    var query = r.do('foo');
    compare(query, done);
  });

  it('do - 4', function(done) {
    var query = r.do(r.db(TEST_DB).table(TEST_TABLE), function(value) {
      return value;
    });
    compare(query, done);
  });

  it('do - 5', function(done) {
    var query = r.do(function() { return 1;},
                     function() { return 2;});
    compare(query, done);
  });

  it('do - 6', function(done) {
    var query = r.do('foo', 'bar',
                     function() { return 2;});
    compare(query, done);
  });

  it('do - 7', function(done) {
    var query = r.do('foo',
                     function(x, y, z) { return 2;});
    compare(query, done);
  });

  it('branch - 1', function(done) {
    var query = r.branch("foo",
    'foo',
    'bar');
    compare(query, done);
  });

  it('branch - 2', function(done) {
    var query = r.branch("",
    'foo',
    'bar');
    compare(query, done);
  });

  it('branch - 3', function(done) {
    var query = r.branch(0,
    'foo',
    'bar');
    compare(query, done);
  });

  it('branch - 4', function(done) {
    var query = r.branch(1,
    'foo',
    'bar');
    compare(query, done);
  });

  it('branch - 5', function(done) {
    var query = r.branch(-1,
    'foo',
    'bar');
    compare(query, done);
  });

  it('branch - 6', function(done) {
    var query = r.branch(10000,
    'foo',
    'bar');
    compare(query, done);
  });

  it('branch - 7', function(done) {
    var query = r.branch([],
    'foo',
    'bar');
    compare(query, done);
  });

  it('branch - 8', function(done) {
    var query = r.branch([1,2,3],
    'foo',
    'bar');
    compare(query, done);
  });

  it('branch - 9', function(done) {
    var query = r.branch([["foo", 2]],
    'foo',
    'bar');
    compare(query, done);
  });

  it('branch - 10', function(done) {
    var query = r.branch([["foo", 2], ["bar", 1]],
    'foo',
    'bar');
    compare(query, done);
  });

  it('branch - 11', function(done) {
    var query = r.branch({},
    'foo',
    'bar');
    compare(query, done);
  });

  it('branch - 12', function(done) {
    var query = r.branch({foo: "bar"},
    'foo',
    'bar');
    compare(query, done);
  });

  it('branch - 13', function(done) {
    var query = r.branch({foo: "bar", buzz: 1},
    'foo',
    'bar');
    compare(query, done);
  });

  it('branch - 14', function(done) {
    var query = r.branch(null,
    'foo',
    'bar');
    compare(query, done);
  });

  it('branch - 15', function(done) {
    var query = r.branch(r.now(),
    'foo',
    'bar');
    compare(query, done);
  });

  it('branch - 16', function(done) {
    var query = r.branch(r.binary(new Buffer("hello")),
    'foo',
    'bar');
    compare(query, done);
  });

  it('branch - 17', function(done) {
    var query = r.branch(r.now,
    'foo',
    'bar');
    compare(query, done);
  });

  it('branch - 18', function(done) {
    var query = r.branch(
      true,
      function() {return 1;},
      'bar'
    );
    compare(query, done);
  });

  it('branch - 19', function(done) {
    var query = r.branch(
      true,
      'bar',
      function() {return 1;}
    );
    compare(query, done);
  });

  it('branch - 20', function(done) {
    var query = r.branch(
      true,
      function() {return 1;},
      function() {return 1;}
    );
    compare(query, done);
  });

  it('branch - 21', function(done) {
    var query = r.branch(
      r.db(TEST_DB).table(TEST_TABLE),
      'foo',
      'bar'
    );
    compare(query, done);
  });

  it('branch - 22', function(done) {
    var query = r.branch(
      true,
      'foo',
      'bar'
    );
    compare(query, done);
  });

  it('branch - 23', function(done) {
    var query = r.branch(
      false,
      'foo',
      'bar'
    );
    compare(query, done);
  });

  it('branch - 24', function(done) {
    var query = r.branch(true,
    r.error('foo'),
    'bar');
    compare(query, done);
  });

  it('error - 1', function(done) {
    var query = r.error('Hello');
    compare(query, done);
  });

  it('error - 2', function(done) {
    var query = r.error('foo', 'bar');
    compare(query, done);
  });

  it('error - 3', function(done) {
    var query = r.error(1);
    compare(query, done);
  });

  it('default - 1', function(done) {
    var query = r.expr(null).default(1);
    compare(query, done);
  });

  it('default - 2', function(done) {
    var query = r.expr({})('foo').default(1);
    compare(query, done);
  });

  it('default - 3', function(done) {
    var query = r.expr([])('foo').default(1);
    compare(query, done);
  });

  it('default - 4', function(done) {
    var query = r.expr([])('foo').default();
    compare(query, done);
  });

  it('default - 5', function(done) {
    var query = r.error('foo').default('bar');
    compare(query, done);
  });

  it('default - 6', function(done) {
    var query = r.expr([1,2,3]).nth(5).default('bar');
    compare(query, done);
  });

  it('default - 7', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).nth(500).default('bar');
    compare(query, done);
  });

  it('range - 1', function(done) {
    var query = r.range().limit(4);
    compare(query, done);
  });

  it('range - 2', function(done) {
    var query = r.range(10);
    compare(query, done);
  });

  it('range - 3', function(done) {
    var query = r.range(-5, 7);
    compare(query, done);
  });

  it('range - 4', function(done) { // less than one batch
    var query = r.range();
    compare(query, done, function(stream1, stream2) {
      var index = 10;
      var result = {
        stream1: [],
        stream2: []
      };
      function get() {
        stream1.next().then(function(row) {
          result.stream1.push(row);
          return stream2.next();
        }).then(function(row) {
          result.stream2.push(row);
          if (index-- > 0) {
            get();
          }
          else {
            assert.deepEqual(result.stream1, result.stream2);
            done();
          }
        });
      }
      get();
    }, true);
  });

  it('range - 6', function(done) {
    var query = r.range(1,2,3,4);
    compare(query, done);
  });

  it('range - 7', function(done) {
    var query = r.range(1, 'foo');
    compare(query, done);
  });

  it('range - 8', function(done) {
    var query = r.range('foo', 2);
    compare(query, done);
  });

  // More tests are defined in datum.js
  it('expr - 1', function(done) {
    var query = r.expr(-2.355);
    compare(query, done);
  });

  it('js - 1', function(done) {
    var query = r.js("'str1' + 'str2'");
    compare(query, done);
  });

  it('js - 2', function(done) {
    var query = r.expr([1,2,3,10]).contains(r.js('(function (row) { return row > 5; })'));
    compare(query, done);
  });

  it('js - 3', function(done) {
    var query = r.expr([1,2,3,4]).contains(r.js('(function (row) { return row > 5; })'));
    compare(query, done);
  });

  it('js - 4', function(done) {
    var query = r.expr([{a: [{b: 1}]}]).contains(r.js('(function (row) { return row.a[0].b == 1; })'));
    compare(query, done);
  });

  it('js - 5', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).filter(r.js('(function (row) { return row > 2; })')).orderBy('id');
    compare(query, done);
  });

  it('js - 6', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).filter(r.js('(function (row) { return row > 3; })')).orderBy('id');
    compare(query, done);
  });

  it('js - 7', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).filter(r.js('(function (row) { return row > 2; })')).orderBy('id');
    compare(query, done);
  });

  it('js - 8', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).filter(r.js('(function (row) { return row.bar[0] > 200; })')).orderBy('id');
    compare(query, done);
  });

  it('js - 9', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).filter(r.js('(function (row) { return row.a.b.c > 200; })'));//.orderBy('id');
    compare(query, done);
  });

  it('js - 10', function(done) {
    var query = r.expr([1,2,3,10]).filter(r.js('(function (row) { return row > 5; })'));
    compare(query, done);
  });

  it('js - 11', function(done) {
    var query = r.expr([1,2,3,10]).filter(r.js('(function (row) { return row.a.b > 5; })'));
    compare(query, done);
  });

  it('js - 12', function(done) {
    var query = r.expr([
        {id: 1, optional: [1]},
        {id: 2, optional: [2]},
        {id: 3},
    ]).filter(r.js('(function (row) { return row.optional[0] == 1; })'));
    compare(query, done);
  });

  it('js - 13', function(done) {
    var query = r.expr([1,2,3,10]).contains(r.js('(function (row) { return row.a.b > 5; })'));
    compare(query, done);
  });

  it('js - 14', function(done) {
    var query = r.expr([1,2,3,10]).forEach(r.js("(function (row) { return { \
      deleted: 0, \
      errors: 0, \
      inserted: 0, \
      replaced: 0, \
      skipped: 1, \
      unchanged: 1 \
    } })"));
    compare(query, done);
  });

  it('js - 15', function(done) {
    var query = r.expr([1,2,3,4,5,6,7]).count(r.js('(function (row) { return row > 5; })'));
    compare(query, done);
  });

  it('js - 16', function(done) {
    var query = r.js('this');
    compare(query, done);
  });

  it('js - 17', function(done) {
    var query = r.expr([1,2,3,10]).forEach(function(value) {
      return {
        deleted: 0,
        errors: 0,
        inserted: 0,
        replaced: 0,
        skipped: value,
        unchanged: 0
      };
    });
    compare(query, done);
  });

  it('coerceTo - 1', function(done) {
    var query = r.expr("foo").coerceTo("array");
    compare(query, done);
  });
  it('coerceTo - 2', function(done) {
    var query = r.expr("foo").coerceTo("string");
    compare(query, done);
  });
  it('coerceTo - 3', function(done) {
    var query = r.expr("foo").coerceTo("object");
    compare(query, done);
  });
  it('coerceTo - 4', function(done) {
    var query = r.expr("foo").coerceTo("array");
    compare(query, done);
  });
  it('coerceTo - 5', function(done) {
    var query = r.expr("foo").coerceTo("binary");
    compare(query, done);
  });
  it('coerceTo - 6', function(done) {
    var query = r.expr(2).coerceTo("array");
    compare(query, done);
  });
  it('coerceTo - 7', function(done) {
    var query = r.expr(2).coerceTo("string");
    compare(query, done);
  });
  it('coerceTo - 8', function(done) {
    var query = r.expr(2).coerceTo("object");
    compare(query, done);
  });
  it('coerceTo - 9', function(done) {
    var query = r.expr(2).coerceTo("array");
    compare(query, done);
  });
  it('coerceTo - 10', function(done) {
    var query = r.expr(2).coerceTo("binary");
    compare(query, done);
  });
  it('coerceTo - 11', function(done) {
    var query = r.expr([]).coerceTo("array");
    compare(query, done);
  });
  it('coerceTo - 12', function(done) {
    var query = r.expr([]).coerceTo("string");
    compare(query, done);
  });
  it('coerceTo - 13', function(done) {
    var query = r.expr([]).coerceTo("object");
    compare(query, done);
  });
  it('coerceTo - 14', function(done) {
    var query = r.expr([]).coerceTo("array");
    compare(query, done);
  });
  it('coerceTo - 15', function(done) {
    var query = r.expr([]).coerceTo("binary");
    compare(query, done);
  });
  it('coerceTo - 16', function(done) {
    var query = r.expr([1,2,3]).coerceTo("array");
    compare(query, done);
  });
  it('coerceTo - 17', function(done) {
    var query = r.expr([1,2,3]).coerceTo("string");
    compare(query, done, function(result) {
      return JSON.parse(result);
    });
  });
  it('coerceTo - 18', function(done) {
    var query = r.expr([1,2,3]).coerceTo("object");
    compare(query, done);
  });
  it('coerceTo - 19', function(done) {
    var query = r.expr([1,2,3]).coerceTo("array");
    compare(query, done);
  });
  it('coerceTo - 20', function(done) {
    var query = r.expr([1,2,3]).coerceTo("binary");
    compare(query, done);
  });
  it('coerceTo - 21', function(done) {
    var query = r.expr([["foo", 2]]).coerceTo("array");
    compare(query, done);
  });
  it('coerceTo - 22', function(done) {
    var query = r.expr([["foo", 2]]).coerceTo("string");
    compare(query, done, function(result) {
      return JSON.parse(result);
    });
  });
  it('coerceTo - 23', function(done) {
    var query = r.expr([["foo", 2]]).coerceTo("object");
    compare(query, done);
  });
  it('coerceTo - 24', function(done) {
    var query = r.expr([["foo", 2]]).coerceTo("array");
    compare(query, done);
  });
  it('coerceTo - 25', function(done) {
    var query = r.expr([["foo", 2]]).coerceTo("binary");
    compare(query, done);
  });
  it('coerceTo - 26', function(done) {
    var query = r.expr([["foo", 2], ["bar", 1]]).coerceTo("array");
    compare(query, done);
  });
  it('coerceTo - 27', function(done) {
    var query = r.expr([["foo", 2], ["bar", 1]]).coerceTo("string");
    compare(query, done, function(result) {
      return JSON.parse(result);
    });
  });
  it('coerceTo - 28', function(done) {
    var query = r.expr([["foo", 2], ["bar", 1]]).coerceTo("object");
    compare(query, done);
  });
  it('coerceTo - 29', function(done) {
    var query = r.expr([["foo", 2], ["bar", 1]]).coerceTo("array");
    compare(query, done);
  });
  it('coerceTo - 30', function(done) {
    var query = r.expr([["foo", 2], ["bar", 1]]).coerceTo("binary");
    compare(query, done);
  });
  it('coerceTo - 31', function(done) {
    var query = r.expr({}).coerceTo("array");
    compare(query, done);
  });
  it('coerceTo - 32', function(done) {
    var query = r.expr({}).coerceTo("string");
    compare(query, done, function(result) {
      return JSON.parse(result);
    });
  });
  it('coerceTo - 33', function(done) {
    var query = r.expr({}).coerceTo("object");
    compare(query, done);
  });
  it('coerceTo - 34', function(done) {
    var query = r.expr({}).coerceTo("array");
    compare(query, done);
  });
  it('coerceTo - 35', function(done) {
    var query = r.expr({}).coerceTo("binary");
    compare(query, done);
  });
  it('coerceTo - 36', function(done) {
    var query = r.expr({foo: "bar"}).coerceTo("array");
    compare(query, done);
  });
  it('coerceTo - 37', function(done) {
    var query = r.expr({foo: "bar"}).coerceTo("string");
    compare(query, done, function(result) {
      return JSON.parse(result);
    });
  });
  it('coerceTo - 38', function(done) {
    var query = r.expr({foo: "bar"}).coerceTo("object");
    compare(query, done);
  });
  it('coerceTo - 39', function(done) {
    var query = r.expr({foo: "bar"}).coerceTo("array");
    compare(query, done);
  });
  it('coerceTo - 40', function(done) {
    var query = r.expr({foo: "bar"}).coerceTo("binary");
    compare(query, done);
  });
  it('coerceTo - 41', function(done) {
    var query = r.expr({foo: "bar", buzz: 1}).coerceTo("array");
    compare(query, done);
  });
  it('coerceTo - 42', function(done) {
    var query = r.expr({foo: "bar", buzz: 1}).coerceTo("string");
    compare(query, done, function(result) {
      return JSON.parse(result);
    });
  });
  it('coerceTo - 43', function(done) {
    var query = r.expr({foo: "bar", buzz: 1}).coerceTo("object");
    compare(query, done);
  });
  it('coerceTo - 44', function(done) {
    var query = r.expr({foo: "bar", buzz: 1}).coerceTo("array");
    compare(query, done);
  });
  it('coerceTo - 45', function(done) {
    var query = r.expr({foo: "bar", buzz: 1}).coerceTo("binary");
    compare(query, done);
  });
  it('coerceTo - 46', function(done) {
    var query = r.expr(null).coerceTo("array");
    compare(query, done);
  });
  it('coerceTo - 47', function(done) {
    var query = r.expr(null).coerceTo("string");
    compare(query, done);
  });
  it('coerceTo - 48', function(done) {
    var query = r.expr(null).coerceTo("object");
    compare(query, done);
  });
  it('coerceTo - 49', function(done) {
    var query = r.expr(null).coerceTo("array");
    compare(query, done);
  });
  it('coerceTo - 50', function(done) {
    var query = r.expr(null).coerceTo("binary");
    compare(query, done);
  });
  it('coerceTo - 51', function(done) {
    var query = r.expr(r.binary(new Buffer("hello"))).coerceTo("array");
    compare(query, done);
  });
  it('coerceTo - 52', function(done) {
    var query = r.expr(r.binary(new Buffer("hello"))).coerceTo("string");
    compare(query, done);
  });
  it('coerceTo - 53', function(done) {
    var query = r.expr(r.binary(new Buffer("hello"))).coerceTo("object");
    compare(query, done);
  });
  it('coerceTo - 54', function(done) {
    var query = r.expr(r.binary(new Buffer("hello"))).coerceTo("array");
    compare(query, done);
  });
  it('coerceTo - 55', function(done) {
    var query = r.expr(r.binary(new Buffer("hello"))).coerceTo("binary");
    compare(query, done);
  });

  it('coerceTo - 55', function(done) {
    var query = r.expr('foo').coerceTo('bar', 'buzz');
    compare(query, done);
  });

  it('typeOf - 1', function(done) {
    var query = r.expr("foo").typeOf();
    compare(query, done);
  });
  it('typeOf - 2', function(done) {
    var query = r.expr(2).typeOf();
    compare(query, done);
  });
  it('typeOf - 3', function(done) {
    var query = r.expr([]).typeOf();
    compare(query, done);
  });
  it('typeOf - 4', function(done) {
    var query = r.expr([1,2,3]).typeOf();
    compare(query, done);
  });
  it('typeOf - 5', function(done) {
    var query = r.expr([["foo", 2]]).typeOf();
    compare(query, done);
  });
  it('typeOf - 6', function(done) {
    var query = r.expr([["foo", 2], ["bar", 1]]).typeOf();
    compare(query, done);
  });
  it('typeOf - 7', function(done) {
    var query = r.expr({}).typeOf();
    compare(query, done);
  });
  it('typeOf - 8', function(done) {
    var query = r.expr({foo: "bar"}).typeOf();
    compare(query, done);
  });
  it('typeOf - 9', function(done) {
    var query = r.expr({foo: "bar", buzz: 1}).typeOf();
    compare(query, done);
  });
  it('typeOf - 10', function(done) {
    var query = r.expr(null).typeOf();
    compare(query, done);
  });
  it('typeOf - 11', function(done) {
    var query = r.expr(r.binary(new Buffer("hello"))).typeOf();
    compare(query, done);
  });
  it('typeOf - 12', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).typeOf();
    compare(query, done);
  });
  it('typeOf - 13', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).between(r.minval, r.maxval).typeOf();
    compare(query, done);
  });
  it('typeOf - 13', function(done) {
    var query = r.js('(function(row) { return row })').typeOf();
    compare(query, done);
  });
  it('typeOf - 14', function(done) {
    var query = r.js('NaN').typeOf();
    compare(query, done);
  });

  it('json - 1', function(done) {
    var query = r.json("[1,2,3]");
    compare(query, done);
  });

  it('json - 2', function(done) {
    var query = r.json("[1,]");
    compare(query, done);
  });

  it('json - 3', function(done) {
    var query = r.json(2);
    compare(query, done);
  });

  it('json - 4', function(done) {
    var query = r.json('foo', 'bar');
    compare(query, done);
  });

  it('json - 5', function(done) {
    var query = r.json("[1,2,3]").count();
    compare(query, done);
  });

  it('json - 6', function(done) {
    var query = r.json(JSON.stringify([1,2,3]));
    compare(query, done);
  });

  it('json - 7', function(done) {
    var query = r.json(JSON.stringify({foo: 'bar', buzz: [1,2,3]}));
    compare(query, done);
  });

  it('json - 8', function(done) {
    var query = r.json(JSON.stringify({foo: 'bar', buzz: [1,2,3]}));
    compare(query, done);
  });

  it('json - 9', function(done) {
    var query = r.json('[1,2,');
    compare(query, done);
  });

  it('toJSON - 1', function(done) {
    var query = r.expr({foo: 'bar', buzz: [1,2,3]}).toJSON();
    compare(query, done, function(result) {
      return JSON.parse(result);
    });
  });

  it('toJSON - 2', function(done) {
    var query = r.expr({foo: 'bar', buzz: [1,2,3]}).toJsonString();
    compare(query, done, function(result) {
      return JSON.parse(result);
    });
  });

  it('toJSON - 3', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).toJSON();
    compare(query, done);
  });

  it('toJSON - 4', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).filter(true).toJSON();
    compare(query, done);
  });

  it('toJSON - 5', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).get(1).toJSON();
    compare(query, done, function(result) {
      return JSON.parse(result);
    });
  });

  it('toJSON - 6', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).orderBy('id').toJSON();
    compare(query, done);
  });

  it('toJSON - 7', function(done) {
    var query = r.expr(JSON.stringify({foo: 'bar'})).toJSON('foo');
    compare(query, done);
  });

  it('uuid - 1', function(done) {
    var query = r.uuid();
    compare(query, done, function(e) {
      assert(/^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/.test(e));
      return(/^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/.test(e));
    });
  });
  it('uuid - 2', function(done) {
    var query = r.uuid('foo');
    compare(query, done);
  });

  /*
  it('forEach - 1', function(done) {
    var query = r.expr([
        {id: 10},
        {id: 1},
        {id: 11},
    ]).forEach(function(value) {
      return r.db(TEST_DB).table(TEST_TABLE).insert(value);
    });
    compare(query, done, function(result) {
      result.first_error = result.first_error.split(':')[0];
      assert(result.first_error.length > 0);
      return result;
    });
  });

  it('forEach - 2', function(done) {
    var query = r.expr([1,2,3,10]).forEach({
      deleted: 0,
      errors: 0,
      inserted: 0,
      replaced: 0,
      skipped: 1,
      unchanged: 1
    });
    compare(query, done);
  });

  it('forEach - 3', function(done) {
    var query = r.expr([
        {id: 1000},
        {id: 1},
        {id: 1001},
    ]).forEach(function(value) {
      return r.db(TEST_DB).table(TEST_TABLE).insert(value, {returnChanges: true});
    });
    compare(query, done, function(result) {
      result.first_error = result.first_error.split(':')[0];
      assert(result.first_error.length > 0);
      return result;
    });
  });

  it('forEach - 4', function(done) {
    var query = r.expr([1,r.db(TEST_DB).table(TEST_TABLE),3,10]).forEach({
      deleted: 0,
      errors: 0,
      inserted: 0,
      replaced: 0,
      skipped: 1,
      unchanged: 1
    });
    compare(query, done);
  });

  it('forEach - 5', function(done) {
    var query = r.expr([1,r.error('foo'),3,10]).forEach({
      deleted: 0,
      errors: 0,
      inserted: 0,
      replaced: 0,
      skipped: 1,
      unchanged: 1
    });
    compare(query, done);
  });

  it('forEach - 6', function(done) {
    var query = r.expr('foo').forEach({
      deleted: 0,
      errors: 0,
      inserted: 0,
      replaced: 0,
      skipped: 1,
      unchanged: 1
    });
    compare(query, done);
  });

  it('forEach - 6', function(done) {
    var query = r.expr('foo').forEach('foo', 'bar');
    compare(query, done);
  });

  it('asc - 1', function(done) {
    // See https://github.com/rethinkdb/rethinkdb/issues/4951
    var query = r.expr({foo: r.asc('foo')});
    compare(query, done, function(error) {
      return /^ASC may only be used as an argument to ORDER_BY/.test(error);
    });
  });

  // Require some work to track frames
//  it('asc - 2', function(done) {
//    var query = r.expr(1).do(function(foo) {
//      return r.asc('foo')
//    })
//    compare(query, done);
//  });

  it('desc - 1', function(done) {
    var query = r.expr({foo: r.desc('foo')});
    compare(query, done, function(error) {
      return /^ASC may only be used as an argument to ORDER_BY/.test(error);
    });
  });

  it('info - 1', function(done) {
    var query = r.expr("foo").info();
    compare(query, done);
  });

  it('info - 2', function(done) {
    var query = r.expr("").info();
    compare(query, done);
  });

  it('info - 3', function(done) {
    var query = r.expr(0).info();
    compare(query, done);
  });

  it('info - 4', function(done) {
    var query = r.expr(1).info();
    compare(query, done);
  });

  it('info - 5', function(done) {
    var query = r.expr(-1).info();
    compare(query, done);
  });

  it('info - 6', function(done) {
    var query = r.expr(10000).info();
    compare(query, done);
  });

  it('info - 7', function(done) {
    var query = r.expr(true).info();
    compare(query, done);
  });

  it('info - 8', function(done) {
    var query = r.expr(false).info();
    compare(query, done);
  });

  it('info - 9', function(done) {
    var query = r.expr([]).info();
    compare(query, done);
  });

  it('info - 10', function(done) {
    var query = r.expr([1,2,3]).info();
    compare(query, done, function(result) {
      result.value = JSON.parse(result.value);
      return result;
    });
  });

  it('info - 11', function(done) {
    var query = r.expr([["foo", 2]]).info();
    compare(query, done, function(result) {
      result.value = JSON.parse(result.value);
      return result;
    });
  });

  it('info - 12', function(done) {
    var query = r.expr([["foo", 2], ["bar", 1]]).info();
    compare(query, done, function(result) {
      result.value = JSON.parse(result.value);
      return result;
    });
  });

  it('info - 13', function(done) {
    var query = r.expr({}).info();
    compare(query, done, function(result) {
      result.value = JSON.parse(result.value);
      return result;
    });
  });

  it('info - 14', function(done) {
    var query = r.expr({foo: "bar"}).info();
    compare(query, done, function(result) {
      result.value = JSON.parse(result.value);
      return result;
    });
  });

  it('info - 15', function(done) {
    var query = r.expr({foo: "bar", buzz: 1}).info();
    compare(query, done, function(result) {
      result.value = JSON.parse(result.value);
      return result;
    });
  });

  it('info - 16', function(done) {
    var query = r.expr(null).info();
    compare(query, done);
  });

  it('info - 17', function(done) {
    var query = r.expr(r.time(1986, 11, 3, 'Z')).info();
    compare(query, done, function(result) {
      result.value = JSON.parse(result.value);
      return result;
    });
  });

//  it('info - 18', function(done) {
//    var query = r.expr(function(x) { return x.add(1) }).info()
//    //compare(query, done);
//    compare(query, done, function(e) { console.log(''); console.log(e); return e; });
//  })

  it('info - 19', function(done) {
    var query = r.expr(r.db(TEST_DB).table(TEST_TABLE)).info();
    compare(query, done, function(result) {
      delete result.id;
      delete result.db.id;
    });
  });

  it('info - 20', function(done) {
    var query = r.expr(r.binary(new Buffer("hello"))).info();
    compare(query, done, function(result) {
      result.value = JSON.parse(result.value);
      return result;
    });
  });

  //TODO Uncomment r.range - 5
  it('http - 1', function(done) {
    var query = r.http('http://httpbin.org/get');
    compare(query, done, function(result) {
      // The distribution may be added in the user agent (like on Travis)
      delete result.headers["User-Agent"];
      return result;
    });
  });

  it('http - 2', function(done) {
    var query = r.expr([1,2,3, r.http('http://httpbin.org/get')]);
    compare(query, done, function(result) {
      // The distribution may be added in the user agent (like on Travis)
      delete result[3].headers["User-Agent"];
      return result;
    });
  });

  /*
  it('range - 5', function(done) { // less than one batch
    var query = r.range();
    compare(query, done, function(stream1, stream2) {
      var index = 50;
      var result = {
        stream1: [],
        stream2: []
      }
      function get() {
        stream1.next().then(function(row) {
          result.stream1.push(row);
          return stream2.next();
        }).then(function(row) {
          result.stream2.push(row);
          if (index-- > 0) {
            get();
          }
          else {
            assert.deepEqual(result.stream1, result.stream2);
            done();
          }
        })
      }
      get();
    }, true);
  });
  */

  /*
    //compare(query, done, function(e) { console.log(JSON.stringify(e, null, 4)); return e; });
  */
});
