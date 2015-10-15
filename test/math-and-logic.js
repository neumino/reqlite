var config = require('./../config.js');

var r = require('rethinkdb');
var assert = require('assert');

var connections = {};
var TEST_DB = 'reqlitetest';
var TEST_TABLE = 'reqlitetestmathandlogic';
var MISSING_ID = 'nonExistingId';
var MISSING_FIELD = 'nonExistingField';
var MISSING_INDEX = 'nonExistingIndex';
var MIN_RANDOM = 1000;
var MAX_RANDOM = 100000;

var compare = require('./util.js').generateCompare(connections);

describe('math-and-logic.js', function(){
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
    }, 300);
  });

  it('add - 1', function(done) {
    var query = r.expr(2).add(3);
    compare(query, done);
  });

  it('add - 2', function(done) {
    var query = r.expr('foo').add('bar');
    compare(query, done);
  });

  it('add - 3', function(done) {
    var query = r.expr(['foo', 'bar']).add(['buzz']);
    compare(query, done);
  });

  it('add - 4', function(done) {
    var query = r.ISO8601('1986-11-03T08:30:00-07:00').add(100);
    compare(query, done);
  });

  it('add - 5', function(done) {
    var query = r.expr(2).add('foo');
    compare(query, done);
  });

  it('add - 6', function(done) {
    var query = r.expr(2).add(4,5,6,9);
    compare(query, done);
  });

  it('add - 7', function(done) {
    var query = r.add();
    compare(query, done);
  });

  it('add - 8', function(done) {
    var query = r.add(r.db(TEST_DB).table(TEST_TABLE));
    compare(query, done);
  });

  it('add - 9', function(done) {
    var query = r.expr(1).add(r.db(TEST_DB).table(TEST_TABLE));
    compare(query, done);
  });

  it('add - 10', function(done) {
    var query = r.expr(1).add(2, r.db(TEST_DB).table(TEST_TABLE));
    compare(query, done);
  });

  it('sub - 1', function(done) {
    var query = r.expr(2).add(-2);
    compare(query, done);
  });

  it('sub - 2', function(done) {
    var query = r.ISO8601('1986-11-03T08:30:00-07:00').sub(100);
    compare(query, done);
  });

  it('sub - 3', function(done) {
    var query = r.ISO8601('1986-11-03T08:30:00-07:00').sub(r.ISO8601('1986-02-03T08:30:00-07:00'));
    compare(query, done);
  });

  it('sub - 4', function(done) {
    var query = r.ISO8601('1986-11-03T08:30:00-07:00').sub('foo');
    compare(query, done);
  });

  it('sub - 4', function(done) {
    var query = r.expr(10).sub(2, 3,4);
    compare(query, done);
  });

  it('sub - 5', function(done) {
    var query = r.sub();
    compare(query, done);
  });

  it('sub - 6', function(done) {
    var query = r.sub(r.db(TEST_DB).table(TEST_TABLE));
    compare(query, done);
  });

  it('sub - 7', function(done) {
    var query = r.expr(1).sub(r.db(TEST_DB).table(TEST_TABLE));
    compare(query, done);
  });

  it('sub - 8', function(done) {
    var query = r.expr(1).sub(2, r.db(TEST_DB).table(TEST_TABLE));
    compare(query, done);
  });

  it('sub - 9', function(done) {
    var query = r.expr('foo').sub('oo');
    compare(query, done);
  });

  it('sub - 10', function(done) {
    var query = r.expr(2).sub('foo');
    compare(query, done);
  });

  it('mul - 1', function(done) {
    var query = r.expr(2).mul(5);
    compare(query, done);
  });

  it('mul - 2', function(done) {
    var query = r.expr([2]).mul(5);
    compare(query, done);
  });

  it('mul - 3', function(done) {
    var query = r.expr(2).mul('foo');
    compare(query, done);
  });

  it('mul - 4', function(done) {
    var query = r.expr([19]).mul('foo');
    compare(query, done);
  });

  it('mul - 5', function(done) {
    var query = r.expr(2).mul(3, 4, 5);
    compare(query, done);
  });

  it('mul - 6', function(done) {
    var query = r.mul();
    compare(query, done);
  });

  it('mul - 7', function(done) {
    var query = r.mul(r.db(TEST_DB).table(TEST_TABLE));
    compare(query, done);
  });

  it('mul - 8', function(done) {
    var query = r.expr(1).mul(r.db(TEST_DB).table(TEST_TABLE));
    compare(query, done);
  });

  it('mul - 9', function(done) {
    var query = r.expr(1).mul(2, r.db(TEST_DB).table(TEST_TABLE));
    compare(query, done);
  });

  it('mul - 10', function(done) {
    var query = r.expr(2).mul('foo');
    compare(query, done);
  });

  it('mul - 11', function(done) {
    var query = r.expr('foo').mul(2);
    compare(query, done);
  });

  it('div - 1', function(done) {
    var query = r.expr(12).div(3);
    compare(query, done);
  });

  it('div - 2', function(done) {
    var query = r.expr(12).div(1);
    compare(query, done);
  });

  it('div - 3', function(done) {
    var query = r.expr(12).div(0);
    compare(query, done);
  });

  it('div - 4', function(done) {
    var query = r.expr(12).div('foo');
    compare(query, done);
  });

  it('div - 5', function(done) {
    var query = r.div();
    compare(query, done);
  });

  it('div - 6', function(done) {
    var query = r.div(r.db(TEST_DB).table(TEST_TABLE));
    compare(query, done);
  });

  it('div - 7', function(done) {
    var query = r.expr(1).div(r.db(TEST_DB).table(TEST_TABLE));
    compare(query, done);
  });

  it('div - 8', function(done) {
    var query = r.expr(1).div(2, r.db(TEST_DB).table(TEST_TABLE));
    compare(query, done);
  });

  it('div - 9', function(done) {
    var query = r.expr(2).div('foo');
    compare(query, done);
  });

  it('div - 10', function(done) {
    var query = r.expr('foo').div(2);
    compare(query, done);
  });

  it('mod - 1', function(done) {
    var query = r.expr(12).mod(7);
    compare(query, done);
  });

  it('mod - 2', function(done) {
    var query = r.expr(12).mod(-7);
    compare(query, done);
  });

  it('mod - 3', function(done) {
    var query = r.expr(-12).mod(7);
    compare(query, done);
  });

  it('mod - 4', function(done) {
    var query = r.expr(-12).mod(-7);
    compare(query, done);
  });

  it('mod - 5', function(done) {
    var query = r.expr(12).mod('foo');
    compare(query, done);
  });

  it('mod - 6', function(done) {
    var query = r.expr(12).mod(0);
    compare(query, done);
  });

  it('mod - 7', function(done) {
    var query = r.expr('foo').mod(0);
    compare(query, done);
  });

  it('mod - 8', function(done) {
    var query = r.expr(12).mod(2.5);
    compare(query, done);
  });

  it('mod - 9', function(done) {
    var query = r.expr(12.5).mod(2.5);
    compare(query, done);
  });

  it('mod - 10', function(done) {
    var query = r.mod();
    compare(query, done);
  });

  it('mod - 11', function(done) {
    var query = r.mod(r.db(TEST_DB).table(TEST_TABLE));
    compare(query, done);
  });

  it('mod - 12', function(done) {
    var query = r.expr(1).mod(r.db(TEST_DB).table(TEST_TABLE));
    compare(query, done);
  });

  it('mod - 13', function(done) {
    var query = r.expr(1).mod(2, r.db(TEST_DB).table(TEST_TABLE));
    compare(query, done);
  });

  it('mod - 14', function(done) {
    var query = r.expr(2).mod('foo');
    compare(query, done);
  });

  it('mod - 15', function(done) {
    var query = r.expr('foo').mod(2);
    compare(query, done);
  });

  it('and - 1', function(done) {
    var query = r.expr("foo").and("bar");
    compare(query, done);
  });

  it('and - 2', function(done) {
    var query = r.expr("foo").and("");
    compare(query, done);
  });

  it('and - 3', function(done) {
    var query = r.expr("foo").and(0);
    compare(query, done);
  });

  it('and - 4', function(done) {
    var query = r.expr("foo").and(1);
    compare(query, done);
  });

  it('and - 5', function(done) {
    var query = r.expr("foo").and(-1);
    compare(query, done);
  });

  it('and - 6', function(done) {
    var query = r.expr("foo").and(10000);
    compare(query, done);
  });

  it('and - 7', function(done) {
    var query = r.expr("foo").and(true);
    compare(query, done);
  });

  it('and - 8', function(done) {
    var query = r.expr("foo").and(false);
    compare(query, done);
  });

  it('and - 9', function(done) {
    var query = r.expr("foo").and([]);
    compare(query, done);
  });

  it('and - 10', function(done) {
    var query = r.expr("foo").and([1,2,3]);
    compare(query, done);
  });

  it('and - 11', function(done) {
    var query = r.expr("foo").and([["foo", 2]]);
    compare(query, done);
  });

  it('and - 12', function(done) {
    var query = r.expr("foo").and([["foo", 2], ["bar", 1]]);
    compare(query, done);
  });

  it('and - 13', function(done) {
    var query = r.expr("foo").and({});
    compare(query, done);
  });

  it('and - 14', function(done) {
    var query = r.expr("foo").and({foo: "bar"});
    compare(query, done);
  });

  it('and - 15', function(done) {
    var query = r.expr("foo").and({foo: "bar", buzz: 1});
    compare(query, done);
  });

  it('and - 16', function(done) {
    var query = r.expr("foo").and(null);
    compare(query, done);
  });

  it('and - 17', function(done) {
    var query = r.expr("foo").and(r.now());
    compare(query, done, function(value) {
      return value instanceof Date;
    });
  });

  it('and - 18', function(done) {
    var query = r.expr("foo").and(function() { return 1; });
    compare(query, done);
  });

  it('and - 19', function(done) {
    var query = r.expr("foo").and(r.db(TEST_DB).table(TEST_TABLE));
    compare(query, done);
  });

  it('and - 20', function(done) {
    var query = r.expr("foo").and(r.binary(new Buffer("hello")));
    compare(query, done);
  });

  it('and - 21', function(done) {
    var query = r.expr("").and("foo");
    compare(query, done);
  });

  it('and - 22', function(done) {
    var query = r.expr("").and("");
    compare(query, done);
  });

  it('and - 23', function(done) {
    var query = r.expr("").and(0);
    compare(query, done);
  });

  it('and - 24', function(done) {
    var query = r.expr("").and(1);
    compare(query, done);
  });

  it('and - 25', function(done) {
    var query = r.expr("").and(-1);
    compare(query, done);
  });

  it('and - 26', function(done) {
    var query = r.expr("").and(10000);
    compare(query, done);
  });

  it('and - 27', function(done) {
    var query = r.expr("").and(true);
    compare(query, done);
  });

  it('and - 28', function(done) {
    var query = r.expr("").and(false);
    compare(query, done);
  });

  it('and - 29', function(done) {
    var query = r.expr("").and([]);
    compare(query, done);
  });

  it('and - 30', function(done) {
    var query = r.expr("").and([1,2,3]);
    compare(query, done);
  });

  it('and - 31', function(done) {
    var query = r.expr("").and([["foo", 2]]);
    compare(query, done);
  });

  it('and - 32', function(done) {
    var query = r.expr("").and([["foo", 2], ["bar", 1]]);
    compare(query, done);
  });

  it('and - 33', function(done) {
    var query = r.expr("").and({});
    compare(query, done);
  });

  it('and - 34', function(done) {
    var query = r.expr("").and({foo: "bar"});
    compare(query, done);
  });

  it('and - 35', function(done) {
    var query = r.expr("").and({foo: "bar", buzz: 1});
    compare(query, done);
  });

  it('and - 36', function(done) {
    var query = r.expr("").and(null);
    compare(query, done);
  });

  it('and - 37', function(done) {
    var query = r.expr("").and(r.now());
    compare(query, done, function(value) {
      return value instanceof Date;
    });
  });

  it('and - 38', function(done) {
    var query = r.expr("").and(function() { return 1; });
    compare(query, done);
  });

  it('and - 39', function(done) {
    var query = r.expr("").and(r.db(TEST_DB).table(TEST_TABLE));
    compare(query, done);
  });

  it('and - 40', function(done) {
    var query = r.expr("").and(r.binary(new Buffer("hello")));
    compare(query, done);
  });

  it('and - 41', function(done) {
    var query = r.expr(0).and("foo");
    compare(query, done);
  });

  it('and - 42', function(done) {
    var query = r.expr(0).and("");
    compare(query, done);
  });

  it('and - 43', function(done) {
    var query = r.expr(0).and(0);
    compare(query, done);
  });

  it('and - 44', function(done) {
    var query = r.expr(0).and(1);
    compare(query, done);
  });

  it('and - 45', function(done) {
    var query = r.expr(0).and(-1);
    compare(query, done);
  });

  it('and - 46', function(done) {
    var query = r.expr(0).and(10000);
    compare(query, done);
  });

  it('and - 47', function(done) {
    var query = r.expr(0).and(true);
    compare(query, done);
  });

  it('and - 48', function(done) {
    var query = r.expr(0).and(false);
    compare(query, done);
  });

  it('and - 49', function(done) {
    var query = r.expr(0).and([]);
    compare(query, done);
  });

  it('and - 50', function(done) {
    var query = r.expr(0).and([1,2,3]);
    compare(query, done);
  });

  it('and - 51', function(done) {
    var query = r.expr(0).and([["foo", 2]]);
    compare(query, done);
  });

  it('and - 52', function(done) {
    var query = r.expr(0).and([["foo", 2], ["bar", 1]]);
    compare(query, done);
  });

  it('and - 53', function(done) {
    var query = r.expr(0).and({});
    compare(query, done);
  });

  it('and - 54', function(done) {
    var query = r.expr(0).and({foo: "bar"});
    compare(query, done);
  });

  it('and - 55', function(done) {
    var query = r.expr(0).and({foo: "bar", buzz: 1});
    compare(query, done);
  });

  it('and - 56', function(done) {
    var query = r.expr(0).and(null);
    compare(query, done);
  });

  it('and - 57', function(done) {
    var query = r.expr(0).and(r.now());
    compare(query, done, function(value) {
      return value instanceof Date;
    });
  });

  it('and - 58', function(done) {
    var query = r.expr(0).and(function() { return 1; });
    compare(query, done);
  });

  it('and - 59', function(done) {
    var query = r.expr(0).and(r.db(TEST_DB).table(TEST_TABLE));
    compare(query, done);
  });

  it('and - 60', function(done) {
    var query = r.expr(0).and(r.binary(new Buffer("hello")));
    compare(query, done);
  });

  it('and - 61', function(done) {
    var query = r.expr(1).and("foo");
    compare(query, done);
  });

  it('and - 62', function(done) {
    var query = r.expr(1).and("");
    compare(query, done);
  });

  it('and - 63', function(done) {
    var query = r.expr(1).and(0);
    compare(query, done);
  });

  it('and - 64', function(done) {
    var query = r.expr(1).and(1);
    compare(query, done);
  });

  it('and - 65', function(done) {
    var query = r.expr(1).and(-1);
    compare(query, done);
  });

  it('and - 66', function(done) {
    var query = r.expr(1).and(10000);
    compare(query, done);
  });

  it('and - 67', function(done) {
    var query = r.expr(1).and(true);
    compare(query, done);
  });

  it('and - 68', function(done) {
    var query = r.expr(1).and(false);
    compare(query, done);
  });

  it('and - 69', function(done) {
    var query = r.expr(1).and([]);
    compare(query, done);
  });

  it('and - 70', function(done) {
    var query = r.expr(1).and([1,2,3]);
    compare(query, done);
  });

  it('and - 71', function(done) {
    var query = r.expr(1).and([["foo", 2]]);
    compare(query, done);
  });

  it('and - 72', function(done) {
    var query = r.expr(1).and([["foo", 2], ["bar", 1]]);
    compare(query, done);
  });

  it('and - 73', function(done) {
    var query = r.expr(1).and({});
    compare(query, done);
  });

  it('and - 74', function(done) {
    var query = r.expr(1).and({foo: "bar"});
    compare(query, done);
  });

  it('and - 75', function(done) {
    var query = r.expr(1).and({foo: "bar", buzz: 1});
    compare(query, done);
  });

  it('and - 76', function(done) {
    var query = r.expr(1).and(null);
    compare(query, done);
  });

  it('and - 77', function(done) {
    var query = r.expr(1).and(r.now());
    compare(query, done, function(value) {
      return value instanceof Date;
    });
  });

  it('and - 78', function(done) {
    var query = r.expr(1).and(function() { return 1; });
    compare(query, done);
  });

  it('and - 79', function(done) {
    var query = r.expr(1).and(r.db(TEST_DB).table(TEST_TABLE));
    compare(query, done);
  });

  it('and - 80', function(done) {
    var query = r.expr(1).and(r.binary(new Buffer("hello")));
    compare(query, done);
  });

  it('and - 81', function(done) {
    var query = r.expr(-1).and("foo");
    compare(query, done);
  });

  it('and - 82', function(done) {
    var query = r.expr(-1).and("");
    compare(query, done);
  });

  it('and - 83', function(done) {
    var query = r.expr(-1).and(0);
    compare(query, done);
  });

  it('and - 84', function(done) {
    var query = r.expr(-1).and(1);
    compare(query, done);
  });

  it('and - 85', function(done) {
    var query = r.expr(-1).and(-1);
    compare(query, done);
  });

  it('and - 86', function(done) {
    var query = r.expr(-1).and(10000);
    compare(query, done);
  });

  it('and - 87', function(done) {
    var query = r.expr(-1).and(true);
    compare(query, done);
  });

  it('and - 88', function(done) {
    var query = r.expr(-1).and(false);
    compare(query, done);
  });

  it('and - 89', function(done) {
    var query = r.expr(-1).and([]);
    compare(query, done);
  });

  it('and - 90', function(done) {
    var query = r.expr(-1).and([1,2,3]);
    compare(query, done);
  });

  it('and - 91', function(done) {
    var query = r.expr(-1).and([["foo", 2]]);
    compare(query, done);
  });

  it('and - 92', function(done) {
    var query = r.expr(-1).and([["foo", 2], ["bar", 1]]);
    compare(query, done);
  });

  it('and - 93', function(done) {
    var query = r.expr(-1).and({});
    compare(query, done);
  });

  it('and - 94', function(done) {
    var query = r.expr(-1).and({foo: "bar"});
    compare(query, done);
  });

  it('and - 95', function(done) {
    var query = r.expr(-1).and({foo: "bar", buzz: 1});
    compare(query, done);
  });

  it('and - 96', function(done) {
    var query = r.expr(-1).and(null);
    compare(query, done);
  });

  it('and - 97', function(done) {
    var query = r.expr(-1).and(r.now());
    compare(query, done, function(value) {
      return value instanceof Date;
    });
  });

  it('and - 98', function(done) {
    var query = r.expr(-1).and(function() { return 1; });
    compare(query, done);
  });

  it('and - 99', function(done) {
    var query = r.expr(-1).and(r.db(TEST_DB).table(TEST_TABLE));
    compare(query, done);
  });

  it('and - 100', function(done) {
    var query = r.expr(-1).and(r.binary(new Buffer("hello")));
    compare(query, done);
  });

  it('and - 101', function(done) {
    var query = r.expr(10000).and("foo");
    compare(query, done);
  });

  it('and - 102', function(done) {
    var query = r.expr(10000).and("");
    compare(query, done);
  });

  it('and - 103', function(done) {
    var query = r.expr(10000).and(0);
    compare(query, done);
  });

  it('and - 104', function(done) {
    var query = r.expr(10000).and(1);
    compare(query, done);
  });

  it('and - 105', function(done) {
    var query = r.expr(10000).and(-1);
    compare(query, done);
  });

  it('and - 106', function(done) {
    var query = r.expr(10000).and(10000);
    compare(query, done);
  });

  it('and - 107', function(done) {
    var query = r.expr(10000).and(true);
    compare(query, done);
  });

  it('and - 108', function(done) {
    var query = r.expr(10000).and(false);
    compare(query, done);
  });

  it('and - 109', function(done) {
    var query = r.expr(10000).and([]);
    compare(query, done);
  });

  it('and - 110', function(done) {
    var query = r.expr(10000).and([1,2,3]);
    compare(query, done);
  });

  it('and - 111', function(done) {
    var query = r.expr(10000).and([["foo", 2]]);
    compare(query, done);
  });

  it('and - 112', function(done) {
    var query = r.expr(10000).and([["foo", 2], ["bar", 1]]);
    compare(query, done);
  });

  it('and - 113', function(done) {
    var query = r.expr(10000).and({});
    compare(query, done);
  });

  it('and - 114', function(done) {
    var query = r.expr(10000).and({foo: "bar"});
    compare(query, done);
  });

  it('and - 115', function(done) {
    var query = r.expr(10000).and({foo: "bar", buzz: 1});
    compare(query, done);
  });

  it('and - 116', function(done) {
    var query = r.expr(10000).and(null);
    compare(query, done);
  });

  it('and - 117', function(done) {
    var query = r.expr(10000).and(r.now());
    compare(query, done, function(value) {
      return value instanceof Date;
    });
  });

  it('and - 118', function(done) {
    var query = r.expr(10000).and(function() { return 1; });
    compare(query, done);
  });

  it('and - 119', function(done) {
    var query = r.expr(10000).and(r.db(TEST_DB).table(TEST_TABLE));
    compare(query, done);
  });

  it('and - 120', function(done) {
    var query = r.expr(10000).and(r.binary(new Buffer("hello")));
    compare(query, done);
  });

  it('and - 121', function(done) {
    var query = r.expr(true).and("foo");
    compare(query, done);
  });

  it('and - 122', function(done) {
    var query = r.expr(true).and("");
    compare(query, done);
  });

  it('and - 123', function(done) {
    var query = r.expr(true).and(0);
    compare(query, done);
  });

  it('and - 124', function(done) {
    var query = r.expr(true).and(1);
    compare(query, done);
  });

  it('and - 125', function(done) {
    var query = r.expr(true).and(-1);
    compare(query, done);
  });

  it('and - 126', function(done) {
    var query = r.expr(true).and(10000);
    compare(query, done);
  });

  it('and - 127', function(done) {
    var query = r.expr(true).and(true);
    compare(query, done);
  });

  it('and - 128', function(done) {
    var query = r.expr(true).and(false);
    compare(query, done);
  });

  it('and - 129', function(done) {
    var query = r.expr(true).and([]);
    compare(query, done);
  });

  it('and - 130', function(done) {
    var query = r.expr(true).and([1,2,3]);
    compare(query, done);
  });

  it('and - 131', function(done) {
    var query = r.expr(true).and([["foo", 2]]);
    compare(query, done);
  });

  it('and - 132', function(done) {
    var query = r.expr(true).and([["foo", 2], ["bar", 1]]);
    compare(query, done);
  });

  it('and - 133', function(done) {
    var query = r.expr(true).and({});
    compare(query, done);
  });

  it('and - 134', function(done) {
    var query = r.expr(true).and({foo: "bar"});
    compare(query, done);
  });

  it('and - 135', function(done) {
    var query = r.expr(true).and({foo: "bar", buzz: 1});
    compare(query, done);
  });

  it('and - 136', function(done) {
    var query = r.expr(true).and(null);
    compare(query, done);
  });

  it('and - 137', function(done) {
    var query = r.expr(true).and(r.now());
    compare(query, done, function(value) {
      return value instanceof Date;
    });
  });

  it('and - 138', function(done) {
    var query = r.expr(true).and(function() { return 1; });
    compare(query, done);
  });

  it('and - 139', function(done) {
    var query = r.expr(true).and(r.db(TEST_DB).table(TEST_TABLE));
    compare(query, done);
  });

  it('and - 140', function(done) {
    var query = r.expr(true).and(r.binary(new Buffer("hello")));
    compare(query, done);
  });

  it('and - 141', function(done) {
    var query = r.expr(false).and("foo");
    compare(query, done);
  });

  it('and - 142', function(done) {
    var query = r.expr(false).and("");
    compare(query, done);
  });

  it('and - 143', function(done) {
    var query = r.expr(false).and(0);
    compare(query, done);
  });

  it('and - 144', function(done) {
    var query = r.expr(false).and(1);
    compare(query, done);
  });

  it('and - 145', function(done) {
    var query = r.expr(false).and(-1);
    compare(query, done);
  });

  it('and - 146', function(done) {
    var query = r.expr(false).and(10000);
    compare(query, done);
  });

  it('and - 147', function(done) {
    var query = r.expr(false).and(true);
    compare(query, done);
  });

  it('and - 148', function(done) {
    var query = r.expr(false).and(false);
    compare(query, done);
  });

  it('and - 149', function(done) {
    var query = r.expr(false).and([]);
    compare(query, done);
  });

  it('and - 150', function(done) {
    var query = r.expr(false).and([1,2,3]);
    compare(query, done);
  });

  it('and - 151', function(done) {
    var query = r.expr(false).and([["foo", 2]]);
    compare(query, done);
  });

  it('and - 152', function(done) {
    var query = r.expr(false).and([["foo", 2], ["bar", 1]]);
    compare(query, done);
  });

  it('and - 153', function(done) {
    var query = r.expr(false).and({});
    compare(query, done);
  });

  it('and - 154', function(done) {
    var query = r.expr(false).and({foo: "bar"});
    compare(query, done);
  });

  it('and - 155', function(done) {
    var query = r.expr(false).and({foo: "bar", buzz: 1});
    compare(query, done);
  });

  it('and - 156', function(done) {
    var query = r.expr(false).and(null);
    compare(query, done);
  });

  it('and - 157', function(done) {
    var query = r.expr(false).and(r.now());
    compare(query, done, function(value) {
      return value instanceof Date;
    });
  });

  it('and - 158', function(done) {
    var query = r.expr(false).and(function() { return 1; });
    compare(query, done);
  });

  it('and - 159', function(done) {
    var query = r.expr(false).and(r.db(TEST_DB).table(TEST_TABLE));
    compare(query, done);
  });

  it('and - 160', function(done) {
    var query = r.expr(false).and(r.binary(new Buffer("hello")));
    compare(query, done);
  });

  it('and - 161', function(done) {
    var query = r.expr([]).and("foo");
    compare(query, done);
  });

  it('and - 162', function(done) {
    var query = r.expr([]).and("");
    compare(query, done);
  });

  it('and - 163', function(done) {
    var query = r.expr([]).and(0);
    compare(query, done);
  });

  it('and - 164', function(done) {
    var query = r.expr([]).and(1);
    compare(query, done);
  });

  it('and - 165', function(done) {
    var query = r.expr([]).and(-1);
    compare(query, done);
  });

  it('and - 166', function(done) {
    var query = r.expr([]).and(10000);
    compare(query, done);
  });

  it('and - 167', function(done) {
    var query = r.expr([]).and(true);
    compare(query, done);
  });

  it('and - 168', function(done) {
    var query = r.expr([]).and(false);
    compare(query, done);
  });

  it('and - 169', function(done) {
    var query = r.expr([]).and([]);
    compare(query, done);
  });

  it('and - 170', function(done) {
    var query = r.expr([]).and([1,2,3]);
    compare(query, done);
  });

  it('and - 171', function(done) {
    var query = r.expr([]).and([["foo", 2]]);
    compare(query, done);
  });

  it('and - 172', function(done) {
    var query = r.expr([]).and([["foo", 2], ["bar", 1]]);
    compare(query, done);
  });

  it('and - 173', function(done) {
    var query = r.expr([]).and({});
    compare(query, done);
  });

  it('and - 174', function(done) {
    var query = r.expr([]).and({foo: "bar"});
    compare(query, done);
  });

  it('and - 175', function(done) {
    var query = r.expr([]).and({foo: "bar", buzz: 1});
    compare(query, done);
  });

  it('and - 176', function(done) {
    var query = r.expr([]).and(null);
    compare(query, done);
  });

  it('and - 177', function(done) {
    var query = r.expr([]).and(r.now());
    compare(query, done, function(value) {
      return value instanceof Date;
    });
  });

  it('and - 178', function(done) {
    var query = r.expr([]).and(function() { return 1; });
    compare(query, done);
  });

  it('and - 179', function(done) {
    var query = r.expr([]).and(r.db(TEST_DB).table(TEST_TABLE));
    compare(query, done);
  });

  it('and - 180', function(done) {
    var query = r.expr([]).and(r.binary(new Buffer("hello")));
    compare(query, done);
  });

  it('and - 181', function(done) {
    var query = r.expr([1,2,3]).and("foo");
    compare(query, done);
  });

  it('and - 182', function(done) {
    var query = r.expr([1,2,3]).and("");
    compare(query, done);
  });

  it('and - 183', function(done) {
    var query = r.expr([1,2,3]).and(0);
    compare(query, done);
  });

  it('and - 184', function(done) {
    var query = r.expr([1,2,3]).and(1);
    compare(query, done);
  });

  it('and - 185', function(done) {
    var query = r.expr([1,2,3]).and(-1);
    compare(query, done);
  });

  it('and - 186', function(done) {
    var query = r.expr([1,2,3]).and(10000);
    compare(query, done);
  });

  it('and - 187', function(done) {
    var query = r.expr([1,2,3]).and(true);
    compare(query, done);
  });

  it('and - 188', function(done) {
    var query = r.expr([1,2,3]).and(false);
    compare(query, done);
  });

  it('and - 189', function(done) {
    var query = r.expr([1,2,3]).and([]);
    compare(query, done);
  });

  it('and - 190', function(done) {
    var query = r.expr([1,2,3]).and([1,2,3]);
    compare(query, done);
  });

  it('and - 191', function(done) {
    var query = r.expr([1,2,3]).and([["foo", 2]]);
    compare(query, done);
  });

  it('and - 192', function(done) {
    var query = r.expr([1,2,3]).and([["foo", 2], ["bar", 1]]);
    compare(query, done);
  });

  it('and - 193', function(done) {
    var query = r.expr([1,2,3]).and({});
    compare(query, done);
  });

  it('and - 194', function(done) {
    var query = r.expr([1,2,3]).and({foo: "bar"});
    compare(query, done);
  });

  it('and - 195', function(done) {
    var query = r.expr([1,2,3]).and({foo: "bar", buzz: 1});
    compare(query, done);
  });

  it('and - 196', function(done) {
    var query = r.expr([1,2,3]).and(null);
    compare(query, done);
  });

  it('and - 197', function(done) {
    var query = r.expr([1,2,3]).and(r.now());
    compare(query, done, function(value) {
      return value instanceof Date;
    });
  });

  it('and - 198', function(done) {
    var query = r.expr([1,2,3]).and(function() { return 1; });
    compare(query, done);
  });

  it('and - 199', function(done) {
    var query = r.expr([1,2,3]).and(r.db(TEST_DB).table(TEST_TABLE));
    compare(query, done);
  });

  it('and - 200', function(done) {
    var query = r.expr([1,2,3]).and(r.binary(new Buffer("hello")));
    compare(query, done);
  });

  it('and - 201', function(done) {
    var query = r.expr([["foo", 2]]).and("foo");
    compare(query, done);
  });

  it('and - 202', function(done) {
    var query = r.expr([["foo", 2]]).and("");
    compare(query, done);
  });

  it('and - 203', function(done) {
    var query = r.expr([["foo", 2]]).and(0);
    compare(query, done);
  });

  it('and - 204', function(done) {
    var query = r.expr([["foo", 2]]).and(1);
    compare(query, done);
  });

  it('and - 205', function(done) {
    var query = r.expr([["foo", 2]]).and(-1);
    compare(query, done);
  });

  it('and - 206', function(done) {
    var query = r.expr([["foo", 2]]).and(10000);
    compare(query, done);
  });

  it('and - 207', function(done) {
    var query = r.expr([["foo", 2]]).and(true);
    compare(query, done);
  });

  it('and - 208', function(done) {
    var query = r.expr([["foo", 2]]).and(false);
    compare(query, done);
  });

  it('and - 209', function(done) {
    var query = r.expr([["foo", 2]]).and([]);
    compare(query, done);
  });

  it('and - 210', function(done) {
    var query = r.expr([["foo", 2]]).and([1,2,3]);
    compare(query, done);
  });

  it('and - 211', function(done) {
    var query = r.expr([["foo", 2]]).and([["foo", 2]]);
    compare(query, done);
  });

  it('and - 212', function(done) {
    var query = r.expr([["foo", 2]]).and([["foo", 2], ["bar", 1]]);
    compare(query, done);
  });

  it('and - 213', function(done) {
    var query = r.expr([["foo", 2]]).and({});
    compare(query, done);
  });

  it('and - 214', function(done) {
    var query = r.expr([["foo", 2]]).and({foo: "bar"});
    compare(query, done);
  });

  it('and - 215', function(done) {
    var query = r.expr([["foo", 2]]).and({foo: "bar", buzz: 1});
    compare(query, done);
  });

  it('and - 216', function(done) {
    var query = r.expr([["foo", 2]]).and(null);
    compare(query, done);
  });

  it('and - 217', function(done) {
    var query = r.expr([["foo", 2]]).and(r.now());
    compare(query, done, function(value) {
      return value instanceof Date;
    });
  });

  it('and - 218', function(done) {
    var query = r.expr([["foo", 2]]).and(function() { return 1; });
    compare(query, done);
  });

  it('and - 219', function(done) {
    var query = r.expr([["foo", 2]]).and(r.db(TEST_DB).table(TEST_TABLE));
    compare(query, done);
  });

  it('and - 220', function(done) {
    var query = r.expr([["foo", 2]]).and(r.binary(new Buffer("hello")));
    compare(query, done);
  });

  it('and - 221', function(done) {
    var query = r.expr([["foo", 2], ["bar", 1]]).and("foo");
    compare(query, done);
  });

  it('and - 222', function(done) {
    var query = r.expr([["foo", 2], ["bar", 1]]).and("");
    compare(query, done);
  });

  it('and - 223', function(done) {
    var query = r.expr([["foo", 2], ["bar", 1]]).and(0);
    compare(query, done);
  });

  it('and - 224', function(done) {
    var query = r.expr([["foo", 2], ["bar", 1]]).and(1);
    compare(query, done);
  });

  it('and - 225', function(done) {
    var query = r.expr([["foo", 2], ["bar", 1]]).and(-1);
    compare(query, done);
  });

  it('and - 226', function(done) {
    var query = r.expr([["foo", 2], ["bar", 1]]).and(10000);
    compare(query, done);
  });

  it('and - 227', function(done) {
    var query = r.expr([["foo", 2], ["bar", 1]]).and(true);
    compare(query, done);
  });

  it('and - 228', function(done) {
    var query = r.expr([["foo", 2], ["bar", 1]]).and(false);
    compare(query, done);
  });

  it('and - 229', function(done) {
    var query = r.expr([["foo", 2], ["bar", 1]]).and([]);
    compare(query, done);
  });

  it('and - 230', function(done) {
    var query = r.expr([["foo", 2], ["bar", 1]]).and([1,2,3]);
    compare(query, done);
  });

  it('and - 231', function(done) {
    var query = r.expr([["foo", 2], ["bar", 1]]).and([["foo", 2]]);
    compare(query, done);
  });

  it('and - 232', function(done) {
    var query = r.expr([["foo", 2], ["bar", 1]]).and([["foo", 2], ["bar", 1]]);
    compare(query, done);
  });

  it('and - 233', function(done) {
    var query = r.expr([["foo", 2], ["bar", 1]]).and({});
    compare(query, done);
  });

  it('and - 234', function(done) {
    var query = r.expr([["foo", 2], ["bar", 1]]).and({foo: "bar"});
    compare(query, done);
  });

  it('and - 235', function(done) {
    var query = r.expr([["foo", 2], ["bar", 1]]).and({foo: "bar", buzz: 1});
    compare(query, done);
  });

  it('and - 236', function(done) {
    var query = r.expr([["foo", 2], ["bar", 1]]).and(null);
    compare(query, done);
  });

  it('and - 237', function(done) {
    var query = r.expr([["foo", 2], ["bar", 1]]).and(r.now());
    compare(query, done, function(value) {
      return value instanceof Date;
    });
  });

  it('and - 238', function(done) {
    var query = r.expr([["foo", 2], ["bar", 1]]).and(function() { return 1; });
    compare(query, done);
  });

  it('and - 239', function(done) {
    var query = r.expr([["foo", 2], ["bar", 1]]).and(r.db(TEST_DB).table(TEST_TABLE));
    compare(query, done);
  });

  it('and - 240', function(done) {
    var query = r.expr([["foo", 2], ["bar", 1]]).and(r.binary(new Buffer("hello")));
    compare(query, done);
  });

  it('and - 241', function(done) {
    var query = r.expr({}).and("foo");
    compare(query, done);
  });

  it('and - 242', function(done) {
    var query = r.expr({}).and("");
    compare(query, done);
  });

  it('and - 243', function(done) {
    var query = r.expr({}).and(0);
    compare(query, done);
  });

  it('and - 244', function(done) {
    var query = r.expr({}).and(1);
    compare(query, done);
  });

  it('and - 245', function(done) {
    var query = r.expr({}).and(-1);
    compare(query, done);
  });

  it('and - 246', function(done) {
    var query = r.expr({}).and(10000);
    compare(query, done);
  });

  it('and - 247', function(done) {
    var query = r.expr({}).and(true);
    compare(query, done);
  });

  it('and - 248', function(done) {
    var query = r.expr({}).and(false);
    compare(query, done);
  });

  it('and - 249', function(done) {
    var query = r.expr({}).and([]);
    compare(query, done);
  });

  it('and - 250', function(done) {
    var query = r.expr({}).and([1,2,3]);
    compare(query, done);
  });

  it('and - 251', function(done) {
    var query = r.expr({}).and([["foo", 2]]);
    compare(query, done);
  });

  it('and - 252', function(done) {
    var query = r.expr({}).and([["foo", 2], ["bar", 1]]);
    compare(query, done);
  });

  it('and - 253', function(done) {
    var query = r.expr({}).and({});
    compare(query, done);
  });

  it('and - 254', function(done) {
    var query = r.expr({}).and({foo: "bar"});
    compare(query, done);
  });

  it('and - 255', function(done) {
    var query = r.expr({}).and({foo: "bar", buzz: 1});
    compare(query, done);
  });

  it('and - 256', function(done) {
    var query = r.expr({}).and(null);
    compare(query, done);
  });

  it('and - 257', function(done) {
    var query = r.expr({}).and(r.now());
    compare(query, done, function(value) {
      return value instanceof Date;
    });
  });

  it('and - 258', function(done) {
    var query = r.expr({}).and(function() { return 1; });
    compare(query, done);
  });

  it('and - 259', function(done) {
    var query = r.expr({}).and(r.db(TEST_DB).table(TEST_TABLE));
    compare(query, done);
  });

  it('and - 260', function(done) {
    var query = r.expr({}).and(r.binary(new Buffer("hello")));
    compare(query, done);
  });

  it('and - 261', function(done) {
    var query = r.expr({foo: "bar"}).and("foo");
    compare(query, done);
  });

  it('and - 262', function(done) {
    var query = r.expr({foo: "bar"}).and("");
    compare(query, done);
  });

  it('and - 263', function(done) {
    var query = r.expr({foo: "bar"}).and(0);
    compare(query, done);
  });

  it('and - 264', function(done) {
    var query = r.expr({foo: "bar"}).and(1);
    compare(query, done);
  });

  it('and - 265', function(done) {
    var query = r.expr({foo: "bar"}).and(-1);
    compare(query, done);
  });

  it('and - 266', function(done) {
    var query = r.expr({foo: "bar"}).and(10000);
    compare(query, done);
  });

  it('and - 267', function(done) {
    var query = r.expr({foo: "bar"}).and(true);
    compare(query, done);
  });

  it('and - 268', function(done) {
    var query = r.expr({foo: "bar"}).and(false);
    compare(query, done);
  });

  it('and - 269', function(done) {
    var query = r.expr({foo: "bar"}).and([]);
    compare(query, done);
  });

  it('and - 270', function(done) {
    var query = r.expr({foo: "bar"}).and([1,2,3]);
    compare(query, done);
  });

  it('and - 271', function(done) {
    var query = r.expr({foo: "bar"}).and([["foo", 2]]);
    compare(query, done);
  });

  it('and - 272', function(done) {
    var query = r.expr({foo: "bar"}).and([["foo", 2], ["bar", 1]]);
    compare(query, done);
  });

  it('and - 273', function(done) {
    var query = r.expr({foo: "bar"}).and({});
    compare(query, done);
  });

  it('and - 274', function(done) {
    var query = r.expr({foo: "bar"}).and({foo: "bar"});
    compare(query, done);
  });

  it('and - 275', function(done) {
    var query = r.expr({foo: "bar"}).and({foo: "bar", buzz: 1});
    compare(query, done);
  });

  it('and - 276', function(done) {
    var query = r.expr({foo: "bar"}).and(null);
    compare(query, done);
  });

  it('and - 277', function(done) {
    var query = r.expr({foo: "bar"}).and(r.now());
    compare(query, done, function(value) {
      return value instanceof Date;
    });
  });

  it('and - 278', function(done) {
    var query = r.expr({foo: "bar"}).and(function() { return 1; });
    compare(query, done);
  });

  it('and - 279', function(done) {
    var query = r.expr({foo: "bar"}).and(r.db(TEST_DB).table(TEST_TABLE));
    compare(query, done);
  });

  it('and - 280', function(done) {
    var query = r.expr({foo: "bar"}).and(r.binary(new Buffer("hello")));
    compare(query, done);
  });

  it('and - 281', function(done) {
    var query = r.expr({foo: "bar", buzz: 1}).and("foo");
    compare(query, done);
  });

  it('and - 282', function(done) {
    var query = r.expr({foo: "bar", buzz: 1}).and("");
    compare(query, done);
  });

  it('and - 283', function(done) {
    var query = r.expr({foo: "bar", buzz: 1}).and(0);
    compare(query, done);
  });

  it('and - 284', function(done) {
    var query = r.expr({foo: "bar", buzz: 1}).and(1);
    compare(query, done);
  });

  it('and - 285', function(done) {
    var query = r.expr({foo: "bar", buzz: 1}).and(-1);
    compare(query, done);
  });

  it('and - 286', function(done) {
    var query = r.expr({foo: "bar", buzz: 1}).and(10000);
    compare(query, done);
  });

  it('and - 287', function(done) {
    var query = r.expr({foo: "bar", buzz: 1}).and(true);
    compare(query, done);
  });

  it('and - 288', function(done) {
    var query = r.expr({foo: "bar", buzz: 1}).and(false);
    compare(query, done);
  });

  it('and - 289', function(done) {
    var query = r.expr({foo: "bar", buzz: 1}).and([]);
    compare(query, done);
  });

  it('and - 290', function(done) {
    var query = r.expr({foo: "bar", buzz: 1}).and([1,2,3]);
    compare(query, done);
  });

  it('and - 291', function(done) {
    var query = r.expr({foo: "bar", buzz: 1}).and([["foo", 2]]);
    compare(query, done);
  });

  it('and - 292', function(done) {
    var query = r.expr({foo: "bar", buzz: 1}).and([["foo", 2], ["bar", 1]]);
    compare(query, done);
  });

  it('and - 293', function(done) {
    var query = r.expr({foo: "bar", buzz: 1}).and({});
    compare(query, done);
  });

  it('and - 294', function(done) {
    var query = r.expr({foo: "bar", buzz: 1}).and({foo: "bar"});
    compare(query, done);
  });

  it('and - 295', function(done) {
    var query = r.expr({foo: "bar", buzz: 1}).and({foo: "bar", buzz: 1});
    compare(query, done);
  });

  it('and - 296', function(done) {
    var query = r.expr({foo: "bar", buzz: 1}).and(null);
    compare(query, done);
  });

  it('and - 297', function(done) {
    var query = r.expr({foo: "bar", buzz: 1}).and(r.now());
    compare(query, done, function(value) {
      return value instanceof Date;
    });
  });

  it('and - 298', function(done) {
    var query = r.expr({foo: "bar", buzz: 1}).and(function() { return 1; });
    compare(query, done);
  });

  it('and - 299', function(done) {
    var query = r.expr({foo: "bar", buzz: 1}).and(r.db(TEST_DB).table(TEST_TABLE));
    compare(query, done);
  });

  it('and - 300', function(done) {
    var query = r.expr({foo: "bar", buzz: 1}).and(r.binary(new Buffer("hello")));
    compare(query, done);
  });

  it('and - 301', function(done) {
    var query = r.expr(null).and("foo");
    compare(query, done);
  });

  it('and - 302', function(done) {
    var query = r.expr(null).and("");
    compare(query, done);
  });

  it('and - 303', function(done) {
    var query = r.expr(null).and(0);
    compare(query, done);
  });

  it('and - 304', function(done) {
    var query = r.expr(null).and(1);
    compare(query, done);
  });

  it('and - 305', function(done) {
    var query = r.expr(null).and(-1);
    compare(query, done);
  });

  it('and - 306', function(done) {
    var query = r.expr(null).and(10000);
    compare(query, done);
  });

  it('and - 307', function(done) {
    var query = r.expr(null).and(true);
    compare(query, done);
  });

  it('and - 308', function(done) {
    var query = r.expr(null).and(false);
    compare(query, done);
  });

  it('and - 309', function(done) {
    var query = r.expr(null).and([]);
    compare(query, done);
  });

  it('and - 310', function(done) {
    var query = r.expr(null).and([1,2,3]);
    compare(query, done);
  });

  it('and - 311', function(done) {
    var query = r.expr(null).and([["foo", 2]]);
    compare(query, done);
  });

  it('and - 312', function(done) {
    var query = r.expr(null).and([["foo", 2], ["bar", 1]]);
    compare(query, done);
  });

  it('and - 313', function(done) {
    var query = r.expr(null).and({});
    compare(query, done);
  });

  it('and - 314', function(done) {
    var query = r.expr(null).and({foo: "bar"});
    compare(query, done);
  });

  it('and - 315', function(done) {
    var query = r.expr(null).and({foo: "bar", buzz: 1});
    compare(query, done);
  });

  it('and - 316', function(done) {
    var query = r.expr(null).and(null);
    compare(query, done);
  });

  it('and - 317', function(done) {
    var query = r.expr(null).and(r.now());
    compare(query, done, function(value) {
      return value instanceof Date;
    });
  });

  it('and - 318', function(done) {
    var query = r.expr(null).and(function() { return 1; });
    compare(query, done);
  });

  it('and - 319', function(done) {
    var query = r.expr(null).and(r.db(TEST_DB).table(TEST_TABLE));
    compare(query, done);
  });

  it('and - 320', function(done) {
    var query = r.expr(null).and(r.binary(new Buffer("hello")));
    compare(query, done);
  });

  it('and - 321', function(done) {
    var query = r.expr(r.now()).and("foo");
    compare(query, done);
  });

  it('and - 322', function(done) {
    var query = r.expr(r.now()).and("");
    compare(query, done);
  });

  it('and - 323', function(done) {
    var query = r.expr(r.now()).and(0);
    compare(query, done);
  });

  it('and - 324', function(done) {
    var query = r.expr(r.now()).and(1);
    compare(query, done);
  });

  it('and - 325', function(done) {
    var query = r.expr(r.now()).and(-1);
    compare(query, done);
  });

  it('and - 326', function(done) {
    var query = r.expr(r.now()).and(10000);
    compare(query, done);
  });

  it('and - 327', function(done) {
    var query = r.expr(r.now()).and(true);
    compare(query, done);
  });

  it('and - 328', function(done) {
    var query = r.expr(r.now()).and(false);
    compare(query, done);
  });

  it('and - 329', function(done) {
    var query = r.expr(r.now()).and([]);
    compare(query, done);
  });

  it('and - 330', function(done) {
    var query = r.expr(r.now()).and([1,2,3]);
    compare(query, done);
  });

  it('and - 331', function(done) {
    var query = r.expr(r.now()).and([["foo", 2]]);
    compare(query, done);
  });

  it('and - 332', function(done) {
    var query = r.expr(r.now()).and([["foo", 2], ["bar", 1]]);
    compare(query, done);
  });

  it('and - 333', function(done) {
    var query = r.expr(r.now()).and({});
    compare(query, done);
  });

  it('and - 334', function(done) {
    var query = r.expr(r.now()).and({foo: "bar"});
    compare(query, done);
  });

  it('and - 335', function(done) {
    var query = r.expr(r.now()).and({foo: "bar", buzz: 1});
    compare(query, done);
  });

  it('and - 336', function(done) {
    var query = r.expr(r.now()).and(null);
    compare(query, done);
  });

  it('and - 337', function(done) {
    var query = r.expr(r.now()).and(r.now());
    compare(query, done, function(value) {
      return value instanceof Date;
    });
  });

  it('and - 338', function(done) {
    var query = r.expr(r.now()).and(function() { return 1; });
    compare(query, done);
  });

  it('and - 339', function(done) {
    var query = r.expr(r.now()).and(r.db(TEST_DB).table(TEST_TABLE));
    compare(query, done);
  });

  it('and - 340', function(done) {
    var query = r.expr(r.now()).and(r.binary(new Buffer("hello")));
    compare(query, done);
  });

  it('and - 341', function(done) {
    var query = r.expr(function() { return 1; }).and("foo");
    compare(query, done);
  });

  it('and - 342', function(done) {
    var query = r.expr(function() { return 1; }).and("");
    compare(query, done);
  });

  it('and - 343', function(done) {
    var query = r.expr(function() { return 1; }).and(0);
    compare(query, done);
  });

  it('and - 344', function(done) {
    var query = r.expr(function() { return 1; }).and(1);
    compare(query, done);
  });

  it('and - 345', function(done) {
    var query = r.expr(function() { return 1; }).and(-1);
    compare(query, done);
  });

  it('and - 346', function(done) {
    var query = r.expr(function() { return 1; }).and(10000);
    compare(query, done);
  });

  it('and - 347', function(done) {
    var query = r.expr(function() { return 1; }).and(true);
    compare(query, done);
  });

  it('and - 348', function(done) {
    var query = r.expr(function() { return 1; }).and(false);
    compare(query, done);
  });

  it('and - 349', function(done) {
    var query = r.expr(function() { return 1; }).and([]);
    compare(query, done);
  });

  it('and - 350', function(done) {
    var query = r.expr(function() { return 1; }).and([1,2,3]);
    compare(query, done);
  });

  it('and - 351', function(done) {
    var query = r.expr(function() { return 1; }).and([["foo", 2]]);
    compare(query, done);
  });

  it('and - 352', function(done) {
    var query = r.expr(function() { return 1; }).and([["foo", 2], ["bar", 1]]);
    compare(query, done);
  });

  it('and - 353', function(done) {
    var query = r.expr(function() { return 1; }).and({});
    compare(query, done);
  });

  it('and - 354', function(done) {
    var query = r.expr(function() { return 1; }).and({foo: "bar"});
    compare(query, done);
  });

  it('and - 355', function(done) {
    var query = r.expr(function() { return 1; }).and({foo: "bar", buzz: 1});
    compare(query, done);
  });

  it('and - 356', function(done) {
    var query = r.expr(function() { return 1; }).and(null);
    compare(query, done);
  });

  it('and - 357', function(done) {
    var query = r.expr(function() { return 1; }).and(r.now());
    compare(query, done, function(value) {
      return value instanceof Date;
    });
  });

  it('and - 358', function(done) {
    var query = r.expr(function() { return 1; }).and(function() { return 1; });
    compare(query, done);
  });

  it('and - 359', function(done) {
    var query = r.expr(function() { return 1; }).and(r.db(TEST_DB).table(TEST_TABLE));
    compare(query, done);
  });

  it('and - 360', function(done) {
    var query = r.expr(function() { return 1; }).and(r.binary(new Buffer("hello")));
    compare(query, done);
  });

  it('and - 361', function(done) {
    var query = r.expr(r.db(TEST_DB).table(TEST_TABLE)).and("foo");
    compare(query, done);
  });

  it('and - 362', function(done) {
    var query = r.expr(r.db(TEST_DB).table(TEST_TABLE)).and("");
    compare(query, done);
  });

  it('and - 363', function(done) {
    var query = r.expr(r.db(TEST_DB).table(TEST_TABLE)).and(0);
    compare(query, done);
  });

  it('and - 364', function(done) {
    var query = r.expr(r.db(TEST_DB).table(TEST_TABLE)).and(1);
    compare(query, done);
  });

  it('and - 365', function(done) {
    var query = r.expr(r.db(TEST_DB).table(TEST_TABLE)).and(-1);
    compare(query, done);
  });

  it('and - 366', function(done) {
    var query = r.expr(r.db(TEST_DB).table(TEST_TABLE)).and(10000);
    compare(query, done);
  });

  it('and - 367', function(done) {
    var query = r.expr(r.db(TEST_DB).table(TEST_TABLE)).and(true);
    compare(query, done);
  });

  it('and - 368', function(done) {
    var query = r.expr(r.db(TEST_DB).table(TEST_TABLE)).and(false);
    compare(query, done);
  });

  it('and - 369', function(done) {
    var query = r.expr(r.db(TEST_DB).table(TEST_TABLE)).and([]);
    compare(query, done);
  });

  it('and - 370', function(done) {
    var query = r.expr(r.db(TEST_DB).table(TEST_TABLE)).and([1,2,3]);
    compare(query, done);
  });

  it('and - 371', function(done) {
    var query = r.expr(r.db(TEST_DB).table(TEST_TABLE)).and([["foo", 2]]);
    compare(query, done);
  });

  it('and - 372', function(done) {
    var query = r.expr(r.db(TEST_DB).table(TEST_TABLE)).and([["foo", 2], ["bar", 1]]);
    compare(query, done);
  });

  it('and - 373', function(done) {
    var query = r.expr(r.db(TEST_DB).table(TEST_TABLE)).and({});
    compare(query, done);
  });

  it('and - 374', function(done) {
    var query = r.expr(r.db(TEST_DB).table(TEST_TABLE)).and({foo: "bar"});
    compare(query, done);
  });

  it('and - 375', function(done) {
    var query = r.expr(r.db(TEST_DB).table(TEST_TABLE)).and({foo: "bar", buzz: 1});
    compare(query, done);
  });

  it('and - 376', function(done) {
    var query = r.expr(r.db(TEST_DB).table(TEST_TABLE)).and(null);
    compare(query, done);
  });

  it('and - 377', function(done) {
    var query = r.expr(r.db(TEST_DB).table(TEST_TABLE)).and(r.now());
    compare(query, done, function(value) {
      return value instanceof Date;
    });
  });

  it('and - 378', function(done) {
    var query = r.expr(r.db(TEST_DB).table(TEST_TABLE)).and(function() { return 1; });
    compare(query, done);
  });

  it('and - 379', function(done) {
    var query = r.expr(r.db(TEST_DB).table(TEST_TABLE)).and(r.db(TEST_DB).table(TEST_TABLE));
    compare(query, done);
  });

  it('and - 380', function(done) {
    var query = r.expr(r.db(TEST_DB).table(TEST_TABLE)).and(r.binary(new Buffer("hello")));
    compare(query, done);
  });

  it('and - 381', function(done) {
    var query = r.expr(r.binary(new Buffer("hello"))).and("foo");
    compare(query, done);
  });

  it('and - 382', function(done) {
    var query = r.expr(r.binary(new Buffer("hello"))).and("");
    compare(query, done);
  });

  it('and - 383', function(done) {
    var query = r.expr(r.binary(new Buffer("hello"))).and(0);
    compare(query, done);
  });

  it('and - 384', function(done) {
    var query = r.expr(r.binary(new Buffer("hello"))).and(1);
    compare(query, done);
  });

  it('and - 385', function(done) {
    var query = r.expr(r.binary(new Buffer("hello"))).and(-1);
    compare(query, done);
  });

  it('and - 386', function(done) {
    var query = r.expr(r.binary(new Buffer("hello"))).and(10000);
    compare(query, done);
  });

  it('and - 387', function(done) {
    var query = r.expr(r.binary(new Buffer("hello"))).and(true);
    compare(query, done);
  });

  it('and - 388', function(done) {
    var query = r.expr(r.binary(new Buffer("hello"))).and(false);
    compare(query, done);
  });

  it('and - 389', function(done) {
    var query = r.expr(r.binary(new Buffer("hello"))).and([]);
    compare(query, done);
  });

  it('and - 390', function(done) {
    var query = r.expr(r.binary(new Buffer("hello"))).and([1,2,3]);
    compare(query, done);
  });

  it('and - 391', function(done) {
    var query = r.expr(r.binary(new Buffer("hello"))).and([["foo", 2]]);
    compare(query, done);
  });

  it('and - 392', function(done) {
    var query = r.expr(r.binary(new Buffer("hello"))).and([["foo", 2], ["bar", 1]]);
    compare(query, done);
  });

  it('and - 393', function(done) {
    var query = r.expr(r.binary(new Buffer("hello"))).and({});
    compare(query, done);
  });

  it('and - 394', function(done) {
    var query = r.expr(r.binary(new Buffer("hello"))).and({foo: "bar"});
    compare(query, done);
  });

  it('and - 395', function(done) {
    var query = r.expr(r.binary(new Buffer("hello"))).and({foo: "bar", buzz: 1});
    compare(query, done);
  });

  it('and - 396', function(done) {
    var query = r.expr(r.binary(new Buffer("hello"))).and(null);
    compare(query, done);
  });

  it('and - 397', function(done) {
    var query = r.expr(r.binary(new Buffer("hello"))).and(r.now());
    compare(query, done, function(value) {
      return value instanceof Date;
    });
  });

  it('and - 398', function(done) {
    var query = r.expr(r.binary(new Buffer("hello"))).and(function() { return 1; });
    compare(query, done);
  });

  it('and - 399', function(done) {
    var query = r.expr(r.binary(new Buffer("hello"))).and(r.db(TEST_DB).table(TEST_TABLE));
    compare(query, done);
  });

  it('and - 400', function(done) {
    var query = r.expr(r.binary(new Buffer("hello"))).and(r.binary(new Buffer("hello")));
    compare(query, done);
  });

  it('and - 401', function(done) {
    var query = r.and();
    compare(query, done);
  });

  it('or - 1', function(done) {
    var query = r.expr("foo").or("foo");
    compare(query, done);
  });

  it('or - 2', function(done) {
    var query = r.expr("foo").or("");
    compare(query, done);
  });

  it('or - 3', function(done) {
    var query = r.expr("foo").or(0);
    compare(query, done);
  });

  it('or - 4', function(done) {
    var query = r.expr("foo").or(1);
    compare(query, done);
  });

  it('or - 5', function(done) {
    var query = r.expr("foo").or(-1);
    compare(query, done);
  });

  it('or - 6', function(done) {
    var query = r.expr("foo").or(10000);
    compare(query, done);
  });

  it('or - 7', function(done) {
    var query = r.expr("foo").or(true);
    compare(query, done);
  });

  it('or - 8', function(done) {
    var query = r.expr("foo").or(false);
    compare(query, done);
  });

  it('or - 9', function(done) {
    var query = r.expr("foo").or([]);
    compare(query, done);
  });

  it('or - 10', function(done) {
    var query = r.expr("foo").or([1,2,3]);
    compare(query, done);
  });

  it('or - 11', function(done) {
    var query = r.expr("foo").or([["foo", 2]]);
    compare(query, done);
  });

  it('or - 12', function(done) {
    var query = r.expr("foo").or([["foo", 2], ["bar", 1]]);
    compare(query, done);
  });

  it('or - 13', function(done) {
    var query = r.expr("foo").or({});
    compare(query, done);
  });

  it('or - 14', function(done) {
    var query = r.expr("foo").or({foo: "bar"});
    compare(query, done);
  });

  it('or - 15', function(done) {
    var query = r.expr("foo").or({foo: "bar", buzz: 1});
    compare(query, done);
  });

  it('or - 16', function(done) {
    var query = r.expr("foo").or(null);
    compare(query, done);
  });

  it('or - 17', function(done) {
    var query = r.expr("foo").or(r.now());
    compare(query, done);
  });

  it('or - 18', function(done) {
    var query = r.expr("foo").or(function() { return 1; });
    compare(query, done);
  });

  it('or - 19', function(done) {
    var query = r.expr("foo").or(r.db(TEST_DB).table(TEST_TABLE));
    compare(query, done);
  });

  it('or - 20', function(done) {
    var query = r.expr("foo").or(r.binary(new Buffer("hello")));
    compare(query, done);
  });

  it('or - 21', function(done) {
    var query = r.expr("").or("foo");
    compare(query, done);
  });

  it('or - 22', function(done) {
    var query = r.expr("").or("");
    compare(query, done);
  });

  it('or - 23', function(done) {
    var query = r.expr("").or(0);
    compare(query, done);
  });

  it('or - 24', function(done) {
    var query = r.expr("").or(1);
    compare(query, done);
  });

  it('or - 25', function(done) {
    var query = r.expr("").or(-1);
    compare(query, done);
  });

  it('or - 26', function(done) {
    var query = r.expr("").or(10000);
    compare(query, done);
  });

  it('or - 27', function(done) {
    var query = r.expr("").or(true);
    compare(query, done);
  });

  it('or - 28', function(done) {
    var query = r.expr("").or(false);
    compare(query, done);
  });

  it('or - 29', function(done) {
    var query = r.expr("").or([]);
    compare(query, done);
  });

  it('or - 30', function(done) {
    var query = r.expr("").or([1,2,3]);
    compare(query, done);
  });

  it('or - 31', function(done) {
    var query = r.expr("").or([["foo", 2]]);
    compare(query, done);
  });

  it('or - 32', function(done) {
    var query = r.expr("").or([["foo", 2], ["bar", 1]]);
    compare(query, done);
  });

  it('or - 33', function(done) {
    var query = r.expr("").or({});
    compare(query, done);
  });

  it('or - 34', function(done) {
    var query = r.expr("").or({foo: "bar"});
    compare(query, done);
  });

  it('or - 35', function(done) {
    var query = r.expr("").or({foo: "bar", buzz: 1});
    compare(query, done);
  });

  it('or - 36', function(done) {
    var query = r.expr("").or(null);
    compare(query, done);
  });

  it('or - 37', function(done) {
    var query = r.expr("").or(r.now());
    compare(query, done);
  });

  it('or - 38', function(done) {
    var query = r.expr("").or(function() { return 1; });
    compare(query, done);
  });

  it('or - 39', function(done) {
    var query = r.expr("").or(r.db(TEST_DB).table(TEST_TABLE));
    compare(query, done);
  });

  it('or - 40', function(done) {
    var query = r.expr("").or(r.binary(new Buffer("hello")));
    compare(query, done);
  });

  it('or - 41', function(done) {
    var query = r.expr(0).or("foo");
    compare(query, done);
  });

  it('or - 42', function(done) {
    var query = r.expr(0).or("");
    compare(query, done);
  });

  it('or - 43', function(done) {
    var query = r.expr(0).or(0);
    compare(query, done);
  });

  it('or - 44', function(done) {
    var query = r.expr(0).or(1);
    compare(query, done);
  });

  it('or - 45', function(done) {
    var query = r.expr(0).or(-1);
    compare(query, done);
  });

  it('or - 46', function(done) {
    var query = r.expr(0).or(10000);
    compare(query, done);
  });

  it('or - 47', function(done) {
    var query = r.expr(0).or(true);
    compare(query, done);
  });

  it('or - 48', function(done) {
    var query = r.expr(0).or(false);
    compare(query, done);
  });

  it('or - 49', function(done) {
    var query = r.expr(0).or([]);
    compare(query, done);
  });

  it('or - 50', function(done) {
    var query = r.expr(0).or([1,2,3]);
    compare(query, done);
  });

  it('or - 51', function(done) {
    var query = r.expr(0).or([["foo", 2]]);
    compare(query, done);
  });

  it('or - 52', function(done) {
    var query = r.expr(0).or([["foo", 2], ["bar", 1]]);
    compare(query, done);
  });

  it('or - 53', function(done) {
    var query = r.expr(0).or({});
    compare(query, done);
  });

  it('or - 54', function(done) {
    var query = r.expr(0).or({foo: "bar"});
    compare(query, done);
  });

  it('or - 55', function(done) {
    var query = r.expr(0).or({foo: "bar", buzz: 1});
    compare(query, done);
  });

  it('or - 56', function(done) {
    var query = r.expr(0).or(null);
    compare(query, done);
  });

  it('or - 57', function(done) {
    var query = r.expr(0).or(r.now());
    compare(query, done);
  });

  it('or - 58', function(done) {
    var query = r.expr(0).or(function() { return 1; });
    compare(query, done);
  });

  it('or - 59', function(done) {
    var query = r.expr(0).or(r.db(TEST_DB).table(TEST_TABLE));
    compare(query, done);
  });

  it('or - 60', function(done) {
    var query = r.expr(0).or(r.binary(new Buffer("hello")));
    compare(query, done);
  });

  it('or - 61', function(done) {
    var query = r.expr(1).or("foo");
    compare(query, done);
  });

  it('or - 62', function(done) {
    var query = r.expr(1).or("");
    compare(query, done);
  });

  it('or - 63', function(done) {
    var query = r.expr(1).or(0);
    compare(query, done);
  });

  it('or - 64', function(done) {
    var query = r.expr(1).or(1);
    compare(query, done);
  });

  it('or - 65', function(done) {
    var query = r.expr(1).or(-1);
    compare(query, done);
  });

  it('or - 66', function(done) {
    var query = r.expr(1).or(10000);
    compare(query, done);
  });

  it('or - 67', function(done) {
    var query = r.expr(1).or(true);
    compare(query, done);
  });

  it('or - 68', function(done) {
    var query = r.expr(1).or(false);
    compare(query, done);
  });

  it('or - 69', function(done) {
    var query = r.expr(1).or([]);
    compare(query, done);
  });

  it('or - 70', function(done) {
    var query = r.expr(1).or([1,2,3]);
    compare(query, done);
  });

  it('or - 71', function(done) {
    var query = r.expr(1).or([["foo", 2]]);
    compare(query, done);
  });

  it('or - 72', function(done) {
    var query = r.expr(1).or([["foo", 2], ["bar", 1]]);
    compare(query, done);
  });

  it('or - 73', function(done) {
    var query = r.expr(1).or({});
    compare(query, done);
  });

  it('or - 74', function(done) {
    var query = r.expr(1).or({foo: "bar"});
    compare(query, done);
  });

  it('or - 75', function(done) {
    var query = r.expr(1).or({foo: "bar", buzz: 1});
    compare(query, done);
  });

  it('or - 76', function(done) {
    var query = r.expr(1).or(null);
    compare(query, done);
  });

  it('or - 77', function(done) {
    var query = r.expr(1).or(r.now());
    compare(query, done);
  });

  it('or - 78', function(done) {
    var query = r.expr(1).or(function() { return 1; });
    compare(query, done);
  });

  it('or - 79', function(done) {
    var query = r.expr(1).or(r.db(TEST_DB).table(TEST_TABLE));
    compare(query, done);
  });

  it('or - 80', function(done) {
    var query = r.expr(1).or(r.binary(new Buffer("hello")));
    compare(query, done);
  });

  it('or - 81', function(done) {
    var query = r.expr(-1).or("foo");
    compare(query, done);
  });

  it('or - 82', function(done) {
    var query = r.expr(-1).or("");
    compare(query, done);
  });

  it('or - 83', function(done) {
    var query = r.expr(-1).or(0);
    compare(query, done);
  });

  it('or - 84', function(done) {
    var query = r.expr(-1).or(1);
    compare(query, done);
  });

  it('or - 85', function(done) {
    var query = r.expr(-1).or(-1);
    compare(query, done);
  });

  it('or - 86', function(done) {
    var query = r.expr(-1).or(10000);
    compare(query, done);
  });

  it('or - 87', function(done) {
    var query = r.expr(-1).or(true);
    compare(query, done);
  });

  it('or - 88', function(done) {
    var query = r.expr(-1).or(false);
    compare(query, done);
  });

  it('or - 89', function(done) {
    var query = r.expr(-1).or([]);
    compare(query, done);
  });

  it('or - 90', function(done) {
    var query = r.expr(-1).or([1,2,3]);
    compare(query, done);
  });

  it('or - 91', function(done) {
    var query = r.expr(-1).or([["foo", 2]]);
    compare(query, done);
  });

  it('or - 92', function(done) {
    var query = r.expr(-1).or([["foo", 2], ["bar", 1]]);
    compare(query, done);
  });

  it('or - 93', function(done) {
    var query = r.expr(-1).or({});
    compare(query, done);
  });

  it('or - 94', function(done) {
    var query = r.expr(-1).or({foo: "bar"});
    compare(query, done);
  });

  it('or - 95', function(done) {
    var query = r.expr(-1).or({foo: "bar", buzz: 1});
    compare(query, done);
  });

  it('or - 96', function(done) {
    var query = r.expr(-1).or(null);
    compare(query, done);
  });

  it('or - 97', function(done) {
    var query = r.expr(-1).or(r.now());
    compare(query, done);
  });

  it('or - 98', function(done) {
    var query = r.expr(-1).or(function() { return 1; });
    compare(query, done);
  });

  it('or - 99', function(done) {
    var query = r.expr(-1).or(r.db(TEST_DB).table(TEST_TABLE));
    compare(query, done);
  });

  it('or - 100', function(done) {
    var query = r.expr(-1).or(r.binary(new Buffer("hello")));
    compare(query, done);
  });

  it('or - 101', function(done) {
    var query = r.expr(10000).or("foo");
    compare(query, done);
  });

  it('or - 102', function(done) {
    var query = r.expr(10000).or("");
    compare(query, done);
  });

  it('or - 103', function(done) {
    var query = r.expr(10000).or(0);
    compare(query, done);
  });

  it('or - 104', function(done) {
    var query = r.expr(10000).or(1);
    compare(query, done);
  });

  it('or - 105', function(done) {
    var query = r.expr(10000).or(-1);
    compare(query, done);
  });

  it('or - 106', function(done) {
    var query = r.expr(10000).or(10000);
    compare(query, done);
  });

  it('or - 107', function(done) {
    var query = r.expr(10000).or(true);
    compare(query, done);
  });

  it('or - 108', function(done) {
    var query = r.expr(10000).or(false);
    compare(query, done);
  });

  it('or - 109', function(done) {
    var query = r.expr(10000).or([]);
    compare(query, done);
  });

  it('or - 110', function(done) {
    var query = r.expr(10000).or([1,2,3]);
    compare(query, done);
  });

  it('or - 111', function(done) {
    var query = r.expr(10000).or([["foo", 2]]);
    compare(query, done);
  });

  it('or - 112', function(done) {
    var query = r.expr(10000).or([["foo", 2], ["bar", 1]]);
    compare(query, done);
  });

  it('or - 113', function(done) {
    var query = r.expr(10000).or({});
    compare(query, done);
  });

  it('or - 114', function(done) {
    var query = r.expr(10000).or({foo: "bar"});
    compare(query, done);
  });

  it('or - 115', function(done) {
    var query = r.expr(10000).or({foo: "bar", buzz: 1});
    compare(query, done);
  });

  it('or - 116', function(done) {
    var query = r.expr(10000).or(null);
    compare(query, done);
  });

  it('or - 117', function(done) {
    var query = r.expr(10000).or(r.now());
    compare(query, done);
  });

  it('or - 118', function(done) {
    var query = r.expr(10000).or(function() { return 1; });
    compare(query, done);
  });

  it('or - 119', function(done) {
    var query = r.expr(10000).or(r.db(TEST_DB).table(TEST_TABLE));
    compare(query, done);
  });

  it('or - 120', function(done) {
    var query = r.expr(10000).or(r.binary(new Buffer("hello")));
    compare(query, done);
  });

  it('or - 121', function(done) {
    var query = r.expr(true).or("foo");
    compare(query, done);
  });

  it('or - 122', function(done) {
    var query = r.expr(true).or("");
    compare(query, done);
  });

  it('or - 123', function(done) {
    var query = r.expr(true).or(0);
    compare(query, done);
  });

  it('or - 124', function(done) {
    var query = r.expr(true).or(1);
    compare(query, done);
  });

  it('or - 125', function(done) {
    var query = r.expr(true).or(-1);
    compare(query, done);
  });

  it('or - 126', function(done) {
    var query = r.expr(true).or(10000);
    compare(query, done);
  });

  it('or - 127', function(done) {
    var query = r.expr(true).or(true);
    compare(query, done);
  });

  it('or - 128', function(done) {
    var query = r.expr(true).or(false);
    compare(query, done);
  });

  it('or - 129', function(done) {
    var query = r.expr(true).or([]);
    compare(query, done);
  });

  it('or - 130', function(done) {
    var query = r.expr(true).or([1,2,3]);
    compare(query, done);
  });

  it('or - 131', function(done) {
    var query = r.expr(true).or([["foo", 2]]);
    compare(query, done);
  });

  it('or - 132', function(done) {
    var query = r.expr(true).or([["foo", 2], ["bar", 1]]);
    compare(query, done);
  });

  it('or - 133', function(done) {
    var query = r.expr(true).or({});
    compare(query, done);
  });

  it('or - 134', function(done) {
    var query = r.expr(true).or({foo: "bar"});
    compare(query, done);
  });

  it('or - 135', function(done) {
    var query = r.expr(true).or({foo: "bar", buzz: 1});
    compare(query, done);
  });

  it('or - 136', function(done) {
    var query = r.expr(true).or(null);
    compare(query, done);
  });

  it('or - 137', function(done) {
    var query = r.expr(true).or(r.now());
    compare(query, done);
  });

  it('or - 138', function(done) {
    var query = r.expr(true).or(function() { return 1; });
    compare(query, done);
  });

  it('or - 139', function(done) {
    var query = r.expr(true).or(r.db(TEST_DB).table(TEST_TABLE));
    compare(query, done);
  });

  it('or - 140', function(done) {
    var query = r.expr(true).or(r.binary(new Buffer("hello")));
    compare(query, done);
  });

  it('or - 141', function(done) {
    var query = r.expr(false).or("foo");
    compare(query, done);
  });

  it('or - 142', function(done) {
    var query = r.expr(false).or("");
    compare(query, done);
  });

  it('or - 143', function(done) {
    var query = r.expr(false).or(0);
    compare(query, done);
  });

  it('or - 144', function(done) {
    var query = r.expr(false).or(1);
    compare(query, done);
  });

  it('or - 145', function(done) {
    var query = r.expr(false).or(-1);
    compare(query, done);
  });

  it('or - 146', function(done) {
    var query = r.expr(false).or(10000);
    compare(query, done);
  });

  it('or - 147', function(done) {
    var query = r.expr(false).or(true);
    compare(query, done);
  });

  it('or - 148', function(done) {
    var query = r.expr(false).or(false);
    compare(query, done);
  });

  it('or - 149', function(done) {
    var query = r.expr(false).or([]);
    compare(query, done);
  });

  it('or - 150', function(done) {
    var query = r.expr(false).or([1,2,3]);
    compare(query, done);
  });

  it('or - 151', function(done) {
    var query = r.expr(false).or([["foo", 2]]);
    compare(query, done);
  });

  it('or - 152', function(done) {
    var query = r.expr(false).or([["foo", 2], ["bar", 1]]);
    compare(query, done);
  });

  it('or - 153', function(done) {
    var query = r.expr(false).or({});
    compare(query, done);
  });

  it('or - 154', function(done) {
    var query = r.expr(false).or({foo: "bar"});
    compare(query, done);
  });

  it('or - 155', function(done) {
    var query = r.expr(false).or({foo: "bar", buzz: 1});
    compare(query, done);
  });

  it('or - 156', function(done) {
    var query = r.expr(false).or(null);
    compare(query, done);
  });

  it('or - 157', function(done) {
    var query = r.expr(false).or(r.now());
    compare(query, done, function(value) {
      return value instanceof Date;
    });
  });

  it('or - 158', function(done) {
    var query = r.expr(false).or(function() { return 1; });
    compare(query, done);
  });

  it('or - 159', function(done) {
    var query = r.expr(false).or(r.db(TEST_DB).table(TEST_TABLE));
    compare(query, done);
  });

  it('or - 160', function(done) {
    var query = r.expr(false).or(r.binary(new Buffer("hello")));
    compare(query, done);
  });

  it('or - 161', function(done) {
    var query = r.expr([]).or("foo");
    compare(query, done);
  });

  it('or - 162', function(done) {
    var query = r.expr([]).or("");
    compare(query, done);
  });

  it('or - 163', function(done) {
    var query = r.expr([]).or(0);
    compare(query, done);
  });

  it('or - 164', function(done) {
    var query = r.expr([]).or(1);
    compare(query, done);
  });

  it('or - 165', function(done) {
    var query = r.expr([]).or(-1);
    compare(query, done);
  });

  it('or - 166', function(done) {
    var query = r.expr([]).or(10000);
    compare(query, done);
  });

  it('or - 167', function(done) {
    var query = r.expr([]).or(true);
    compare(query, done);
  });

  it('or - 168', function(done) {
    var query = r.expr([]).or(false);
    compare(query, done);
  });

  it('or - 169', function(done) {
    var query = r.expr([]).or([]);
    compare(query, done);
  });

  it('or - 170', function(done) {
    var query = r.expr([]).or([1,2,3]);
    compare(query, done);
  });

  it('or - 171', function(done) {
    var query = r.expr([]).or([["foo", 2]]);
    compare(query, done);
  });

  it('or - 172', function(done) {
    var query = r.expr([]).or([["foo", 2], ["bar", 1]]);
    compare(query, done);
  });

  it('or - 173', function(done) {
    var query = r.expr([]).or({});
    compare(query, done);
  });

  it('or - 174', function(done) {
    var query = r.expr([]).or({foo: "bar"});
    compare(query, done);
  });

  it('or - 175', function(done) {
    var query = r.expr([]).or({foo: "bar", buzz: 1});
    compare(query, done);
  });

  it('or - 176', function(done) {
    var query = r.expr([]).or(null);
    compare(query, done);
  });

  it('or - 177', function(done) {
    var query = r.expr([]).or(r.now());
    compare(query, done, function(value) {
      return value instanceof Date;
    });
  });

  it('or - 178', function(done) {
    var query = r.expr([]).or(function() { return 1; });
    compare(query, done);
  });

  it('or - 179', function(done) {
    var query = r.expr([]).or(r.db(TEST_DB).table(TEST_TABLE));
    compare(query, done);
  });

  it('or - 180', function(done) {
    var query = r.expr([]).or(r.binary(new Buffer("hello")));
    compare(query, done);
  });

  it('or - 181', function(done) {
    var query = r.expr([1,2,3]).or("foo");
    compare(query, done);
  });

  it('or - 182', function(done) {
    var query = r.expr([1,2,3]).or("");
    compare(query, done);
  });

  it('or - 183', function(done) {
    var query = r.expr([1,2,3]).or(0);
    compare(query, done);
  });

  it('or - 184', function(done) {
    var query = r.expr([1,2,3]).or(1);
    compare(query, done);
  });

  it('or - 185', function(done) {
    var query = r.expr([1,2,3]).or(-1);
    compare(query, done);
  });

  it('or - 186', function(done) {
    var query = r.expr([1,2,3]).or(10000);
    compare(query, done);
  });

  it('or - 187', function(done) {
    var query = r.expr([1,2,3]).or(true);
    compare(query, done);
  });

  it('or - 188', function(done) {
    var query = r.expr([1,2,3]).or(false);
    compare(query, done);
  });

  it('or - 189', function(done) {
    var query = r.expr([1,2,3]).or([]);
    compare(query, done);
  });

  it('or - 190', function(done) {
    var query = r.expr([1,2,3]).or([1,2,3]);
    compare(query, done);
  });

  it('or - 191', function(done) {
    var query = r.expr([1,2,3]).or([["foo", 2]]);
    compare(query, done);
  });

  it('or - 192', function(done) {
    var query = r.expr([1,2,3]).or([["foo", 2], ["bar", 1]]);
    compare(query, done);
  });

  it('or - 193', function(done) {
    var query = r.expr([1,2,3]).or({});
    compare(query, done);
  });

  it('or - 194', function(done) {
    var query = r.expr([1,2,3]).or({foo: "bar"});
    compare(query, done);
  });

  it('or - 195', function(done) {
    var query = r.expr([1,2,3]).or({foo: "bar", buzz: 1});
    compare(query, done);
  });

  it('or - 196', function(done) {
    var query = r.expr([1,2,3]).or(null);
    compare(query, done);
  });

  it('or - 197', function(done) {
    var query = r.expr([1,2,3]).or(r.now());
    compare(query, done, function(value) {
      return value instanceof Date;
    });
  });

  it('or - 198', function(done) {
    var query = r.expr([1,2,3]).or(function() { return 1; });
    compare(query, done);
  });

  it('or - 199', function(done) {
    var query = r.expr([1,2,3]).or(r.db(TEST_DB).table(TEST_TABLE));
    compare(query, done);
  });

  it('or - 200', function(done) {
    var query = r.expr([1,2,3]).or(r.binary(new Buffer("hello")));
    compare(query, done);
  });

  it('or - 201', function(done) {
    var query = r.expr([["foo", 2]]).or("foo");
    compare(query, done);
  });

  it('or - 202', function(done) {
    var query = r.expr([["foo", 2]]).or("");
    compare(query, done);
  });

  it('or - 203', function(done) {
    var query = r.expr([["foo", 2]]).or(0);
    compare(query, done);
  });

  it('or - 204', function(done) {
    var query = r.expr([["foo", 2]]).or(1);
    compare(query, done);
  });

  it('or - 205', function(done) {
    var query = r.expr([["foo", 2]]).or(-1);
    compare(query, done);
  });

  it('or - 206', function(done) {
    var query = r.expr([["foo", 2]]).or(10000);
    compare(query, done);
  });

  it('or - 207', function(done) {
    var query = r.expr([["foo", 2]]).or(true);
    compare(query, done);
  });

  it('or - 208', function(done) {
    var query = r.expr([["foo", 2]]).or(false);
    compare(query, done);
  });

  it('or - 209', function(done) {
    var query = r.expr([["foo", 2]]).or([]);
    compare(query, done);
  });

  it('or - 210', function(done) {
    var query = r.expr([["foo", 2]]).or([1,2,3]);
    compare(query, done);
  });

  it('or - 211', function(done) {
    var query = r.expr([["foo", 2]]).or([["foo", 2]]);
    compare(query, done);
  });

  it('or - 212', function(done) {
    var query = r.expr([["foo", 2]]).or([["foo", 2], ["bar", 1]]);
    compare(query, done);
  });

  it('or - 213', function(done) {
    var query = r.expr([["foo", 2]]).or({});
    compare(query, done);
  });

  it('or - 214', function(done) {
    var query = r.expr([["foo", 2]]).or({foo: "bar"});
    compare(query, done);
  });

  it('or - 215', function(done) {
    var query = r.expr([["foo", 2]]).or({foo: "bar", buzz: 1});
    compare(query, done);
  });

  it('or - 216', function(done) {
    var query = r.expr([["foo", 2]]).or(null);
    compare(query, done);
  });

  it('or - 217', function(done) {
    var query = r.expr([["foo", 2]]).or(r.now());
    compare(query, done);
  });

  it('or - 218', function(done) {
    var query = r.expr([["foo", 2]]).or(function() { return 1; });
    compare(query, done);
  });

  it('or - 219', function(done) {
    var query = r.expr([["foo", 2]]).or(r.db(TEST_DB).table(TEST_TABLE));
    compare(query, done);
  });

  it('or - 220', function(done) {
    var query = r.expr([["foo", 2]]).or(r.binary(new Buffer("hello")));
    compare(query, done);
  });

  it('or - 221', function(done) {
    var query = r.expr([["foo", 2], ["bar", 1]]).or("foo");
    compare(query, done);
  });

  it('or - 222', function(done) {
    var query = r.expr([["foo", 2], ["bar", 1]]).or("");
    compare(query, done);
  });

  it('or - 223', function(done) {
    var query = r.expr([["foo", 2], ["bar", 1]]).or(0);
    compare(query, done);
  });

  it('or - 224', function(done) {
    var query = r.expr([["foo", 2], ["bar", 1]]).or(1);
    compare(query, done);
  });

  it('or - 225', function(done) {
    var query = r.expr([["foo", 2], ["bar", 1]]).or(-1);
    compare(query, done);
  });

  it('or - 226', function(done) {
    var query = r.expr([["foo", 2], ["bar", 1]]).or(10000);
    compare(query, done);
  });

  it('or - 227', function(done) {
    var query = r.expr([["foo", 2], ["bar", 1]]).or(true);
    compare(query, done);
  });

  it('or - 228', function(done) {
    var query = r.expr([["foo", 2], ["bar", 1]]).or(false);
    compare(query, done);
  });

  it('or - 229', function(done) {
    var query = r.expr([["foo", 2], ["bar", 1]]).or([]);
    compare(query, done);
  });

  it('or - 230', function(done) {
    var query = r.expr([["foo", 2], ["bar", 1]]).or([1,2,3]);
    compare(query, done);
  });

  it('or - 231', function(done) {
    var query = r.expr([["foo", 2], ["bar", 1]]).or([["foo", 2]]);
    compare(query, done);
  });

  it('or - 232', function(done) {
    var query = r.expr([["foo", 2], ["bar", 1]]).or([["foo", 2], ["bar", 1]]);
    compare(query, done);
  });

  it('or - 233', function(done) {
    var query = r.expr([["foo", 2], ["bar", 1]]).or({});
    compare(query, done);
  });

  it('or - 234', function(done) {
    var query = r.expr([["foo", 2], ["bar", 1]]).or({foo: "bar"});
    compare(query, done);
  });

  it('or - 235', function(done) {
    var query = r.expr([["foo", 2], ["bar", 1]]).or({foo: "bar", buzz: 1});
    compare(query, done);
  });

  it('or - 236', function(done) {
    var query = r.expr([["foo", 2], ["bar", 1]]).or(null);
    compare(query, done);
  });

  it('or - 237', function(done) {
    var query = r.expr([["foo", 2], ["bar", 1]]).or(r.now());
    compare(query, done);
  });

  it('or - 238', function(done) {
    var query = r.expr([["foo", 2], ["bar", 1]]).or(function() { return 1; });
    compare(query, done);
  });

  it('or - 239', function(done) {
    var query = r.expr([["foo", 2], ["bar", 1]]).or(r.db(TEST_DB).table(TEST_TABLE));
    compare(query, done);
  });

  it('or - 240', function(done) {
    var query = r.expr([["foo", 2], ["bar", 1]]).or(r.binary(new Buffer("hello")));
    compare(query, done);
  });

  it('or - 241', function(done) {
    var query = r.expr({}).or("foo");
    compare(query, done);
  });

  it('or - 242', function(done) {
    var query = r.expr({}).or("");
    compare(query, done);
  });

  it('or - 243', function(done) {
    var query = r.expr({}).or(0);
    compare(query, done);
  });

  it('or - 244', function(done) {
    var query = r.expr({}).or(1);
    compare(query, done);
  });

  it('or - 245', function(done) {
    var query = r.expr({}).or(-1);
    compare(query, done);
  });

  it('or - 246', function(done) {
    var query = r.expr({}).or(10000);
    compare(query, done);
  });

  it('or - 247', function(done) {
    var query = r.expr({}).or(true);
    compare(query, done);
  });

  it('or - 248', function(done) {
    var query = r.expr({}).or(false);
    compare(query, done);
  });

  it('or - 249', function(done) {
    var query = r.expr({}).or([]);
    compare(query, done);
  });

  it('or - 250', function(done) {
    var query = r.expr({}).or([1,2,3]);
    compare(query, done);
  });

  it('or - 251', function(done) {
    var query = r.expr({}).or([["foo", 2]]);
    compare(query, done);
  });

  it('or - 252', function(done) {
    var query = r.expr({}).or([["foo", 2], ["bar", 1]]);
    compare(query, done);
  });

  it('or - 253', function(done) {
    var query = r.expr({}).or({});
    compare(query, done);
  });

  it('or - 254', function(done) {
    var query = r.expr({}).or({foo: "bar"});
    compare(query, done);
  });

  it('or - 255', function(done) {
    var query = r.expr({}).or({foo: "bar", buzz: 1});
    compare(query, done);
  });

  it('or - 256', function(done) {
    var query = r.expr({}).or(null);
    compare(query, done);
  });

  it('or - 257', function(done) {
    var query = r.expr({}).or(r.now());
    compare(query, done);
  });

  it('or - 258', function(done) {
    var query = r.expr({}).or(function() { return 1; });
    compare(query, done);
  });

  it('or - 259', function(done) {
    var query = r.expr({}).or(r.db(TEST_DB).table(TEST_TABLE));
    compare(query, done);
  });

  it('or - 260', function(done) {
    var query = r.expr({}).or(r.binary(new Buffer("hello")));
    compare(query, done);
  });

  it('or - 261', function(done) {
    var query = r.expr({foo: "bar"}).or("foo");
    compare(query, done);
  });

  it('or - 262', function(done) {
    var query = r.expr({foo: "bar"}).or("");
    compare(query, done);
  });

  it('or - 263', function(done) {
    var query = r.expr({foo: "bar"}).or(0);
    compare(query, done);
  });

  it('or - 264', function(done) {
    var query = r.expr({foo: "bar"}).or(1);
    compare(query, done);
  });

  it('or - 265', function(done) {
    var query = r.expr({foo: "bar"}).or(-1);
    compare(query, done);
  });

  it('or - 266', function(done) {
    var query = r.expr({foo: "bar"}).or(10000);
    compare(query, done);
  });

  it('or - 267', function(done) {
    var query = r.expr({foo: "bar"}).or(true);
    compare(query, done);
  });

  it('or - 268', function(done) {
    var query = r.expr({foo: "bar"}).or(false);
    compare(query, done);
  });

  it('or - 269', function(done) {
    var query = r.expr({foo: "bar"}).or([]);
    compare(query, done);
  });

  it('or - 270', function(done) {
    var query = r.expr({foo: "bar"}).or([1,2,3]);
    compare(query, done);
  });

  it('or - 271', function(done) {
    var query = r.expr({foo: "bar"}).or([["foo", 2]]);
    compare(query, done);
  });

  it('or - 272', function(done) {
    var query = r.expr({foo: "bar"}).or([["foo", 2], ["bar", 1]]);
    compare(query, done);
  });

  it('or - 273', function(done) {
    var query = r.expr({foo: "bar"}).or({});
    compare(query, done);
  });

  it('or - 274', function(done) {
    var query = r.expr({foo: "bar"}).or({foo: "bar"});
    compare(query, done);
  });

  it('or - 275', function(done) {
    var query = r.expr({foo: "bar"}).or({foo: "bar", buzz: 1});
    compare(query, done);
  });

  it('or - 276', function(done) {
    var query = r.expr({foo: "bar"}).or(null);
    compare(query, done);
  });

  it('or - 277', function(done) {
    var query = r.expr({foo: "bar"}).or(r.now());
    compare(query, done);
  });

  it('or - 278', function(done) {
    var query = r.expr({foo: "bar"}).or(function() { return 1; });
    compare(query, done);
  });

  it('or - 279', function(done) {
    var query = r.expr({foo: "bar"}).or(r.db(TEST_DB).table(TEST_TABLE));
    compare(query, done);
  });

  it('or - 280', function(done) {
    var query = r.expr({foo: "bar"}).or(r.binary(new Buffer("hello")));
    compare(query, done);
  });

  it('or - 281', function(done) {
    var query = r.expr({foo: "bar", buzz: 1}).or("foo");
    compare(query, done);
  });

  it('or - 282', function(done) {
    var query = r.expr({foo: "bar", buzz: 1}).or("");
    compare(query, done);
  });

  it('or - 283', function(done) {
    var query = r.expr({foo: "bar", buzz: 1}).or(0);
    compare(query, done);
  });

  it('or - 284', function(done) {
    var query = r.expr({foo: "bar", buzz: 1}).or(1);
    compare(query, done);
  });

  it('or - 285', function(done) {
    var query = r.expr({foo: "bar", buzz: 1}).or(-1);
    compare(query, done);
  });

  it('or - 286', function(done) {
    var query = r.expr({foo: "bar", buzz: 1}).or(10000);
    compare(query, done);
  });

  it('or - 287', function(done) {
    var query = r.expr({foo: "bar", buzz: 1}).or(true);
    compare(query, done);
  });

  it('or - 288', function(done) {
    var query = r.expr({foo: "bar", buzz: 1}).or(false);
    compare(query, done);
  });

  it('or - 289', function(done) {
    var query = r.expr({foo: "bar", buzz: 1}).or([]);
    compare(query, done);
  });

  it('or - 290', function(done) {
    var query = r.expr({foo: "bar", buzz: 1}).or([1,2,3]);
    compare(query, done);
  });

  it('or - 291', function(done) {
    var query = r.expr({foo: "bar", buzz: 1}).or([["foo", 2]]);
    compare(query, done);
  });

  it('or - 292', function(done) {
    var query = r.expr({foo: "bar", buzz: 1}).or([["foo", 2], ["bar", 1]]);
    compare(query, done);
  });

  it('or - 293', function(done) {
    var query = r.expr({foo: "bar", buzz: 1}).or({});
    compare(query, done);
  });

  it('or - 294', function(done) {
    var query = r.expr({foo: "bar", buzz: 1}).or({foo: "bar"});
    compare(query, done);
  });

  it('or - 295', function(done) {
    var query = r.expr({foo: "bar", buzz: 1}).or({foo: "bar", buzz: 1});
    compare(query, done);
  });

  it('or - 296', function(done) {
    var query = r.expr({foo: "bar", buzz: 1}).or(null);
    compare(query, done);
  });

  it('or - 297', function(done) {
    var query = r.expr({foo: "bar", buzz: 1}).or(r.now());
    compare(query, done);
  });

  it('or - 298', function(done) {
    var query = r.expr({foo: "bar", buzz: 1}).or(function() { return 1; });
    compare(query, done);
  });

  it('or - 299', function(done) {
    var query = r.expr({foo: "bar", buzz: 1}).or(r.db(TEST_DB).table(TEST_TABLE));
    compare(query, done);
  });

  it('or - 300', function(done) {
    var query = r.expr({foo: "bar", buzz: 1}).or(r.binary(new Buffer("hello")));
    compare(query, done);
  });

  it('or - 301', function(done) {
    var query = r.expr(null).or("foo");
    compare(query, done);
  });

  it('or - 302', function(done) {
    var query = r.expr(null).or("");
    compare(query, done);
  });

  it('or - 303', function(done) {
    var query = r.expr(null).or(0);
    compare(query, done);
  });

  it('or - 304', function(done) {
    var query = r.expr(null).or(1);
    compare(query, done);
  });

  it('or - 305', function(done) {
    var query = r.expr(null).or(-1);
    compare(query, done);
  });

  it('or - 306', function(done) {
    var query = r.expr(null).or(10000);
    compare(query, done);
  });

  it('or - 307', function(done) {
    var query = r.expr(null).or(true);
    compare(query, done);
  });

  it('or - 308', function(done) {
    var query = r.expr(null).or(false);
    compare(query, done);
  });

  it('or - 309', function(done) {
    var query = r.expr(null).or([]);
    compare(query, done);
  });

  it('or - 310', function(done) {
    var query = r.expr(null).or([1,2,3]);
    compare(query, done);
  });

  it('or - 311', function(done) {
    var query = r.expr(null).or([["foo", 2]]);
    compare(query, done);
  });

  it('or - 312', function(done) {
    var query = r.expr(null).or([["foo", 2], ["bar", 1]]);
    compare(query, done);
  });

  it('or - 313', function(done) {
    var query = r.expr(null).or({});
    compare(query, done);
  });

  it('or - 314', function(done) {
    var query = r.expr(null).or({foo: "bar"});
    compare(query, done);
  });

  it('or - 315', function(done) {
    var query = r.expr(null).or({foo: "bar", buzz: 1});
    compare(query, done);
  });

  it('or - 316', function(done) {
    var query = r.expr(null).or(null);
    compare(query, done);
  });

  it('or - 317', function(done) {
    var query = r.expr(null).or(r.now());
    compare(query, done, function(value) {
      return value instanceof Date;
    });
  });

  it('or - 318', function(done) {
    var query = r.expr(null).or(function() { return 1; });
    compare(query, done);
  });

  it('or - 319', function(done) {
    var query = r.expr(null).or(r.db(TEST_DB).table(TEST_TABLE));
    compare(query, done);
  });

  it('or - 320', function(done) {
    var query = r.expr(null).or(r.binary(new Buffer("hello")));
    compare(query, done);
  });

  it('or - 321', function(done) {
    var query = r.expr(r.now()).or("foo");
    compare(query, done, function(value) {
      return value instanceof Date;
    });
  });

  it('or - 322', function(done) {
    var query = r.expr(r.now()).or("");
    compare(query, done, function(value) {
      return value instanceof Date;
    });
  });

  it('or - 323', function(done) {
    var query = r.expr(r.now()).or(0);
    compare(query, done, function(value) {
      return value instanceof Date;
    });
  });

  it('or - 324', function(done) {
    var query = r.expr(r.now()).or(1);
    compare(query, done, function(value) {
      return value instanceof Date;
    });
  });

  it('or - 325', function(done) {
    var query = r.expr(r.now()).or(-1);
    compare(query, done, function(value) {
      return value instanceof Date;
    });
  });

  it('or - 326', function(done) {
    var query = r.expr(r.now()).or(10000);
    compare(query, done, function(value) {
      return value instanceof Date;
    });
  });

  it('or - 327', function(done) {
    var query = r.expr(r.now()).or(true);
    compare(query, done, function(value) {
      return value instanceof Date;
    });
  });

  it('or - 328', function(done) {
    var query = r.expr(r.now()).or(false);
    compare(query, done, function(value) {
      return value instanceof Date;
    });
  });

  it('or - 329', function(done) {
    var query = r.expr(r.now()).or([]);
    compare(query, done, function(value) {
      return value instanceof Date;
    });
  });

  it('or - 330', function(done) {
    var query = r.expr(r.now()).or([1,2,3]);
    compare(query, done, function(value) {
      return value instanceof Date;
    });
  });

  it('or - 331', function(done) {
    var query = r.expr(r.now()).or([["foo", 2]]);
    compare(query, done, function(value) {
      return value instanceof Date;
    });
  });

  it('or - 332', function(done) {
    var query = r.expr(r.now()).or([["foo", 2], ["bar", 1]]);
    compare(query, done, function(value) {
      return value instanceof Date;
    });
  });

  it('or - 333', function(done) {
    var query = r.expr(r.now()).or({});
    compare(query, done, function(value) {
      return value instanceof Date;
    });
  });

  it('or - 334', function(done) {
    var query = r.expr(r.now()).or({foo: "bar"});
    compare(query, done, function(value) {
      return value instanceof Date;
    });
  });

  it('or - 335', function(done) {
    var query = r.expr(r.now()).or({foo: "bar", buzz: 1});
    compare(query, done, function(value) {
      return value instanceof Date;
    });
  });

  it('or - 336', function(done) {
    var query = r.expr(r.now()).or(null);
    compare(query, done, function(value) {
      return value instanceof Date;
    });
  });

  it('or - 337', function(done) {
    var query = r.expr(r.now()).or(r.now());
    compare(query, done, function(value) {
      return value instanceof Date;
    });
  });

  it('or - 338', function(done) {
    var query = r.expr(r.now()).or(function() { return 1; });
    compare(query, done, function(value) {
      return value instanceof Date;
    });
  });

  it('or - 339', function(done) {
    var query = r.expr(r.now()).or(r.db(TEST_DB).table(TEST_TABLE));
    compare(query, done, function(value) {
      return value instanceof Date;
    });
  });

  it('or - 340', function(done) {
    var query = r.expr(r.now()).or(r.binary(new Buffer("hello")));
    compare(query, done, function(value) {
      return value instanceof Date;
    });
  });

  it('or - 341', function(done) {
    var query = r.expr(function() { return 1; }).or("foo");
    compare(query, done);
  });

  it('or - 342', function(done) {
    var query = r.expr(function() { return 1; }).or("");
    compare(query, done);
  });

  it('or - 343', function(done) {
    var query = r.expr(function() { return 1; }).or(0);
    compare(query, done);
  });

  it('or - 344', function(done) {
    var query = r.expr(function() { return 1; }).or(1);
    compare(query, done);
  });

  it('or - 345', function(done) {
    var query = r.expr(function() { return 1; }).or(-1);
    compare(query, done);
  });

  it('or - 346', function(done) {
    var query = r.expr(function() { return 1; }).or(10000);
    compare(query, done);
  });

  it('or - 347', function(done) {
    var query = r.expr(function() { return 1; }).or(true);
    compare(query, done);
  });

  it('or - 348', function(done) {
    var query = r.expr(function() { return 1; }).or(false);
    compare(query, done);
  });

  it('or - 349', function(done) {
    var query = r.expr(function() { return 1; }).or([]);
    compare(query, done);
  });

  it('or - 350', function(done) {
    var query = r.expr(function() { return 1; }).or([1,2,3]);
    compare(query, done);
  });

  it('or - 351', function(done) {
    var query = r.expr(function() { return 1; }).or([["foo", 2]]);
    compare(query, done);
  });

  it('or - 352', function(done) {
    var query = r.expr(function() { return 1; }).or([["foo", 2], ["bar", 1]]);
    compare(query, done);
  });

  it('or - 353', function(done) {
    var query = r.expr(function() { return 1; }).or({});
    compare(query, done);
  });

  it('or - 354', function(done) {
    var query = r.expr(function() { return 1; }).or({foo: "bar"});
    compare(query, done);
  });

  it('or - 355', function(done) {
    var query = r.expr(function() { return 1; }).or({foo: "bar", buzz: 1});
    compare(query, done);
  });

  it('or - 356', function(done) {
    var query = r.expr(function() { return 1; }).or(null);
    compare(query, done);
  });

  it('or - 357', function(done) {
    var query = r.expr(function() { return 1; }).or(r.now());
    compare(query, done);
  });

  it('or - 358', function(done) {
    var query = r.expr(function() { return 1; }).or(function() { return 1; });
    compare(query, done);
  });

  it('or - 359', function(done) {
    var query = r.expr(function() { return 1; }).or(r.db(TEST_DB).table(TEST_TABLE));
    compare(query, done);
  });

  it('or - 360', function(done) {
    var query = r.expr(function() { return 1; }).or(r.binary(new Buffer("hello")));
    compare(query, done);
  });

  it('or - 361', function(done) {
    var query = r.expr(r.db(TEST_DB).table(TEST_TABLE)).or("foo");
    compare(query, done);
  });

  it('or - 362', function(done) {
    var query = r.expr(r.db(TEST_DB).table(TEST_TABLE)).or("");
    compare(query, done);
  });

  it('or - 363', function(done) {
    var query = r.expr(r.db(TEST_DB).table(TEST_TABLE)).or(0);
    compare(query, done);
  });

  it('or - 364', function(done) {
    var query = r.expr(r.db(TEST_DB).table(TEST_TABLE)).or(1);
    compare(query, done);
  });

  it('or - 365', function(done) {
    var query = r.expr(r.db(TEST_DB).table(TEST_TABLE)).or(-1);
    compare(query, done);
  });

  it('or - 366', function(done) {
    var query = r.expr(r.db(TEST_DB).table(TEST_TABLE)).or(10000);
    compare(query, done);
  });

  it('or - 367', function(done) {
    var query = r.expr(r.db(TEST_DB).table(TEST_TABLE)).or(true);
    compare(query, done);
  });

  it('or - 368', function(done) {
    var query = r.expr(r.db(TEST_DB).table(TEST_TABLE)).or(false);
    compare(query, done);
  });

  it('or - 369', function(done) {
    var query = r.expr(r.db(TEST_DB).table(TEST_TABLE)).or([]);
    compare(query, done);
  });

  it('or - 370', function(done) {
    var query = r.expr(r.db(TEST_DB).table(TEST_TABLE)).or([1,2,3]);
    compare(query, done);
  });

  it('or - 371', function(done) {
    var query = r.expr(r.db(TEST_DB).table(TEST_TABLE)).or([["foo", 2]]);
    compare(query, done);
  });

  it('or - 372', function(done) {
    var query = r.expr(r.db(TEST_DB).table(TEST_TABLE)).or([["foo", 2], ["bar", 1]]);
    compare(query, done);
  });

  it('or - 373', function(done) {
    var query = r.expr(r.db(TEST_DB).table(TEST_TABLE)).or({});
    compare(query, done);
  });

  it('or - 374', function(done) {
    var query = r.expr(r.db(TEST_DB).table(TEST_TABLE)).or({foo: "bar"});
    compare(query, done);
  });

  it('or - 375', function(done) {
    var query = r.expr(r.db(TEST_DB).table(TEST_TABLE)).or({foo: "bar", buzz: 1});
    compare(query, done);
  });

  it('or - 376', function(done) {
    var query = r.expr(r.db(TEST_DB).table(TEST_TABLE)).or(null);
    compare(query, done);
  });

  it('or - 377', function(done) {
    var query = r.expr(r.db(TEST_DB).table(TEST_TABLE)).or(r.now());
    compare(query, done);
  });

  it('or - 378', function(done) {
    var query = r.expr(r.db(TEST_DB).table(TEST_TABLE)).or(function() { return 1; });
    compare(query, done);
  });

  it('or - 379', function(done) {
    var query = r.expr(r.db(TEST_DB).table(TEST_TABLE)).or(r.db(TEST_DB).table(TEST_TABLE));
    compare(query, done);
  });

  it('or - 380', function(done) {
    var query = r.expr(r.db(TEST_DB).table(TEST_TABLE)).or(r.binary(new Buffer("hello")));
    compare(query, done);
  });

  it('or - 381', function(done) {
    var query = r.expr(r.binary(new Buffer("hello"))).or("foo");
    compare(query, done);
  });

  it('or - 382', function(done) {
    var query = r.expr(r.binary(new Buffer("hello"))).or("");
    compare(query, done);
  });

  it('or - 383', function(done) {
    var query = r.expr(r.binary(new Buffer("hello"))).or(0);
    compare(query, done);
  });

  it('or - 384', function(done) {
    var query = r.expr(r.binary(new Buffer("hello"))).or(1);
    compare(query, done);
  });

  it('or - 385', function(done) {
    var query = r.expr(r.binary(new Buffer("hello"))).or(-1);
    compare(query, done);
  });

  it('or - 386', function(done) {
    var query = r.expr(r.binary(new Buffer("hello"))).or(10000);
    compare(query, done);
  });

  it('or - 387', function(done) {
    var query = r.expr(r.binary(new Buffer("hello"))).or(true);
    compare(query, done);
  });

  it('or - 388', function(done) {
    var query = r.expr(r.binary(new Buffer("hello"))).or(false);
    compare(query, done);
  });

  it('or - 389', function(done) {
    var query = r.expr(r.binary(new Buffer("hello"))).or([]);
    compare(query, done);
  });

  it('or - 390', function(done) {
    var query = r.expr(r.binary(new Buffer("hello"))).or([1,2,3]);
    compare(query, done);
  });

  it('or - 391', function(done) {
    var query = r.expr(r.binary(new Buffer("hello"))).or([["foo", 2]]);
    compare(query, done);
  });

  it('or - 392', function(done) {
    var query = r.expr(r.binary(new Buffer("hello"))).or([["foo", 2], ["bar", 1]]);
    compare(query, done);
  });

  it('or - 393', function(done) {
    var query = r.expr(r.binary(new Buffer("hello"))).or({});
    compare(query, done);
  });

  it('or - 394', function(done) {
    var query = r.expr(r.binary(new Buffer("hello"))).or({foo: "bar"});
    compare(query, done);
  });

  it('or - 395', function(done) {
    var query = r.expr(r.binary(new Buffer("hello"))).or({foo: "bar", buzz: 1});
    compare(query, done);
  });

  it('or - 396', function(done) {
    var query = r.expr(r.binary(new Buffer("hello"))).or(null);
    compare(query, done);
  });

  it('or - 397', function(done) {
    var query = r.expr(r.binary(new Buffer("hello"))).or(r.now());
    compare(query, done);
  });

  it('or - 398', function(done) {
    var query = r.expr(r.binary(new Buffer("hello"))).or(function() { return 1; });
    compare(query, done);
  });

  it('or - 399', function(done) {
    var query = r.expr(r.binary(new Buffer("hello"))).or(r.db(TEST_DB).table(TEST_TABLE));
    compare(query, done);
  });

  it('or - 400', function(done) {
    var query = r.expr(r.binary(new Buffer("hello"))).or(r.binary(new Buffer("hello")));
    compare(query, done);
  });

  it('or - 401', function(done) {
    var query = r.or();
    compare(query, done);
  });

  it('eq - 1', function(done) {
    var query = r.expr(2).eq(1);
    compare(query, done);
  });

  it('eq - 2', function(done) {
    var query = r.expr(2).eq(2);
    compare(query, done);
  });

  it('eq - 3', function(done) {
    var query = r.expr(2).eq(2, 3);
    compare(query, done);
  });

  it('eq - 4', function(done) {
    var query = r.expr(2).eq(2, 2);
    compare(query, done);
  });

  it('eq - 5', function(done) {
    var query = r.expr(false).eq(true);
    compare(query, done);
  });

  it('eq - 6', function(done) {
    var query = r.expr(true).eq(false);
    compare(query, done);
  });

  it('eq - 7', function(done) {
    var query = r.expr('foo').eq('foo');
    compare(query, done);
  });

  it('eq - 8', function(done) {
    var query = r.expr('foo').eq('bar');
    compare(query, done);
  });

  it('eq - 9', function(done) {
    var query = r.expr([1,2,3,4]).eq([1,2,3,4]);
    compare(query, done);
  });

  it('eq - 10', function(done) {
    var query = r.expr({}).eq({});
    compare(query, done);
  });

  it('eq - 11', function(done) {
    var query = r.expr({foo: {bar: 'lol'}}).eq({foo: {bar: 'lol'}});
    compare(query, done);
  });

  it('eq - 12', function(done) {
    var query = r.now().eq(r.now());
    compare(query, done);
  });

  it('eq - 13', function(done) {
    var query = r.eq('bar');
    compare(query, done);
  });

  it('eq - 14', function(done) {
    var query = r.expr('bar').eq(r.db(TEST_DB).table(TEST_TABLE));
    compare(query, done);
  });

  it('eq - 15', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).eq('bar');
    compare(query, done);
  });

  it('ne - 1', function(done) {
    var query = r.expr(2).ne(1);
    compare(query, done);
  });

  it('ne - 2', function(done) {
    var query = r.expr(2).ne(2);
    compare(query, done);
  });

  it('ne - 3', function(done) {
    var query = r.expr(2).ne(2, 3);
    compare(query, done);
  });

  it('ne - 4', function(done) {
    var query = r.expr(2).ne(2, 2);
    compare(query, done);
  });

  it('ne - 5', function(done) {
    var query = r.expr(false).ne(true);
    compare(query, done);
  });

  it('ne - 6', function(done) {
    var query = r.expr(true).ne(false);
    compare(query, done);
  });

  it('ne - 7', function(done) {
    var query = r.expr('foo').ne('foo');
    compare(query, done);
  });

  it('ne - 8', function(done) {
    var query = r.expr('foo').ne('bar');
    compare(query, done);
  });

  it('ne - 9', function(done) {
    var query = r.expr([1,2,3,4]).ne([1,2,3,4]);
    compare(query, done);
  });

  it('ne - 10', function(done) {
    var query = r.expr({}).ne({});
    compare(query, done);
  });

  it('ne - 11', function(done) {
    var query = r.expr({foo: {bar: 'lol'}}).ne({foo: {bar: 'lol'}});
    compare(query, done);
  });

  it('ne - 12', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).ne('bar');
    compare(query, done);
  });

  it('ne - 13', function(done) {
    var query = r.expr('bar').eq(r.db(TEST_DB).table(TEST_TABLE));
    compare(query, done);
  });

  it('ne - 14', function(done) {
    var query = r.ne('bar');
    compare(query, done);
  });

  it('lt - 1', function(done) {
    var query = r.expr(2).lt(1);
    compare(query, done);
  });

  it('lt - 2', function(done) {
    var query = r.expr(2).lt(2);
    compare(query, done);
  });

  it('lt - 3', function(done) {
    var query = r.expr(2).lt(3);
    compare(query, done);
  });

  it('lt - 4', function(done) {
    var query = r.expr(2).lt('foo');
    compare(query, done);
  });

  it('lt - 5', function(done) {
    var query = r.expr('foo').lt(2);
    compare(query, done);
  });

  it('lt - 6', function(done) {
    var query = r.expr('foo').lt(null);
    compare(query, done);
  });

  it('lt - 7', function(done) {
    var query = r.expr(null).lt('foo');
    compare(query, done);
  });

  it('lt - 8', function(done) {
    var query = r.expr(false).lt(true);
    compare(query, done);
  });

  it('lt - 9', function(done) {
    var query = r.expr(true).lt(false);
    compare(query, done);
  });

  it('lt - 10', function(done) {
    var query = r.expr(true).lt('foo');
    compare(query, done);
  });

  it('lt - 11', function(done) {
    var query = r.expr(true).lt(2);
    compare(query, done);
  });

  it('lt - 12', function(done) {
    var query = r.expr(true).lt(null);
    compare(query, done);
  });

  it('lt - 13', function(done) {
    var query = r.expr(true).lt([1,2,3]);
    compare(query, done);
  });

  it('lt - 14', function(done) {
    var query = r.expr([1,2]).lt([1,2,3]);
    compare(query, done);
  });

  it('lt - 15', function(done) {
    var query = r.expr([1,2,3]).lt([1,2]);
    compare(query, done);
  });

  it('lt - 16', function(done) {
    var query = r.expr([1,2,3]).lt([2,2]);
    compare(query, done);
  });

  it('lt - 17', function(done) {
    var query = r.now().lt([2,2]);
    compare(query, done);
  });

  it('lt - 18', function(done) {
    var query = r.now().lt(2);
    compare(query, done);
  });

  it('lt - 19', function(done) {
    var query = r.now().lt('foo');
    compare(query, done);
  });

  it('lt - 20', function(done) {
    var query = r.now().lt(null);
    compare(query, done);
  });

  it('lt - 21', function(done) {
    var query = r.now().lt(false);
    compare(query, done);
  });

  it('lt - 22', function(done) {
    var query = r.now().lt(true);
    compare(query, done);
  });

  it('lt - 23', function(done) {
    var query = r.now().lt({});
    compare(query, done);
  });

  it('lt - 24', function(done) {
    var query = r.expr('foo').lt('bar');
    compare(query, done);
  });

  it('lt - 25', function(done) {
    var query = r.expr('foo').lt(2);
    compare(query, done);
  });

  it('lt - 26', function(done) {
    var query = r.expr('foo').lt(r.now());
    compare(query, done);
  });

  it('lt - 27', function(done) {
    var query = r.expr(2).lt(new Buffer('Hello world'));
    compare(query, done);
  });

  it('lt - 28', function(done) {
    var query = r.expr('foo').lt(new Buffer('Hello world'));
    compare(query, done);
  });

  it('lt - 29', function(done) {
    var query = r.expr(new Buffer('Hello world')).lt(new Buffer('Hello world'));
    compare(query, done);
  });

  it('lt - 30', function(done) {
    var query = r.now().lt(new Buffer('Hello world'));
    compare(query, done);
  });

  it('lt - 31', function(done) {
    var query = r.expr(true).lt(new Buffer('Hello world'));
    compare(query, done);
  });

  it('lt - 32', function(done) {
    var query = r.expr(false).lt(new Buffer('Hello world'));
    compare(query, done);
  });

  it('lt - 33', function(done) {
    var query = r.expr(null).lt(new Buffer('Hello world'));
    compare(query, done);
  });

  it('lt - 34', function(done) {
    var query = r.expr({foo: 'bar'}).lt(new Buffer('Hello world'));
    compare(query, done);
  });

  it('lt - 35', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).lt('bar');
    compare(query, done);
  });

  it('lt - 36', function(done) {
    var query = r.expr('foo').lt(r.db(TEST_DB).table(TEST_TABLE));
    compare(query, done);
  });

  it('lt - 37', function(done) {
    var query = r.lt('foo');
    compare(query, done);
  });

  it('lt - 38', function(done) {
    var query = r.lt();
    compare(query, done);
  });

  it('lt - 39', function(done) {
    var query = r.expr({ bar: [ 100, 101, 102 ], foo: 10, id: 1 }).lt({ bar: [ 200, 201, 202 ], foo: 20, id: 2 });
    compare(query, done);
  });

  it('lt - 40', function(done) {
    var query = r.expr({ bbar: [ 100, 101, 102 ], foo: 10, id: 1 }).lt({ bar: [ 200, 201, 202 ], foo: 20, id: 2 });
    compare(query, done);
  });

  it('lt - 41', function(done) {
    var query = r.expr({ aar: [ 100, 101, 102 ], foo: 10, id: 1 }).lt({ bar: [ 200, 201, 202 ], foo: 20, id: 2 });
    compare(query, done);
  });

  it('lt - 42', function(done) {
    var query = r.expr({ bar: [ 100, 101, 102 ], foo: 10, id: 1 }).lt({ bar: [ 200, 201, 202 ], foo: 20, id: 2 });
    compare(query, done);
  });

  it('lt - 43', function(done) {
    var query =r.expr({ bar: [ 100, 101, 102 ], foo: 20, id: 1 }).lt({ bar: [ 400, 401, 402 ], foo: 10, id: 4 });
    compare(query, done);
  });

  it('lt - 44', function(done) {
    var query = r.expr({
      id: 4,
      foo: 10,
      bar: [ 400, 401, 402 ],
    }).lt({
      id: 1,
      foo: 20,
      bar: [ 100, 101, 102 ],
    });
    compare(query, done);
  });

  it('lt - 45', function(done) {
    var query = r.expr({
      "new_val": {
        "bar": 0,
        "copyId": 2,
        "foo": 2,
        "id": 2
      },
      "old_val": {
        "bar": 0,
        "copyId": 2,
        "foo": 1,
        "id": 2
      }
    }).lt({
      "new_val": {
        "bar": 0,
        "copyId": 3,
        "foo": 2,
        "id": 3
      },
      "old_val": {
        "bar": 0,
        "copyId": 3,
        "foo": 1,
        "id": 3
      }
    });
    compare(query, done);
  });

  it('gt - 1', function(done) {
    var query = r.expr(2).gt(1);
    compare(query, done);
  });

  it('gt - 2', function(done) {
    var query = r.expr(2).gt(2);
    compare(query, done);
  });

  it('gt - 3', function(done) {
    var query = r.expr(2).gt(3);
    compare(query, done);
  });

  it('gt - 4', function(done) {
    var query = r.expr(2).gt('foo');
    compare(query, done);
  });

  it('gt - 5', function(done) {
    var query = r.expr('foo').gt(2);
    compare(query, done);
  });

  it('gt - 6', function(done) {
    var query = r.expr('foo').gt(null);
    compare(query, done);
  });

  it('gt - 7', function(done) {
    var query = r.expr(null).gt('foo');
    compare(query, done);
  });

  it('gt - 8', function(done) {
    var query = r.expr(false).gt(true);
    compare(query, done);
  });

  it('gt - 9', function(done) {
    var query = r.expr(true).gt(false);
    compare(query, done);
  });

  it('gt - 10', function(done) {
    var query = r.expr(true).gt('foo');
    compare(query, done);
  });

  it('gt - 11', function(done) {
    var query = r.expr(true).gt(2);
    compare(query, done);
  });

  it('gt - 12', function(done) {
    var query = r.expr(true).gt(null);
    compare(query, done);
  });

  it('gt - 13', function(done) {
    var query = r.expr(true).gt([1,2,3]);
    compare(query, done);
  });

  it('gt - 14', function(done) {
    var query = r.expr([1,2]).gt([1,2,3]);
    compare(query, done);
  });

  it('gt - 15', function(done) {
    var query = r.expr([1,2,3]).gt([1,2]);
    compare(query, done);
  });

  it('gt - 16', function(done) {
    var query = r.expr([1,2,3]).gt([2,2]);
    compare(query, done);
  });

  it('gt - 17', function(done) {
    var query = r.now().gt([2,2]);
    compare(query, done);
  });

  it('gt - 18', function(done) {
    var query = r.now().gt(2);
    compare(query, done);
  });

  it('gt - 19', function(done) {
    var query = r.now().gt('foo');
    compare(query, done);
  });

  it('gt - 20', function(done) {
    var query = r.now().gt(null);
    compare(query, done);
  });

  it('gt - 21', function(done) {
    var query = r.now().gt(false);
    compare(query, done);
  });

  it('gt - 22', function(done) {
    var query = r.now().gt(true);
    compare(query, done);
  });

  it('gt - 23', function(done) {
    var query = r.now().gt({});
    compare(query, done);
  });

  it('gt - 24', function(done) {
    var query = r.expr('foo').gt('bar');
    compare(query, done);
  });

  it('gt - 25', function(done) {
    var query = r.expr('foo').gt(2);
    compare(query, done);
  });

  it('gt - 26', function(done) {
    var query = r.expr('foo').gt(r.now());
    compare(query, done);
  });

  it('gt - 27', function(done) {
    var query = r.expr(2).gt(new Buffer('Hello world'));
    compare(query, done);
  });

  it('gt - 28', function(done) {
    var query = r.expr('foo').gt(new Buffer('Hello world'));
    compare(query, done);
  });

  it('gt - 29', function(done) {
    var query = r.expr(new Buffer('Hello world')).gt(new Buffer('Hello world'));
    compare(query, done);
  });

  it('gt - 30', function(done) {
    var query = r.now().gt(new Buffer('Hello world'));
    compare(query, done);
  });

  it('gt - 31', function(done) {
    var query = r.expr(true).gt(new Buffer('Hello world'));
    compare(query, done);
  });

  it('gt - 32', function(done) {
    var query = r.expr(false).gt(new Buffer('Hello world'));
    compare(query, done);
  });

  it('gt - 33', function(done) {
    var query = r.expr(null).gt(new Buffer('Hello world'));
    compare(query, done);
  });

  it('gt - 34', function(done) {
    var query = r.expr({foo: 'bar'}).gt(new Buffer('Hello world'));
    compare(query, done);
  });

  it('gt - 35', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).gt('bar');
    compare(query, done);
  });

  it('gt - 36', function(done) {
    var query = r.expr('foo').gt(r.db(TEST_DB).table(TEST_TABLE));
    compare(query, done);
  });

  it('gt - 37', function(done) {
    var query = r.gt('foo');
    compare(query, done);
  });

  it('gt - 38', function(done) {
    var query = r.gt();
    compare(query, done);
  });

  it('le - 1', function(done) {
    var query = r.expr(2).le(1);
    compare(query, done);
  });

  it('le - 2', function(done) {
    var query = r.expr(2).le(2);
    compare(query, done);
  });

  it('le - 3', function(done) {
    var query = r.expr(2).le(3);
    compare(query, done);
  });

  it('le - 4', function(done) {
    var query = r.expr(2).le('foo');
    compare(query, done);
  });

  it('le - 5', function(done) {
    var query = r.expr('foo').le(2);
    compare(query, done);
  });

  it('le - 6', function(done) {
    var query = r.expr('foo').le(null);
    compare(query, done);
  });

  it('le - 7', function(done) {
    var query = r.expr(null).le('foo');
    compare(query, done);
  });

  it('le - 8', function(done) {
    var query = r.expr(false).le(true);
    compare(query, done);
  });

  it('le - 9', function(done) {
    var query = r.expr(true).le(false);
    compare(query, done);
  });

  it('le - 10', function(done) {
    var query = r.expr(true).le('foo');
    compare(query, done);
  });

  it('le - 11', function(done) {
    var query = r.expr(true).le(2);
    compare(query, done);
  });

  it('le - 12', function(done) {
    var query = r.expr(true).le(null);
    compare(query, done);
  });

  it('le - 13', function(done) {
    var query = r.expr(true).le([1,2,3]);
    compare(query, done);
  });

  it('le - 14', function(done) {
    var query = r.expr([1,2]).le([1,2,3]);
    compare(query, done);
  });

  it('le - 15', function(done) {
    var query = r.expr([1,2,3]).le([1,2]);
    compare(query, done);
  });

  it('le - 16', function(done) {
    var query = r.expr([1,2,3]).le([2,2]);
    compare(query, done);
  });

  it('le - 17', function(done) {
    var query = r.now().le([2,2]);
    compare(query, done);
  });

  it('le - 18', function(done) {
    var query = r.now().le(2);
    compare(query, done);
  });

  it('le - 19', function(done) {
    var query = r.now().le('foo');
    compare(query, done);
  });

  it('le - 20', function(done) {
    var query = r.now().le(null);
    compare(query, done);
  });

  it('le - 21', function(done) {
    var query = r.now().le(false);
    compare(query, done);
  });

  it('le - 22', function(done) {
    var query = r.now().le(true);
    compare(query, done);
  });

  it('le - 23', function(done) {
    var query = r.now().le({});
    compare(query, done);
  });

  it('le - 24', function(done) {
    var query = r.expr('foo').le('bar');
    compare(query, done);
  });

  it('le - 25', function(done) {
    var query = r.expr('foo').le(2);
    compare(query, done);
  });

  it('le - 26', function(done) {
    var query = r.expr('foo').le(r.now());
    compare(query, done);
  });

  it('le - 27', function(done) {
    var query = r.expr(2).le(new Buffer('Hello world'));
    compare(query, done);
  });

  it('le - 28', function(done) {
    var query = r.expr('foo').le(new Buffer('Hello world'));
    compare(query, done);
  });

  it('le - 29', function(done) {
    var query = r.expr(new Buffer('Hello world')).le(new Buffer('Hello world'));
    compare(query, done);
  });

  it('le - 30', function(done) {
    var query = r.now().le(new Buffer('Hello world'));
    compare(query, done);
  });

  it('le - 31', function(done) {
    var query = r.expr(true).le(new Buffer('Hello world'));
    compare(query, done);
  });

  it('le - 32', function(done) {
    var query = r.expr(false).le(new Buffer('Hello world'));
    compare(query, done);
  });

  it('le - 33', function(done) {
    var query = r.expr(null).le(new Buffer('Hello world'));
    compare(query, done);
  });

  it('le - 34', function(done) {
    var query = r.expr({foo: 'bar'}).le(new Buffer('Hello world'));
    compare(query, done);
  });

  it('le - 35', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).le('bar');
    compare(query, done);
  });

  it('le - 36', function(done) {
    var query = r.expr('foo').le(r.db(TEST_DB).table(TEST_TABLE));
    compare(query, done);
  });

  it('le - 37', function(done) {
    var query = r.le('foo');
    compare(query, done);
  });

  it('le - 38', function(done) {
    var query = r.le();
    compare(query, done);
  });

  it('ge - 1', function(done) {
    var query = r.expr(2).ge(1);
    compare(query, done);
  });

  it('ge - 2', function(done) {
    var query = r.expr(2).ge(2);
    compare(query, done);
  });

  it('ge - 3', function(done) {
    var query = r.expr(2).ge(3);
    compare(query, done);
  });

  it('ge - 4', function(done) {
    var query = r.expr(2).ge('foo');
    compare(query, done);
  });

  it('ge - 5', function(done) {
    var query = r.expr('foo').ge(2);
    compare(query, done);
  });

  it('ge - 6', function(done) {
    var query = r.expr('foo').ge(null);
    compare(query, done);
  });

  it('ge - 7', function(done) {
    var query = r.expr(null).ge('foo');
    compare(query, done);
  });

  it('ge - 8', function(done) {
    var query = r.expr(false).ge(true);
    compare(query, done);
  });

  it('ge - 9', function(done) {
    var query = r.expr(true).ge(false);
    compare(query, done);
  });

  it('ge - 10', function(done) {
    var query = r.expr(true).ge('foo');
    compare(query, done);
  });

  it('ge - 11', function(done) {
    var query = r.expr(true).ge(2);
    compare(query, done);
  });

  it('ge - 12', function(done) {
    var query = r.expr(true).ge(null);
    compare(query, done);
  });

  it('ge - 13', function(done) {
    var query = r.expr(true).ge([1,2,3]);
    compare(query, done);
  });

  it('ge - 14', function(done) {
    var query = r.expr([1,2]).ge([1,2,3]);
    compare(query, done);
  });

  it('ge - 15', function(done) {
    var query = r.expr([1,2,3]).ge([1,2]);
    compare(query, done);
  });

  it('ge - 16', function(done) {
    var query = r.expr([1,2,3]).ge([2,2]);
    compare(query, done);
  });

  it('ge - 17', function(done) {
    var query = r.now().ge([2,2]);
    compare(query, done);
  });

  it('ge - 18', function(done) {
    var query = r.now().ge(2);
    compare(query, done);
  });

  it('ge - 19', function(done) {
    var query = r.now().ge('foo');
    compare(query, done);
  });

  it('ge - 20', function(done) {
    var query = r.now().ge(null);
    compare(query, done);
  });

  it('ge - 21', function(done) {
    var query = r.now().ge(false);
    compare(query, done);
  });

  it('ge - 22', function(done) {
    var query = r.now().ge(true);
    compare(query, done);
  });

  it('ge - 23', function(done) {
    var query = r.now().ge({});
    compare(query, done);
  });

  it('ge - 24', function(done) {
    var query = r.expr('foo').ge('bar');
    compare(query, done);
  });

  it('ge - 25', function(done) {
    var query = r.expr('foo').ge(2);
    compare(query, done);
  });

  it('ge - 26', function(done) {
    var query = r.expr('foo').ge(r.now());
    compare(query, done);
  });

  it('ge - 27', function(done) {
    var query = r.expr(2).ge(new Buffer('Hello world'));
    compare(query, done);
  });

  it('ge - 28', function(done) {
    var query = r.expr('foo').ge(new Buffer('Hello world'));
    compare(query, done);
  });

  it('ge - 29', function(done) {
    var query = r.expr(new Buffer('Hello world')).ge(new Buffer('Hello world'));
    compare(query, done);
  });

  it('ge - 30', function(done) {
    var query = r.now().ge(new Buffer('Hello world'));
    compare(query, done);
  });

  it('ge - 31', function(done) {
    var query = r.expr(true).ge(new Buffer('Hello world'));
    compare(query, done);
  });

  it('ge - 32', function(done) {
    var query = r.expr(false).ge(new Buffer('Hello world'));
    compare(query, done);
  });

  it('ge - 33', function(done) {
    var query = r.expr(null).ge(new Buffer('Hello world'));
    compare(query, done);
  });

  it('ge - 34', function(done) {
    var query = r.expr({foo: 'bar'}).ge(new Buffer('Hello world'));
    compare(query, done);
  });

  it('ge - 35', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).ge('bar');
    compare(query, done);
  });

  it('ge - 36', function(done) {
    var query = r.expr('foo').ge(r.db(TEST_DB).table(TEST_TABLE));
    compare(query, done);
  });

  it('ge - 37', function(done) {
    var query = r.ge('foo');
    compare(query, done);
  });

  it('ge - 38', function(done) {
    var query = r.ge();
    compare(query, done);
  });

  it('eq - 2 - 1', function(done) {
    var query = r.expr(2).eq(1);
    compare(query, done);
  });

  it('eq - 2 - 2', function(done) {
    var query = r.expr(2).eq(2);
    compare(query, done);
  });

  it('eq - 2 - 3', function(done) {
    var query = r.expr(2).eq(3);
    compare(query, done);
  });

  it('eq - 2 - 4', function(done) {
    var query = r.expr(2).eq('foo');
    compare(query, done);
  });

  it('eq - 2 - 5', function(done) {
    var query = r.expr('foo').eq(2);
    compare(query, done);
  });

  it('eq - 2 - 6', function(done) {
    var query = r.expr('foo').eq(null);
    compare(query, done);
  });

  it('eq - 2 - 7', function(done) {
    var query = r.expr(null).eq('foo');
    compare(query, done);
  });

  it('eq - 2 - 8', function(done) {
    var query = r.expr(false).eq(true);
    compare(query, done);
  });

  it('eq - 2 - 9', function(done) {
    var query = r.expr(true).eq(false);
    compare(query, done);
  });

  it('eq - 2 - 10', function(done) {
    var query = r.expr(true).eq('foo');
    compare(query, done);
  });

  it('eq - 2 - 11', function(done) {
    var query = r.expr(true).eq(2);
    compare(query, done);
  });

  it('eq - 2 - 12', function(done) {
    var query = r.expr(true).eq(null);
    compare(query, done);
  });

  it('eq - 2 - 13', function(done) {
    var query = r.expr(true).eq([1,2,3]);
    compare(query, done);
  });

  it('eq - 2 - 14', function(done) {
    var query = r.expr([1,2]).eq([1,2,3]);
    compare(query, done);
  });

  it('eq - 2 - 15', function(done) {
    var query = r.expr([1,2,3]).eq([1,2]);
    compare(query, done);
  });

  it('eq - 2 - 16', function(done) {
    var query = r.expr([1,2,3]).eq([2,2]);
    compare(query, done);
  });

  it('eq - 2 - 17', function(done) {
    var query = r.now().eq([2,2]);
    compare(query, done);
  });

  it('eq - 2 - 18', function(done) {
    var query = r.now().eq(2);
    compare(query, done);
  });

  it('eq - 2 - 19', function(done) {
    var query = r.now().eq('foo');
    compare(query, done);
  });

  it('eq - 2 - 20', function(done) {
    var query = r.now().eq(null);
    compare(query, done);
  });

  it('eq - 2 - 21', function(done) {
    var query = r.now().eq(false);
    compare(query, done);
  });

  it('eq - 2 - 22', function(done) {
    var query = r.now().eq(true);
    compare(query, done);
  });

  it('eq - 2 - 23', function(done) {
    var query = r.now().eq({});
    compare(query, done);
  });

  it('eq - 2 - 24', function(done) {
    var query = r.expr('foo').eq('bar');
    compare(query, done);
  });

  it('eq - 2 - 25', function(done) {
    var query = r.expr('foo').eq(2);
    compare(query, done);
  });

  it('eq - 2 - 26', function(done) {
    var query = r.expr('foo').eq(r.now());
    compare(query, done);
  });

  it('eq - 2 - 27', function(done) {
    var query = r.expr(2).eq(new Buffer('Hello world'));
    compare(query, done);
  });

  it('eq - 2 - 28', function(done) {
    var query = r.expr('foo').eq(new Buffer('Hello world'));
    compare(query, done);
  });

  it('eq - 2 - 29', function(done) {
    var query = r.expr(new Buffer('Hello world')).eq(new Buffer('Hello world'));
    compare(query, done);
  });

  it('eq - 2 - 30', function(done) {
    var query = r.now().eq(new Buffer('Hello world'));
    compare(query, done);
  });

  it('eq - 2 - 31', function(done) {
    var query = r.expr(true).eq(new Buffer('Hello world'));
    compare(query, done);
  });

  it('eq - 2 - 32', function(done) {
    var query = r.expr(false).eq(new Buffer('Hello world'));
    compare(query, done);
  });

  it('eq - 2 - 33', function(done) {
    var query = r.expr(null).eq(new Buffer('Hello world'));
    compare(query, done);
  });

  it('eq - 2 - 34', function(done) {
    var query = r.expr({foo: 'bar'}).eq(new Buffer('Hello world'));
    compare(query, done);
  });

  it('ne - 2 - 1', function(done) {
    var query = r.expr(2).ne(1);
    compare(query, done);
  });

  it('ne - 2 - 2', function(done) {
    var query = r.expr(2).ne(2);
    compare(query, done);
  });

  it('ne - 2 - 3', function(done) {
    var query = r.expr(2).ne(3);
    compare(query, done);
  });

  it('ne - 2 - 4', function(done) {
    var query = r.expr(2).ne('foo');
    compare(query, done);
  });

  it('ne - 2 - 5', function(done) {
    var query = r.expr('foo').ne(2);
    compare(query, done);
  });

  it('ne - 2 - 6', function(done) {
    var query = r.expr('foo').ne(null);
    compare(query, done);
  });

  it('ne - 2 - 7', function(done) {
    var query = r.expr(null).ne('foo');
    compare(query, done);
  });

  it('ne - 2 - 8', function(done) {
    var query = r.expr(false).ne(true);
    compare(query, done);
  });

  it('ne - 2 - 9', function(done) {
    var query = r.expr(true).ne(false);
    compare(query, done);
  });

  it('ne - 2 - 10', function(done) {
    var query = r.expr(true).ne('foo');
    compare(query, done);
  });

  it('ne - 2 - 11', function(done) {
    var query = r.expr(true).ne(2);
    compare(query, done);
  });

  it('ne - 2 - 12', function(done) {
    var query = r.expr(true).ne(null);
    compare(query, done);
  });

  it('ne - 2 - 13', function(done) {
    var query = r.expr(true).ne([1,2,3]);
    compare(query, done);
  });

  it('ne - 2 - 14', function(done) {
    var query = r.expr([1,2]).ne([1,2,3]);
    compare(query, done);
  });

  it('ne - 2 - 15', function(done) {
    var query = r.expr([1,2,3]).ne([1,2]);
    compare(query, done);
  });

  it('ne - 2 - 16', function(done) {
    var query = r.expr([1,2,3]).ne([2,2]);
    compare(query, done);
  });

  it('ne - 2 - 17', function(done) {
    var query = r.now().ne([2,2]);
    compare(query, done);
  });

  it('ne - 2 - 18', function(done) {
    var query = r.now().ne(2);
    compare(query, done);
  });

  it('ne - 2 - 19', function(done) {
    var query = r.now().ne('foo');
    compare(query, done);
  });

  it('ne - 2 - 20', function(done) {
    var query = r.now().ne(null);
    compare(query, done);
  });

  it('ne - 2 - 21', function(done) {
    var query = r.now().ne(false);
    compare(query, done);
  });

  it('ne - 2 - 22', function(done) {
    var query = r.now().ne(true);
    compare(query, done);
  });

  it('ne - 2 - 23', function(done) {
    var query = r.now().ne({});
    compare(query, done);
  });

  it('ne - 2 - 24', function(done) {
    var query = r.expr('foo').ne('bar');
    compare(query, done);
  });

  it('ne - 2 - 25', function(done) {
    var query = r.expr('foo').ne(2);
    compare(query, done);
  });

  it('ne - 2 - 26', function(done) {
    var query = r.expr('foo').ne(r.now());
    compare(query, done);
  });

  it('ne - 2 - 27', function(done) {
    var query = r.expr(2).ne(new Buffer('Hello world'));
    compare(query, done);
  });

  it('ne - 2 - 28', function(done) {
    var query = r.expr('foo').ne(new Buffer('Hello world'));
    compare(query, done);
  });

  it('ne - 2 - 29', function(done) {
    var query = r.expr(new Buffer('Hello world')).ne(new Buffer('Hello world'));
    compare(query, done);
  });

  it('ne - 2 - 30', function(done) {
    var query = r.now().ne(new Buffer('Hello world'));
    compare(query, done);
  });

  it('ne - 2 - 31', function(done) {
    var query = r.expr(true).ne(new Buffer('Hello world'));
    compare(query, done);
  });

  it('ne - 2 - 32', function(done) {
    var query = r.expr(false).ne(new Buffer('Hello world'));
    compare(query, done);
  });

  it('ne - 2 - 33', function(done) {
    var query = r.expr(null).ne(new Buffer('Hello world'));
    compare(query, done);
  });

  it('ne - 2 - 34', function(done) {
    var query = r.expr({foo: 'bar'}).ne(new Buffer('Hello world'));
    compare(query, done);
  });

  it('not - 1', function(done) {
    var query = r.expr(true).not();
    compare(query, done);
  });

  it('not - 2', function(done) {
    var query = r.expr(false).not();
    compare(query, done);
  });

  it('not - 3', function(done) {
    var query = r.expr('hello').not();
    compare(query, done);
  });

  it('not - 4', function(done) {
    var query = r.expr(null).not();
    compare(query, done);
  });

  it('not - 5', function(done) {
    var query = r.expr(0).not();
    compare(query, done);
  });

  it('not - 6', function(done) {
    var query = r.expr('').not();
    compare(query, done);
  });

  it('not - 7', function(done) {
    var query = r.not();
    compare(query, done);
  });

  it('not - 8', function(done) {
    var query = r.not(r.db(TEST_DB).table(TEST_TABLE));
    compare(query, done);
  });

  it('random - 1', function(done) {
    var query = r.random();
    compare(query, done, function(result) {
      return (0 <= result) && (result < 1);
    });
  });

  it('random - 2', function(done) {
    var query = r.random(MAX_RANDOM);
    compare(query, done, function(result) {
      return (0 <= result) && (result < MAX_RANDOM) && (Math.floor(result) === result);
    });
  });

  it('random - 3', function(done) {
    var query = r.random(MAX_RANDOM, {float: true});
    compare(query, done, function(result) {
      return (0 <= result) && (result < MAX_RANDOM) && (Math.floor(result) === result);
    });
  });

  it('random - 3', function(done) {
    var query = r.random(MIN_RANDOM, MAX_RANDOM);
    compare(query, done, function(result) {
      return (0 <= result) && (result < MAX_RANDOM) && (Math.floor(result) === result);
    });
  });

  it('random - 4', function(done) {
    var query = r.random(1,2,3,4,5);
    compare(query, done, function(result) {
      return (0 <= result) && (result < 1);
    });
  });

  it('random - 5', function(done) {
    var query = r.random('foo');
    compare(query, done, function(result) {
      return (0 <= result) && (result < 1);
    });
  });

  it('random - 6', function(done) {
    var query = r.random(1, 'foo');
    compare(query, done, function(result) {
      return (0 <= result) && (result < 1);
    });
  });

  it('random - 7', function(done) {
    var query = r.random(1, 4, {foo: 'bar'});
    compare(query, done, function(result) {
      return (0 <= result) && (result < 1);
    });
  });

  it('floor - 1', function(done) {
    var query = r.floor(1);
    compare(query, done);
  });

  it('floor - 2', function(done) {
    var query = r.floor(1.5);
    compare(query, done);
  });

  it('floor - 3', function(done) {
    var query = r.floor(1.8);
    compare(query, done);
  });

  it('floor - 4', function(done) {
    var query = r.floor('foo');
    compare(query, done);
  });

  it('floor - 5', function(done) {
    var query = r.floor();
    compare(query, done);
  });

  it('floor - 6', function(done) {
    var query = r.floor(1.2, 2.8);
    compare(query, done);
  });

  it('ceil - 1', function(done) {
    var query = r.ceil(1);
    compare(query, done);
  });

  it('ceil - 2', function(done) {
    var query = r.ceil(1.5);
    compare(query, done);
  });

  it('ceil - 3', function(done) {
    var query = r.ceil(1.8);
    compare(query, done);
  });

  it('ceil - 4', function(done) {
    var query = r.ceil('foo');
    compare(query, done);
  });

  it('ceil - 5', function(done) {
    var query = r.ceil();
    compare(query, done);
  });

  it('ceil - 6', function(done) {
    var query = r.ceil(1.2, 2.8);
    compare(query, done);
  });

  it('round - 1', function(done) {
    var query = r.round(1);
    compare(query, done);
  });

  it('round - 2', function(done) {
    var query = r.round(1.5);
    compare(query, done);
  });

  it('round - 3', function(done) {
    var query = r.round(1.8);
    compare(query, done);
  });

  it('round - 4', function(done) {
    var query = r.round('foo');
    compare(query, done);
  });

  it('round - 5', function(done) {
    var query = r.round();
    compare(query, done);
  });

  it('round - 6', function(done) {
    var query = r.round(1.2, 2.8);
    compare(query, done);
  });
  /*
  */
});
