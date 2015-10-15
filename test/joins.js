var config = require('./../config.js');

var r = require('rethinkdb');
var assert = require('assert');

var connections = {};
var TEST_DB = 'reqlitetest';
var TEST_TABLE = 'reqlitetestjoins';
var TEST_TABLE2 = 'reqlitetestjoins2';
var MISSING_ID = 'nonExistingId';
var MISSING_FIELD = 'nonExistingField';
var MISSING_INDEX = 'nonExistingIndex';

var compare = require('./util.js').generateCompare(connections);

describe('joins.js', function(){
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
        this.query = r.db(TEST_DB).tableDrop(TEST_TABLE2);
        return this.query.run(connections.rethinkdb);
      }).catch(function() { // ignore errors
      }).finally(function() {
        return this.query.run(connections.reqlite);
      }).catch(function() { // ignore errors
      }).finally(function() {
        this.query = r.db(TEST_DB).tableCreate(TEST_TABLE2);
        return this.query.run(connections.rethinkdb);
      }).catch(function() { // ignore errors
      }).finally(function() {
        return this.query.run(connections.reqlite);
      }).catch(function() { // ignore errors
      }).finally(function() {
        this.query = r.db(TEST_DB).table(TEST_TABLE).insert([
          {id: 1, table: 1, foo: 10, bar: [100, 101, 102], optional: 1},
          {id: 2, table: 1, foo: 10, bar: [200, 201, 202], optional: 1},
          {id: 3, table: 1, foo: 20, bar: [300, 301, 302], optional: 2},
          {id: 4, table: 1, foo: 20, bar: [400, 401, 402]},
        ]);
        return this.query.run(connections.rethinkdb);
      }).catch(function() { // ignore errors
      }).finally(function() {
        return this.query.run(connections.reqlite);
      }).catch(function() { // ignore errors
      }).finally(function() {
        this.query = r.db(TEST_DB).table(TEST_TABLE2).insert([
          {id: 1, table: 2, foo: 10, bar: [100, 101, 102]},
          {id: 2, table: 2, foo: 20, bar: [200, 201, 202], optional: 2},
          {id: 3, table: 2, foo: 30, bar: [300, 301, 302], optional: 1},
          {id: 4, table: 2, foo: 40, bar: [400, 401, 402]},
        ]);
        return this.query.run(connections.rethinkdb);
      }).catch(function() { // ignore errors
      }).finally(function() {
        return this.query.run(connections.reqlite);
      }).catch(function() { // ignore errors
      }).finally(function() {
        this.query = r.db(TEST_DB).table(TEST_TABLE2).indexCreate('foo');
        return this.query.run(connections.rethinkdb);
      }).catch(function() { // ignore errors
      }).finally(function() {
        return this.query.run(connections.reqlite);
      }).catch(function() { // ignore errors
      }).finally(function() {
        this.query = r.db(TEST_DB).table(TEST_TABLE2).indexCreate('optional');
        return this.query.run(connections.rethinkdb);
      }).catch(function() { // ignore errors
      }).finally(function() {
        return this.query.run(connections.reqlite);
      }).catch(function() { // ignore errors
      }).finally(function() {
        this.query = r.db(TEST_DB).table(TEST_TABLE2).indexCreate('bar');
        return this.query.run(connections.rethinkdb);
      }).catch(function() { // ignore errors
      }).finally(function() {
        return this.query.run(connections.reqlite);
      }).catch(function() { // ignore errors
      }).finally(function() {
        this.query = r.db(TEST_DB).table(TEST_TABLE2).indexCreate('barmulti', r.row('bar'), {multi: true});
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
        this.query = r.db(TEST_DB).table(TEST_TABLE2).indexWait();
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

  it('check init join - 1', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).orderBy(r.row);
    compare(query, done);
  });

  it('innerJoin - 1', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).innerJoin(
        r.db(TEST_DB).table(TEST_TABLE2),
        true
    ).orderBy(r.row);
    compare(query, done);
  });

  it('innerJoin - 2', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).innerJoin(
        r.db(TEST_DB).table(TEST_TABLE2),
        function(left, right) {
          return left('id').eq(right('id'));
        }
    ).orderBy(r.row('left')('id'), r.row('right')('id'));
    compare(query, done);
  });

  it('innerJoin - 3', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).innerJoin(
        r.db(TEST_DB).table(TEST_TABLE2),
        r.js('(function (left, right) { return left.id == right.id; })')
    ).orderBy(r.row('left')('id'), r.row('right')('id'));
    compare(query, done);
  });

  it('innerJoin - 4', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).innerJoin();
    compare(query, done);
  });

  it('innerJoin - 5', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).innerJoin(
        r.db(TEST_DB).table(TEST_TABLE2),
        function(left) { return left('id').eq(left('id')); }
    ).orderBy(r.row('left')('id'), r.row('right')('id'));
    compare(query, done, function(error) {
      return /^Expected 1 argument but found 2/.test(error);
    });
  });

  it('innerJoin - 6', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).innerJoin(
        r.db(TEST_DB).table(TEST_TABLE2),
        function(left, right, extra) { return left('id').eq(right('id')); }
    ).orderBy(r.row('left')('id'), r.row('right')('id'));
    compare(query, done, function(error) {
      return /^Expected 3 arguments but found 2/.test(error);
    });
  });

  it('innerJoin - 7', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).innerJoin(
        'foo',
        function(left, right, extra) { return left('id').eq(right('id')); }
    ).orderBy(r.row('left')('id'), r.row('right')('id'));
    compare(query, done);
  });

  it('innerJoin - 8', function(done) {
    var query = r.expr('foo').innerJoin(
        r.db(TEST_DB).table(TEST_TABLE2),
        function(left, right) { return left('id').eq(right('id')); }
    ).orderBy(r.row('left')('id'), r.row('right')('id'));
    compare(query, done);
  });

  it('innerJoin - 9', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE2).innerJoin(
        r.db(TEST_DB).table(TEST_TABLE2),
        function(left, right) { return left('optional').eq(right('optional')); }
    ).orderBy(r.row('left')('id'), r.row('right')('id'));
    compare(query, done, function(error) {
      var message = error.split(':')[0];
      assert(message.length > 0);
      return message;
    });
  });

  it('innerJoin - 19', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).innerJoin(
        r.db(TEST_DB).table(TEST_TABLE2),
        r.js('(function (left, right) { return left.id == right.optional.foo; })')
    ).orderBy(r.row('left')('id'), r.row('right')('id'));
    compare(query, done);
  });

  it('innerJoin - 11', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).innerJoin(
        r.db(TEST_DB).table(TEST_TABLE2),
        false
    ).orderBy(r.row('left')('id'), r.row('right')('id'));
    compare(query, done);
  });

  it('innerJoin - 12', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).innerJoin(
        r.db(TEST_DB).table(TEST_TABLE2),
        function(left, right) {
          return left('id').gt(right('id'));
        }
    ).orderBy(r.row('left')('id'), r.row('right')('id'));
    compare(query, done);
  });

  it('innerJoin - 13', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).innerJoin(
        r.db(TEST_DB).table(TEST_TABLE2),
        function(left, right) {
          return left(MISSING_FIELD).gt(right(MISSING_FIELD));
        }
    ).orderBy(r.row('left')('id'), r.row('right')('id'));
    compare(query, done, function(error) {
      var message = error.split(':')[0];
      assert(message.length > 0);
      return message;
    });
  });

  it('outerJoin - 1', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).outerJoin(
        r.db(TEST_DB).table(TEST_TABLE2),
        true
    ).orderBy(r.row('left')('id'), r.row('right')('id'));
    compare(query, done);
  });

  it('outerJoin - 2', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).outerJoin(
        r.db(TEST_DB).table(TEST_TABLE2),
        function(left, right) {
          return left('id').eq(right('id'));
        }
    ).orderBy(r.row('left')('id'), r.row('right')('id'));
    compare(query, done);
  });

  it('outerJoin - 3', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).outerJoin(
        r.db(TEST_DB).table(TEST_TABLE2),
        r.js('(function (left, right) { return left.id == right.id; })')
    ).orderBy(r.row('left')('id'), r.row('right')('id'));
    compare(query, done);
  });

  it('outerJoin - 4', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).outerJoin();
    compare(query, done);
  });

  it('outerJoin - 5', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).outerJoin(
        r.db(TEST_DB).table(TEST_TABLE2),
        function(left) { return left('id').eq(left('id')); }
    ).orderBy(r.row('left')('id'), r.row('right')('id'));
    compare(query, done, function(error) {
      return /^Expected 1 argument but found 2/.test(error);
    });
  });

  it('outerJoin - 6', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).outerJoin(
        r.db(TEST_DB).table(TEST_TABLE2),
        function(left, right, extra) { return left('id').eq(right('id')); }
    ).orderBy(r.row('left')('id'), r.row('right')('id'));
    compare(query, done, function(error) {
      return /^Expected 3 arguments but found 2/.test(error);
    });
  });

  it('outerJoin - 7', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).outerJoin(
        'foo',
        function(left, right, extra) { return left('id').eq(right('id')); }
    ).orderBy(r.row('left')('id'), r.row('right')('id'));
    compare(query, done);
  });

  it('outerJoin - 8', function(done) {
    var query = r.expr('foo').outerJoin(
        r.db(TEST_DB).table(TEST_TABLE2),
        function(left, right) { return left('id').eq(right('id')); }
    ).orderBy(r.row('left')('id'), r.row('right')('id'));
    compare(query, done);
  });

  it('outerJoin - 9', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE2).outerJoin(
        r.db(TEST_DB).table(TEST_TABLE2),
        function(left, right) { return left('optional').eq(right('optional')); }
    ).orderBy(r.row('left')('id'), r.row('right')('id'));
    compare(query, done, function(error) {
      var message = error.split(':')[0];
      assert(message.length > 0);
      return message;
    });
  });

  it('outerJoin - 19', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).outerJoin(
        r.db(TEST_DB).table(TEST_TABLE2),
        r.js('(function (left, right) { return left.id == right.optional.foo; })')
    ).orderBy(r.row('left')('id'), r.row('right')('id'));
    compare(query, done);
  });

  it('outerJoin - 11', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).outerJoin(
        r.db(TEST_DB).table(TEST_TABLE2),
        false
    ).orderBy(r.row('left')('id'), r.row('right')('id'));
    compare(query, done);
  });

  it('outerJoin - 12', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).outerJoin(
        r.db(TEST_DB).table(TEST_TABLE2),
        function(left, right) {
          return left('id').gt(right('id'));
        }
    ).orderBy(r.row('left')('id'), r.row('right')('id'));
    compare(query, done);
  });

  it('outerJoin - 13', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).outerJoin(
        r.db(TEST_DB).table(TEST_TABLE2),
        function(left, right) {
          return left(MISSING_FIELD).gt(right(MISSING_FIELD));
        }
    ).orderBy(r.row('left')('id'), r.row('right')('id'));
    compare(query, done, function(error) {
      var message = error.split(':')[0];
      assert(message.length > 0);
      return message;
    });
  });

  it('eqJoin - 1', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).eqJoin(
        'id',
        r.db(TEST_DB).table(TEST_TABLE2)
    ).orderBy(r.row('left')('id'), r.row('right')('id'));
    compare(query, done);
  });

  it('eqJoin - 2', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).eqJoin(
        'foo', // match nothing
        r.db(TEST_DB).table(TEST_TABLE2)
    ).orderBy(r.row('left')('id'), r.row('right')('id'));
    compare(query, done);
  });

  it('eqJoin - 3', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).eqJoin(
        'foo',
        r.db(TEST_DB).table(TEST_TABLE2),
        {index: 'foo'}
    ).orderBy(r.row('left')('id'), r.row('right')('id'));
    compare(query, done);
  });

  it('eqJoin - 4', function(done) {
    var query = r.expr([{foo: 101}, {foo: 201}]).eqJoin(
        'foo',
        r.db(TEST_DB).table(TEST_TABLE2),
        {index: 'bar'}
    ).orderBy(r.row('left')('id'), r.row('right')('id'));
    compare(query, done);
  });

  it('eqJoin - 5', function(done) {
    var query = r.expr([{foo: [200, 201, 202]}, {foo: [100, 101, 102]}]).eqJoin(
        'foo',
        r.db(TEST_DB).table(TEST_TABLE2),
        {index: 'bar'}
    ).orderBy(r.row('left')('foo'), r.row('right')('id'));
    compare(query, done);
  });

  it('eqJoin - 6', function(done) {
    var query = r.expr('foo').eqJoin(
        'id',
        r.db(TEST_DB).table(TEST_TABLE2)
    ).orderBy(r.row('left')('id'), r.row('right')('id'));
    compare(query, done);
  });

  it('eqJoin - 7', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).eqJoin(
        'id',
        [1,2,3,4,5]
    ).orderBy(r.row('left')('id'), r.row('right')('id'));
    compare(query, done, function(error) {
      var message = error.split(':')[0];
      assert(message.length > 0);
      return message;
    });
  });

  it('eqJoin - 8', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).eqJoin(
        r.row(MISSING_FIELD),
        r.db(TEST_DB).table(TEST_TABLE)
    ).orderBy(r.row('left')('id'), r.row('right')('id'));
    compare(query, done);
  });

  it('eqJoin - 9', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).eqJoin(
        r.row('foo'),
        r.db(TEST_DB).table(TEST_TABLE2),
        {index: 'foo'}
    ).orderBy(r.row('left')('id'), r.row('right')('id'));
    compare(query, done);
  });

  it('eqJoin - 10', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).eqJoin(
        r.row('optional'),
        r.db(TEST_DB).table(TEST_TABLE2),
        {index: 'optional'}
    ).orderBy(r.row('left')('id'), r.row('right')('id'));
    compare(query, done);
  });

  it('zip - 1', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).eqJoin(
        'id',
        r.db(TEST_DB).table(TEST_TABLE2)
    ).zip().orderBy(r.row);
    compare(query, done);
  });

  it('zip - 2', function(done) {
    var query = r.expr('foo').zip();
    compare(query, done);
  });

  it('zip - 3', function(done) {
    var query = r.expr([{left: {}, right: {}}]).zip('foo');
    compare(query, done);
  });


  /*
  */
});
