var config = require('./../config.js');

var r = require('rethinkdb');
var assert = require('assert');

var connections = {};
var TEST_DB = 'reqlitetest';
var TEST_TABLE = 'reqlitetestaggregation';
var MISSING_ID = 'nonExistingId';
var MISSING_FIELD = 'nonExistingField';
var MISSING_INDEX = 'nonExistingIndex';

var compare = require('./util.js').generateCompare(connections);

describe('aggregation.js', function(){
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
      }).catch(function(e) { // ignore errors
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
    }, 400);
  });

  it('group - 1', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).group('id').count();
    compare(query, done, function(result) {
      result.sort(function(a, b) {
        return a.group - b.group;
      });
      return result;
    });
  });

  it('group - 2', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).group('foo').count();
    compare(query, done, function(result) {
      result.sort(function(a, b) {
        return a.group - b.group;
      });
      return result;
    });
  });

  it('group - 3', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).group({index: 'id'}).count();
    compare(query, done, function(result) {
      result.sort(function(a, b) {
        return a.group - b.group;
      });
      return result;
    });
  });

  it('group - 4', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).group(MISSING_FIELD).count();
    compare(query, done, function(result) {
      result.sort(function(a, b) {
        return a.group - b.group;
      });
      return result;
    });
  });

  it('group - 5', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).group({index: 'optional'}).count();
    compare(query, done, function(result) {
      result.sort(function(a, b) {
        return a.group - b.group;
      });
      return result;
    });
  });

  it('group - 6', function(done) {
    var query = r.expr([
      {id: 1},
      {id: 1},
      {id: 2},
      {id: 3}
    ]).group('id');
    compare(query, done, function(result) {
      result.sort(function(a, b) {
        return a.group - b.group;
      });
      return result;
    });
  });

  it('group - 7', function(done) {
    var query = r.expr([
      {id: 1},
      {},
      {},
      {id: 3}
    ]).group('id');
    compare(query, done, function(result) {
      result.sort(function(a, b) {
        return a.group - b.group;
      });
      return result;
    });
  });

  it('group - 8', function(done) {
    var query = r.expr([
      {id: 1},
      {},
      {},
      {id: 3}
    ]).group('id').count().ungroup().orderBy(r.row('group'));
    compare(query, done);
  });

  it('group - 9', function(done) {
    var query = r.expr([
      {id: 1, foo: 2},
      {id: 1, foo: 2},
      {id: 1, foo: 3},
      {id: 3, foo: 3}
    ]).group('id', 'foo');
    compare(query, done, function(result) {
      result.sort(function(a, b) {
        if (a.group > b.group) {
          return 1;
        }
        else if (a.group < b.group) {
          return -1;
        }
        return 0;
      });
      return result;
    });
  });

  it('group - 10', function(done) {
    var query = r.expr([
      {id: 1},
      {id: 1},
      {},
      {id: 3}
    ]).group('id').ungroup().count();
    compare(query, done);
  });

  it('group - 11', function(done) {
    var query = r.expr([
      {id: 1},
      {id: 1},
      {},
      {id: 3}
    ]).group();
    compare(query, done);
  });

  it('group - 12',  function(done) {
    var query = r.expr('foo').group('id');
    compare(query, done);
  });

  it('group - 13',  function(done) {
    var query = r.expr([
      {id: 1},
      {id: 1},
      {},
      {id: 3}
    ]).group(1);
    compare(query, done);
  });

  it('group - 14',  function(done) {
    var query = r.expr([
      {id: 1},
      {id: 1},
      {},
      {id: 3}
    ]).group('id', {bar: 2});
    compare(query, done);
  });

  it('group - 15',  function(done) {
    var query = r.expr([
      {id: 1},
      {id: 1},
      {},
      {id: 3}
    ]).group(function(doc) {
      return doc('id');
    }).ungroup().orderBy('group').map(function(doc) {
      return r.branch(
        doc('group').eq(1),
        doc.merge(function(x) { return { reduction: doc('reduction').orderBy(function(y) { return y; }) };}),
        doc
      );
    });
    compare(query, done);
  });

  it('group - 16',  function(done) {
    var query = r.expr([
      {id: 1},
      {id: 1},
      {},
      {id: 3}
    ]).group(function(doc) {
      return doc('id');
    }).ungroup().orderBy('group').map(function(doc) {
      return r.branch(
        doc('group').eq(1),
        doc.merge(function(x) { return { reduction: x('reduction').orderBy(function(y) { return y; }) };}),
        doc
      );
    });
    compare(query, done);
  });

  it('group - 17',  function(done) {
    var query = r.expr([
      {id: 1},
      {id: 1},
      {},
      {id: 3}
    ]).group(function(doc) {
      return doc('id').default(r.error('bar'));
    });
    compare(query, done);
  });

  it('group - 18',  function(done) {
    var query = r.expr([
      {id: 1},
      {id: 1},
      {},
      {id: 3}
    ]).group('id').typeOf();
    compare(query, done);
  });

  it('group - 19',  function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).group('id').typeOf();
    compare(query, done);
  });

  it('group - 20',  function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).group('id').count().typeOf();
    compare(query, done);
  });

  it('group - 21',  function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).group().count();
    compare(query, done);
  });

  it('group - 22', function(done) {
    var query = r.expr([
        {name: "Michel", grownUp: true},
        {name: "Laurent", grownUp: true},
        {name: "Sophie", grownUp: true},
        {name: "Luke", grownUp: false},
        {name: "Mino", grownUp: false}
    ]).group('grownUp').orderBy(r.row).ungroup().orderBy(r.row);
    compare(query, done);
  });

  it('group - 23', function(done) {
    var query = r.expr([
      {id: 1, x: 3},
      {id: 1, x: 4},
      {id: 2, x: 5},
      {id: 3, x: 6}
    ]).group("id").map(function(item){return 2;}).ungroup().orderBy(r.row);
    compare(query, done);
  });

  it('ungroup - 1', function(done) {
    var query = r.expr([
      {id: 1, foo: 2},
      {id: 1, foo: 2},
      {id: 1, foo: 3},
      {id: 3, foo: 3}
    ]).group('id', 'foo').ungroup().map(function(doc) {
      return doc('group');
    }).orderBy(r.row);
    compare(query, done);
  });

  it('ungroup - 2', function(done) {
    var query = r.expr([
      {id: 1, foo: 2},
      {id: 1, foo: 2},
      {id: 1, foo: 3},
      {id: 3, foo: 3}
    ]).group('id', 'foo').ungroup('foo').map(function(doc) {
      return doc('id');
    }).orderBy(r.row);
    compare(query, done);
  });

  it('ungroup - 3', function(done) {
    var query = r.expr([
      {id: 1, foo: 2},
      {id: 1, foo: 2},
      {id: 1, foo: 3},
      {id: 3, foo: 3}
    ]).ungroup();
    compare(query, done, function(error) {
      var result = error.split(':')[0];
      assert(result.length > 0);
      return result;
    });
  });

  it('reduce - 1', function(done) {
    var query = r.expr([1, 2, 3, 4, 5,]).reduce(function(left, right) {
      return left.add(right);
    });
    compare(query, done);
  });

  it('reduce - 2', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).map(function(doc) {
      return doc('id');
    }).reduce(function(left, right) {
      return left.add(right);
    });
    compare(query, done);
  });

  it('reduce - 3', function(done) {
    var query = r.expr([]).reduce(function(left, right) {
      return left.add(right);
    });
    compare(query, done);
  });

  it('reduce - 3', function(done) {
    var query = r.expr([1]).reduce(function(left, right) {
      return left.add(right);
    });
    compare(query, done);
  });

  it('reduce - 4', function(done) {
    var query = r.expr([100]).reduce(function(left, right) {
      return left.add(right);
    });
    compare(query, done);
  });

  it('reduce - 5', function(done) {
    var query = r.expr(100).reduce(function(left, right) {
      return left.add(right);
    });
    compare(query, done);
  });

  it('reduce - 6', function(done) {
    var query = r.expr([1,2,3]).reduce(function(left) {
      return left;
    });
    compare(query, done);
  });

  it('reduce - 7', function(done) {
    var query = r.expr([1,2,3]).reduce(function(left, right, extra) {
      return left;
    });
    compare(query, done, function(error) {
      return /^Expected 3 arguments but found 2/.test(error);
    });
  });

  it('reduce - 8', function(done) {
    var query = r.expr([1,2,3]).reduce(function(left, right) {
      return r.db(TEST_DB).table(TEST_TABLE);
    });
    compare(query, done);
  });

  it('reduce - 9', function(done) {
    var query = r.expr([1,2,3]).reduce(function(left, right) {
      return left;
    }, function(left, right) {
      return left;
    });
    compare(query, done);
  });

  it('reduce - 10', function(done) {
    var query = r.expr([1,2,3]).reduce(1);
    compare(query, done);
  });

  it('reduce - 11', function(done) {
    var query = r.expr([1,2,3]).reduce(1,2);
    compare(query, done);
  });

  it('reduce - 12', function(done) {
    var query = r.expr([1,2,3]).reduce(r.js('(function(left, right) { return left+right; })'));
    compare(query, done);
  });

  it('reduce - 13', function(done) {
    var query = r.expr([1,2,0]).reduce(r.js('(function(left, right) { return left/right; })'));
    compare(query, done);
  });

  it('reduce - 14', function(done) {
    var query = r.expr([1,2,0]).reduce(r.js('(function(left, right) { return left.a.b; })'));
    compare(query, done);
  });

  it('reduce - 15', function(done) {
    var query = r.expr([1,2,0]).reduce(r.js('(function(left, right) { return NaN; })'));
    compare(query, done);
  });

  it('reduce - 16', function(done) {
    var query = r.expr([1,2,0]).reduce(r.js('(function(left, right) { return undefined; })'));
    compare(query, done);
  });

  it('count - 1', function(done) {
    var query = r.expr([10, 11, 12, 13, 14]).count();
    compare(query, done);
  });

  it('count - 2', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).count();
    compare(query, done);
  });

  it('count - 3', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).between(r.minval, r.maxval).count();
    compare(query, done);
  });

  it('count - 4', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).map(function(doc) {
      return doc('id');
    }).count(2);
    compare(query, done);
  });

  it('count - 5', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).count(function(doc) {
      return doc('id').gt(2);
    });
    compare(query, done);
  });

  it('count - 6', function(done) {
    var query = r.expr([]).count('foo', 'bar');
    compare(query, done);
  });

  it('count - 7', function(done) {
    var query = r.expr('foo').count();
    compare(query, done);
  });

  it('count - 8', function(done) {
    var query = r.expr(new Buffer('foobarbuzz')).count();
    compare(query, done);
  });

  it('count - 9', function(done) {
    var query = r.expr(new Buffer('foobarbuzzlol')).count();
    compare(query, done);
  });

  it('count - 10', function(done) {
    var query = r.expr(new Buffer('foobarbuzzlol')).count(function(value) {
      return value.eq(2);
    });
    compare(query, done);
  });

  it('count - 11', function(done) {
    var query = r.expr([1,2,3,4,2,3,4,2,2,15]).count(2);
    compare(query, done);
  });

  it('count - 12', function(done) {
    var query = r.expr([1,2,3,4,2,3,4,2,2,15]).count(r.row.eq(2));
    compare(query, done);
  });

  it('count - 13', function(done) {
    var query = r.expr([1,2,3,4,2,3,4,2,2,15]).count(true);
    compare(query, done);
  });

  it('count - 14', function(done) {
    var query = r.expr([1,2,3,4,2,3,4,2,2,15]).count(r.row.eq(2).or(true));
    compare(query, done);
  });

  it('sum - 1', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).map(function(doc) {
      return doc('id');
    }).sum();
    compare(query, done);
  });

  it('sum - 2', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).sum('id');
    compare(query, done);
  });

  it('sum - 3', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).sum(function(doc) {
      return doc('id');
    });
    compare(query, done);
  });

  it('sum - 4', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).sum(function(doc) {
      return doc('optional');
    });
    compare(query, done);
  });

  it('sum - 5', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).sum(function(doc) {
      return doc('bar').nth(2);
    });
    compare(query, done);
  });

  it('sum - 6', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).sum('foo', 'bar');
    compare(query, done);
  });

  it('sum - 7', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).sum(1);
    compare(query, done);
  });

  it('sum - 8', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).sum(function(doc) {
      return r.branch(
          doc('id').eq(2),
          r.error('foobar'),
          doc('id')
      );
    });
    compare(query, done);
  });

  it('avg - 1', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).map(function(doc) {
      return doc('id');
    }).avg();
    compare(query, done);
  });

  it('avg - 2', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).avg('id');
    compare(query, done);
  });

  it('avg - 3', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).avg(function(doc) {
      return doc('id');
    });
    compare(query, done);
  });

  it('avg - 4', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).avg(function(doc) {
      return doc('optional');
    });
    compare(query, done);
  });

  it('avg - 5', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).avg(function(doc) {
      return doc('bar').nth(2);
    });
    compare(query, done);
  });

  it('avg - 6', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).avg('foo', 'bar');
    compare(query, done);
  });

  it('avg - 7', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).avg(1);
    compare(query, done);
  });

  it('avg - 8', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).avg(function(doc) {
      return r.branch(
          doc('id').eq(2),
          r.error('foobar'),
          doc('id')
      );
    });
    compare(query, done);
  });

  it('min - 1', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).map(function(doc) {
      return doc('id');
    }).min();
    compare(query, done);
  });

  it('min - 2', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).min('id');
    compare(query, done);
  });

  it('min - 3', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).min(function(doc) {
      return doc('id');
    });
    compare(query, done);
  });

  it('min - 4', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).min(function(doc) {
      return doc('optional');
    });
    compare(query, done, function(doc) {
      return (doc.id === 1) || (doc.id === 2);
    });
  });

  it('min - 5', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).min(function(doc) {
      return doc('bar').nth(2);
    });
    compare(query, done);
  });

  it('min - 6', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).min({index: 'foo'});
    compare(query, done);
  });

  it('min - 7', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).min({index: 'barmulti'});
    compare(query, done);
  });

  it('min - 8', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).min({index: 'barmulti'});
    compare(query, done);
  });

  it('min - 9', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).min('foo', {index: 'barmulti'});
    compare(query, done);
  });

  it('min -10', function(done) {
    var query = r.expr([]).min();
    compare(query, done);
  });

  it('max - 1', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).map(function(doc) {
      return doc('id');
    }).max();
    compare(query, done);
  });

  it('max - 2', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).max('id');
    compare(query, done);
  });

  it('max - 3', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).max(function(doc) {
      return doc('id');
    });
    compare(query, done);
  });

  it('max - 4', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).max(function(doc) {
      return doc('optional');
    });
    compare(query, done);
  });

  it('max - 5', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).max(function(doc) {
      return doc('bar').nth(2);
    });
    compare(query, done);
  });

  it('max - 6', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).max({index: 'foo'});
    compare(query, done);
  });

  it('max - 7', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).max({index: 'barmulti'});
    compare(query, done);
  });

  it('max - 8', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).max('foo', 'bar');
    compare(query, done);
  });

  it('max - 9', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).max(1);
    compare(query, done);
  });

  it('max - 10', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).max(function(doc) {
      return r.branch(
          doc('id').eq(2),
          r.error('bar'),
          doc('id')
      );
    });
    compare(query, done);
  });

  it('distinct - 1', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).distinct().orderBy(r.row('id'));
    compare(query, done);
  });

  it('distinct - 2', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).map(function(doc) {
      return doc('id');
    }).distinct().orderBy(r.row);
    compare(query, done);
  });

  it('distinct - 3', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).distinct({index: 'foo'}).orderBy(r.row);
    compare(query, done);
  });

  it('distinct - 4', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).distinct({index: 'barmulti'}).orderBy(r.row);
    compare(query, done);
  });

  it('distinct - 5', function(done) {
    var query = r.expr('foo').distinct();
    compare(query, done);
  });

  it('distinct - 6', function(done) {
    var query = r.expr([1,2,3,4,1,2,5,6,4,5]).distinct().orderBy(r.row);
    compare(query, done);
  });

  it('distinct - 7', function(done) {
    // If you add orderBy, the backtrace is broken in RethinkDB's side...
    var query = r.db(TEST_DB).table(TEST_TABLE).distinct({index: MISSING_INDEX});//.orderBy(r.row);
    compare(query, done);
  });

  it('distinct - 8', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).distinct({index: 42});
    compare(query, done);
  });

  it('distinct - 9', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).filter(true).distinct();
    compare(query, done);
  });

  it('distinct - 10', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).between(r.minval, r.maxval).distinct({index: 'id'}).orderBy(r.row);
    compare(query, done);
  });

  it('contains - 1', function(done) {
    var query = r.expr([1,2,3,4]).contains(2);
    compare(query, done);
  });

  it('contains - 2', function(done) {
    var query = r.expr([1,2,3,4]).contains(20);
    compare(query, done);
  });

  it('contains - 3', function(done) {
    var query = r.expr([1,2,3,4]).contains(2, 3, 1);
    compare(query, done);
  });

  it('contains - 4', function(done) {
    var query = r.expr([1,2,3,4]).contains(2, 3, 20);
    compare(query, done);
  });

  it('contains - 5', function(done) {
    var query = r.expr([1,2,3,4]).contains(function(value) {
      return value.eq(2);
    });
    compare(query, done);
  });

  it('contains - 6', function(done) {
    var query = r.expr([1,2,3,4]).contains(function(value) {
      return value.eq(999);
    });
    compare(query, done);
  });

  it('contains - 7', function(done) {
    var query = r.expr([1,2,3,4]).contains(function(value) {
      return value.eq(2);
    }, function(value) {
      return value.eq(3);
    });
    compare(query, done);
  });

  it('contains - 8', function(done) {
    var query = r.expr([1,2,3,4]).contains(function(value) {
      return value.eq(2);
    }, function(value) {
      return value.eq(999);
    });
    compare(query, done);
  });

  it('contains - 9', function(done) {
    var query = r.expr([1,2,3,4]).contains(function(value) {
      return {};
    });
    compare(query, done);
  });

  it('contains - 10', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).contains(function(doc) {
      return doc('id').eq(2);
    });
    compare(query, done);
  });

  it('contains - 11', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).contains(function(doc) {
      return doc(MISSING_FIELD).eq(2);
    });
    compare(query, done, function(error) {
      var result = error.split(':')[0];
      assert(result.length > 0);
      return result;
    });
  });

  it('contains - 12', function(done) {
    var query = r.expr('foo').contains('o');
    compare(query, done);
  });

  it('contains - 13', function(done) {
    var query = r.expr([1,2,3]).contains();
    compare(query, done);
  });

  it('contains - 14', function(done) {
    var query = r.expr([1,2,3]).contains(1,2,3);
    compare(query, done);
  });

  it('contains - 15', function(done) {
    var query = r.expr([1,2,3]).contains(1,2,4);
    compare(query, done);
  });

  it('contains - 16', function(done) {
    var query = r.expr([1,2,3]).contains(r.db(TEST_DB).table(TEST_TABLE));
    compare(query, done);
  });

  it('contains - 17', function(done) {
    var query = r.expr([1,2,3]).contains(function(value) {
      return r.db(TEST_DB).table(TEST_TABLE);
    });
    compare(query, done);
  });

  it('contains - 18', function(done) {
    var query = r.expr([1,2,3]).contains();
    compare(query, done);
  });

  /* contains currently doesn't accept r.row -- https://github.com/rethinkdb/rethinkdb/issues/4125
  it('contains - X', function(done) {
    var query = r.expr([1,2,3,4]).contains(r.row.eq(2))
    compare(query, done);
  });
  */

  it('fold - 1', function(done) {
    var query = r.expr(['hello', 'world', 'foo', 'bar', 'buzz']).orderBy(r.row)
      .fold('', function (acc, word) {
          return acc.add(r.branch(acc.eq(''), '', ', ')).add(word);
      });
    compare(query, done);
  });

  it('fold - 2', function(done) {
    var query = r.expr(['hello', 'world', 'foo', 'bar', 'buzz']).fold(0, function(acc, row) {
        return acc.add(1);
      }, {emit: function (prev_acc, row, new_acc) {
        return r.branch(prev_acc.mod(2).eq(0), [row], []);
      }
    });
    compare(query, done);
  });

  it('fold - 3', function(done) {
    var query = r.expr(['hello', 'world', 'foo', 'bar', 'buzz']).orderBy(r.row)
      .fold('', 'notafunction');
    compare(query, done, function(error) {
      var result = error.split(':')[0];
      return result;
    });
  });

  /*
  */

});
