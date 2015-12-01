var config = require('./../config.js');

var r = require('rethinkdb');
var assert = require('assert');

var connections = {};
var TEST_DB = 'reqlitetest';
var TEST_TABLE = 'reqlitetestdocumentmanipulation';
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

describe('document-manipulation.js', function(){
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
    }, 400);
  });

  it('r.row - 1', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).filter(r.row('id').eq(3));
    compare(query, done);
  });

  it('r.row - 2', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).map(r.row('id')).orderBy(r.row);
    compare(query, done);
  });

  it('pluck - 1', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).pluck('id').orderBy(r.row('id'));
    compare(query, done);
  });

  it('pluck - 2', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).pluck('id', 'foo').orderBy(r.row('id'));
    compare(query, done);
  });

  it('pluck - 3', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).pluck(MISSING_FIELD).orderBy(r.row('id'));
    compare(query, done);
  });

  it('pluck - 4', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).pluck('id', MISSING_FIELD).orderBy(r.row('id'));
    compare(query, done);
  });

  it('pluck - 5', function(done) {
    var query = r.expr({id: 1, foo: 'bar'}).pluck('id');
    compare(query, done);
  });

  it('pluck - 6', function(done) {
    var query = r.expr({id: 1, foo: 'bar'}).pluck('id', MISSING_FIELD);
    compare(query, done);
  });

  it('pluck - 7', function(done) {
    var query = r.expr({id: 1, foo: 'bar'}).pluck(MISSING_FIELD);
    compare(query, done);
  });

  it('pluck - 8', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).get(1).pluck('id', MISSING_FIELD);
    compare(query, done);
  });

  it('pluck - 9', function(done) {
    var query = r.expr(COMPLEX_OBJECT).pluck('id');
    compare(query, done);
  });

  it('pluck - 10', function(done) {
    var query = r.expr(COMPLEX_OBJECT).pluck('buzz');
    compare(query, done);
  });

  it('pluck - 11', function(done) {
    var query = r.expr(COMPLEX_OBJECT).pluck({buzz: true});
    compare(query, done);
  });

  it('pluck - 12', function(done) {
    var query = r.expr(COMPLEX_OBJECT).pluck({buzz: {hello: true}});
    compare(query, done);
  });

  it('pluck - 13', function(done) {
    var query = r.expr(COMPLEX_OBJECT).pluck({buzz: ['hello']});
    compare(query, done);
  });

  it('pluck - 14', function(done) {
    var query = r.expr(COMPLEX_OBJECT).pluck({buzz: ['hello', 'world']});
    compare(query, done);
  });

  it('pluck - 15', function(done) {
    var query = r.expr(COMPLEX_OBJECT).pluck({buzz: ['hello', {world: ['nested']}]});
    compare(query, done);
  });

  it('pluck - 16', function(done) {
    var query = r.expr(COMPLEX_OBJECT).pluck({buzz: ['hello', {world: {nested: true}}]});
    compare(query, done);
  });

  it('pluck - 17', function(done) {
    var query = r.expr(COMPLEX_OBJECT).pluck({buzz: ['hello', {world: {MISSING_FIELD: true}}]});
    compare(query, done);
  });

  it('pluck - 18', function(done) {
    var query = r.expr(COMPLEX_OBJECT).pluck({MISSING_FIELD: {MISSING_FIELD: true}});
    compare(query, done);
  });

  it('pluck - 19', function(done) {
    var query = r.expr(COMPLEX_OBJECT).pluck({bonjour: {monde: true}});
    compare(query, done);
  });

  it('pluck - 20', function(done) {
    var query = r.expr(COMPLEX_OBJECT).pluck({bonjour: ['monde']});
    compare(query, done);
  });

  it('pluck - 21', function(done) {
    var query = r.expr(COMPLEX_OBJECT).pluck('buzz', {buzz: ['hello']});
    compare(query, done);
  });

  it('pluck - 22', function(done) {
    var query = r.expr(COMPLEX_OBJECT).pluck({buzz: ['hello']}, 'buzz');
    compare(query, done);
  });

  it('pluck - 23', function(done) {
    var query = r.expr('foo').pluck('bar');
    compare(query, done);
  });

  it('pluck - 24', function(done) {
    var query = r.expr({foo: 'bar'}).pluck();
    compare(query, done);
  });

  it('pluck - 25', function(done) {
    var query = r.expr({foo: 'bar'}).pluck('fo', r.db(TEST_DB).table(TEST_TABLE));
    compare(query, done);
  });

  it('pluck - 26', function(done) {
    var query = r.expr(COMPLEX_OBJECT).pluck({foo: false});
    compare(query, done);
  });

  it('pluck - 27', function(done) {
    var query = r.expr(COMPLEX_OBJECT).pluck({foo: null});
    compare(query, done);
  });

  it('pluck - 28', function(done) {
    var query = r.expr(COMPLEX_OBJECT).pluck({foo: {bar: true}});
    compare(query, done);
  });

  it('pluck - 29', function(done) {
    var query = r.expr(COMPLEX_OBJECT).pluck(true);
    compare(query, done);
  });

  it('without - 1', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).without('foo').orderBy(r.row('id'));
    compare(query, done);
  });

  it('without - 2', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).without('foo', 'bar').orderBy(r.row('id'));
    compare(query, done);
  });

  it('without - 3', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).without(MISSING_FIELD).orderBy(r.row('id'));
    compare(query, done);
  });

  it('without - 4', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).without('foo', MISSING_FIELD).orderBy(r.row('id'));
    compare(query, done);
  });

  it('without - 5', function(done) {
    var query = r.expr({id: 1, foo: 'bar'}).without('id');
    compare(query, done);
  });

  it('without - 6', function(done) {
    var query = r.expr({id: 1, foo: 'bar'}).without('id', MISSING_FIELD);
    compare(query, done);
  });

  it('without - 7', function(done) {
    var query = r.expr({id: 1, foo: 'bar'}).without(MISSING_FIELD);
    compare(query, done);
  });

  it('without - 8', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).get(1).without('id', MISSING_FIELD);
    compare(query, done);
  });

  it('without - 9', function(done) {
    var query = r.expr(COMPLEX_OBJECT).without('id');
    compare(query, done);
  });

  it('without - 10', function(done) {
    var query = r.expr(COMPLEX_OBJECT).without('buzz');
    compare(query, done);
  });

  it('without - 11', function(done) {
    var query = r.expr(COMPLEX_OBJECT).without({buzz: true});
    compare(query, done);
  });

  it('without - 12', function(done) {
    var query = r.expr(COMPLEX_OBJECT).without({buzz: {hello: true}});
    compare(query, done);
  });

  it('without - 13', function(done) {
    var query = r.expr(COMPLEX_OBJECT).without({buzz: ['hello']});
    compare(query, done);
  });

  it('without - 14', function(done) {
    var query = r.expr(COMPLEX_OBJECT).without({buzz: ['hello', 'world']});
    compare(query, done);
  });

  it('without - 15', function(done) {
    var query = r.expr(COMPLEX_OBJECT).without({buzz: ['hello', {world: ['nested']}]});
    compare(query, done);
  });

  it('without - 16', function(done) {
    var query = r.expr(COMPLEX_OBJECT).without({buzz: ['hello', {world: {nested: true}}]});
    compare(query, done);
  });

  it('without - 17', function(done) {
    var query = r.expr(COMPLEX_OBJECT).without({buzz: ['hello', {world: {MISSING_FIELD: true}}]});
    compare(query, done);
  });

  it('without - 18', function(done) {
    var query = r.expr(COMPLEX_OBJECT).without({MISSING_FIELD: {MISSING_FIELD: true}});
    compare(query, done);
  });

  it('without - 19', function(done) {
    var query = r.expr(COMPLEX_OBJECT).without({bonjour: {monde: true}});
    compare(query, done);
  });

  it('without - 20', function(done) {
    var query = r.expr(COMPLEX_OBJECT).without({bonjour: ['monde']});
    compare(query, done);
  });

  it('without - 21', function(done) {
    var query = r.expr(COMPLEX_OBJECT).without({buzz: ['hello']}, 'buzz');
    compare(query, done);
  });

  it('without - 22', function(done) {
    var query = r.expr(COMPLEX_OBJECT).without('buzz', {buzz: ['hello']});
    compare(query, done);
  });

  it('without - 23', function(done) {
    var query = r.expr('foo').without('bar');
    compare(query, done);
  });

  it('without - 24', function(done) {
    var query = r.expr({foo: 'bar'}).without();
    compare(query, done);
  });

  it('without - 25', function(done) {
    var query = r.expr({foo: 'bar'}).without('fo', r.db(TEST_DB).table(TEST_TABLE));
    compare(query, done);
  });

  it('without - 26', function(done) {
    var query = r.expr(COMPLEX_OBJECT).without({foo: false});
    compare(query, done);
  });

  it('without - 27', function(done) {
    var query = r.expr(COMPLEX_OBJECT).without({foo: null});
    compare(query, done);
  });

  it('without - 28', function(done) {
    var query = r.expr(COMPLEX_OBJECT).without({foo: {bar: true}});
    compare(query, done);
  });

  it('without - 29', function(done) {
    var query = r.expr(COMPLEX_OBJECT).without(true);
    compare(query, done);
  });

  it('without - 29', function(done) {
    var query = r.expr(COMPLEX_OBJECT).without(['id']);
    compare(query, done);
  });

  it('without - 30', function(done) {
    var query = r.expr(COMPLEX_OBJECT).without({buzz: { world: { nested: { unknown: true }}}});
    compare(query, done);
  });

  it('without - 31', function(done) {
    var query = r.expr(COMPLEX_OBJECT).without({buzz: { world: { nested: { unknown: { ciao: true} }}}});
    compare(query, done);
  });

  it('without - 32', function(done) {
    var query = r.expr(COMPLEX_OBJECT).without('buzz', {buzz: { world: true }});
    compare(query, done);
  });

  it('without - 33', function(done) {
    var query = r.expr(COMPLEX_OBJECT).without({buzz: { world: true }}, 'buzz');
    compare(query, done);
  });

  it('without - 34', function(done) {
    var query = r.expr(COMPLEX_OBJECT).without({foo: { world: true }}, 'foo');
    compare(query, done);
  });

  it('without - 35', function(done) {
    var query = r.expr(COMPLEX_OBJECT).without('foo', {foo: { world: true }});
    compare(query, done);
  });

  it('without - 36', function(done) {
    var query = r.expr({
      foo: 1
    }).pluck('foo', {foo: {bar : true}});
    compare(query, done);
  });

  it('without - 37', function(done) {
    var query = r.expr({
      foo: 1
    }).pluck({foo: {bar : true}}, 'foo');
    compare(query, done);
  });

  it('without - 38', function(done) {
    var mergedoc = r.db(TEST_DB).table(TEST_TABLE).get(1);
    var query = r.expr(COMPLEX_OBJECT).merge({"baz": mergedoc}).without("buzz");
    compare(query, done);
  });
  
  it('without - 39', function(done) {
    var mergedoc = r.db(TEST_DB).table(TEST_TABLE).get(1);
    var query = r.expr(COMPLEX_OBJECT).without("buzz").merge({"baz": mergedoc});
    compare(query, done);
  });

  it('merge - 1', function(done) {
    var query = r.expr({foo: 'bar'}).merge({foo: 'lol'});
    compare(query, done);
  });

  it('merge - 2', function(done) {
    var query = r.expr({foo: {bar: 2}}).merge({foo: {bar: 3}});
    compare(query, done);
  });

  it('merge - 3', function(done) {
    var query = r.expr({foo: {bar: 2}}).merge({foo: {bar: 3, buzz: 4}});
    compare(query, done);
  });

  it('merge - 4', function(done) {
    var query = r.expr({foo: {bar: 2}}).merge({foo: {buzz: 4}});
    compare(query, done);
  });

  it('merge - 5', function(done) {
    var query = r.expr({foo: {bar: 2}}).merge({foo: r.literal({buzz: 4})});
    compare(query, done);
  });

  it('merge - 6', function(done) {
    var query = r.expr({foo: {bar: [1,2,3,4]}}).merge({foo: r.literal({buzz: 4})});
    compare(query, done);
  });

  it('merge - 7', function(done) {
    var query = r.expr({foo: {bar: [1,2,3,4]}}).merge({foo: {bar: [10,20]}});
    compare(query, done);
  });

  it('merge - 8', function(done) {
    var query = r.expr({foo: 10, bar: 2}).merge({foo: r.row('foo').add(1)});
    compare(query, done);
  });

  it('merge - 9', function(done) {
    var query = r.expr({foo: 10, bar: 2}).merge({bar: r.row('foo').add(1)});
    compare(query, done);
  });

  it('merge - 10', function(done) {
    var query = r.expr({a: [1,2]}).merge(function(doc) {
      return {b: doc('a').count() };
    });
    compare(query, done);
  });

  it('merge - 11', function(done) {
    var query = r.expr({foo: {bar: 2}}).merge({foo: {buzz: 4}}, {bar :'lol'});
    compare(query, done);
  });

  it('merge - 12', function(done) {
    var query = r.expr({foo: {bar: 2}}).merge();
    compare(query, done);
  });

  it('merge - 13', function(done) {
    var query = r.expr({foo: {bar: 2}}).merge('foo');
    compare(query, done);
  });

  it('merge - 14', function(done) {
    var query = r.expr({foo: {bar: 2}}).merge({foo: 'bar'});
    compare(query, done);
  });

  it('merge - 15', function(done) {
    var query = r.expr('foo').merge({bar: 1});
    compare(query, done);
  });

  it('merge - 16', function(done) {
    var query = r.expr({foo: 2}).merge({bar: 2}, {bar: 1});
    compare(query, done);
  });

  it('merge - 17', function(done) {
    var query = r.expr({foo: 2}).merge('foo', {bar: 1});
    compare(query, done);
  });

  it('merge - 18', function(done) {
    var query = r.expr({foo: 2}).merge({foo: r.row('foo').mul(r.row('foo'))}, {foo: r.row('foo').mul(r.row('foo'))});
    compare(query, done);
  });

  it('append - 1', function(done) {
    var query = r.expr([]).append(1);
    compare(query, done);
  });

  it('append - 2', function(done) {
    var query = r.expr([1,2,3,4]).append(10);
    compare(query, done);
  });

  it('append - 3', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).map(function(doc) {
      return doc('id');
    }).orderBy(r.row).append(10);
    compare(query, done);
  });

  it('append - 4', function(done) {
    var query = r.expr(1).append();
    compare(query, done);
  });

  it('append - 5', function(done) {
    var query = r.expr([]).append(r.db(TEST_DB).table(TEST_TABLE));
    compare(query, done);
  });

  it('append - 6', function(done) {
    var query = r.expr(1).append(r.db(TEST_DB).table(TEST_TABLE));
    compare(query, done);
  });

  it('append - 7', function(done) {
    var query = r.expr(1).append(2, r.db(TEST_DB).table(TEST_TABLE));
    compare(query, done);
  });

  it('append - 8', function(done) {
    var query = r.expr(2).append('foo');
    compare(query, done);
  });

  it('append - 9', function(done) {
    var query = r.expr('foo').append(2);
    compare(query, done);
  });

  it('append - 10', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).get(1).update(function(doc) {
      return {
        bar: doc('bar').append(103).filter(r.expr(true))
      }, {returnChanges: true}
    });
    compare(query, done);
  });

  it('append - 11', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).get(1).update({
        bar: r.row('bar').append(104).filter(r.expr(true))
      }, {returnChanges: true});
    compare(query, done);
  });

  it('append - 10 - b', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).get(1).update({
      bar: r.row('bar').filter(r.expr(true)).append(104)
    }, {returnChanges: true});
    compare(query, done);
  });

  it('prepend - 1', function(done) {
    var query = r.expr([]).prepend(1);
    compare(query, done);
  });

  it('prepend - 2', function(done) {
    var query = r.expr([1,2,3,4]).prepend(10);
    compare(query, done);
  });

  it('prepend - 3', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).map(function(doc) {
      return doc('id');
    }).orderBy(r.row).prepend(10);
    compare(query, done);
  });

  it('prepend - 4', function(done) {
    var query = r.expr(1).prepend();
    compare(query, done);
  });

  it('prepend - 5', function(done) {
    var query = r.expr([]).prepend(r.db(TEST_DB).table(TEST_TABLE));
    compare(query, done);
  });

  it('prepend - 6', function(done) {
    var query = r.expr(1).prepend(r.db(TEST_DB).table(TEST_TABLE));
    compare(query, done);
  });

  it('prepend - 7', function(done) {
    var query = r.expr(1).prepend(2, r.db(TEST_DB).table(TEST_TABLE));
    compare(query, done);
  });

  it('prepend - 8', function(done) {
    var query = r.expr(2).prepend('foo');
    compare(query, done);
  });

  it('prepend - 9', function(done) {
    var query = r.expr('foo').prepend(2);
    compare(query, done);
  });

  it('difference - 1', function(done) {
    var query = r.expr([1,2,3]).difference([1,2,3]);
    compare(query, done);
  });

  it('difference - 2', function(done) {
    var query = r.expr([1,2,3]).difference([2,3]);
    compare(query, done);
  });

  it('difference - 3', function(done) {
    var query = r.expr([1,2,3]).difference([4,5]);
    compare(query, done);
  });

  it('difference - 4', function(done) {
    var query = r.expr([{id: 1}, {id: 2}, {id: 3}]).difference([{id: 2}, {id: 4}]);
    compare(query, done);
  });

  it('difference - 5', function(done) {
    var query = r.expr([{id: {nested: 1}}, {id: {nested: 2}}, {id: {nested: 3}}]).difference([{id: {nested: 2}}, {id: {nested: 4}}]);
    compare(query, done);
  });

  it('difference - 6', function(done) {
    var query = r.expr([1,2,3]).difference();
    compare(query, done);
  });

  it('difference - 7', function(done) {
    var query = r.expr([1,2,3]).difference('foo');
    compare(query, done);
  });

  it('difference - 8', function(done) {
    var query = r.expr('bar').difference([1,2,3]);
    compare(query, done);
  });

  it('difference - 9', function(done) {
    var query = r.expr([1,2,3]).difference(r.db(TEST_DB).table(TEST_TABLE));
    compare(query, done);
  });

  it('difference - 10', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).difference([1,2,3]).orderBy('id');
    compare(query, done);
  });

  it('setInsert - 1', function(done) {
    var query = r.expr([{id: {nested: 1}}, {id: {nested: 2}}, {id: {nested: 3}}]).setInsert({id: {nested: 2}});
    compare(query, done);
  });

  it('setInsert - 2', function(done) {
    var query = r.expr([{id: {nested: 1}}, {id: {nested: 2}}, {id: {nested: 3}}]).setInsert({id: {nested: 5}});
    compare(query, done);
  });

  it('setInsert - 3', function(done) {
    var query = r.expr([{id: {nested: 1}}, {id: {nested: 2}}, {id: {nested: 1}}, {id: {nested: 3}}]).setInsert({id: {nested: 5}});
    compare(query, done);
  });

  it('setInsert - 4', function(done) {
    var query = r.expr([1,2,3]).setInsert();
    compare(query, done);
  });

  it('setInsert - 5', function(done) {
    var query = r.expr([1,2,3]).setInsert('foo');
    compare(query, done);
  });

  it('setInsert - 6', function(done) {
    var query = r.expr('foo').setInsert([1,2,3]);
    compare(query, done);
  });

  it('setInsert - 7', function(done) {
    var query = r.expr('foo').setInsert(r.db(TEST_DB).table(TEST_TABLE));
    compare(query, done);
  });

  it('setInsert - 8', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).setInsert([1,2,3]);
    compare(query, done);
  });

  it('setInsert - 9', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).setInsert('bar');
    compare(query, done);
  });

  it('setUnion - 1', function(done) {
    var query = r.expr([{id: {nested: 1}}, {id: {nested: 2}}, {id: {nested: 3}}]).setUnion([{id: {nested: 2}}]);
    compare(query, done);
  });

  it('setUnion - 2', function(done) {
    var query = r.expr([{id: {nested: 1}}, {id: {nested: 2}}, {id: {nested: 3}}]).setUnion([{id: {nested: 5}}]);
    compare(query, done);
  });

  it('setUnion - 3', function(done) {
    var query = r.expr([{id: {nested: 1}}, {id: {nested: 2}}, {id: {nested: 3}}]).setUnion([{id: {nested: 2}}, {id: {nested: 4}}]);
    compare(query, done);
  });

  it('setUnion - 4', function(done) {
    var query = r.expr([{id: {nested: 1}}, {id: {nested: 2}}, {id: {nested: 1}}, {id: {nested: 3}}])
      .setUnion([{id: {nested: 2}}, {id: {nested: 4}}]);
    compare(query, done);
  });

  it('setUnion - 5', function(done) {
    var query = r.expr([1,2,3]).setUnion();
    compare(query, done);
  });

  it('setUnion - 6', function(done) {
    var query = r.expr([1,2,3]).setUnion('foo');
    compare(query, done);
  });

  it('setUnion - 7', function(done) {
    var query = r.expr('foo').setUnion([1,2,3]);
    compare(query, done);
  });

  it('setUnion - 8', function(done) {
    var query = r.expr('foo').setUnion(r.db(TEST_DB).table(TEST_TABLE));
    compare(query, done);
  });

  it('setUnion - 9', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).setUnion([1,2,3]);
    compare(query, done);
  });

  it('setUnion - 10', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).setUnion('bar');
    compare(query, done);
  });

  it('setIntersection - 1', function(done) {
    var query = r.expr([{id: {nested: 1}}, {id: {nested: 2}}, {id: {nested: 3}}])
      .setIntersection([{id: {nested: 2}}]);
    compare(query, done);
  });

  it('setIntersection - 2', function(done) {
    var query = r.expr([{id: {nested: 1}}, {id: {nested: 2}}, {id: {nested: 3}}])
      .setIntersection([{id: {nested: 4}}]);
    compare(query, done);
  });

  it('setIntersection - 3', function(done) {
    var query = r.expr([{id: {nested: 1}}, {id: {nested: 2}}, {id: {nested: 1}}, {id: {nested: 3}}])
      .setIntersection([{id: {nested: 2}}, {id: {nested: 4}}]);
    compare(query, done);
  });

  it('setIntersection - 4', function(done) {
    var query = r.expr([1,2,3]).setIntersection();
    compare(query, done);
  });

  it('setIntersection - 5', function(done) {
    var query = r.expr([1,2,3]).setIntersection('foo');
    compare(query, done);
  });

  it('setIntersection - 6', function(done) {
    var query = r.expr('foo').setIntersection([1,2,3]);
    compare(query, done);
  });

  it('setIntersection - 7', function(done) {
    var query = r.expr('foo').setIntersection(r.db(TEST_DB).table(TEST_TABLE));
    compare(query, done);
  });

  it('setIntersection - 8', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).setIntersection([1,2,3]);
    compare(query, done);
  });

  it('setIntersection - 9', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).setIntersection('bar');
    compare(query, done);
  });

  it('setDifference - 1', function(done) {
    var query = r.expr([{id: {nested: 1}}, {id: {nested: 2}}, {id: {nested: 3}}])
      .setDifference([{id: {nested: 2}}]);
    compare(query, done);
  });

  it('setDifference - 2', function(done) {
    var query = r.expr([{id: {nested: 1}}, {id: {nested: 2}}, {id: {nested: 3}}])
      .setDifference([{id: {nested: 4}}]);
    compare(query, done);
  });

  it('setDifference - 3', function(done) {
    var query = r.expr([{id: {nested: 1}}, {id: {nested: 2}}, {id: {nested: 3}}])
      .setDifference([{id: {nested: 2}}, {id: {nested: 4}}]);
    compare(query, done);
  });

  it('setDifference - 4', function(done) {
    var query = r.expr([{id: {nested: 1}}, {id: {nested: 2}}, {id: {nested: 1}}, {id: {nested: 3}}])
      .setDifference([{id: {nested: 2}}, {id: {nested: 4}}]);
    compare(query, done);
  });

  it('setDifference - 5', function(done) {
    var query = r.expr([1,2,3]).setDifference();
    compare(query, done);
  });

  it('setDifference - 6', function(done) {
    var query = r.expr([1,2,3]).setDifference('foo');
    compare(query, done);
  });

  it('setDifference - 7', function(done) {
    var query = r.expr('foo').setDifference([1,2,3]);
    compare(query, done);
  });

  it('setDifference - 8', function(done) {
    var query = r.expr('foo').setDifference(r.db(TEST_DB).table(TEST_TABLE));
    compare(query, done);
  });

  it('setDifference - 9', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).setDifference([1,2,3]);
    compare(query, done);
  });

  it('setDifference - 10', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).setDifference('bar');
    compare(query, done);
  });

  it('getField - 1', function(done) {
    var query = r.expr(COMPLEX_OBJECT).getField('id');
    compare(query, done);
  });

  it('getField - 2', function(done) {
    var query = r.expr(COMPLEX_OBJECT).getField('buzz').getField('hello');
    compare(query, done);
  });

  it('getField - 3', function(done) {
    var query = r.expr(COMPLEX_OBJECT).getField('buzz').getField('hello').nth(2);
    compare(query, done);
  });

  it('getField - 4', function(done) {
    var query = r.expr(COMPLEX_OBJECT).getField(MISSING_FIELD);
    compare(query, done, function(err) {
      var result = err.match(/^No attribute `nonExistingField` in object:/)[0];
      return result;
    });
  });

  it('getField - 5', function(done) {
    var query = r.expr(COMPLEX_OBJECT).getField(r.args(['id', 'foo']));
    compare(query, done);
  });

  it('getField - 6', function(done) {
    var query = r.expr(COMPLEX_OBJECT).getField(r.args());
    compare(query, done, function(error) {
      return /^Expected 1 argument but found 0/.test(error);
    });
  });

  it('getField - 7', function(done) {
    var query = r.expr(COMPLEX_OBJECT).getField(r.db(TEST_DB).table(TEST_TABLE));
    compare(query, done);
  });

  it('getField - 8', function(done) {
    var query = r.expr(null).getField('foo');
    compare(query, done);
  });

  it('getField - 9', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).getField('id').orderBy(r.row);
    compare(query, done);
  });

  it('getField - 10', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).getField('optional').orderBy(r.row);
    compare(query, done);
  });

  it('() - 1', function(done) {
    var query = r.expr(COMPLEX_OBJECT)('id');
    compare(query, done);
  });

  it('() - 2', function(done) {
    var query = r.expr(COMPLEX_OBJECT)('buzz')('hello');
    compare(query, done);
  });

  it('() - 3', function(done) {
    var query = r.expr(COMPLEX_OBJECT)('buzz')('hello')(2);
    compare(query, done);
  });

  it('() - 4', function(done) {
    var query = r.expr(COMPLEX_OBJECT)(MISSING_FIELD);
    compare(query, done, function(err) {
      var result = err.match(/^No attribute `nonExistingField` in object:/)[0];
      return result;
    });
  });

  it('() - 5', function(done) {
    var query = r.expr([COMPLEX_OBJECT])('id');
    compare(query, done);
  });

  it('() - 6', function(done) {
    var query = r.expr('foo')('id');
    compare(query, done);
  });

  it('() - 7', function(done) {
    var query = r.expr({})(1);
    compare(query, done);
  });

  it('() - 8', function(done) {
    var query = r.expr([])('foo');
    compare(query, done);
  });

  it('() - 9', function(done) {
    var query = r.expr([[]])('foo');
    compare(query, done);
  });

  it('() - 10', function(done) {
    var query = r.expr(null)('foo');
    compare(query, done);
  });

  it('hasFields - 1', function(done) {
    var query = r.expr(COMPLEX_OBJECT).hasFields('id');
    compare(query, done);
  });

  it('hasFields - 2', function(done) {
    var query = r.expr(COMPLEX_OBJECT).hasFields(MISSING_FIELD);
    compare(query, done);
  });

  it('hasFields - 3', function(done) {
    var query = r.expr(COMPLEX_OBJECT).hasFields({'buzz': true});
    compare(query, done);
  });

  it('hasFields - 4', function(done) {
    var fields = {};
    fields[MISSING_FIELD] = true;
    var query = r.expr(COMPLEX_OBJECT).hasFields(fields);
    compare(query, done);
  });

  it('hasFields - 5', function(done) {
    var query = r.expr(COMPLEX_OBJECT).hasFields({'buzz': ['hello']});
    compare(query, done);
  });

  it('hasFields - 6', function(done) {
    var query = r.expr(COMPLEX_OBJECT).hasFields({'buzz': [MISSING_FIELD]});
    compare(query, done);
  });

  it('hasFields - 7', function(done) {
    var query = r.expr(COMPLEX_OBJECT).hasFields({'buzz': [MISSING_FIELD, 'hello']});
    compare(query, done);
  });

  it('hasFields - 8', function(done) {
    var query = r.expr(COMPLEX_OBJECT).hasFields(['id', 'foo']);
    compare(query, done);
  });

  it('hasFields - 9', function(done) {
    var query = r.expr(COMPLEX_OBJECT).hasFields({'buzz': {'bonjour': ['monde']}});
    compare(query, done);
  });

  it('hasFields - 10', function(done) {
    var query = r.expr(COMPLEX_OBJECT).hasFields({'bonjour': ['monde']});
    compare(query, done);
  });

  it('hasFields - 11', function(done) {
    var query = r.expr(COMPLEX_OBJECT).hasFields({'bonjour': [MISSING_FIELD]});
    compare(query, done);
  });

  it('hasFields - 12', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).hasFields('foo').orderBy('id');
    compare(query, done);
  });

  it('hasFields - 13', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).hasFields('optional').orderBy('id');
    compare(query, done);
  });

  it('hasFields - 14', function(done) {
    var query = r.expr('foo').hasFields('foo');
    compare(query, done);
  });

  it('hasFields - 15', function(done) {
    var query = r.expr('foo').hasFields();
    compare(query, done);
  });

  it('hasFields - 16', function(done) {
    var query = r.expr({}).hasFields();
    compare(query, done);
  });

  it('hasFields - 17', function(done) {
    var query = r.expr({}).hasFields(r.db(TEST_DB).table(TEST_TABLE));
    compare(query, done);
  });

  it('hasFields - 18', function(done) {
    var query = r.expr([{}, {}]).hasFields();
    compare(query, done);
  });

  it('insertAt - 1', function(done) {
    var query = r.expr([1,2,3,4,5]).insertAt(0, 'foo');
    compare(query, done);
  });

  it('insertAt - 2', function(done) {
    var query = r.expr([1,2,3,4,5]).insertAt(2, 'foo');
    compare(query, done);
  });

  it('insertAt - 3', function(done) {
    var query = r.expr([1,2,3,4,5]).insertAt(4, 'foo');
    compare(query, done);
  });

  it('insertAt - 4', function(done) {
    var query = r.expr([1,2,3,4,5]).insertAt(5, 'foo');
    compare(query, done);
  });

  it('insertAt - 5', function(done) {
    var query = r.expr([1,2,3,4,5]).insertAt(200, 'foo');
    compare(query, done);
  });

  it('insertAt - 6', function(done) {
    var query = r.expr([1,2,3,4,5]).insertAt(-1, 'foo');
    compare(query, done);
  });

  it('insertAt - 7', function(done) {
    var query = r.expr([1,2,3,4,5]).insertAt(-200, 'foo');
    compare(query, done);
  });

  it('insertAt - 8', function(done) {
    var query = r.expr([1,2,3,4,5]).insertAt('foo', 'foo');
    compare(query, done);
  });

  it('insertAt - 9', function(done) {
    var query = r.expr([1,2,3,4,5]).insertAt(1,2,3);
    compare(query, done);
  });

  it('insertAt - 10', function(done) {
    var query = r.expr('foo').insertAt(1,2);
    compare(query, done);
  });

  it('insertAt - 11', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).insertAt(1,2);
    compare(query, done);
  });

  it('insertAt - 12', function(done) {
    var query = r.expr([1,2,3]).add(r.expr([1,2,3,4,5]).insertAt(200, 'foo'));
    compare(query, done);
  });

  it('spliceAt - 1', function(done) {
    var query = r.expr([1,2,3,4,5]).spliceAt(0, ['foo', 'bar']);
    compare(query, done);
  });

  it('spliceAt - 2', function(done) {
    var query = r.expr([1,2,3,4,5]).spliceAt(2, ['foo', 'bar']);
    compare(query, done);
  });

  it('spliceAt - 3', function(done) {
    var query = r.expr([1,2,3,4,5]).spliceAt(5, ['foo', 'bar']);
    compare(query, done);
  });

  it('spliceAt - 4', function(done) {
    var query = r.expr([1,2,3,4,5]).spliceAt(200, ['foo', 'bar']);
    compare(query, done);
  });

  it('spliceAt - 5', function(done) {
    var query = r.expr([1,2,3,4,5]).spliceAt(-1, ['foo', 'bar']);
    compare(query, done);
  });

  it('spliceAt - 6', function(done) {
    var query = r.expr([1,2,3,4,5]).spliceAt(-200, ['foo', 'bar']);
    compare(query, done);
  });

  it('spliceAt - 7', function(done) {
    var query = r.expr([1,2,3,4,5]).spliceAt(1,2,3);
    compare(query, done);
  });

  it('spliceAt - 8', function(done) {
    var query = r.expr([1,2,3,4,5]).spliceAt(1);
    compare(query, done);
  });

  it('spliceAt - 9', function(done) {
    var query = r.expr('foo').spliceAt(-1, [1,2]);
    compare(query, done);
  });

  it('spliceAt - 10', function(done) {
    var query = r.expr([1,2,3,4]).spliceAt(-1, 'bar');
    compare(query, done);
  });

  it('spliceAt - 11', function(done) {
    var query = r.expr([1,2,3,4]).spliceAt(-1, []);
    compare(query, done);
  });

  it('spliceAt - 12', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).spliceAt(-1, []);
    compare(query, done);
  });

  it('deleteAt - 1', function(done) {
    var query = r.expr([1,2,3,4,5]).deleteAt(0, 2);
    compare(query, done);
  });

  it('deleteAt - 2', function(done) {
    var query = r.expr([1,2,3,4,5]).deleteAt(2, 3);
    compare(query, done);
  });

  it('deleteAt - 3', function(done) {
    var query = r.expr([1,2,3,4,5]).deleteAt(2, 6);
    compare(query, done);
  });

  it('deleteAt - 4', function(done) {
    var query = r.expr([1,2,3,4,5]).deleteAt(2);
    compare(query, done);
  });

  it('deleteAt - 5', function(done) {
    var query = r.expr([1,2,3,4,5]).deleteAt(-1);
    compare(query, done);
  });

  it('deleteAt - 6', function(done) {
    var query = r.expr([1,2,3,4,5]).deleteAt(100);
    compare(query, done);
  });

  it('deleteAt - 7', function(done) {
    var query = r.expr([1,2,3,4,5]).deleteAt(-100);
    compare(query, done);
  });

  it('deleteAt - 8', function(done) {
    var query = r.expr([1,2,3,4,5]).deleteAt(-1, 2);
    compare(query, done);
  });

  it('deleteAt - 9', function(done) {
    var query = r.expr([1,2,3,4,5]).deleteAt(-100, 200);
    compare(query, done);
  });

  it('deleteAt - 9', function(done) {
    var query = r.expr([1,2,3,4,5]).deleteAt(0, -200);
    compare(query, done);
  });

  it('deleteAt - 10', function(done) {
    var query = r.expr([1,2,3,4,5]).deleteAt(1,2,3);
    compare(query, done);
  });

  it('deleteAt - 11', function(done) {
    var query = r.expr('foo').deleteAt(1,2);
    compare(query, done);
  });

  it('deleteAt - 12', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).deleteAt(1,2);
    compare(query, done);
  });

  it('deleteAt - 13', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).deleteAt('foo');
    compare(query, done);
  });

  it('deleteAt - 14', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).deleteAt(2, 'foo');
    compare(query, done);
  });

  it('changeAt - 1', function(done) {
    var query = r.expr([1,2,3,4,5]).changeAt(0, 'foo');
    compare(query, done);
  });

  it('changeAt - 2', function(done) {
    var query = r.expr([1,2,3,4,5]).changeAt(2, 'foo');
    compare(query, done);
  });

  it('changeAt - 3', function(done) {
    var query = r.expr([1,2,3,4,5]).changeAt(5, 'foo');
    compare(query, done);
  });

  it('changeAt - 4', function(done) {
    var query = r.expr([1,2,3,4,5]).changeAt(100, 'foo');
    compare(query, done);
  });

  it('changeAt - 5', function(done) {
    var query = r.expr([1,2,3,4,5]).changeAt(-1, 'foo');
    compare(query, done);
  });

  it('changeAt - 6', function(done) {
    var query = r.expr([1,2,3,4,5]).changeAt(-100, 'foo');
    compare(query, done);
  });

  it('changeAt - 7', function(done) {
    var query = r.expr([1,2,3,4,5]).changeAt(1,2,3);
    compare(query, done);
  });

  it('changeAt - 8', function(done) {
    var query = r.expr([1,2,3,4,5]).changeAt(1);
    compare(query, done);
  });

  it('changeAt - 9', function(done) {
    var query = r.expr('foo').changeAt(1,2);
    compare(query, done);
  });

  it('changeAt - 10', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).changeAt(1,2);
    compare(query, done);
  });

  it('keys - 1', function(done) {
    var query = r.expr({foo: 1, bar: 2, buzz: 3}).keys().orderBy(r.row);
    compare(query, done);
  });

  it('keys - 2', function(done) {
    function Foo() {
      this.foo = 1;
      this.bar = 2;
      this.buzz = 3;
    }
    Foo.prototype.extraBar = 2;
    var query = r.expr(new Foo()).keys().orderBy(r.row);
    compare(query, done);
  });

  it('keys - 3', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).get(1).keys().orderBy(r.row);
    compare(query, done);
  });

  it('keys - 4', function(done) {
    var query = r.expr(COMPLEX_OBJECT).keys().orderBy(r.row);
    compare(query, done);
  });

  it('keys - 5', function(done) {
    var query = r.expr('foo').keys();
    compare(query, done);
  });

  it('keys - 6', function(done) {
    var query = r.expr('foo').keys().orderBy(r.row);
    compare(query, done);
  });

  it('keys - 7', function(done) {
    var query = r.expr({}).keys(1, 2);
    compare(query, done);
  });

  it('values - 1', function(done) {
    var query = r.expr({foo: 1, bar: 2, buzz: 3}).values().orderBy(r.row);
    compare(query, done);
  });

  it('values - 2', function(done) {
    function Foo() {
      this.foo = 1;
      this.bar = 2;
      this.buzz = 3;
    }
    Foo.prototype.extraBar = 2;
    var query = r.expr(new Foo()).values().orderBy(r.row);
    compare(query, done);
  });

  it('values - 3', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).get(1).values().orderBy(r.row);
    compare(query, done);
  });

  it('values - 4', function(done) {
    var query = r.expr(COMPLEX_OBJECT).values().orderBy(r.row);
    compare(query, done);
  });

  it('values - 5', function(done) {
    var query = r.expr('foo').values();
    compare(query, done);
  });

  it('values - 6', function(done) {
    var query = r.expr('foo').values().orderBy(r.row);
    compare(query, done);
  });

  it('values - 7', function(done) {
    var query = r.expr({}).values(1, 2);
    compare(query, done);
  });


  it('literal - 1', function(done) {
    var query = r.expr({foo: {bar: 1, buzz: 2}, hello: 3, world: 4}).merge({
      foo: r.literal({bar: 2})
    });
    compare(query, done);
  });

  it('literal - 2 - pre', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).insert({id: 100, foo: {bar: 1, buzz: 2}});
    compare(query, done);
  });

  it('literal - 2', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).get(100).update({foo: r.literal({bar: 10})});
    compare(query, done);
  });

  it('literal - 2 - post', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).get(100);
    compare(query, done);
  });

  it('literal - 3', function(done) {
    var query = r.expr({foo: 'bar'}).merge({foo: r.literal()});
    compare(query, done);
  });

  it('literal - 4 - pre', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).get(100).update({foo: 'bar'});
    compare(query, done);
  });

  it('literal - 4', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).get(100).update({foo: r.literal()});
    compare(query, done);
  });

  it('literal - 4 - post', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).get(100);
    compare(query, done);
  });

  it('literal - 5', function(done) {
    var query = r.expr({}).merge({a: r.literal('a', 'b')});
    compare(query, done);
  });

  it('object - 1', function(done) {
    var query = r.object('foo', 1, 'bar', 2);
    compare(query, done);
  });

  it('object - 2', function(done) {
    var query = r.object('foo', 1, 'bar');
    compare(query, done);
  });

  it('object - 3', function(done) {
    var query = r.object('foo', 1, 1, 2);
    compare(query, done);
  });

  it('object - 4', function(done) {
    var query = r.object('foo', 1, 'foo', 2);
    compare(query, done);
  });

  it('object - 5', function(done) {
    var query = r.object('foo', {hello: 'world'}, 'foo', {ciao: 'amici'});
    compare(query, done, function(error) {
      var result = error.split('.')[0];
      assert(result.length > 0);
      return result;
    });
  });

  it('object - 6', function(done) {
    var query = r.object(1, 1, 'bar', 2);
    compare(query, done);
  });

  it('object - 7', function(done) {
    var query = r.object();
    compare(query, done);
  });

  /*
  */
});
