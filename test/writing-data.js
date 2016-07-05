var config = require('./../config.js');

var r = require('rethinkdb');
var assert = require('assert');

var connections = {};
var TEST_DB = 'reqlitetest';
var TEST_TABLE = 'reqlitetestwritingdata';
var MISSING_ID = 'nonExistingId';

var compare = require('./util.js').generateCompare(connections);

// Use integers for primary keys if possible. That way we can easily delete
// generated primary keys with just looking for strings.

describe('writing-data.js', function(){
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

  it('insert - 1', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).insert({id: 1, foo: 2, bar: 3});
    compare(query, done);
  });
  it('insert - 1 - follow up', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).get(1);
    compare(query, done);
  });

  it('insert - 2', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).insert([
      {id: 2, foo: 0, bar: 0},
      {id: 4, foo: 0, bar: 0},
      {id: 3, foo: 0, bar: 0},
    ]);
    compare(query, done);
  });
  it('insert - 2 - follow up', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).orderBy('id');
    compare(query, done);
  });

  it('insert - 3', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).insert({id: new Buffer('foo')});
    compare(query, done);
  });

  it('insert - 3 - follow up', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).get(new Buffer('foo'));
    compare(query, done);
  });

  it('insert - 3 - clean', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).get(new Buffer('foo')).delete();
    compare(query, done);
  });

  it('insert - 4', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).insert('foo');
    compare(query, done);
  });

  it('insert - 5', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).insert(2);
    compare(query, done);
  });

  it('insert - 6', function(done) {
    var query = r.expr('foo').insert({});
    compare(query, done);
  });

  it('insert - 7', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).insert({foo: 'bar'}, {buzz: 'lol'});
    compare(query, done);
  });

  it('insert - 8', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).insert({id: 100, foo: 'bar'}, {returnChanges: true});
    compare(query, done);
  });

  it('insert - 8 - changes always', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).insert({id: 101, foo: 'bar'}, {returnChanges: 'always'});
    compare(query, done);
  });

  it('insert - 9', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).insert([
      {id: 102, foo: 'bar102'},
      {id: 103, foo: 'bar103'},
      {id: 104, foo: 'bar104'}
    ], {returnChanges: true}).do(function(result) {
      return result.merge({
        changes: result('changes').orderBy(function(change) {
          return change;
        })
      });
    });
    compare(query, done);
    //compare(query, done, function(e) { console.log(''); console.log(JSON.stringify(e, null, 4)); return e; });
  });

  it('insert - 9 - changes always', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).insert([
      {id: 105, foo: 'bar105'},
      {id: 106, foo: 'bar106'},
      {id: 107, foo: 'bar107'}
    ], {returnChanges: 'always'}).do(function(result) {
      return result.merge({
        changes: result('changes').orderBy(function(change) {
          return change;
        })
      });
    });
    compare(query, done);
    //compare(query, done, function(e) { console.log(''); console.log(JSON.stringify(e, null, 4)); return e; });
  });

  it('insert - 10', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).insert({
      id: 103, // primary key already used
      foo: 'bar<new>'
    }, {returnChanges: true});
    compare(query, done, function(e) {
      delete e.first_error;
      return e;
    });
  });

  it('insert - 10 - changes always', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).insert({
      id: 103, // primary key already used
      foo: 'bar<always>'
    }, {returnChanges: 'always'});
    compare(query, done, function(e) {
      delete e.first_error;
      delete e.changes[0].error;
      return e;
    });
  });

  it('insert - 11', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).insert({
      id: 103, // primary key already used
      foo: 'bar<new>'
    }, {conflict: 'replace', returnChanges: true});
    compare(query, done);
  });

  it('insert - 11 - changes always', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).insert({
      id: 103, // primary key already used
      foo: 'bar<always>'
    }, {conflict: 'replace', returnChanges: 'always'});
    compare(query, done);
  });

  it('insert - 12', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).insert({
      id: 103, // primary key already used
      buzz: 'extra<buzz>'
    }, {conflict: 'update', returnChanges: true});
    compare(query, done);
  });

  it('insert - 12 - changes always', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).insert({
      id: 103, // primary key already used
      buzz: 'extra<always>'
    }, {conflict: 'update', returnChanges: 'always'});
    compare(query, done);
  });

  it('insert - 13', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).insert([{}, {}, {}]);
    compare(query, done, function(result) {
      assert.equal(result.generated_keys.length, 3);
      delete result.generated_keys;
      return result;
    });
  });
  it('insert - 13 - follow up', function(done) {
    // We need to clean here as we have different primary keys in reqlite and rethinkdb
    var query = r.db(TEST_DB).table(TEST_TABLE).filter(function(doc) {
      return doc('id').typeOf().eq("STRING");
    }).delete();
    compare(query, done);
  });

  it('insert - 14', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).insert({id: true});
    compare(query, done);
  });
  it('insert - 14 - follow up', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).get(true);
    compare(query, done);
  });
  it('insert - 14 - follow up - 2', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).get(true).delete();
    compare(query, done);
  });

  it('insert - 15', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).insert({id: null});
    compare(query, done);
  });
  it('insert - 15 - follow up', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).get(null);
    compare(query, done);
  });
  it('insert - 15 - follow up - 2', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).get(null).delete();
    compare(query, done);
  });

  it('insert - 16', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).insert({id: r.point(0, 0)});
    compare(query, done);
  });
  it('insert - 16 - follow up', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).get(r.point(0, 0));
    compare(query, done);
  });
  it('insert - 16 - follow up - 2', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).get(r.point(0, 0)).delete();
    compare(query, done);
  });

  it('insert - 17', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).insert({id: r.time(1986, 11, 3, 'Z')});
    compare(query, done);
  });

  it('insert - 17 - follow up', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).get(r.time(1986, 11, 3, 'Z'));
    compare(query, done);
  });
  it('insert - 17 - follow up - 2', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).insert({id: r.time(1986, 11, 3, '+00:00')});
    compare(query, done, function(e) {
      delete e.first_error;
      return e;
    });
  });
  it('insert - 17 - follow up - 3', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).get(r.time(1986, 11, 3, 'Z')).delete();
    compare(query, done);
  });

  it('insert - 18', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).insert({id: new Buffer('Hello world')});
    compare(query, done);
  });
  it('insert - 18 - follow up', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).get(new Buffer('Hello world'));
    compare(query, done);
  });
  it('insert - 18 - follow up - 2', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).get(new Buffer('Hello world')).delete();
    compare(query, done);
  });

  it('insert - 19', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).insert({}, {returnChanges: true});
    compare(query, done, function(result) {
      assert(typeof result.generated_keys[0] === 'string');
      assert(result.changes.length === 1);
      delete result.generated_keys;
      delete result.changes;
      return result;
    });
  });
  it('insert - 19 - follow up', function(done) {
    // We need to clean here as we have different primary keys in reqlite and rethinkdb
    var query = r.db(TEST_DB).table(TEST_TABLE).filter(function(doc) {
      return doc('id').typeOf().eq("STRING");
    }).delete();
    compare(query, done);
  });

  it('insert - 20', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).insert({}, {returnChanges: 'always'});
    compare(query, done, function(result) {
      assert(typeof result.generated_keys[0] === 'string');
      assert(result.changes.length === 1);
      delete result.generated_keys;
      delete result.changes;
      return result;
    });
  });
  it('insert - 20 - follow up', function(done) {
    // We need to clean here as we have different primary keys in reqlite and rethinkdb
    var query = r.db(TEST_DB).table(TEST_TABLE).filter(function(doc) {
      return doc('id').typeOf().eq("STRING");
    }).delete();
    compare(query, done);
  });


  it('update - 1', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).get(1).update({foo: 20, bar: 30});
    compare(query, done);
  });
  it('update - 1 - follow up', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).get(1);
    compare(query, done);
  });

  it('update - 2', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).get(MISSING_ID).update({foo: 20, bar: 30});
    compare(query, done);
  });

  it('update - 3', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).get(1).update({buzz: 4});
    compare(query, done);
  });
  it('update - 3 - follow up', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).get(1);
    compare(query, done);
  });

  it('update - 4', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).get(1).update({foo: r.row('foo').add(1) });
    compare(query, done);
  });
  it('update - 4 - follow up', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).get(1);
    compare(query, done);
  });

  it('update - 5', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).get(1).update(function(doc) {
      return {foo: doc('foo').add(1) };
    });
    compare(query, done);
  });
  it('update - 5 - follow up', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).orderBy('id');
    compare(query, done);
  });

  it('update - 6', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).filter({id: 1}).update(function(doc) {
      return {foo: doc('foo').add(1) };
    });
    compare(query, done);
  });
  it('update - 6 - follow up', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).orderBy('id');
    compare(query, done);
  });

  it('update - 7', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).update(function(doc) {
      return {foo: doc('foo').add(1) };
    });
    compare(query, done);
  });
  it('update - 7 - follow up', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).orderBy('id');
    compare(query, done);
  });

  it('update - 8', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).between(r.minval, r.maxval).update({copyId: r.row('id')});
    compare(query, done);
  });

  it('update - 9', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).coerceTo("ARRAY").update({copyId: r.row('id')});
    compare(query, done, function(error) {
      var message = error.split(':')[0];
      assert(message.length > 0);
      return message;
    });
  });

  it('update - 10', function(done) {
    var query = r.expr('foo').update({copyId: r.row('id')});
    compare(query, done, function(error) {
      var message = error.split(':')[0];
      assert(message.length > 0);
      return message;
    });
  });

  it('update - 11', function(done) {
    var query = r.expr('foo').update({copyId: r.row('id')});
    compare(query, done, function(error) {
      var message = error.split(':')[0];
      assert(message.length > 0);
      return message;
    });
  });

  it('update - 12', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).update({copyId: null});
    compare(query, done);
  });

  it('update - 13', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).update(r.js('(function(doc) { return {copyId: doc.id} })'));
    compare(query, done);
  });

  it('update - 14', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).update(r.js('(function(doc) { return {copyId: doc.id} })'), {nonAtomic: true});
    compare(query, done);
  });
  it('update - 14 - follow up', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).orderBy('id');
    compare(query, done);
  });

  it('update - 15', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).update(function(doc) {
      return r.db(TEST_DB).table(TEST_TABLE).get(doc('id'));
    });
    compare(query, done);
  });

  it('update - 16', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).update(function(doc) {
      return r.db(TEST_DB).table(TEST_TABLE).get(doc('id'));
    }, {nonAtomic: true});
    compare(query, done);
  });
  it('update - 16 - follow up', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).orderBy('id');
    compare(query, done);
  });

  it('update - 17', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).update(function(doc) {
      return r.db(TEST_DB).table(TEST_TABLE);
    }, {nonAtomic: true});
    compare(query, done);
  });

  it('update - 18', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).orderBy({index: 'id'}).limit(1).update(function(doc) {
      return {foo: doc('id')('bar')};
    }, {nonAtomic: true});
    compare(query, done);
  });

  it('update - 19', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).update(function(doc) {
      return { foo: r.expr(1).add('foo') };
    }, {nonAtomic: true});
    compare(query, done);
  });

  it('update - 20', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).update({ foo: r.expr(1).add('foo') });
    compare(query, done);
  });

  it('update - 21', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).update(r.js('(function(doc) { return {copyId: doc.id+NaN} })'), {nonAtomic: true});
    compare(query, done);
  });

  it('update - 22', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).get(1).update({foo: 20, bar: 30}, {returnChanges: true});
    compare(query, done);
  });
  it('update - 22 - follow up', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).get(1);
    compare(query, done);
  });

  it('update - 23', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).get(MISSING_ID).update({foo: 20, bar: 30}, {returnChanges: true});
    compare(query, done);
  });

  it('update - 24', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).get(1).update({buzz: 4}, {returnChanges: true});
    compare(query, done);
  });

  it('update - 24 - follow up', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).get(1);
    compare(query, done);
  });

  it('update - 25', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).get(1).update({foo: r.row('foo').add(1) }, {returnChanges: true});
    compare(query, done);
  });

  it('update - 25 - follow up', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).get(1);
    compare(query, done);
  });

  it('update - 26', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).get(1).update({}, {returnChanges: true});
    compare(query, done);
  });

  it('update - 27', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).get(1).update({}, {returnChanges: 'always'});
    compare(query, done);
  });

  it('update - 28', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).get(1).update({id: 2}, {returnChanges: 'always'});
    compare(query, done, function(result) {
      delete result.first_error;
      delete result.changes[0].error;
      return result;
    });
  });

  it('update - 29', function(done) {
    var query = r.expr([1,2,3]).forEach(function(x) {
      return r.db(TEST_DB).table(TEST_TABLE).update(r.js('(function(doc) { return {copyId: doc.id} })'));
    });
    compare(query, done);
  });

  it('update - 30', function(done) {
    var query = r.expr([1,2,3]).forEach(function(x) {
      return r.db(TEST_DB).table(TEST_TABLE).update(function() {
        return r.db(TEST_DB).table(TEST_TABLE).get(1);
      });
    });
    compare(query, done);
  });

  it('update - 31', function(done) {
    var query = r.expr([1,2,3]).forEach(function(x) {
      return r.db(TEST_DB).table(TEST_TABLE).update(function() {
        return r.random();
      });
    });
    compare(query, done);
  });

  it('update - 32', function(done) {
    var query = r.expr([1,2,3]).forEach(function(x) {
      return r.db(TEST_DB).table(TEST_TABLE).update(function() {
        return r.table(TEST_TABLE);
      });
    });
    compare(query, done);
  });

  it('update - 33', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).update(null);
    compare(query, done);
  });

  it('update - 34', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).get(1).update({foo: {bar: {buzz: 2}}});
    compare(query, done);
  });
  it('update - 34 - follow up', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).get(1);
    compare(query, done);
  });
  it('update - 34 - more update', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).get(1).update({foo: {bar2: {buzz2: 3}}});
    compare(query, done);
  });
  it('update - 34 - more follow up', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).get(1);
    compare(query, done);
  });

  it('replace - 1', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).get(1).replace({id: 1, foo: 200});
    compare(query, done);
  });

  it('replace - 1 - follow up', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).get(1);
    compare(query, done);
  });

  it('replace - 2', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).get(1).replace({id: 1, foo: r.row('foo').add(1)});
    compare(query, done);
  });
  it('replace - 2 - follow up', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).get(1);
    compare(query, done);
  });

  it('replace - 3', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).get(1).replace(function(doc) {
      return {id: 1, foo: doc('foo').add(1)};
    });
    compare(query, done);
  });
  it('replace - 3 - follow up', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).get(1);
    compare(query, done);
  });

  it('replace - 4', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).filter({id: 1}).replace(function(doc) {
      return {id: 1, foo: doc('foo').add(1)};
    });
    compare(query, done);
  });

  it('replace - 4 - follow up', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).orderBy('id');
    compare(query, done);
  });

  it('replace - 5', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).replace(function(doc) {
      return doc.merge({foo: doc('foo').add(1)});
    }, {returnChanges: true}).do(function(result) {
      return result.merge({
        changes: result('changes').orderBy(function(change) {
          return change;
        })
      });
    });
    compare(query, done);
  });

  it('replace - 5 - follow up', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).orderBy('id');
    compare(query, done);
  });

  it('replace - 6', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).replace(function(doc) {
      return {id: 1, foo: doc('foo').add(1)};
    });
    compare(query, done, function(result) {
      delete result.first_error;
      return result;
    });
  });
  it('replace - 6 - follow up', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).orderBy('id');
    compare(query, done);
  });

  it('replace - 7', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).get(1).replace({id: 1, foo: 200}, {returnChanges: true});
    compare(query, done);
  });

  it('replace - 8', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).get(1).replace({id: 1, foo: r.row('foo').add(1)}, {returnChanges: true});
    compare(query, done);
  });

  it('replace - 9', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).get(1).replace(function(doc) {
      return {id: 1, foo: doc('foo').add(1)};
    }, {returnChanges: true});
    compare(query, done);
  });

  it('replace - 10', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).filter({id: 1}).replace(function(doc) {
      return {id: 1, foo: doc('foo').add(1)};
    }, {returnChanges: true});
    compare(query, done);
  });

  it('replace - 11', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).replace(function(doc) {
      return {id: 1, foo: doc('foo').add(1)};
    }, {returnChanges: true}).do(function(result) {
      return result.merge({
        changes: result('changes').orderBy(function(change) {
          return change;
        })
      });
    });
    compare(query, done, function(result) {
      delete result.first_error;
      return result;
    });
  });
  it('replace - 11 - follow up', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).orderBy('id');
    compare(query, done);
  });

  it('replace - 12', function(done) {
    var query = r.expr({id: 2}).replace({id: 1});
    compare(query, done, function(error) {
      var message = error.split(':')[0];
      assert(message.length > 0);
      return message;
    });
  });

  it('replace - 13 - pre', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).insert({
      id: "merge/replace",
      foo: [ {bar: "lol"}, {bar: "buzz"} ]
    });
    compare(query, done);
  });

  it('replace - 13', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).get("merge/replace").replace({
      id: "merge/replace",
      foo: [ {buzz: "om"}, 2 ]
    });
    compare(query, done);
  });

  it('replace - 13 - follow up', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).get("merge/replace");
    compare(query, done);
  });

  it('replace - 14 - pre', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).insert({
      id: "merge/replace/array",
      foo: [ {bar: "lol"}, 2 ]
    });
    compare(query, done);
  });

  it('replace - 14', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).get("merge/replace/array").replace({
      id: "merge/replace/array",
      foo: [ {buzz: "om"}, {foo: "bar"} ]
    });
    compare(query, done);
  });

  it('replace - 14 - follow up', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).get("merge/replace/array");
    compare(query, done);
  });

  it('replace - 15 - pre', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).insert({
      id: "deep/nested",
      foo: {
        bar: [1, 2, 3, {bar: { buzz: "lol"}}],
        yo: {
          lo: {
            carpe: 1
          },
          diem: 3
        }
      }
    });
    compare(query, done);
  });

  it('replace - 15', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).get("deep/nested").replace({
      id: "deep/nested",
      foo: {
        bar: [10, 20, {hello: "bonjour"}, {bar: { buzz: "yolo"}}],
        yo: {
          lo: {
            nocarpe: 3
          },
          diem: 2
        },
        boo: "om"
      }
    });
    compare(query, done);
  });

  it('replace - 15 - follow up', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).get("deep/nested");
    compare(query, done);
  });

  it('replace - 16 - pre', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).insert({
      id: "array/length",
      foo: [1,2,3,4]
    });
    compare(query, done);
  });

  it('replace - 16', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).get("array/length").replace({
      id: "array/length",
      foo: [1,2,3,4,5,6]
    });
    compare(query, done);
  });

  it('replace - 16 - follow up', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).get("array/length");
    compare(query, done);
  });

  it('replace - 17', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).get("array/length").replace({
      id: "array/length",
      foo: [1,2]
    });
    compare(query, done);
  });

  it('replace - 17 - follow up', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).get("array/length");
    compare(query, done);
  });

  it('replace - 18', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).get("array/length").update({}, {returnChanges: 'always'});
    compare(query, done);
  });

  it('replace - 19', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).get("array/length").update({id: 2}, {returnChanges: 'always'});
    compare(query, done, function(result) {
      delete result.first_error;
      delete result.changes[0].error;
      return result;
    });
  });

  it('replace - 20', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).get("array/length").replace(null)
    compare(query, done);
  });

  it('delete - 1', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).get(1).delete();
    compare(query, done);
  });
  it('delete - 1 - follow up', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).get(1);
    compare(query, done);
  });

  it('delete - 2', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).get(MISSING_ID).delete();
    compare(query, done);
  });

  it('delete - 3', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).get(2).delete({returnChanges: true});
    compare(query, done);
  });

  it('delete - 4', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).filter({id: 3}).delete({returnChanges: true});
    compare(query, done);
  });

  it('delete - 5', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).insert([
      {id: 10, foo: 10},
      {id: 20, foo: 20},
      {id: 30, foo: 30},
      {id: 40, foo: 40},
      {id: 50, foo: 50},
      {id: 60, foo: 60},
      {id: 70, foo: 70}
    ]);
    compare(query, done);
  });

  it('delete - 6', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).filter(true).delete({returnChanges: true}).do(function(result) {
      return result.merge({
        changes: result('changes').orderBy(function(change) {
          return change('old_val')('id');
        })
      });
    });
    compare(query, done);
  });

  it('delete - 7', function(done) {
    var query = r.db(TEST_DB).table(TEST_TABLE).limit(1).delete({returnChanges: 'always'})
    compare(query, done);
  });

  /*
  */
});
