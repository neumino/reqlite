var config = require(__dirname+'/../config.js');
var r = require('rethinkdb');
var assert = require('assert');
var nothing = require(__dirname+'/../lib/index.js');


var connection;

var michel = {id: 1, age: 27, name: "Michel", coins: [1, 2, 5, 10]};
var laurent = {id: 2, age: 29, name: "Laurent", coins: [2, 5]};
var sophie = {id: 3, age: 22, name: "Sophie", coins: [1, 2, 10]};
var lucky = {id: 4, age: 11, name: "Lucky", coins: []};

describe('select.js', function(){
  before(function(done) {
    setTimeout(function() { // Delay for nodemon to restart the server
      r.connect(config, function(err, conn) {
        connection = conn;

        r.dbCreate("reqlitetest").run(connection, function() {
          // Ignore the error
          r.db("reqlitetest").tableDrop("select").run(connection, function(err, result) {
            r.db("reqlitetest").tableCreate("select").run(connection, function(err, result) {
              r.db("reqlitetest").table("select").insert([michel, laurent, sophie, lucky])
                .run(connection, function(err, result) {

                assert.equal(result.inserted, 4);
                r.db("reqlitetest").table("select").indexCreate("age").run(connection, function(err, result) {
                  r.db("reqlitetest").table("select").indexCreate("coins", function(doc) { return doc("coins") }, {multi: true}).run(connection, function(err, result) {
                    r.db("reqlitetest").table("select").indexWait("age", "coins").run(connection, function(err, result) {
                      done();
                    });
                  });
                });
              });
            });
          });
        });
      });
    }, 100)
  });

  it('get - document', function(done) {
    r.table("select").get(1).run(connection).then(function(result) {
      assert.deepEqual(result, michel);
      done();
    }).error(done);
  });
  it('get - non existing document', function(done) {
    r.table("select").get("nonExisting").run(connection).then(function(result) {
      assert.deepEqual(result, null);
      done();
    }).error(done);
  });
  it('getAll - pk', function(done) {
    r.table("select").getAll(1).run(connection).then(function(result) {
      assert.deepEqual(result, [michel]);
      done();
    }).error(done);
  });
  it('getAll - multiple pk', function(done) {
    r.table("select").getAll(1, 2).run(connection).then(function(result) {
      assert.deepEqual(result, [michel, laurent]);
      done();
    }).error(done);
  });
  it('getAll - secondary', function(done) {
    r.table("select").getAll(27, {index: "age"}).run(connection).then(function(result) {
      assert.deepEqual(result, [michel]);
      done();
    }).error(done);
  });
  it('getAll - multiple secondary', function(done) {
    r.table("select").getAll(27, 29, {index: "age"}).run(connection).then(function(result) {
      result.sort(function(left, right) { return left.age-right.age });
      assert.deepEqual(result, [michel, laurent]);
      done();
    }).error(done);
  });
  it('getAll - multi', function(done) {
    r.table("select").getAll(2, {index: "coins"}).run(connection).then(function(result) {
      result.sort(function(left, right) { return left.age-right.age });
      assert.deepEqual(result, [sophie, michel, laurent]);
      done();
    }).error(done);
  });
  it('getAll - multi', function(done) {
    r.table("select").getAll(2, 1, {index: "coins"}).run(connection).then(function(result) {
      result.sort(function(left, right) { return left.age-right.age });
      assert.deepEqual(result, [sophie, sophie, michel, michel, laurent]);
      done();
    }).error(done);
  });
  it('between - pk', function(done) {
    r.table("select").between(1, 3).run(connection).then(function(result) {
      result.sort(function(left, right) { return left.age-right.age });
      assert.deepEqual(result, [michel, laurent]);
      done();
    }).error(done);
  });
  it('between - secondary', function(done) {
    r.table("select").between(25, 30, {index: "age"}).run(connection).then(function(result) {
      result.sort(function(left, right) { return left.age-right.age });
      assert.deepEqual(result, [michel, laurent]);
      done();
    }).error(done);
  });
  it('between - secondary - multi', function(done) {
    r.table("select").between(1, 3, {index: "coins", rightBound: "closed"}).run(connection).then(function(result) {
      result.sort(function(left, right) { return left.age-right.age });
      assert.deepEqual(result, [sophie, sophie, michel, michel, laurent]);
      done();
    }).error(done);
  });
  it('filter - object', function(done) {
    r.table("select").filter({name: "Michel"}).run(connection).then(function(result) {
      assert.deepEqual(result, [michel]);
      done();
    }).error(done);
  });
  it('filter - function', function(done) {
    r.table("select").filter(function(doc) { return doc("name").eq("Michel") }).run(connection).then(function(result) {
      assert.deepEqual(result, [michel]);
      done();
    }).error(done);
  });
  it('filter - missing field', function(done) {
    r.table("select").filter(function(doc) { return doc("nonExisting").eq("foo") }).run(connection).then(function(result) {
      assert.deepEqual(result, []);
      done();
    }).error(done);
  });
  it('filter - missing field - default true', function(done) {
    r.table("select").filter(function(doc) { return doc("nonExisting").eq("foo") }, {default: true}).run(connection).then(function(result) {
      result.sort(function(left, right) { return left.age-right.age });
      assert.deepEqual(result, [lucky, sophie, michel, laurent]);
      done();
    }).error(done);
  });
  it('filter - missing field - default r.error', function(done) {
    r.table("select").filter(function(doc) { return doc("nonExisting").eq("foo") }, {default: r.error("Field not found")}).run(connection).then(function(result) {
      done(new Error("Was expecting error"));
    }).error(function(err) {
      assert(err.message.match(/^Field not found/));
      done();
    }).error(done);
  });







  after(function() {
    connection.close();
  });
});
