var config = require(__dirname+'/../config.js');
var r = require('rethinkdb');
var assert = require('assert');
var nothing = require(__dirname+'/../lib/index.js');


var connection;

var michel = {id: 1, age: 27, name: "Michel", coins: [1, 2, 5, 10], human: true};
var laurent = {id: 2, age: 29, name: "Laurent", coins: [2, 5], human: true};
var sophie = {id: 3, age: 22, name: "Sophie", coins: [1, 2, 10], human: true};
var lucky = {id: 4, age: 11, name: "Lucky", coins: []};


describe('string.js', function(){
  before(function(done) {
    setTimeout(function() { // Delay for nodemon to restart the server
      r.connect(config, function(err, conn) {
        connection = conn;

        r.dbCreate("reqlitetest").run(connection, function() {
          // Ignore the error
          r.db("reqlitetest").tableDrop("str").run(connection, function(err, result) {
            r.db("reqlitetest").tableCreate("str").run(connection, function(err, result) {
              r.db("reqlitetest").table("str").insert([michel, laurent, sophie, lucky])
                .run(connection, function(err, result) {

                assert.equal(result.inserted, 4);
                r.db("reqlitetest").table("str").indexCreate("age").run(connection, function(err, result) {
                r.db("reqlitetest").table("str").indexCreate("coins").run(connection, function(err, result) {
                  r.db("reqlitetest").table("str").indexWait("age", "coins").run(connection, function(err, result) {
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

  it('split - 1', function(done) {
    r.expr("bonjour").split().run(connection).then(function(result) {
      assert.deepEqual(result, ['b', 'o', 'n', 'j', 'o', 'u', 'r']);
      done();
    }).error(done);
  });
  it('split - 2', function(done) {
    r.expr("bonjour").split('', 4).run(connection).then(function(result) {
      assert.deepEqual(result, ['b', 'o', 'n', 'j', 'our']);
      done();
    }).error(done);
  });
  it('split - 3', function(done) {
    r.expr("bonjour").split('', 40).run(connection).then(function(result) {
      assert.deepEqual(result, ['b', 'o', 'n', 'j', 'o', 'u', 'r']);
      done();
    }).error(done);
  });
  it('split - 4', function(done) {
    r.expr("bonjour").split('', 7).run(connection).then(function(result) {
      assert.deepEqual(result, ['b', 'o', 'n', 'j', 'o', 'u', 'r']);
      done();
    }).error(done);
  });
  it('split - 5', function(done) {
    r.expr("bonjour").split('=', 7).run(connection).then(function(result) {
      assert.deepEqual(result, ['bonjour']);
      done();
    }).error(done);
  });




  /*
  it('upcase', function(done) {
    r.expr("foo").upcase().run(connection).then(function(result) {
      assert.equal(result, "FOO");
      done();
    }).error(done);
  });
  it('downcase', function(done) {
    r.expr("FOO").downcase().run(connection).then(function(result) {
      assert.equal(result, "foo");
      done();
    }).error(done);
  });
  */


  after(function() {
    connection.close();
  });
});
