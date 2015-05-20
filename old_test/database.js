var config = require(__dirname+'/../config.js');
var r = require('rethinkdb');
var assert = require('assert');
var nothing = require(__dirname+'/../lib/index.js');


var connection;

describe('Manipulating databases', function(){
  before(function(done) {
    setTimeout(function() { // Delay for nodemon to restart the server
      r.connect(config).then(function(conn) {
        connection = conn;
        r.dbDrop("testdbcreate").run(connection).error(function() {
          // Ignore the error
        }).finally(function(result) {
          done();
        });
      }).error(done);
    }, 100)
  });

  it('List databases', function(done) {
    r.dbList().run(connection).then(function(result) {
      assert(Array.isArray(result));
      done();
    }).error(done);
  });

  it('Create a database', function(done) {
    r.dbCreate("testdbcreate").run(connection).then(function(result) {
      assert.deepEqual(result, {created: 1})
      return r.dbList().run(connection);
    }).then(function(result) {
      assert.notEqual(result.indexOf("testdbcreate"), -1)
      done();
    }).error(done);
  });

  it('Drop a database', function(done) {
    r.dbDrop("testdbcreate").run(connection).then(function(result) {
      assert.deepEqual(result, {dropped: 1})
      return r.dbList().run(connection);
    }).then(function(result) {
      assert.equal(result.indexOf("testdbcreate"), -1)
      done();
    }).error(done);
  });

  it('Recreate the test (`testdbcreate`) database', function(done) {
    r.dbCreate("testdbcreate").run(connection).then(function(result) {
      assert.deepEqual(result, {created: 1})
      done();
    }).error(done);
  });

  after(function() {
    connection.close();
  });
});
