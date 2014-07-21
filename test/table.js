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
                r.dbCreate("testtablecreate").run(connection).error(function() {
                    // Ignore the error
                }).finally(function(result) {
                    done();
                });
            }).error(done);
        }, 100)
    });

    it('List tables', function(done) {
        r.db("testtablecreate").tableList().run(connection).then(function(result) {
            assert.equal(result.indexOf("testtablecreate"), -1)
            done();
        }).error(done);
    });

    it('Create a table', function(done) {
        r.db("testtablecreate").tableCreate("test").run(connection).then(function(result) {
            assert.deepEqual(result, {created: 1})
            return r.db("testtablecreate").tableList().run(connection);
        }).then(function(result) {
            assert.deepEqual(result, ["test"])
            done();
        }).error(done);
    });

    it('Drop a table', function(done) {
        r.db("testtablecreate").tableDrop("test").run(connection).then(function(result) {
            assert.deepEqual(result, {dropped: 1})
            return r.db("testtablecreate").tableList().run(connection);
        }).then(function(result) {
            assert.deepEqual(result, [])
            done();
        }).error(done);
    });



    after(function() {
        connection.close();
    });
});
