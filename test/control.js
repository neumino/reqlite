var config = require(__dirname+'/../config.js');
var r = require('rethinkdb');
var assert = require('assert');
var nothing = require(__dirname+'/../lib/index.js');


var connection;

describe('control.js', function(){
    before(function(done) {
        setTimeout(function() { // Delay for nodemon to restart the server
            r.connect(config, function(err, conn) {
                connection = conn;

                r.dbCreate("reqlitetest").run(connection, function(err, result) {
                    // Ignore the error
                    r.db("reqlitetest").tableCreate("control").run(connection, function(err, result) {
                        done();
                    });
                });
            });
        }, 100)
    });

    it('r.error', function(done) {
        r.error("foo").run(connection).then(function(result) {
            done(new Error("Was expecting an error"));
        }).error(function(err) {
            assert(err.message.match(/^foo/));
            done();
        });
    });
    it('r.error - wront type', function(done) {
        r.error(1).run(connection).then(function(result) {
            done(new Error("Was expecting an error"));
        }).error(function(err) {
            assert(err.message.match(/^Expected type STRING but found NUMBER/));
            done();
        });
    });

    it('default', function(done) {
        r.expr({a: 1})('b').default("bar").run(connection).then(function(result) {
            assert.equal(result, "bar");
            done();
        }).error(done);
    });

    it('json - 1', function(done) {
        r.json("2").run(connection).then(function(result) {
            assert.equal(result, 2);
            done();
        }).error(done);
    });
    it('json - 2', function(done) {
        r.json("[1,2,3]").run(connection).then(function(result) {
            assert.deepEqual(result, [1,2,3]);
            done();
        }).error(done);
    });
    it('json - 3', function(done) {
        r.json("[1,2,3]").map(function() {
            return 2
        }).run(connection).then(function(result) {
            assert.deepEqual(result, [2, 2, 2]);
            done();
        }).error(done);
    });
    it('json - 4', function(done) {
        r.json(",3]").run(connection).then(function(result) {
            done(new Error("Was expecting an error"));
        }).error(function(err) {
            assert(err.message.match(/^Failed to parse ",3]" as JSON/))
            done();
        });
    });


    after(function() {
        connection.close();
    });
});
