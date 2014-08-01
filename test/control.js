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
    it('default', function(done) {
        r.expr({a: 1})('b').default("bar").run(connection).then(function(result) {
            assert.equal(result, "bar");
            done();
        }).error(done);
    });


    after(function() {
        connection.close();
    });
});
