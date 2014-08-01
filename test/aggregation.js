var config = require(__dirname+'/../config.js');
var r = require('rethinkdb');
var assert = require('assert');
var nothing = require(__dirname+'/../lib/index.js');


var connection;

var michel = {id: 1, age: 27, name: "Michel", coins: [1, 2, 5, 10]};
var laurent = {id: 2, age: 29, name: "Laurent", coins: [2, 5]};
var sophie = {id: 3, age: 22, name: "Sophie", coins: [1, 2, 10]};
var lucky = {id: 4, age: 11, name: "Lucky", coins: []};



describe('Operators', function(){
    before(function(done) {
        setTimeout(function() { // Delay for nodemon to restart the server
            r.connect(config, function(err, conn) {
                connection = conn;

                r.dbCreate("reqlitetest").run(connection, function() {
                    // Ignore the error
                    r.db("reqlitetest").tableDrop("agg").run(connection, function(err, result) {
                        r.db("reqlitetest").tableCreate("agg").run(connection, function(err, result) {
                            r.db("reqlitetest").table("agg").insert([michel, laurent, sophie, lucky])
                                .run(connection, function(err, result) {

                                assert.equal(result.inserted, 4);
                                r.db("reqlitetest").table("agg").indexCreate("age").run(connection, function(err, result) {
                                    r.db("reqlitetest").table("agg").indexCreate("coins", function(doc) { return doc("coins") }, {multi: true}).run(connection, function(err, result) {
                                        r.db("reqlitetest").table("agg").indexWait("age", "coins").run(connection, function(err, result) {
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
    // array bool date null number string object

    it('contains - value - true', function(done) {
        r.expr([1,2,3]).contains(1).run(connection).then(function(result) {
           assert.equal(result, true);
           done();
        }).error(done);
    });
    it('contains - value - false', function(done) {
        r.expr([1,2,3]).contains(4).run(connection).then(function(result) {
           assert.equal(result, false);
           done();
        }).error(done);
    });
    it('contains - function - true', function(done) {
        r.expr([1,2,3]).contains(function(row) { return r.row.eq(2) }).run(connection).then(function(result) {
           assert.equal(result, true);
           done();
        }).error(done);
    });
    it('contains - function - false', function(done) {
        r.expr([1,2,3]).contains(function(row) { return r.row.eq(4) }).run(connection).then(function(result) {
           assert.equal(result, false);
           done();
        }).error(done);
    });


    after(function() {
        connection.close();
    });
});
