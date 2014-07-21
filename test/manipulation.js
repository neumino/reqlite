var config = require(__dirname+'/../config.js');
var r = require('rethinkdb');
var assert = require('assert');
var nothing = require(__dirname+'/../lib/index.js');


var connection;

var michel = {id: 1, age: 27, name: "Michel", coins: [1, 2, 5, 10]};
var laurent = {id: 2, age: 29, name: "Laurent", coins: [2, 5]};
var sophie = {id: 3, age: 22, name: "Sophie", coins: [1, 2, 10]};
var lucky = {id: 4, age: 11, name: "Lucky", coins: []};

describe('manipulation.js', function(){
    before(function(done) {
        setTimeout(function() { // Delay for nodemon to restart the server
            r.connect(config, function(err, conn) {
                connection = conn;

                r.dbCreate("reqlitetest").run(connection, function() {
                    // Ignore the error
                    r.db("reqlitetest").tableDrop("manipulation").run(connection, function(err, result) {
                        r.db("reqlitetest").tableCreate("manipulation").run(connection, function(err, result) {
                            r.db("reqlitetest").table("manipulation").insert([michel, laurent, sophie, lucky])
                                .run(connection, function(err, result) {

                                assert.equal(result.inserted, 4);
                                r.db("reqlitetest").table("manipulation").indexCreate("age").run(connection, function(err, result) {
                                    r.db("reqlitetest").table("manipulation").indexCreate("coins", function(doc) { return doc("coins") }, {multi: true}).run(connection, function(err, result) {
                                        r.db("reqlitetest").table("manipulation").indexWait("age", "coins").run(connection, function(err, result) {
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

    it('r.row', function(done) {
        r.table("manipulation").filter(r.row("id").eq(1)).run(connection).then(function(result) {
            assert.deepEqual(result, [michel]);
            done();
        }).error(done);
    });

    it('pluck - object', function(done) {
        r.expr(michel).pluck("age", "name").run(connection).then(function(result) {
            assert.deepEqual(result, {name: "Michel", age: 27});
            done();
        }).error(done);
    });
    it('pluck - sequence', function(done) {
        r.expr([michel, laurent]).pluck("age", "name").run(connection).then(function(result) {
            assert.deepEqual(result, [{name: "Michel", age: 27}, {name: "Laurent", age: 29}]);
            done();
        }).error(done);
    });

    it('without - object', function(done) {
        r.expr(michel).without("id", "coins").run(connection).then(function(result) {
            assert.deepEqual(result, {name: "Michel", age: 27});
            done();
        }).error(done);
    });
    it('without - sequence', function(done) {
        r.expr([michel, laurent]).without("id", "coins").run(connection).then(function(result) {
            assert.deepEqual(result, [{name: "Michel", age: 27}, {name: "Laurent", age: 29}]);
            done();
        }).error(done);
    });

    it('merge - object', function(done) {
        r.expr({a: 1, b: 2}).merge({c: 3, b: 22}).run(connection).then(function(result) {
            assert.deepEqual(result, {a: 1, b: 22, c: 3});
            done();
        }).error(done);
    });

    it('merge - sequence', function(done) {
        r.expr([{a: 1, b: 2}, {a: 11, b: 32}]).merge({c: 3, b: 22}).run(connection).then(function(result) {
            assert.deepEqual(result, [{a: 1, b: 22, c: 3}, {a: 11, b: 22, c: 3}]);
            done();
        }).error(done);
    });

    it('append', function(done) {
        r.expr([1,2,3,4]).append(10).run(connection).then(function(result) {
            assert.deepEqual(result, [1,2,3,4,10]);
            done();
        }).error(done);
    });

    it('prepend', function(done) {
        r.expr([1,2,3,4]).prepend(10).run(connection).then(function(result) {
            assert.deepEqual(result, [10,1,2,3,4]);
            done();
        }).error(done);
    });
    //TODO difference, setInsert, setIntersection, setDifference, ..., changeAt

    it('keys', function(done) {
        r.expr({a: 1, b: 2, c: {d: 4}}).keys().run(connection).then(function(result) {
            assert.deepEqual(result, ['a', 'b', 'c']);
            done();
        }).error(done);
    });


    after(function() {
        connection.close();
    });
});
