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

    it('difference', function(done) {
        r.expr([10,10,11,12]).difference([10, 13]).run(connection).then(function(result) {
            result.sort(function(left, right) { return left-right });
            assert.deepEqual(result, [11, 12]);
            done();
        }).error(done);
    });
    it('setInsert', function(done) {
        r.expr([10,11,12,11]).setInsert(13).run(connection).then(function(result) {
            result.sort(function(left, right) { return left-right });
            assert.deepEqual(result, [10, 11, 12, 13]);
            done();
        }).error(done);
    });
    it('setUnion - 1', function(done) {
        r.expr([10,11,12,11]).setUnion([14]).run(connection).then(function(result) {
            result.sort(function(left, right) { return left-right });
            assert.deepEqual(result, [10, 11, 12, 14]);
            done();
        }).error(done);
    });
    it('setUnion - 2', function(done) {
        r.expr([10,11,12,11]).setUnion([12, 14]).run(connection).then(function(result) {
            result.sort(function(left, right) { return left-right });
            assert.deepEqual(result, [10, 11, 12, 14]);
            done();
        }).error(done);
    });
    it('setIntersection', function(done) {
        r.expr([10,11,12,11]).setIntersection([12, 14]).run(connection).then(function(result) {
            result.sort(function(left, right) { return left-right });
            assert.deepEqual(result, [12]);
            done();
        }).error(done);
    });
    it('setDifference', function(done) {
        r.expr([10,11,12,11]).setDifference([12, 14]).run(connection).then(function(result) {
            result.sort(function(left, right) { return left-right });
            assert.deepEqual(result, [10, 11]);
            done();
        }).error(done);
    });
    it('hasFields - object - 1', function(done) {
        r.expr({a: 1}).hasFields('a').run(connection).then(function(result) {
            assert.equal(result, true);
            done();
        }).error(done);
    });
    it('hasFields - object - 2', function(done) {
        r.expr({a: 1}).hasFields('a', 'b').run(connection).then(function(result) {
            assert.equal(result, false);
            done();
        }).error(done);
    });
    it('hasFields - object - 3', function(done) {
        r.expr({a: 1, b: 2}).hasFields('a', 'b').run(connection).then(function(result) {
            assert.equal(result, true);
            done();
        }).error(done);
    });
    it('hasFields - object - 4', function(done) {
        r.expr({a: 1, b: 2}).hasFields('a', 'b', 'a').run(connection).then(function(result) {
            assert.equal(result, true);
            done();
        }).error(done);
    });
    it('hasFields - sequence - 1', function(done) {
        r.expr([{a: 1}, {b: 2}, {a: 3}]).hasFields('a').run(connection).then(function(result) {
            assert.deepEqual(result, [{a: 1}, {a: 3}]);
            done();
        }).error(done);
    });
    it('hasFields - sequence - 1', function(done) {
        r.expr([{a: 1}, {b: 2}, {a: 3}]).hasFields('a', 'b').run(connection).then(function(result) {
            assert.deepEqual(result, []);
            done();
        }).error(done);
    });

    it('insertAt - 1', function(done) {
        r.expr([10, 11, 12, 13]).insertAt(0, 20).run(connection).then(function(result) {
            assert.deepEqual(result, [20,10,11,12,13]);
            done();
        }).error(done);
    });
    it('insertAt - 2', function(done) {
        r.expr([10, 11, 12, 13]).insertAt(2, 20).run(connection).then(function(result) {
            assert.deepEqual(result, [10,11,20, 12,13]);
            done();
        }).error(done);
    });
    it('insertAt - 3', function(done) {
        r.expr([10, 11, 12, 13]).insertAt(4, 20).run(connection).then(function(result) {
            assert.deepEqual(result, [10,11,12,13,20]);
            done();
        }).error(done);
    });
    it('insertAt - 4', function(done) {
        r.expr([10, 11, 12, 13]).insertAt(90, 20).run(connection).then(function(result) {
            done(new Error("Was expecting an error"));
        }).error(function(err) {
            assert(err.message.match(/^Index `90` out of bounds for array of size: `4`/));
            done();
        });
    });
    it('spliceAt - 1', function(done) {
        r.expr([10, 11, 12, 13]).spliceAt(0, [20, 21]).run(connection).then(function(result) {
            assert.deepEqual(result, [20,21,10,11,12,13]);
            done();
        }).error(done);
    });
    it('spliceAt - 2', function(done) {
        r.expr([10, 11, 12, 13]).spliceAt(0, [20,10, 21]).run(connection).then(function(result) {
            assert.deepEqual(result, [20,10,21,10,11,12,13]);
            done();
        }).error(done);
    });
    it('spliceAt - 3', function(done) {
        r.expr([10, 11, 12, 13]).spliceAt(90, [20,10, 21]).run(connection).then(function(result) {
            done(new Error("Was expecting an error"));
        }).error(function(err) {
            assert(err.message.match(/^Index `90` out of bounds for array of size: `4`/));
            done();
        });

    });

    
    /*
    it('keys', function(done) {
        r.expr({foo:2, bar: 1, buzz: 12}).keys().run(connection).then(function(result) {
            result.sort();
            assert.deepEqual(result, ["bar", "buzz", "foo"]);
            done();
        }).error(done);
    });
    it('object', function(done) {
        r.object("a", 1, "b", 10).run(connection).then(function(result) {
            assert.deepEqual(result, {a: 1, b: 10});
            done();
        }).error(done);
    });
    it('object - odd number of arguments', function(done) {
        r.object("a", 1, "b").run(connection).then(function(result) {
            done(new Error("Was expecting an error"));
        }).error(function(err) {
            assert(err.message.match(/^OBJECT expects an even number of arguments \(but found 3/))
            done();
        });
    });
    it('object - non string key', function(done) {
        r.object(2, 1).run(connection).then(function(result) {
            done(new Error("Was expecting an error"));
        }).error(function(err) {
            assert(err.message.match(/^Expected type STRING but found NUMBER/))
            done();
        });
    });
    */

    after(function() {
        connection.close();
    });
});
