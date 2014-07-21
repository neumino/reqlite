var config = require(__dirname+'/../config.js');
var r = require('rethinkdb');
var assert = require('assert');
var nothing = require(__dirname+'/../lib/index.js');


var connection;

var michel = {id: 1, age: 27, name: "Michel", coins: [1, 2, 5, 10], human: true};
var laurent = {id: 2, age: 29, name: "Laurent", coins: [2, 5], human: true};
var sophie = {id: 3, age: 22, name: "Sophie", coins: [1, 2, 10], human: true};
var lucky = {id: 4, age: 11, name: "Lucky", coins: []};


describe('join.js', function(){
    before(function(done) {
        setTimeout(function() { // Delay for nodemon to restart the server
            r.connect(config, function(err, conn) {
                connection = conn;

                r.dbCreate("reqlitetest").run(connection, function() {
                    // Ignore the error
                    r.db("reqlitetest").tableDrop("transformation").run(connection, function(err, result) {
                        r.db("reqlitetest").tableCreate("transformation").run(connection, function(err, result) {
                            r.db("reqlitetest").table("transformation").insert([michel, laurent, sophie, lucky])
                                .run(connection, function(err, result) {

                                assert.equal(result.inserted, 4);
                                r.db("reqlitetest").table("transformation").indexCreate("age").run(connection, function(err, result) {
                                r.db("reqlitetest").table("transformation").indexCreate("coins").run(connection, function(err, result) {
                                    r.db("reqlitetest").table("transformation").indexWait("age", "coins").run(connection, function(err, result) {
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

    it('map - table', function(done) {
        r.table("transformation").map(r.row("age")).run(connection).then(function(result) {
            result.sort(function(left, right) { return left-right });
            assert.deepEqual(result, [11, 22, 27, 29,]);
            done();
        }).error(done);
    });
    it('map - sequence', function(done) {
        r.expr([michel, sophie, lucky, laurent]).map(r.row("age")).run(connection).then(function(result) {
            result.sort(function(left, right) { return left-right });
            assert.deepEqual(result, [11, 22, 27, 29,]);
            done();
        }).error(done);
    });
    it('with fields - table', function(done) {
        r.table("transformation").withFields("age", "human").run(connection).then(function(result) {
            result.sort(function(left, right) { return left.age-right.age });
            assert.deepEqual(result, [{human: true, age: 22}, {human: true, age: 27}, {human: true, age: 29}]);
            done();
        }).error(done);
    });
    it('with fields - sequence', function(done) {
        r.expr([michel, sophie, lucky, laurent]).withFields("age", "human").run(connection).then(function(result) {
            result.sort(function(left, right) { return left.age-right.age });
            assert.deepEqual(result, [{human: true, age: 22}, {human: true, age: 27}, {human: true, age: 29}]);
            done();
        }).error(done);
    });
    it('concatMap', function(done) {
        r.table("transformation").concatMap(r.row("coins")).run(connection).then(function(result) {
            result.sort(function(left, right) { return left-right });
            assert.deepEqual(result, [1,1,2,2,2,5,5,10,10]);
            done();
        }).error(done);
    });
    it('orderBy - table - string', function(done) {
        r.table("transformation").orderBy("age").run(connection).then(function(result) {
            assert.deepEqual(result, [lucky, sophie, michel, laurent]);
            done();
        }).error(done);
    });
    it('orderBy - table - r.asc - string', function(done) {
        r.table("transformation").orderBy(r.asc("age")).run(connection).then(function(result) {
            assert.deepEqual(result, [lucky, sophie, michel, laurent]);
            done();
        }).error(done);
    });
    it('orderBy - table - r.desc - string', function(done) {
        r.table("transformation").orderBy(r.desc("age")).run(connection).then(function(result) {
            assert.deepEqual(result, [laurent, michel, sophie, lucky]);
            done();
        }).error(done);
    });
    it('orderBy - table - index', function(done) {
        r.table("transformation").orderBy({index: "age"}).run(connection).then(function(result) {
            assert.deepEqual(result, [lucky, sophie, michel, laurent]);
            done();
        }).error(done);
    });
    it('orderBy - table - index - asc', function(done) {
        r.table("transformation").orderBy({index: r.asc("age")}).run(connection).then(function(result) {
            assert.deepEqual(result, [lucky, sophie, michel, laurent]);
            done();
        }).error(done);
    });
    it('orderBy - table - index - desc', function(done) {
        r.table("transformation").orderBy({index: r.desc("age")}).run(connection).then(function(result) {
            assert.deepEqual(result, [laurent, michel, sophie, lucky]);
            done();
        }).error(done);
    });
    it('orderBy - table - r.row', function(done) {
        r.table("transformation").orderBy(r.row("age")).run(connection).then(function(result) {
            assert.deepEqual(result, [lucky, sophie, michel, laurent]);
            done();
        }).error(done);
    });
    it('orderBy - table - function', function(done) {
        r.table("transformation").orderBy(function(doc) { return doc("age") }).run(connection).then(function(result) {
            assert.deepEqual(result, [lucky, sophie, michel, laurent]);
            done();
        }).error(done);
    });

    it('skip - table', function(done) {
        r.table("transformation").skip(2).run(connection).then(function(result) {
            assert.deepEqual(result.length, 2);
            done();
        }).error(done);
    });
    it('skip - sequence', function(done) {
        r.expr([michel, sophie, lucky, laurent]).skip(2).run(connection).then(function(result) {
            assert.deepEqual(result, [lucky, laurent]);
            done();
        }).error(done);
    });
    it('limit - table', function(done) {
        r.table("transformation").limit(2).run(connection).then(function(result) {
            assert.deepEqual(result.length, 2);
            done();
        }).error(done);
    });
    it('limit - sequence', function(done) {
        r.expr([michel, sophie, lucky, laurent]).limit(2).run(connection).then(function(result) {
            assert.deepEqual(result, [michel, sophie]);
            done();
        }).error(done);
    });
    it('slice - both argument', function(done) {
        r.expr([10, 11, 12, 13, 14, 15]).slice(2, 4).run(connection).then(function(result) {
            assert.deepEqual(result, [12, 13]);
            done();
        }).error(done);
    });
    it('slice - both argument - rightBound closed', function(done) {
        r.expr([10, 11, 12, 13, 14, 15]).slice(2, 4, {rightBound: "closed"}).run(connection).then(function(result) {
            assert.deepEqual(result, [12, 13, 14]);
            done();
        }).error(done);
    });
    it('slice - both argument - leftBound open', function(done) {
        r.expr([10, 11, 12, 13, 14, 15]).slice(2, 4, {leftBound: "open"}).run(connection).then(function(result) {
            assert.deepEqual(result, [13]);
            done();
        }).error(done);
    });
    it('slice - start only', function(done) {
        r.expr([10, 11, 12, 13, 14, 15]).slice(2).run(connection).then(function(result) {
            assert.deepEqual(result, [12, 13, 14, 15]);
            done();
        }).error(done);
    });
    it('slice - start only - leftBound open', function(done) {
        r.expr([10, 11, 12, 13, 14, 15]).slice(2,{leftBound: "open"}).run(connection).then(function(result) {
            assert.deepEqual(result, [13, 14, 15]);
            done();
        }).error(done);
    });
    it('slice - start only - rightBound open', function(done) {
        r.expr([10, 11, 12, 13, 14, 15]).slice(2,{rightBound: "open"}).run(connection).then(function(result) {
            assert.deepEqual(result, [12, 13, 14, 15]);
            done();
        }).error(done);
    });
    it('slice - start only - rightBound open', function(done) {
        r.expr([10, 11, 12, 13, 14, 15]).slice(2,{rightBound: "closed"}).run(connection).then(function(result) {
            assert.deepEqual(result, [12, 13, 14, 15]);
            done();
        }).error(done);
    });
    it('nth', function(done) {
        r.expr([10, 11, 12, 13, 14, 15]).nth(2).run(connection).then(function(result) {
            assert.deepEqual(result, 12);
            done();
        }).error(done);
    });
    it('nth - negative', function(done) {
        r.expr([10, 11, 12, 13, 14, 15]).nth(-2).run(connection).then(function(result) {
            assert.deepEqual(result, 14);
            done();
        }).error(done);
    });
    it('nth - out of bound', function(done) {
        r.expr([10, 11, 12, 13, 14, 15]).nth(200).run(connection).then(function(result) {
            done(new Error("Was expecting an error"));
        }).error(function(err) {
            assert(err.message.match(/^Index out of bounds/))
            done();
        });
    });
    it('indexesOf', function(done) {
        r.expr([10, 11, 12, 13, 14, 15, 12]).indexesOf(12).run(connection).then(function(result) {
            assert.deepEqual(result, [2,6]);
            done();
        }).error(done);
    });
    it('indexesOf - no result', function(done) {
        r.expr([10, 11, 12, 13, 14, 15, 12]).indexesOf(1200).run(connection).then(function(result) {
            assert.deepEqual(result, []);
            done();
        }).error(done);
    });
    it('indexesOf - function', function(done) {
        r.expr([10, 11, 12, 13, 14, 15, 12]).indexesOf(r.row.eq(12)).run(connection).then(function(result) {
            assert.deepEqual(result, [2, 6]);
            done();
        }).error(done);
    });
    it('isEmpty - false', function(done) {
        r.expr([10, 11, 12, 13, 14, 15, 12]).isEmpty().run(connection).then(function(result) {
            assert.deepEqual(result, false);
            done();
        }).error(done);
    });
    it('isEmpty - true', function(done) {
        r.expr([]).isEmpty().run(connection).then(function(result) {
            assert.deepEqual(result, true);
            done();
        }).error(done);
    });
    it('union', function(done) {
        r.expr([10,11,12]).union([13,14,15]).run(connection).then(function(result) {
            assert.deepEqual(result, [10,11,12,13,14,15]);
            done();
        }).error(done);
    });
    it('sample', function(done) {
        r.expr([10, 11, 12, 13, 14, 15, 12]).sample(2).run(connection).then(function(result) {
            assert.deepEqual(result.length, 2);
            done();
        }).error(done);
    });
















    after(function() {
        connection.close();
    });
});
