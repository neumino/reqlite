var config = require(__dirname+'/../config.js');
var r = require('rethinkdb');
var assert = require('assert');
var nothing = require(__dirname+'/../lib/index.js');


var connection;

var michel = {id: 1, age: 27, name: "Michel", link: 2};
var laurent = {id: 2, age: 29, name: "Laurent", link: 3};
var sophie = {id: 3, age: 22, name: "Sophie", link: 1};
var lucky = {id: 4, age: 11, name: "Lucky", link: 3};

describe('join.js', function(){
    before(function(done) {
        setTimeout(function() { // Delay for nodemon to restart the server
            r.connect(config, function(err, conn) {
                connection = conn;

                r.dbCreate("reqlitetest").run(connection, function() {
                    // Ignore the error
                    r.db("reqlitetest").tableDrop("join").run(connection, function(err, result) {
                        r.db("reqlitetest").tableCreate("join").run(connection, function(err, result) {
                            r.db("reqlitetest").table("join").insert([michel, laurent, sophie, lucky])
                                .run(connection, function(err, result) {

                                assert.equal(result.inserted, 4);
                                r.db("reqlitetest").table("join").indexCreate("link").run(connection, function(err, result) {
                                    r.db("reqlitetest").table("join").indexWait("link").run(connection, function(err, result) {
                                        done();
                                    });
                                });
                            });
                        });
                    });
                });
            });
        }, 100)
    });

    it('inner - false', function(done) {
        r.table("join").innerJoin(r.table("join"), function() { return false}).run(connection).then(function(result) {
            assert.deepEqual(result, []);
            done();
        }).error(done);
    });
    it('inner - self', function(done) {
        r.table("join").innerJoin(r.table("join"), function(left, right) { return left("id").eq(right("id"))}).run(connection).then(function(result) {
            result.sort(function(left, right) { return left.left.age-right.left.age });
            assert.deepEqual(result, [
                {left: lucky, right: lucky},
                {left: sophie, right: sophie},
                {left: michel, right: michel},
                {left: laurent, right: laurent}
                ]);
            done();
        }).error(done);
    });
    it('inner - true', function(done) {
        r.table("join").innerJoin(r.table("join"), function() { return true}).run(connection).then(function(result) {
            result.sort(function(left, right) {
                if (left.left.age-right.left.age === 0) {
                    return left.right.age-right.right.age;
                }
                else {
                    return left.left.age-right.left.age;
                }
            });
            assert.deepEqual(result, [
                {left: lucky, right: lucky},{left: lucky, right: sophie},{left: lucky, right: michel},{left: lucky, right: laurent},
                {left: sophie, right: lucky},{left: sophie, right: sophie},{left: sophie, right: michel},{left: sophie, right: laurent},
                {left: michel, right: lucky},{left: michel, right: sophie},{left: michel, right: michel},{left: michel, right: laurent},
                {left: laurent, right: lucky},{left: laurent, right: sophie},{left: laurent, right: michel},{left: laurent, right: laurent},
                ]);

            done();
        }).error(done);
    });


    it('outer - false', function(done) {
        r.table("join").outerJoin(r.table("join"), function() { return false}).run(connection).then(function(result) {
            result.sort(function(left, right) { return left.left.age-right.left.age });
            assert.deepEqual(result, [
                {left: lucky},
                {left: sophie},
                {left: michel},
                {left: laurent}
                ]);
            done();

        }).error(done);
    });
    it('outer - self', function(done) {
        r.table("join").innerJoin(r.table("join"), function(left, right) { return left("id").eq(right("id"))}).run(connection).then(function(result) {
            result.sort(function(left, right) { return left.left.age-right.left.age });
            assert.deepEqual(result, [
                {left: lucky, right: lucky},
                {left: sophie, right: sophie},
                {left: michel, right: michel},
                {left: laurent, right: laurent}
                ]);
            done();
        }).error(done);
    });

    it('outer - true', function(done) {
        r.table("join").outerJoin(r.table("join"), function() { return true}).run(connection).then(function(result) {
            result.sort(function(left, right) {
                if (left.left.age-right.left.age === 0) {
                    return left.right.age-right.right.age;
                }
                else {
                    return left.left.age-right.left.age;
                }
            });
            assert.deepEqual(result, [
                {left: lucky, right: lucky},{left: lucky, right: sophie},{left: lucky, right: michel},{left: lucky, right: laurent},
                {left: sophie, right: lucky},{left: sophie, right: sophie},{left: sophie, right: michel},{left: sophie, right: laurent},
                {left: michel, right: lucky},{left: michel, right: sophie},{left: michel, right: michel},{left: michel, right: laurent},
                {left: laurent, right: lucky},{left: laurent, right: sophie},{left: laurent, right: michel},{left: laurent, right: laurent},
                ]);

            done();
        }).error(done);
    });

    it('eqjoin - self - pk', function(done) {
        r.table("join").eqJoin("id", r.table("join")).run(connection).then(function(result) {
            result.sort(function(left, right) { return left.left.age-right.left.age });
            assert.deepEqual(result, [
                {left: lucky, right: lucky},
                {left: sophie, right: sophie},
                {left: michel, right: michel},
                {left: laurent, right: laurent}
                ]);
            done();
        }).error(done);
    });
    it('eqjoin - self - secondary', function(done) {
        r.table("join").eqJoin("id", r.table("join"), {index: "link"}).run(connection).then(function(result) {
            result.sort(function(left, right) {
                if (left.left.age-right.left.age === 0) {
                    return left.right.age-right.right.age;
                }
                else {
                    return left.left.age-right.left.age;
                }
            });
            assert.deepEqual(result, [
                {left: sophie, right: lucky},
                {left: sophie, right: laurent},
                {left: michel, right: sophie},
                {left: laurent, right: michel}
                ]);
            done();
        }).error(done);
    });
    it('zip', function(done) {
        r.table("join").eqJoin("id", r.table("join"), {index: "link"})
            .map(function(doc) {
                return {
                    left: doc("left"),
                    right: doc("right").without("id")
                }
            }).zip().run(connection).then(function(result) {

            result.sort(function(left, right) {
                if (left.id === right.id) {
                    return left.age-right.age
                }
                else {
                    return left.id-right.id
                }
            });
            assert.deepEqual(result, [
                {id: 1, age: 22, name: "Sophie", link: 1},
                {id: 2, age: 27, name: "Michel", link: 2},
                {id: 3, age: 11, name: "Lucky", link: 3},
                {id: 3, age: 29, name: "Laurent", link: 3}
                ]);
            done();
        }).error(done);
    });


    after(function() {
        connection.close();
    });
});
