var config = require(__dirname+'/../config.js');
var r = require('rethinkdb');
var assert = require('assert');
var nothing = require(__dirname+'/../lib/index.js');


var connection;

var michel = {id: 1, age: 27, name: "Michel", coins: [1, 2, 5, 10]};
var laurent = {id: 2, age: 29, name: "Laurent", coins: [2, 5]};
var sophie = {id: 3, age: 22, name: "Sophie", coins: [1, 2, 10]};
var lucky = {id: 4, age: 11, name: "Lucky", coins: []};

describe('writing.js', function(){
    before(function(done) {
        setTimeout(function() { // Delay for nodemon to restart the server
            r.connect(config, function(err, conn) {
                connection = conn;

                r.dbCreate("reqlitetest").run(connection, function() {
                    // Ignore the error
                    r.db("reqlitetest").tableDrop("writing").run(connection, function(err, result) {
                        r.db("reqlitetest").tableCreate("writing").run(connection, function(err, result) {
                            done();
                        });
                    });
                });
            });
        }, 100)
    });

    it('insert', function(done) {
        r.table("writing").insert({
            id: 1,
            name: "Michel"
        }).run(connection).then(function(result) {
            assert.deepEqual(result, {
                deleted: 0,
                errors: 0,
                inserted: 1,
                replaced: 0,
                skipped: 0,
                unchanged: 0
            })

            return r.table("writing").get(1).run(connection);
        }).then(function(result) {
            assert.deepEqual(result, {id: 1, name: "Michel"});
            done();
        }).error(done);
    });

    it('insert - returnVals', function(done) {
        r.table("writing").insert({
            id: 2,
            name: "Marc"
        }, {returnVals: true}).run(connection).then(function(result) {
            assert.deepEqual(result, {
                deleted: 0,
                errors: 0,
                inserted: 1,
                replaced: 0,
                skipped: 0,
                unchanged: 0,
                old_val: null,
                new_val: {
                    id: 2,
                    name: "Marc"
                }
            });
            return r.table("writing").get(2).run(connection);
        }).then(function(result) {
            assert.deepEqual(result, {id: 2, name: "Marc"});
            done();
        }).error(done);
    });

    it('insert - generate primary key', function(done) {
        var pk;

        r.table("writing").insert({
            name: "Slava"
        }, {returnVals: true}).run(connection).then(function(result) {
            assert.equal(result.inserted, 1);
            assert.equal(result.generated_keys.length, 1);
            assert.equal(typeof result.generated_keys[0], 'string');
            pk = result.generated_keys[0];
            return r.table("writing").get(pk).run(connection);
        }).then(function(result) {
            assert.deepEqual(result, {id: pk, name: "Slava"});
            done();
        }).error(done);

    });


    it('insert - batch', function(done) {
        r.table("writing").insert([{id: 3, name: "Daniel"}, {id: 4, name: "Josh"}]).run(connection).then(function(result) {
            assert.equal(result.inserted, 2);
            return r.table("writing").getAll(3, 4).run(connection);
        }).then(function(result) {
            assert.equal(result.length, 2);
            result.sort(function(left, right) { return left.id -right.id });
            assert.deepEqual(result[0], {id: 3, name: "Daniel"});
            assert.deepEqual(result[1], {id: 4, name: "Josh"});
            done();
        }).error(done);
    });

    it('delete - point delete', function(done) {
        var query = r.table("writing").get(1).delete();

        query.run(connection).then(function(result) {
            assert.equal(result.deleted, 1);
            return r.table("writing").get(1).run(connection);
        }).then(function(result) {
            assert.equal(result, null);
            done();
        }).error(done);
    });

    it('delete - range', function(done) {
        r.table("writing").delete().run(connection).then(function(result) {
            assert(result.deleted > 0);
            return r.table("writing").run(connection);
        }).then(function(result) {
            assert.equal(result.length, 0);
            done();
        }).error(done);
    });

    it('update - point update', function(done) {
        r.table("writing").insert({id: 1, name: "Michel"}).run(connection).then(function(result) {
            assert.equal(result.inserted, 1);
            return r.table("writing").get(1).update({name: "Slava", foo: "bar"}).run(connection);
        }).then(function(result) {
            assert.equal(result.replaced, 1);
            return r.table("writing").get(1).run(connection);
        }).then(function(result) {
            assert.deepEqual(result, {id: 1, name: "Slava", foo: "bar"});
            done();
        }).error(done);
    });

    it('update - changing the primary key should fail', function(done) {
        r.table("writing").insert({id: 2, name: "Marc"}).run(connection).then(function(result) {
            assert.equal(result.inserted, 1);
            return r.table("writing").get(2).update({id: 3}).run(connection);
        }).then(function(result) {
            assert.equal(result.replaced, 0);
            assert.equal(result.inserted, 0);
            assert.equal(typeof result.first_error, 'string');
            done();
        }).error(done);
    });

    it('replace - Point replace', function(done) {
        r.table("writing").insert({id: 4, name: "Michel"}).run(connection).then(function(result) {
            assert.equal(result.inserted, 1);
            return r.table("writing").get(4).replace({id: 4, foo: "bar"}).run(connection);
        }).then(function(result) {
            assert.equal(result.replaced, 1);
            return r.table("writing").get(4).run(connection);
        }).then(function(result) {
            assert.deepEqual(result, {id: 4, foo: "bar"});
            done();
        }).error(done);
    });

    it('replace - Changing the primary key should fail', function(done) {
        r.table("writing").insert({id: 5, name: "Marc"}).run(connection).then(function(result) {
            assert.equal(result.inserted, 1);
            return r.table("writing").get(5).update({id: 6}).run(connection);
        }).then(function(result) {
            assert.equal(result.replaced, 0);
            assert.equal(result.inserted, 0);
            assert.equal(typeof result.first_error, 'string');
            done();
        }).error(done);
    });

    it('delete - point delete', function(done) {
        r.table("writing").get(5).delete().run(connection).then(function(result) {
            assert.equal(result.deleted, 1);
            return r.table("writing").get(5).run(connection);
        }).then(function(result) {
            assert.equal(result, null);
            done();
        }).error(done);
    });
    it('delete - range delete', function(done) {
        r.table("writing").delete().run(connection).then(function(result) {
            assert(result.deleted > 0);
            return r.table("writing").run(connection);
        }).then(function(result) {
            assert.equal(result.length, 0);
            done();
        }).error(done);
    });
    it('sync', function(done) {
        r.table("writing").sync().run(connection).then(function(result) {
            assert.deepEqual(result, {synced: 1});
            done();
        }).error(done);
    });


    after(function() {
        connection.close();
    });
});
