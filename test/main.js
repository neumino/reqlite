var config = require(__dirname+'/../config.js');
var r = require('rethinkdb');
var assert = require('assert');
var nothing = require(__dirname+'/../lib/index.js');


var connection;

// Test for the first version of ReQLite

describe('Tests for the first version of ReQLite', function(){
    before(function(done) {
        setTimeout(function() { // Delay for nodemon to restart the server
            r.connect(config).then(function(conn) {
                connection = conn;
                r.dbDrop("reqlite").run(connection).error(function() {
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
        r.dbCreate("reqlite").run(connection).then(function(result) {
            assert.deepEqual(result, {created: 1})
            return r.dbList().run(connection);
        }).then(function(result) {
            assert.notEqual(result.indexOf("reqlite"), -1)
            done();
        }).error(done);
    });

    it('Drop a database', function(done) {
        r.dbDrop("reqlite").run(connection).then(function(result) {
            assert.deepEqual(result, {dropped: 1})
            return r.dbList().run(connection);
        }).then(function(result) {
            assert.equal(result.indexOf("reqlite"), -1);
            done();
        }).error(done);
    });

    it('Recreate the test (`reqlite`) database', function(done) {
        r.dbCreate("reqlite").run(connection).then(function(result) {
            assert.deepEqual(result, {created: 1})
            done();
        }).error(done);
    });

    it('List tables', function(done) {
        r.db("reqlite").tableList().run(connection).then(function(result) {
            assert.equal(result.indexOf("reqlite"), -1)
            done();
        }).error(done);
    });

    it('Create a table', function(done) {
        r.db("reqlite").tableCreate("test").run(connection).then(function(result) {
            assert.deepEqual(result, {created: 1})
            return r.db("reqlite").tableList().run(connection);
        }).then(function(result) {
            assert.notEqual(result.indexOf("test"), -1);
            done();
        }).error(done);
    });

    it('Drop a table', function(done) {
        r.db("reqlite").tableDrop("test").run(connection).then(function(result) {
            assert.deepEqual(result, {dropped: 1})
            return r.db("reqlite").tableList().run(connection);
        }).then(function(result) {
            assert.deepEqual(result, [])
            done();
        }).error(done);
    });

    it('ReCreate the test `test` table', function(done) {
        r.db("reqlite").tableCreate("test").run(connection).then(function(result) {
            assert.deepEqual(result, {created: 1})
            done();
        }).error(done);
    });

    it('Insert', function(done) {
        r.db("reqlite").table("test").insert({
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

            return r.db("reqlite").table("test").get(1).run(connection);
        }).then(function(result) {
            assert.deepEqual(result, {id: 1, name: "Michel"});
            done();
        }).error(done);
    });

    it('Insert - returnVals', function(done) {
        r.db("reqlite").table("test").insert({
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
            return r.db("reqlite").table("test").get(2).run(connection);
        }).then(function(result) {
            assert.deepEqual(result, {id: 2, name: "Marc"});
            done();
        }).error(done);
    });

    it('Insert - generate primary key', function(done) {
        var pk;

        r.db("reqlite").table("test").insert({
            name: "Slava"
        }, {returnVals: true}).run(connection).then(function(result) {
            assert.equal(result.inserted, 1);
            assert.equal(result.generated_keys.length, 1);
            assert.equal(typeof result.generated_keys[0], 'string');
            pk = result.generated_keys[0];
            return r.db("reqlite").table("test").get(pk).run(connection);
        }).then(function(result) {
            assert.deepEqual(result, {id: pk, name: "Slava"});
            done();
        }).error(done);

    });


    it('Insert - Batch', function(done) {
        r.db("reqlite").table("test").insert([{id: 3, name: "Daniel"}, {id: 4, name: "Josh"}]).run(connection).then(function(result) {
            assert.equal(result.inserted, 2);
            return r.db("reqlite").table("test").getAll(3, 4).run(connection);
        }).then(function(result) {
            assert.equal(result.length, 2);
            result.sort(function(left, right) { return left.id -right.id });
            assert.deepEqual(result[0], {id: 3, name: "Daniel"});
            assert.deepEqual(result[1], {id: 4, name: "Josh"});
            done();
        }).error(done);
    });

    it('Delete - Point delete', function(done) {
        var query = r.db("reqlite").table("test").get(1).delete();

        query.run(connection).then(function(result) {
            assert.equal(result.deleted, 1);
            return r.db("reqlite").table("test").get(1).run(connection);
        }).then(function(result) {
            assert.equal(result, null);
            done();
        }).error(done);
    });

    it('Delete - Range', function(done) {
        r.db("reqlite").table("test").delete().run(connection).then(function(result) {
            assert(result.deleted > 0);
            return r.db("reqlite").table("test").run(connection);
        }).then(function(result) {
            assert.equal(result.length, 0);
            done();
        }).error(done);
    });

    it('Update - Point update', function(done) {
        r.db("reqlite").table("test").insert({id: 1, name: "Michel"}).run(connection).then(function(result) {
            assert.equal(result.inserted, 1);
            return r.db("reqlite").table("test").get(1).update({name: "Slava", foo: "bar"}).run(connection);
        }).then(function(result) {
            assert.equal(result.replaced, 1);
            return r.db("reqlite").table("test").get(1).run(connection);
        }).then(function(result) {
            assert.deepEqual(result, {id: 1, name: "Slava", foo: "bar"});
            done();
        }).error(done);
    });

    it('Update - Changing the primary key should fail', function(done) {
        r.db("reqlite").table("test").insert({id: 2, name: "Marc"}).run(connection).then(function(result) {
            assert.equal(result.inserted, 1);
            return r.db("reqlite").table("test").get(2).update({id: 3}).run(connection);
        }).then(function(result) {
            assert.equal(result.replaced, 0);
            assert.equal(result.inserted, 0);
            assert.equal(typeof result.first_error, 'string');
            done();
        }).error(done);
    });

    it('Replace - Point replace', function(done) {
        r.db("reqlite").table("test").insert({id: 4, name: "Michel"}).run(connection).then(function(result) {
            assert.equal(result.inserted, 1);
            return r.db("reqlite").table("test").get(4).replace({id: 4, foo: "bar"}).run(connection);
        }).then(function(result) {
            assert.equal(result.replaced, 1);
            return r.db("reqlite").table("test").get(4).run(connection);
        }).then(function(result) {
            assert.deepEqual(result, {id: 4, foo: "bar"});
            done();
        }).error(done);
    });

    it('Replace - Changing the primary key should fail', function(done) {
        r.db("reqlite").table("test").insert({id: 5, name: "Marc"}).run(connection).then(function(result) {
            assert.equal(result.inserted, 1);
            return r.db("reqlite").table("test").get(5).update({id: 6}).run(connection);
        }).then(function(result) {
            assert.equal(result.replaced, 0);
            assert.equal(result.inserted, 0);
            assert.equal(typeof result.first_error, 'string');
            done();
        }).error(done);
    });

    it('Retrieve - table', function(done) {
        var query = r.db("reqlite").table("test").delete().run(connection).then(function(result) {
            return r.db("reqlite").table("test").run(connection);
        }).then(function(result) {
            assert.equal(result.length, 0);
            return r.db("reqlite").table("test").insert([{id: 1},{id: 0},{id: 4},{id: 3},{id: 2}]).run(connection)
        }).then(function(result) {
            assert.equal(result.inserted, 5);
            return r.db("reqlite").table("test").run(connection);
        }).then(function(result) {
            result.sort(function(left, right) { return left.id - right.id })
            for(var i=0; i<5; i++) {
                assert.deepEqual({id: i}, result[i]);
            }
            done();
        }).error(done);
    });

    it('Retrieve - get', function(done) {
        r.db("reqlite").table("test").get(4).run(connection).then(function(result) {
            assert.deepEqual({id: 4}, result);
            done();
        }).error(done);
    });
    it('Retrieve - getAll - primary key', function(done) {
        r.db("reqlite").table("test").getAll(1, 4).run(connection).then(function(result) {
            result.sort(function(left, right) { return left.id - right.id })
            assert.deepEqual({id: 1}, result[0]);
            assert.deepEqual({id: 4}, result[1]);
            done();
        }).error(done);
    });
    it('Retrieve - filter - object', function(done) {
        r.db("reqlite").table("test").filter({id: 2}).run(connection).then(function(result) {
            assert.deepEqual([{id: 2}], result);
            done();
        }).error(done);
    });
    it('Retrieve - filter - function', function(done) {
        r.db("reqlite").table("test").filter(function(doc) { return doc("id").eq(2) }).run(connection).then(function(result) {
            assert.deepEqual([{id: 2}], result);
            done();
        }).error(done);
    });

    it('OrderBy - field', function(done) {
        r.db("reqlite").table("test").orderBy('id').run(connection).then(function(result) {
            for(var i=0; i<5; i++) {
                assert.deepEqual({id: i}, result[i]);
            }
            done();
        }).error(done);
    });

    it('OrderBy - field - asc', function(done) {
        r.db("reqlite").table("test").orderBy(r.asc('id')).run(connection).then(function(result) {
            for(var i=0; i<5; i++) {
                assert.deepEqual({id: i}, result[i]);
            }
            done();
        }).error(done);
    });
    it('OrderBy - field - desc', function(done) {
        r.db("reqlite").table("test").orderBy(r.desc('id')).run(connection).then(function(result) {
            for(var i=0; i<5; i++) {
                assert.deepEqual({id: 4-i}, result[i]);
            }
            done();
        }).error(done);
    });
    it('OrderBy - function', function(done) {
        r.db("reqlite").table("test").orderBy(function(doc) { return doc('id').mul(-1) }).run(connection).then(function(result) {
            for(var i=0; i<5; i++) {
                assert.deepEqual({id: 4-i}, result[i]);
            }
            done();
        }).error(done);
    });
    it('OrderBy - function - desc', function(done) {
        r.db("reqlite").table("test").orderBy(r.desc(function(doc) { return doc('id').mul(-1) })).run(connection).then(function(result) {
            for(var i=0; i<5; i++) {
                assert.deepEqual({id: i}, result[i]);
            }
            done();
        }).error(done);
    });
});
