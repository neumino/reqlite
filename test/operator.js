var config = require(__dirname+'/../config.js');
var r = require('rethinkdb');
var assert = require('assert');
var nothing = require(__dirname+'/../lib/index.js');


var connection;


describe('Operators', function(){
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
    // array bool date null number string object

    it('Comparator 1', function(done) {
        r.expr([1,2,3]).lt([1,2]).run(connection).then(function(result) {
           assert.equal(result, false);
           done();
        }).error(done);
    });
    it('Comparator 2', function(done) {
        r.expr([1,2,3]).lt([1,2,3]).run(connection).then(function(result) {
           assert.equal(result, false);
           done();
        }).error(done);
    });
    it('Comparator 3', function(done) {
        r.expr([1,2,3]).lt([1,2,3,4]).run(connection).then(function(result) {
           assert.equal(result, true);
           done();
        }).error(done);
    });
    it('Comparator 4', function(done) {
        r.expr([1,2,3]).lt(true).run(connection).then(function(result) {
           assert.equal(result, true);
           done();
        }).error(done);
    });
    it('Comparator 5', function(done) {
        r.expr([1,2,3]).lt(r.now()).run(connection).then(function(result) {
           assert.equal(result, true);
           done();
        }).error(done);
    });
    it('Comparator 6', function(done) {
        r.expr([1,2,3]).lt(null).run(connection).then(function(result) {
           assert.equal(result, true);
           done();
        }).error(done);
    });
    it('Comparator 7', function(done) {
        r.expr([1,2,3]).lt(20).run(connection).then(function(result) {
           assert.equal(result, true);
           done();
        }).error(done);
    });
    it('Comparator 8', function(done) {
        r.expr([1,2,3]).lt("bar").run(connection).then(function(result) {
           assert.equal(result, true);
           done();
        }).error(done);
    });
    it('Comparator 9', function(done) {
        r.expr([1,2,3]).lt({foo: "bar"}).run(connection).then(function(result) {
           assert.equal(result, true);
           done();
        }).error(done);
    });
    it('Comparator 10', function(done) {
        r.expr(true).lt(false).run(connection).then(function(result) {
           assert.equal(result, false);
           done();
        }).error(done);
    });
    it('Comparator 11', function(done) {
        r.expr(false).lt(true).run(connection).then(function(result) {
           assert.equal(result, true);
           done();
        }).error(done);
    });
    it('Comparator 12', function(done) {
        r.expr(true).lt(r.now()).run(connection).then(function(result) {
           assert.equal(result, true);
           done();
        }).error(done);
    });
    it('Comparator 13', function(done) {
        r.expr(true).lt(10).run(connection).then(function(result) {
           assert.equal(result, true);
           done();
        }).error(done);
    });
    it('Comparator 14', function(done) {
        r.expr(true).lt('foo').run(connection).then(function(result) {
           assert.equal(result, true);
           done();
        }).error(done);
    });
    it('Comparator 15', function(done) {
        r.expr(true).lt({foo: "bar"}).run(connection).then(function(result) {
           assert.equal(result, true);
           done();
        }).error(done);
    });
    it('Comparator 16', function(done) {
        r.expr(r.now()).lt(r.now().add(60)).run(connection).then(function(result) {
           assert.equal(result, true);
           done();
        }).error(done);
    });
    it('Comparator 17', function(done) {
        r.expr(r.now()).lt(10).run(connection).then(function(result) {
           assert.equal(result, true);
           done();
        }).error(done);
    });
    it('Comparator 18', function(done) {
        r.expr(r.now()).lt('foo').run(connection).then(function(result) {
           assert.equal(result, true);
           done();
        }).error(done);
    });
    it('Comparator 19', function(done) {
        r.expr(r.now()).lt({foo: "bar"}).run(connection).then(function(result) {
           assert.equal(result, true);
           done();
        }).error(done);
    });
    it('Comparator 20', function(done) {
        r.expr(10).lt(11).run(connection).then(function(result) {
           assert.equal(result, true);
           done();
        }).error(done);
    });
    it('Comparator 21', function(done) {
        r.expr(10).lt(9).run(connection).then(function(result) {
           assert.equal(result, false);
           done();
        }).error(done);
    });
    it('Comparator 22', function(done) {
        r.expr(10).lt('foo').run(connection).then(function(result) {
           assert.equal(result, true);
           done();
        }).error(done);
    });
    it('Comparator 23', function(done) {
        r.expr(10).lt({foo: "bar"}).run(connection).then(function(result) {
           assert.equal(result, true);
           done();
        }).error(done);
    });
    it('Comparator 24', function(done) {
        r.expr('foo').lt('bar').run(connection).then(function(result) {
           assert.equal(result, false);
           done();
        }).error(done);
    });
    it('Comparator 25', function(done) {
        r.expr('aaa').lt('bar').run(connection).then(function(result) {
           assert.equal(result, true);
           done();
        }).error(done);
    });
    it('Comparator 26', function(done) {
        r.expr(10).lt({foo: "bar"}).run(connection).then(function(result) {
           assert.equal(result, true);
           done();
        }).error(done);
    });
    it('Comparator 27', function(done) {
        r.expr({b: 1}).lt({b: 1}).run(connection).then(function(result) {
           assert.equal(result, false);
           done();
        }).error(done);
    });
    it('Comparator 28', function(done) {
        r.expr({b: 1}).lt({a: 1}).run(connection).then(function(result) {
           assert.equal(result, false);
           done();
        }).error(done);
    });
    it('Comparator 29', function(done) {
        r.expr({b: 1}).lt({c: 1}).run(connection).then(function(result) {
           assert.equal(result, false);
           done();
        }).error(done);
    });

    it('Add - number', function(done) {
        r.expr(20).add(12).run(connection).then(function(result) {
           assert.equal(result, 32);
           done();
        }).error(done);
    });
    it('Add - string', function(done) {
        r.expr("foo").add("bar").run(connection).then(function(result) {
           assert.equal(result, "foobar");
           done();
        }).error(done);
    });
    it('Add - Date - number', function(done) {
        var now = new Date();
        r.expr(now).add(333).toEpochTime().run(connection).then(function(result) {
           assert.equal(result, now.getTime()/1000+333);
           done();
        }).error(done);
    });
    it('Sub - number', function(done) {
        r.expr(20).sub(12).run(connection).then(function(result) {
           assert.equal(result, 8);
           done();
        }).error(done);
    });
    it('Sub - date', function(done) {
        var now = new Date();
        r.expr(now).sub(333).toEpochTime().run(connection).then(function(result) {
           assert.equal(result, now.getTime()/1000-333);
           done();
        }).error(done);

    });
    it('Mul - number', function(done) {
        r.expr(20).mul(12, 7).run(connection).then(function(result) {
           assert.equal(result, 20*12*7);
           done();
        }).error(done);
    });
    it('Mod - number - 1', function(done) {
        r.expr(20).mod(7).run(connection).then(function(result) {
           assert.equal(result, 6);
           done();
        }).error(done);
    });
    it('Mod - number - 2', function(done) {
        r.expr(19).mod(7).run(connection).then(function(result) {
           assert.equal(result, 5);
           done();
        }).error(done);
    });
    it('Mod - number - 3', function(done) {
        r.expr(17).mod(7).run(connection).then(function(result) {
           assert.equal(result, 3);
           done();
        }).error(done);
    });
    it('Mod - error - 1', function(done) {
        r.expr('17').mod(7).run(connection).then(function(result) {
           done(new Error("Was expecting an error"));
        }).error(function(err) {
            assert(err.message.match(/^Expected type NUMBER but found STRING/));
            done();
        });
    });
    it('Mod - error - 2', function(done) {
        r.expr(17).mod('7').run(connection).then(function(result) {
           done(new Error("Was expecting an error"));
        }).error(function(err) {
            assert(err.message.match(/^Expected type NUMBER but found STRING/));
            done();
        });
    });
    it('Mod - error - 3', function(done) {
        r.expr(17.2).mod(7).run(connection).then(function(result) {
           done(new Error("Was expecting an error"));
        }).error(function(err) {
            assert(err.message.match(/^Number not an integer: 17.2/));
            done();
        });
    });
    it('Mod - error - 4', function(done) {
        r.expr(17).mod(7.4).run(connection).then(function(result) {
           done(new Error("Was expecting an error"));
        }).error(function(err) {
            assert(err.message.match(/^Number not an integer: 7.4/));
            done();
        });
    });

    after(function() {
        connection.close();
    });
});
