var assert = require('assert');

module.exports.removeId = function(doc) {
  delete doc.id;
  return doc;
}

module.exports.generateCompare = function(connections) {
  return function(query, done, transform, raw) {
    if (typeof transform !== 'function') {
      transform = function(x) { return x }
    }
    query.run(connections.rethinkdb).then(function(resultrethinkdb) {
      query.run(connections.reqlite).then(function(resultreqlite) {
        if ((resultrethinkdb != null) && typeof resultrethinkdb.toArray === 'function') {
          if (raw === true) {
            transform(resultreqlite, resultrethinkdb); // The user has to call done here
          }
          else {
            resultrethinkdb.toArray().then(function(resultrethinkdb) {
              assert.deepEqual(transform(resultreqlite), transform(resultrethinkdb));
              done();
            });
          }
        }
        else {
          assert.deepEqual(transform(resultreqlite), transform(resultrethinkdb));
          done();
        }
      }).error(function(error) {
        done(new Error("Reqlite failed with"+JSON.stringify(error.message)+'\n'+JSON.stringify(error.stack)));
      }).catch(done);
    }).error(function(errorrethinkdb) {
      query.run(connections.reqlite).then(function(resultreqlite) {
        //console.log(resultreqlite);
        done(new Error("Rethinkdb failed with"+JSON.stringify(errorrethinkdb.message)+'\n'+JSON.stringify(errorrethinkdb.stack)+'\nReqlite result'+JSON.stringify(resultreqlite, null, 2)));
      }).error(function(errorreqlite) {
        assert.equal(transform(errorreqlite.message), transform(errorrethinkdb.message));
        done();
      });
    }).catch(done);
  }
}
