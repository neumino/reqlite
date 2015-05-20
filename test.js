var r = require('rethinkdb');

r.connect().then(function(connection) {
  r.db('test').table('test').insert({id: 6, foo: 'bar'}).run(connection).finally(function() {
    var query = r.db('test').table('test').get(6).changes();
    query.run(connection).then(function(feed) {
      feed.next().then(function(change) {
        return feed.close();
      }).then(function() {
        done();
      }).catch(errorHandler);

      r.db('test').table('test').get(6).delete().run(connection).then(function(result) {
        console.log('insert done');
      }).catch(errorHandler);
    }).catch(errorHandler);
  }).catch(errorHandler);
}).catch(errorHandler);

function errorHandler(error) {
  console.error('ERRORRRRRRRRRRRRRRRRRRRRRRRRRRRRRRR');
  console.error(error);
  throw error;
}

function done() {
  console.log('Done');
  process.exit(0);
}
