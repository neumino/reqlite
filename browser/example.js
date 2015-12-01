var Reqlite = require('reqlite');
var r = require('rethinkdbdash')({pool: false});

var server = new Reqlite();
var fakeTcpConnection = server.createConnection();

r.connect({
  connection: fakeTcpConnection
}).bind({}).then(function(connection) {
  this.connection = connection;
  console.log('Connected');
  return r.expr('Ok').run(this.connection);
}).then(function(result) {
  console.log(JSON.stringify(result, null, 2));
  return r.dbCreate('blog').run(this.connection);
}).then(function(result) {
  console.log(JSON.stringify(result, null, 2));
  return r.db('blog').tableCreate('post').run(this.connection);
}).then(function(result) {
  console.log(JSON.stringify(result, null, 2));
  return r.db('blog').table('post').insert([{
    title: 'First post',
    author: 'Michel'
  },
  {
    title: 'Second post',
    author: 'Daniel'
  },
  ]).run(this.connection);
}).then(function(result) {
  console.log(JSON.stringify(result, null, 2));
  return r.db('blog').table('post').run(this.connection);
}).then(function(result) {
  console.log(JSON.stringify(result, null, 2));
  return r.db('blog').table('post').filter({title: 'First post'}).update({
    updated: r.now()
  }).run(this.connection);
}).then(function(result) {
  console.log(JSON.stringify(result, null, 2));
  return r.db('blog').table('post').run(this.connection);
}).then(function(result) {
  console.log(JSON.stringify(result, null, 2));
  return r.http('http://httpbin.org').run(this.connection);
}).then(function(result) {
  console.log(JSON.stringify(result, null, 2));
  return this.connection.close();
}).then(function() {
  console.log('Done');
}).catch(function(error) {
  console.log(error);
});
