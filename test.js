var nothing = require('./lib/index.js');
var r = require('rethinkdb');

console.log("Opening a connection");
r.connect({}).then(function(conn) {
    console.log("Connection opened");
    r.db('test').table('test').indexCreate(function(doc) {
        return doc("field");
    }).run(conn).then(function(result) {
        console.log("Query1 executed");
        console.log(result);
    });
}).error(function(err) {
    console.log(err);
});
