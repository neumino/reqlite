var rethinkdb = require('rethinkdb');
var Server = require('../lib');


function connect(options, callback) {
  // {host, port, path, db}
  // Temporarily unset process.browser so rethinkdb uses a TcpConnection
  var oldProcessDotBrowser = process.browser;
  process.browser = false;
  rethinkdb.connect(options, callback);
  process.browser = oldProcessDotBrowser;
}



module.exports = {
  Server: Server,
  rethinkdb: rethinkdb,
  connect: connect,
};
