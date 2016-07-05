var net = require('net');
var constants = require(__dirname+"/constants.js");
var protodef = require(__dirname+"/protodef.js");
var Connection = require(__dirname+"/connection.js");
var LocalConnection = require(__dirname+"/local_connection.js");
var util = require(__dirname+"/utils.js");
var os = require("os");


// Create a new TCP server -- used for tests
function Server(options) {
  var self = this;
  options = options || {};

  self.serverVersion = '2.0.2';
  self.authKey = "";
  self.protocolVersion = protodef.VersionDummy.Version.V1_0;
  self.minProtocolVersion = 0;
  self.maxProtocolVersion = 0;
  self.port = options['driver-port'] || 28015;

  self._connections = {};
  self.nextConnectionId = 0;

  if (options.debug === true) {
    util._debug = true;
  }
  self.silent = options.silent || constants.SILENT || false;

  self.databases = {};
  self.init();
  self.id = util.uuid();
  self.name = options.name || os.hostname()+util.s4().slice(1);

  self._server = self._createServer();
}

Server.prototype.createConnection = function() {
  return new LocalConnection(this).getClientSocket();
}

Server.prototype.getNextId = function() {
    return this.nextConnectionId++;
}

Server.prototype.removeConnection = function(id) {
  delete this._connections[id];
}

Server.prototype._createServer = function() {
  var self = this;
  if (!net || !net.createServer) {
    return;
  }

  var server = net.createServer(function(connection) { //'connection' listener
    var con = new Connection(connection, self, {
      version: self.protocolVersion,
      authKey: self.authKey,
      minProtocolVersion: self.minProtocolVersion,
      maxProtocolVersion: self.maxProtocolVersion
    });
    // push to connections array
    self._connections[con.id] = con;
  });
  server.listen(self.port, function() { //'listening' listener
    if (self.silent === false) {
      var output = "Running rethinkdb "+self.serverVersion+" (GCC 4.9.2)...\n\
Running on Linux 4.0.1-1-ARCH x86_64\n\
Loading data from directory /home/michel/tmp/rethinkdb_data\n\
Listening for intracluster connections on port 29015\n\
Listening for client driver connections on port "+self.port+"\n\
Listening for administrative HTTP connections on port 8080\n\
Listening on addresses: 127.0.0.1, ::1\n\
To fully expose RethinkDB on the network, bind to all addresses by running rethinkdb with the `--bind all` command line option.\n\
Server ready, \"h9_z00\" 218b42a9-47dc-48eb-90a7-7cda4f206bcf\n";
      process.stderr.write(output);
    }
  });

  server.on('error', function(error) {
    util.log('Error emitted on a server');
    util.log(error);
    if (self.silent === false) {
      var output = "info: Server got SIGINT from pid 0, uid 0; shutting down...\n\
info: Shutting down client connections...\n\
info: All client connections closed.\n\
info: Shutting down storage engine... (This may take a while if you had a lot of unflushed data in the writeback cache.)\n\
info: Storage engine shut down.\n";
      process.stderr.write(output);
    }
  });

  return server;
}

Server.prototype.stop = function(cb) {
  var self = this;
  for(var key in self._connections) {
    var connection = self._connections[key];
    connection.connection.end();
  }
  self._server.close(cb);
}

module.exports = Server;

var Database = require(__dirname+"/database.js");

Server.prototype.init = function() {
  var self = this;
  self.databases['rethinkdb'] = new Database('rethinkdb');

  self.databases['rethinkdb'].tableCreate('cluster_config', {meta: true});
  self.databases['rethinkdb'].table('cluster_config').insert({
    heartbeat_timeout_secs: 10,
    id: "heartbeat"
  }, {meta: true});
  self.databases['rethinkdb'].table('cluster_config');
  self.databases['rethinkdb'].tableCreate('current_issues', {meta: true});
  self.databases['rethinkdb'].tableCreate('db_config', {meta: true});
  self.databases['rethinkdb'].tableCreate('jobs', {meta: true});
  self.databases['rethinkdb'].tableCreate('logs', {meta: true});
  self.databases['rethinkdb'].tableCreate('server_config', {meta: true});
  self.databases['rethinkdb'].tableCreate('server_status', {meta: true});
  self.databases['rethinkdb'].tableCreate('stats', {meta: true});
  self.databases['rethinkdb'].tableCreate('table_config', {meta: true});
  self.databases['rethinkdb'].tableCreate('table_status', {meta: true});
};
