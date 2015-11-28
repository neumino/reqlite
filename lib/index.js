var net = require('net');
var constants = require("./constants.js");
var protodef = require("./protodef.js");
var Connection = require("./connection.js");
var util = require("./utils/main.js");
var os = require("os");


// Create a new TCP server -- used for tests
function Server(options) {
  var self = this;
  options = options || {};

  self.serverVersion = '2.0.2';
  self.authKey = "";
  self.protocolVersion = protodef.VersionDummy.Version.V0_4;
  self.protocol = protodef.VersionDummy.Protocol.JSON; // Support for JSON protocol only
  self.port = options['driver-port'] || 28015;

  if (options.debug === true) {
    util._debug = true;
  }
  self.silent = options.silent || constants.SILENT || false;

  self.databases = {};
  self.init();
  self.id = util.uuid();
  self.name = options.name || os.hostname()+util.s4().slice(1);

  var server = net.createServer(function(connection) { //'connection' listener
    new Connection(connection, self, {
      version: self.protocolVersion,
      authKey: self.authKey,
      protocol: self.protocol
    });
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
}

module.exports = Server;

var Database = require("./database.js");

Server.prototype.init = function() {
  var self = this;
  self.databases['rethinkdb'] = new Database('rethinkdb');

  self.databases['rethinkdb'].tableCreate('cluster_config', {meta: true});
  self.databases['rethinkdb'].table('cluster_config').insert({
    id: "auth",
    auth_key: (self.authKey === '') ? null : self.authKey
  }, {meta: true});
  self.databases['rethinkdb'].table('cluster_config').insert({
    heartbeat_timeout_secs: 10,
    id: "heartbeat"
  }, {meta: true});
  self.databases['rethinkdb'].table('cluster_config').get('auth').then(function(doc) {
    doc.addListener('change', function(change) {
      if (change.new_val != null) {
        self.authKey = change.new_val.auth_key;
      }
    });

    doc.toDatum = function() {
      return {
        id: 'auth',
        auth_key: (this.doc.auth_key === null || this.doc.auth_key === '') ? null : {hidden: true}
      };
    };

  });

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
