var net = require('net');
var protodef = require(__dirname+"/protodef.js");
var Query = require(__dirname+"/query.js");
var util = require('util');

// Create a new TCP server -- used for tests
function Server(options) {
    var self = this;

    self.authKey = "";
    self.version = protodef.VersionDummy.Version.V0_3;
    self.protocol = protodef.VersionDummy.Protocol.JSON; // Support for JSON protocol only
    self.port = options['driver-port'] || 28015;

    self.databases = {};

    var server = net.createServer(function(connection) { //'connection' listener
        new Connection(connection, self, {
            version: self.version,
            authKey: self.authKey,
            protocol: self.protocol
        });
    });
    server.listen(self.port, function() { //'listening' listener
        var output = "info: Creating directory /home/michel/rethinkdb_data\n\
info: Creating a default database for your convenience. (This is because you ran 'rethinkdb' without 'create', 'serve', or '--join', and the directory '/home/michel/rethinkdb_data' did not already exist.)\n\
info: Running rethinkdb 1.13.1 (GCC 4.9.0)...\n\
info: Running on Linux 3.15.5-1-ARCH x86_64\n\
info: Using cache size of 4124 MB\n\
info: Loading data from directory /home/michel/rethinkdb_data\n\
info: Listening for intracluster connections on port 29015\n\
info: Listening for client driver connections on port "+self.port+"\n\
info: Listening for administrative HTTP connections on port 8080\n\
info: Listening on addresses: 127.0.0.1, ::1\n\
info: To fully expose RethinkDB on the network, bind to all addresses\n\
info: by running rethinkdb with the `--bind all` command line option.\n\
info: Server ready\n";

        process.stderr.write(output)
    });
    server.on('error', function(error) {
        var output = "info: Server got SIGINT from pid 0, uid 0; shutting down...\n\
info: Shutting down client connections...\n\
info: All client connections closed.\n\
info: Shutting down storage engine... (This may take a while if you had a lot of unflushed data in the writeback cache.)\n\
info: Storage engine shut down.\n"

        process.stderr.write(output)
    });
}

function Connection(connection, server, options) {
    var self = this;

    self.connection = connection;
    self.options = options;
    self.server = server;

    self.open = false;
    self.buffer = new Buffer(0);
    self.version;
    self.auth;
    self.protocol;

    self.connection.on('connect', function() {
        //console.log("New connection");
        self.open = true;
    });
    self.connection.on('data', function(data) {
        self.buffer = Buffer.concat([self.buffer, data])
        self.read();
    });
    self.connection.on("end", function() {
        //console.log("End connection");
    });
}
Connection.prototype.read = function() {
    var self = this;

    if (self.version === undefined) {
        if (self.buffer.length >= 4) {
            var version = self.buffer.readUInt32LE(0)
            self.buffer = self.buffer.slice(4);

            if (version !== self.options.version) {
                //TODO Send appropriate error
                self.connection.end();
            }
            else {
                self.version = version;
            }
            
            self.read();
        }
        // else, we need more data
    }
    else if (self.auth === undefined) {
        if (self.buffer.length >= 4) {
            var authKeyLength = self.buffer.readUInt32LE(0)
            if (self.buffer.length >= authKeyLength+4) {
                self.buffer = self.buffer.slice(4);
                var authKey = self.buffer.slice(0, authKeyLength).toString();

                if (authKey !== self.options.authKey) {
                    //TODO Send appropriate error
                    self.connection.end();
                }
                else {
                    self.auth = true;
                    self.read();
                }
            }
        }
    }
    else if (self.protocol === undefined) {
        if (self.buffer.length >= 4) {
            var protocol = self.buffer.readUInt32LE(0)
            self.buffer = self.buffer.slice(4);

            if (protocol !== self.options.protocol) {
                //TODO Send appropriate error
                self.connection.end();
            }
            else {
                self.protocol = protocol;
            }
            self.connection.write("SUCCESS\u0000");
            console.log("Success sent back")
            self.read();
        }
    }
    else {
        if (self.buffer.length >= 8+4) { // 8 for the token, 4 for the query's length
            var token = self.buffer.readUInt32LE(0);
            console.log("token", token)
            var queryLength = self.buffer.readUInt32LE(8);
            console.log("queryLength", queryLength)

            // TODO Similate async
            if (self.buffer.length >= queryLength+8+4) {
                self.buffer = self.buffer.slice(8+4);
                var queryStr = self.buffer.slice(0, queryLength).toString();
                self.buffer = self.buffer.slice(queryLength);
                try {
                    console.log("Parsing");
                    //TODO Split things for better error handling
                    var query = JSON.parse(queryStr);


                    response = new Query(self.server, query).run();
                    console.log(response);

                    var tokenBuffer = new Buffer(8);
                    tokenBuffer.writeUInt32LE(token, 0)
                    tokenBuffer.writeUInt32LE(0, 4)

                    var responseBuffer = new Buffer(JSON.stringify(response));
                    var responseLengthBuffer = new Buffer(4);
                    responseLengthBuffer.writeUInt32LE(responseBuffer.length, 0);
                    self.connection.write(Buffer.concat([tokenBuffer, responseLengthBuffer, responseBuffer]))
                    console.log("Sent");
                }
                catch(err) {
                    //TODO Send appropriate error
                    console.log("crashing");
                    console.log(err);
                    self.connection.write("Error, could not parse query");
                }
            }
        }
    }

}

module.exports = Server;
