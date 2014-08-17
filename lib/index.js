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
    self.port = options.port || 28015;

    self.databases = {};

    var server = net.createServer(function(connection) { //'connection' listener
        new Connection(connection, self, {
            version: self.version,
            authKey: self.authKey,
            protocol: self.protocol
        });
    });
    server.listen(self.port, function() { //'listening' listener
        console.log('Listening on port', self.port);
    });
    server.on('error', function(error) {
        console.log('Error', util.inspect(error, {depth: null}));
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


// If called with `start`, we automatically start a server
var argv = require('minimist')(process.argv.slice(2));
if (argv._[0] === 'start') {
    options = {};
    return new Server(options);
}
