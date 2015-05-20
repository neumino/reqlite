var net = require('net');
var protodef = require(__dirname+"/protodef.js");
var Query = require(__dirname+"/query.js");
var util = require('util');
var protodef = require(__dirname+"/protodef.js");
var queryTypes = protodef.Query.QueryType;

// Create a new TCP server -- used for tests
function Server(options) {
  var self = this;

  self.authKey = "";
  self.version = protodef.VersionDummy.Version.V0_4;
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
    var output = "Running rethinkdb 2.0.1 (GCC 4.9.2)...\n\
Running on Linux 4.0.1-1-ARCH x86_64\n\
Loading data from directory /home/michel/tmp/rethinkdb_data\n\
Listening for intracluster connections on port 29015\n\
Listening for client driver connections on port "+self.port+"\n\
Listening for administrative HTTP connections on port 8080\n\
Listening on addresses: 127.0.0.1, ::1\n\
To fully expose RethinkDB on the network, bind to all addresses by running rethinkdb with the `--bind all` command line option.\n\
Server ready, \"h9_z00\" 218b42a9-47dc-48eb-90a7-7cda4f206bcf\n";
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
  self.queries = {} // token -> Query

  self.connection.on('connect', function() {
    self.open = true;
  });
  self.connection.on('data', function(data) {
    self.buffer = Buffer.concat([self.buffer, data])
    self.read();
  });
  self.connection.on("end", function() {
  });
  self.connection.on("error", function(error) {
    util.log(error);
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
      self.read();
    }
  }
  else {
    while (self.buffer.length >= 8+4) { // 8 for the token, 4 for the query's length
      var token = self.buffer.readUInt32LE(0);
      var queryLength = self.buffer.readUInt32LE(8);

      // TODO Similate async?
      if (self.buffer.length >= queryLength+8+4) {
        self.buffer = self.buffer.slice(8+4);
        var queryStr = self.buffer.slice(0, queryLength).toString();
        self.buffer = self.buffer.slice(queryLength);
        try {
          //TODO Split things for better error handling
          var queryAST = JSON.parse(queryStr);
          var queryType = queryAST[0];

          var response;
          if (queryType === queryTypes.START) {
            var query = new Query(self.server, queryAST);
            response = query.run();
            if (query.isComplete() === false) {
              this.queries[token] = query;
            }
            this.send(token, response);
          }
          else if (queryType === queryTypes.CONTINUE) {
            if (this.queries[token] === undefined) {
              throw new Error("TOKEN NOT FOUND")
              //TODO throw
            }

            response = self.queries[token].continue();
            response.then(function(finalResult) {
              self.send(token, finalResult);
              if ((self.queries[token] != null)
                  && (self.queries[token].isComplete() === true)) {
                delete self.queries[token];
              }
            }).catch(function(error) {
              throw error;
            })
          }
          else if (queryType === queryTypes.STOP) {
            //TODO Clean listeners
            if (this.queries[token] === undefined) {
              throw new Error("TOKEN NOT FOUND")
            }

            this.queries[token].stop();
            for(var i=0; i< self.queries[token].result.callbacks.length+1; i++) {
              response = {
                t: protodef.Response.ResponseType.SUCCESS_SEQUENCE,
                r: [],
                n: []
              };
              self.send(token, response);
            }
            delete self.queries[token];
          }

        }
        catch(err) {
          //TODO This should not happen
          var response = {
            t: protodef.Response.ResponseType.RUNTIME_ERROR,
            r: [err.message],
            b: err.frames || []
          }
          self.send(token, response);
        }
      }
      else {
        break;
      }
    }
  }
}

Connection.prototype.send = function(token, response) {
  var tokenBuffer = new Buffer(8);
  tokenBuffer.writeUInt32LE(token, 0)
  tokenBuffer.writeUInt32LE(0, 4)

  var responseBuffer = new Buffer(JSON.stringify(response));
  var responseLengthBuffer = new Buffer(4);
  responseLengthBuffer.writeUInt32LE(responseBuffer.length, 0);
  this.connection.write(Buffer.concat([tokenBuffer, responseLengthBuffer, responseBuffer]))
}

module.exports = Server;
