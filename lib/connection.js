function Connection(connection, server, options) {
  var self = this;

  self.connection = connection;
  self.options = options;
  self.server = server;

  self.open = false;
  self.buffer = new Buffer(0);
  self.protocolVersion;
  self.auth;
  self.protocol;
  self.queries = {}; // token -> Query

  self.connection.on('connect', function() {
    self.open = true;
  });
  self.connection.on('data', function(data) {
    self.buffer = Buffer.concat([self.buffer, data]);
    self.read();
  });
  self.connection.on("end", function() {
  });
  self.connection.on("error", function(error) {
    util.log(error);
  });
}

module.exports = Connection;

var protodef = require("./protodef.js");
var Query = require("./query.js");
var util = require("./utils/main.js");
var Query = require("./query.js");
var queryTypes = protodef.Query.QueryType;

Connection.prototype.read = function() {
  var self = this;

  if (self.protocolVersion === undefined) {
    if (self.buffer.length >= 4) {
      var version = self.buffer.readUInt32LE(0);
      self.buffer = self.buffer.slice(4);

      if (version !== self.options.version) {
        //TODO Send appropriate error
        self.connection.end();
      }
      else {
        self.protocolVersion = version;
      }

      self.read();
    }
    // else, we need more data
  }
  else if (self.auth === undefined) {
    if (self.buffer.length >= 4) {
      var authKeyLength = self.buffer.readUInt32LE(0);
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
      var protocol = self.buffer.readUInt32LE(0);
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
        (function(token) {
          self.buffer = self.buffer.slice(8+4);
          var queryStr = self.buffer.slice(0, queryLength).toString();
          self.buffer = self.buffer.slice(queryLength);
          try {
            //TODO Split things for better error handling
            var queryAST = JSON.parse(queryStr);
            var queryType = queryAST[0];
            util.log('=== Query ===========================================', token);
            util.log(JSON.stringify(queryAST));


            var queryResult;
            if (queryType === queryTypes.START) {
              //TODO Pass options
              var query = new Query(self.server, queryAST, {}, token);
              queryResult = query.run();
              Promise.resolve(queryResult).then(function(response) {
                if (query.isComplete() === false) {
                  self.queries[token] = query;
                }
                self.send(token, response);
                util.log('=== Response ===========================================', token);
                util.log(JSON.stringify(response, null, 2));
                if (response.debug != null) {
                  util.log(response.debug);
                }
              }).catch(function(error) {
                util.log('queryResult - response', error);
              });

            }
            else if (queryType === queryTypes.CONTINUE) {
              if (self.queries[token] === undefined) {
                throw new Error("TOKEN NOT FOUND");
                //TODO throw
              }

              response = self.queries[token].continue();
              response.then(function(finalResult) {
                util.log('=== Delayed response ===========================================', token);
                util.log(finalResult);
                util.log(self.queries[token].token);
                self.send(token, finalResult);
                if ((self.queries[token] != null)
                    && (self.queries[token].isComplete() === true)) {
                  delete self.queries[token];
                }
              }).catch(function(error) {
                throw error;
              });
            }
            else if (queryType === queryTypes.STOP) {
              //TODO Clean listeners
              if (self.queries[token] === undefined) {
                throw new Error("TOKEN NOT FOUND");
              }

              self.queries[token].stop();
              for(var i=0; i<self.queries[token].result.callbacks.length+1; i++) {
                response = {
                  t: protodef.Response.ResponseType.SUCCESS_SEQUENCE,
                  r: [],
                  n: []
                };
                self.send(token, response);
              }
              delete self.queries[token];
              util.log('=== Stop esponse ===========================================', token);
              util.log(response);
            }
            else if (queryType === queryTypes.NOREPLY_WAIT) {
              response = {
                t: protodef.Response.ResponseType.SUCCESS_ATOM,
                r: []
              };
              self.send(token, response);
            }
          }
          catch(err) {
            util.log('=== Error ===========================================', token);
            util.log(err);
            //TODO This should not happen
            var response = {
              t: protodef.Response.ResponseType.RUNTIME_ERROR,
              r: [err.message],
              b: err.frames || []
            };
            self.send(token, response);
          }
        })(token);
      }
      else {
        break;
      }

    }
  }
};

Connection.prototype.send = function(token, response) {
  var tokenBuffer = new Buffer(8);
  tokenBuffer.writeUInt32LE(token, 0);
  tokenBuffer.writeUInt32LE(0, 4);

  var responseBuffer = new Buffer(JSON.stringify(response));
  var responseLengthBuffer = new Buffer(4);
  responseLengthBuffer.writeUInt32LE(responseBuffer.length, 0);
  this.connection.write(Buffer.concat([tokenBuffer, responseLengthBuffer, responseBuffer]));
};


