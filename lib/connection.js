var crypto = require('crypto');
var KEY_LENGTH = 32; // Because we are currently using SHA 256
var NULL_BUFFER = new Buffer('\0', 'binary');

function Connection(connection, server, options) {
  var self = this;

  self.id = server.getNextId();
  self.connection = connection;
  self.options = options;
  self.server = server;

  self.user = undefined;
  self.password = undefined;
  self.salt = new Buffer(crypto.randomBytes(16)).toString('base64')+'=='
  self.serverNonce = new Buffer(crypto.randomBytes(17)).toString('base64')
  self.clientNonce = undefined;
  self.iterations = 4096;

  self.open = false;
  self.buffer = new Buffer(0);
  self.state = 0;
  self.protocol = undefined;
  self.ignoreIncoming = false; // Used to handle asynchronous operation in the handshake.
  self.queries = {}; // token -> Query

  self.connection.on('connect', function() {
    self.open = true;
  });
  self.connection.on('data', function(data) {
    self.buffer = Buffer.concat([self.buffer, data]);
    self.read();
  });
  self.connection.on("end", function() {
    self.server.removeConnection(self.id);
  });
  self.connection.on("error", function(error) {
    util.log(error);
  });
}

module.exports = Connection;

var protodef = require(__dirname+"/protodef.js");
var Query = require(__dirname+"/query.js");
var util = require(__dirname+"/utils.js");
var Query = require(__dirname+"/query.js");
var queryTypes = protodef.Query.QueryType;

Connection.prototype.read = function() {
  var self = this;
  if (self.ignoreIncoming === true) {
    return;
  }
  if (self.state === 0) {
    if (self.buffer.length >= 4) {
      var version = self.buffer.readUInt32LE(0);
      self.buffer = self.buffer.slice(4);

      if (version !== self.options.version) {
        //TODO Send appropriate error
        self.connection.end();
      }
      else {
        self.protocolVersion = version;
        self.connection.write(Buffer.concat([new Buffer(JSON.stringify({
          success: true,
          min_protocol_version: self.options.minProtocolVersion,
          max_protocol_version: self.options.maxProtocolVersion
        })), NULL_BUFFER]));
      }
      self.state++;
      self.read();
    }
    // else, we need more data
  }
  else if (self.state === 1) {
    for(var i=0; i<self.buffer.length; i++) {
      if (self.buffer[i] === 0) {
        var authMessage = self.buffer.slice(0, i).toString();
        self.buffer = self.buffer.slice(i+1);
        try {
          self.auth = JSON.parse(authMessage);
          // {'': '', 'n': <user>, 'r': <client nonce>}
          var authentication = util.splitCommaEqual(self.auth.authentication);
          self.user = authentication.n;
          self.password = ''; // TODO: Pull password from the database.
          self.clientNonce = authentication.r;
          self.connection.write(Buffer.concat([new Buffer(JSON.stringify({
            success: true,
            authentication: self.getAuthentication()
          })), NULL_BUFFER]));
          self.state++;
          self.read();
        } catch(error) {
          // TODO Send appropriate error
          self.connection.end();
        }
      }
    }
  }
  else if (self.state === 2) {
    for(var i=0; i<self.buffer.length; i++) {
      if (self.buffer[i] === 0) {
        var proof = self.buffer.slice(0, i).toString();
        var authMessage = util.splitCommaEqual(JSON.parse(proof).authentication);
        if (self.getFullNonce() !== authMessage.r) {
          // Wrong nonce, terminate the connection.
          // TODO Send appropriate error
          self.connection.end();
        }
        var authMessage =
            "n=" + self.user + ",r=" + self.clientNonce + "," +
            self.getAuthentication() + "," +
            "c=biws,r=" + self.getFullNonce();
        // TODO Compute clientSignature
        // TODO Recover client key
        // TODO Apply hash on client key and compare to the storedKey
        self.ignoreIncoming = true;
        var salt = new Buffer(self.salt, 'base64')
        crypto.pbkdf2(self.password, salt, self.iterations, KEY_LENGTH, "sha256", function(error, saltedPassword) {
          self.ignoreIncoming = false;
          if (error != null) {
            return;
          }
          var serverKey = crypto.createHmac("sha256", saltedPassword).update("Server Key").digest()
          var serverSignature = crypto.createHmac("sha256", serverKey).update(authMessage).digest()
          self.buffer = self.buffer.slice(i+1);
          self.connection.write(Buffer.concat([new Buffer(JSON.stringify({
            success: true,
            authentication: 'v='+serverSignature.toString('base64')
          })), NULL_BUFFER]));
          self.state++;
          self.read();
        })
      }
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
          self.handleQuery(token, queryStr);

        })(token);
      }
      else {
        break;
      }

    }
  }
};

Connection.prototype.handleQuery = function(token, queryStr) {
  var self = this;
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
    else if (queryType === queryTypes.SERVER_INFO) {
      response = {
        t: protodef.Response.ResponseType.SERVER_INFO,
        r: [{
          name: self.server.name,
          id: self.server.id
        }]
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
}

Connection.prototype.send = function(token, response) {
  var tokenBuffer = new Buffer(8);
  tokenBuffer.writeUInt32LE(token, 0);
  tokenBuffer.writeUInt32LE(0, 4);

  var responseBuffer = new Buffer(JSON.stringify(response));
  var responseLengthBuffer = new Buffer(4);
  responseLengthBuffer.writeUInt32LE(responseBuffer.length, 0);
  this.connection.write(Buffer.concat([tokenBuffer, responseLengthBuffer, responseBuffer]));
};


Connection.prototype.getFullNonce = function() {
  return this.clientNonce+this.serverNonce;
}

Connection.prototype.getAuthentication = function() {
  return 'r='+this.getFullNonce()+',s='+this.salt+',i='+this.iterations;
}
