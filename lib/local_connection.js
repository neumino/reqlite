var _util = require('util');
var Connection = require(__dirname+"/connection.js");
var EventEmitter = require('events').EventEmitter;

function LocalConnection(server) {
  var self = this;
  this.server = server;
  this.clientSocket = new Socket();
  this.serverSocket = new Socket();

  this.clientSocket._setOther(this.serverSocket);
  this.serverSocket._setOther(this.clientSocket);

  this.connection = new Connection(this.serverSocket, this.server, {
    version: this.server.protocolVersion,
    authKey: this.server.authKey,
    protocol: this.server.protocol
  });
  //TODO Bind listener to destroy the connection

  setTimeout(function() {
    self.clientSocket.emit('connect');
  }, 0);
}

LocalConnection.prototype.getClientSocket = function() {
  return this.clientSocket;
}

LocalConnection.prototype.getServerSocket = function() {
  return this.serverSocket;
}

function Socket() {
  this.open = true;
}
_util.inherits(Socket, EventEmitter);

Socket.prototype._setOther = function(socket) {
  this.other = socket;
}
Socket.prototype.write = function(buffer) {
  this.other.emit('data', buffer);
}

//TODO Implement these as needed
Socket.prototype.connect = function(buffer) {}
Socket.prototype.end = function(buffer) {}
Socket.prototype.pause = function(buffer) {}
Socket.prototype.ref = function(buffer) {}
Socket.prototype.resume = function(buffer) {}
Socket.prototype.setEncoding = function(buffer) {}
Socket.prototype.setKeepAlive = function(buffer) {}
Socket.prototype.setNoDelay = function(buffer) {}
Socket.prototype.setTimeout = function(buffer) {}
Socket.prototype.unref = function(buffer) {}

module.exports = LocalConnection;
