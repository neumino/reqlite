var _util = require('util');
var EventEmitter = require('events').EventEmitter;

function LocalConnection() {
  this.clientSocket = new Socket();
  this.serverSocket = new Socket();

  this.clientSocket._setOther(this.serverSocket);
  this.serverSocket._setOther(this.clientSocket);
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


module.exports = LocalConnection;
