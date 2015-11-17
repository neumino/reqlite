// this is super messy right now
// and should become redundant after #rethinkdb/4316 is fixed
var inherits = require('util').inherits;

var EventEmitter = require('events').EventEmitter;

var notImpl = function(name){
  throw new Error('Not implemented in TcpPolyfill: ' + name);
};

var servers = {};


var Server = module.exports.Server = function(onConnect){
  this.onConnect = onConnect;
  var t = this;
  ['close', 'address', 'unref', 'ref']
    .forEach(function(name) { t[name] = function(){ notImpl(name); }; });
};
inherits(Server, EventEmitter);

Server.prototype.listen = function(port, done){
  if(!port){
    throw new Error('no port specified');
  }
  if(servers[port]){
    throw new Error('server already started at'+  port);
  }
  servers[port] = this;
  done();
};


var Socket = module.exports.Socket = function(){};
inherits(Socket, EventEmitter);

Socket.prototype.connect = function(port, host, connectListener){
  if(!servers[port]){
    throw new Error('server not found');
  }
  // ack!
  var s = this.__pipe();
  var t = this;
  servers[port].onConnect(s);
  setTimeout(function(){
    s.emit('connect');
    t.emit('connect');
    if(connectListener){
      connectListener();
    }
  }, 0);
};

Socket.prototype.end = function(data){
  if(data){
    this.write(data);
  }
  this.emit('end');
  this.emit('close');
};

Socket.prototype.destroy = function(){
  this.closed = true;
  // this.emit('destroy');
  this.removeAllListeners(); // ?
};
Socket.prototype.write = function(data){
  var t = this;
  if(typeof data === 'string'){
    data = new Buffer(data);
  }
  setTimeout(function(){ t.__piped.emit('data', data); }, 0);

};

Socket.prototype.__pipe = function(){
  // create another socket that connects to this one.
  var s = this.__piped = new Socket();
  s.__piped = this;

  return s;
};

Socket.prototype.setNoDelay = function(){
  // console.log('socket.setNoDelay - not implemented - no op');
};
Socket.prototype.setKeepAlive = function(){
  // console.log('socket.setKeepAlive - not implemented - no op')
};


module.exports.createConnection = module.exports.connect = function connect() {
  var opts = {};
  if (arguments[0] && typeof arguments[0] === 'object') {
    opts.port = arguments[0].port;
    opts.host = arguments[0].host;
    opts.connectListener = arguments[1];
  } else if (Number(arguments[0]) > 0) {
    opts.port = arguments[0];
    opts.host = arguments[1];
    opts.connectListener = arguments[2];
  } else {
    throw new Error('Unsupported arguments for net.connect');
  }
  var socket = new Socket();
  socket.connect(opts.port, opts.host, opts.connectListener);
  return socket;
};


module.exports.createServer = function create(onConnect){
  return new Server(onConnect);
};

var isIPv4 = module.exports.isIPv4 = function(){ return true; };
var isIPv6 = module.exports.isIPv6 = function(){ return false; };
module.exports.isIP = function(input){ return isIPv4(input) ? 4 : isIPv6(input) ? 6 : 0; };

