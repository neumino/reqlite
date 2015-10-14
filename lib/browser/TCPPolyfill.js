import {EventEmitter} from 'events';

const notImpl = name => () => {
  throw new Error('Not implemented in TcpPolyfill: ' + name);
};

const servers = {};

export class Server extends EventEmitter{
  constructor(onConnect){
    super();
    this.onConnect = onConnect || () => {};
    // maxConnections
    // connections
    // getConnections


    // :listening
    // :connection
    // :close
    // :error

  }
  listen(port = 3141, done){
    this.emit('listening');
    // todo check port is a number
    if(servers[port]){
      throw new Error(`server already started at ${port}`);
    }
    servers[port] = this;
    done();
    // port[, hostname][, backlog][, callback])
    // path[, callback])
    // handle[, callback])
    // options[, callback])

  }
  close(){
    throw new Error('reqlite.server.close not implemented');
  }

  // er...
  address(){}
  unref(){}
  ref(){}


}

export class Socket extends EventEmitter{
  closed = false

  constructor(options){
    super();


    ['setEncoding', 'pause', 'resume', 'setTimeout', 'address', 'unref', 'ref']
      .forEach(name => this[name] = notImpl(name));



  // :close
  // :error
  // :message



  }
  connect(port, host, connectListener = () => {}){
    if(!servers[port]){
      throw new Error('server not found');
    }
    // ack!
    let s = this.__pipe();
    servers[port].onConnect(s);
    setTimeout(()=> {
      s.emit('connect');
      this.emit('connect');
    }, 0);
  }
  end(data){
    if(data){
      this.write(data);
    }
    this.emit('end');
    this.emit('close');

  }
  destroy(){
    this.closed = true;
    // this.emit('destroy');
    this.removeAllListeners(); // ?
  }
  write(data){
    if(typeof data === 'string'){
      data = new Buffer(data);
    }
    // console.log('writing', data);
    // this.emit('out:write', data);
    setTimeout(() => this.__piped.emit('data', data), 0);

  }

  __pipe(){
    let s = this.__piped = new Socket();
    s.__piped = this;

    return s;

    // create another socket that connects to this one.
  }
  setNoDelay(){
    // console.log('socket.setNoDelay - not implemented - no op');
  }
  setKeepAlive(){
    // console.log('socket.setKeepAlive - not implemented - no op')
  }
}


export function connect(...args) {
  const opts = {};
  if (args[0] && typeof args[0] === 'object') {
    opts.port = args[0].port;
    opts.host = args[0].host;
    opts.connectListener = args[1];
  } else if (Number(args[0]) > 0) {
    opts.port = args[0];
    opts.host = args[1];
    opts.connectListener = args[2];
  } else {
    throw new Error('Unsupported arguments for net.connect');
  }
  const socket = new Socket();
  socket.connect(opts.port, opts.host, opts.connectListener);
  return socket;
}

export const createConnection = connect;

export function create(onConnect){
  return new Server(onConnect);

}

export const createServer = create;

// This is wrong, but irrelevant for connecting via websocket
export const isIPv4 = input => true;
export const isIPv6 = input => false;
export const isIP = input => isIPv4(input) ? 4 : isIPv6(input) ? 6 : 0;
