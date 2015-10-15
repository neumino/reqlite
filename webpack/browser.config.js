var webpack = require('webpack');
var path = require('path');
var FunctionModulePlugin = require('webpack/lib/FunctionModulePlugin');
var NodeTemplatePlugin = require('webpack/lib/node/NodeTemplatePlugin');
var TcpPolyfillPlugin = require('./TCPPolyfillPlugin');
var TlsStubPlugin = require('./TLSStubPlugin');

var config = module.exports = {
    entry: ['./lib/browser/index'],
    output: {
      library: 'Reqlite',
      libraryTarget: 'umd',
      path: path.join(__dirname, '../dist'),
      filename: 'browser.js'
    },
    module: {
      loaders: [
        { test: /\.js$/, loaders: ['babel'], exclude: /node_modules/ },
        { test: /\.json$/, loader: 'json-loader' }
      ]
    },
    node: {
      fs: 'empty'
    }
  };

  // Very similar behavior to setting config.target to 'node', except it doesn't
  // set the 'net' or 'tls' modules as external. That way, we can use
  // TcpPolyfillPlugin and TlsStubPlugin to override those modules.
  //
  // For node.js target, we leave tls in externals because it's needed for ws.
  config.target = function(compiler) {
    var nodeNatives = Object.keys(process.binding('natives'));
    var mocks = ['net', 'tls'];

    var externals = nodeNatives.filter(function(x) {
      return mocks.indexOf(x) < 0;
    });
    compiler.apply(
      new NodeTemplatePlugin(config.output, false),
      new FunctionModulePlugin(config.output),
      new webpack.ExternalsPlugin('commonjs', externals),
      new webpack.LoaderTargetPlugin('node'),
      new webpack.DefinePlugin({ "global.GENTLY": false }),
      new TcpPolyfillPlugin(/node_modules\/rethinkdb|node_modules\/superagent|lib/),
      new TlsStubPlugin(/node_modules\/rethinkdb|node_modules\/superagent|lib/)
    );
  };

