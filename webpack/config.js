// use `npm run browser`

var webpack = require('webpack');
var path = require('path');
var FunctionModulePlugin = require('webpack/lib/FunctionModulePlugin');
var NodeTemplatePlugin = require('webpack/lib/node/NodeTemplatePlugin');
var TcpPolyfillPlugin = require('./TCPPolyfillPlugin');
var TlsStubPlugin = require('./TLSStubPlugin');

var config = module.exports = {
    entry: ['./webpack/src'],
    output: {
      library: 'Reqlite',
      libraryTarget: 'umd',
      path: path.join(__dirname, '../'),
      filename: 'browser.js'
    },
    node: {
      // fs: 'empty'
      events: true
    },
    resolve:{
      packageAlias: 'browser'
    }
  };

  // Don't set the 'net' or 'tls' modules as external. That way, we can use
  // TcpPolyfillPlugin and TlsStubPlugin to override those modules.
  config.target = function(compiler) {
    var nodeNatives = Object.keys(process.binding('natives'));
    var mocks = ['net', 'tls'];

    var externals = nodeNatives.filter(function(x) {
      return mocks.indexOf(x) < 0;
    });
    compiler.apply(
      new NodeTemplatePlugin(config.output, false),
      new FunctionModulePlugin(config.output),
      new webpack.LoaderTargetPlugin('web'),
      new webpack.ExternalsPlugin('commonjs', externals),

      // new webpack.DefinePlugin({ "global.GENTLY": false }),
      new TcpPolyfillPlugin(/node_modules\/rethinkdb|lib/),
      new TlsStubPlugin(/node_modules\/rethinkdb|lib/)
    );
  };

