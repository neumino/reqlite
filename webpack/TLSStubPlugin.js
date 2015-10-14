// Webpack plugin to intercept require('tls') and call TlsStub.
//
// For all requires where the context (the path to the file issuing the require
// statement) matches the specified regular expression, replace require('tls')
// with TlsStub. This allows the rethinkdb driver to run in the browser, as
// long as the ssl property is not set in r.connect().
var path = require('path');

function TlsStubPlugin(contextPattern) {
  this.contextPattern = contextPattern;
}

TlsStubPlugin.prototype.apply = function(compiler) {
  var contextPattern = this.contextPattern;
  compiler.plugin('normal-module-factory', function(nmf) {
    nmf.plugin('before-resolve', function(result, callback) {
      if (!result){
        return callback();
      }
      if (/^tls$/.test(result.request)) {
        if (contextPattern.test(result.context)) {
          result.request = path.join(__dirname ,'../lib/browser/TLSStub.js');
        }
      }
      return callback(null, result);
    });
  });
};

module.exports = TlsStubPlugin;
