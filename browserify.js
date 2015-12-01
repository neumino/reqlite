var browserify = require('browserify');
var fs = require('fs');

var REQUIRE_FILES = [
	'asc.js',
  'changes.js',
  'connection.js',
  'constants.js',
  'database.js',
  'date.js',
  'desc.js',
  'document.js',
  'error.js',
  'geo.js',
  'group.js',
  'literal.js',
  'local_connection.js',
  'maxval.js',
  'minval.js',
  'missing_doc.js',
  'node.js',
  'protodef.js',
  'query.js',
  'selection.js',
  'sequence.js',
  'table.js',
  'utils.js'
];

var b = browserify('./lib/')
b.add('./lib/index.js')
for(var i=0; i<REQUIRE_FILES.length; i++) {
  b.require('./lib/'+REQUIRE_FILES[i], {expose: '/lib/'+REQUIRE_FILES[i]})
}
b.require('./lib/index.js', {expose: 'reqlite'})
b.bundle(function(err, result) {
  if (err) {
    console.log(err);
    return;
  }
  fs.writeFileSync('./browser/reqlite.js', result);
});
