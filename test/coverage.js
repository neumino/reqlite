var protodef = require('./../lib/protodef.js');
var keys = Object.keys(protodef.Term.TermType);

var fs = require('fs');

// Test that the term appears somewhere in the file, which find terms that were not implemented
describe('coverage.js', function(){
  it('All terms should be present in query.js', function (done) {
    var str = fs.readFileSync(__dirname+'/../lib/query.js', 'utf8');
    var ignoredKeys = { // not implemented since we use the JSON protocol
      DATUM: true,
      MAKE_OBJ: true,
      BETWEEN_DEPRECATED: true,
    };
    var missing = [];
    for(var i=0; i<keys.length; i++) {
      if (ignoredKeys[keys[i]] === true) {
        continue;
      }
      if (str.match(new RegExp(keys[i])) === null) {
        missing.push(keys[i]);
      }
    }

    if (missing.length > 0) {
      done(new Error('Some terms were not found: '+JSON.stringify(missing)));
    }
    else {
      done();
    }
  });
});
