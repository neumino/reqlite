var Promise = require('bluebird');

  new Promise(function(resolve, reject) {
    throw new Error('foo');
  }).then(function(foo) {
    return 3
  }).catch(function() {
    console.log(1);
  }).catch(function(result) {
    console.log(2);
  });
