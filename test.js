var Promise = require('bluebird');

  new Promise(function(resolve, reject) {
    resolve(2);
  }).then(function(foo) {
    return 3
  }).finally(function() {
    return 4
  }).then(function(result) {
    console.log(result);
  });
