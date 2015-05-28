var Promise = require('bluebird');

Promise.map([100,200,150,400,50], function(value) {
 return new Promise(function(r, rr) {
   setTimeout(function() { return r(value) }, value)
 })
}, {concurrency: 10}).then(function(result) {
  console.log(result);
})

