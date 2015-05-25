var Promise = require('bluebird');

Promise.resolve(1).bind({}).then(function(foo) {
  this.foo = 1;
  return new Promise(function(resolve, reject) {
    resolve(2);
  }).bind({}).then(function(foo) {
    this.foo = 2;
  });
}).then(function(result) {
  console.log(this.foo);
});
