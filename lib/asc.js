function Asc(value) {
  this.value = value;
}
module.exports = Asc;

var Error = require(__dirname+"/error.js");

Asc.prototype.toJSON = function() {
  throw new Error.ReqlRuntimeError('ASC may only be used as an argument to ORDER_BY', []);
};

Asc.prototype.toDatum = function(query) {
  //TODO check everywhere that an instance of ASC is not returned.
  throw new Error.ReqlRuntimeError('ASC may only be used as an argument to ORDER_BY', query.frames);
};

