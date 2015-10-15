function Desc(value) {
  this.value = value; //TODO Remove when done debugging
}
module.exports = Desc;

var Error = require("./error.js");

Desc.prototype.toJSON = function() {
  throw new Error.ReqlRuntimeError('DESC may only be used as an argument to ORDER_BY', []);
};

Desc.prototype.toDatum = function(query) {
  //TODO check everywhere that an instance of DESC is not returned.
  throw new Error.ReqlRuntimeError('DESC may only be used as an argument to ORDER_BY', query.frames);
};

