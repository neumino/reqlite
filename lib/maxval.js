function Maxval() {
  this.maxval = true; //TODO Remove when done debugging
}
module.exports = Maxval;

var Error = require("./error.js");

Maxval.prototype.toJSON = function() {
  throw new Error.ReqlRuntimeError('Cannot convert `r.maxval` to JSON', query.frames)
}

