var Error = require(__dirname+"/error.js");

function Minval() {
  this.minval = true; //TODO Remove when done debugging
}

Minval.prototype.toJSON = function() {
  throw new Error.ReqlRuntimeError('Cannot convert `r.minval` to JSON', query.frames)
}

module.exports = Minval;
