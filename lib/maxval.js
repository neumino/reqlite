var Error = require(__dirname+"/error.js");

function Maxval() {
  this.maxval = true; //TODO Remove when done debugging
}

Maxval.prototype.toJSON = function() {
  throw new Error.ReqlRuntimeError('Cannot convert `r.maxval` to JSON', query.frames)
}

module.exports = Maxval;
