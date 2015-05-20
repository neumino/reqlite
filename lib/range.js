var Error = require(__dirname+"/error.js");

function Range() {
  this.minval = true; //TODO Remove when done debugging
}

Range.prototype.toJSON = function(query) {
  throw new Error.ReqlRuntimeError('Cannot convert `r.minval` to JSON', query.frames)
}

Range.prototype.toDatum = function(query) {
  //TODO Implement streams...
  //throw new Error.ReqlRuntimeError('Cannot return', query.frames)
}

module.exports = Range;
