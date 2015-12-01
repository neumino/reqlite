function Minval() {
  this.minval = true; //TODO Remove when done debugging
}
module.exports = Minval;

var Error = require(__dirname+"/error.js");

Minval.prototype.toJSON = function() {
  throw new Error.ReqlRuntimeError('Cannot convert `r.minval` to JSON' /* ?? , query.frames */);
};
