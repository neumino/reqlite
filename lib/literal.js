var Error = require(__dirname+"/error.js");

function Literal(value) {
  this.value = value;
  this.literal = true; //TODO Remove when done debugging
}

Literal.prototype.toDatum = function() {
  return this.value;
}

module.exports = Literal;
