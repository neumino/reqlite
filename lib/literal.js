function Literal(value) {
  this.value = value;
  this.literal = true; //TODO Remove when done debugging
}
module.exports = Literal;

Literal.prototype.toDatum = function() {
  return this.value;
};

