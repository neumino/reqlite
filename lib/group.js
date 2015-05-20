var util = require(__dirname+"/utils/main.js");
var Sequence = require(__dirname+"/sequence.js");

function Group(groups) {
  this.groups = groups || new Sequence(); // {group : <group>, reduction: <reduction>}
  // this.type?
}

// Import methods from Sequence
var keys = Object.keys(Sequence.prototype);
for(var i=0; i<keys.length; i++) {
  (function(key) {
    Group.prototype[key] = function() {
      var groups = new Group();
      for(var k=0; k<this.groups.length; k++) {
        groups.addGroup(this.groups.get(k).group, this.groups.get(k).reduction[key].apply(this.groups.get(k).reduction, arguments))
      }
      return groups;
    }
  })(keys[i]);
}

Group.prototype.typeOf = function() {
  if ((this.groups.length > 0)
      && (this.groups.get(0).reduction instanceof Sequence)) {
    return "GROUPED_STREAM";
  }
  else {
    return "GROUPED_DATA";
  }
}

Group.prototype.toDatum = function() {
  var result = [];
  for(var i=0; i<this.groups.length; i++) {
    result.push({
      group: util.toDatum(this.groups.get(i).group),
      reduction: util.toDatum(this.groups.get(i).reduction)
    })
  }
  return result;
}
Group.prototype.ungroup = function() {
  var result = new Sequence();
  for(var i=0; i<this.groups.length; i++) {
    result.push(this.groups.get(i));
  }
  return result;
}

Group.prototype.addGroup = function(group, value) {
  this.groups.push({
    group: group,
    reduction: value
  })
}
Group.prototype.push = function(group, value) {
  var found = false;
  for(var i=0; i<this.groups.length; i++) {
    if (util.eq(this.groups.get(i).group, group)) {
      this.groups.get(i).reduction.push(value);
      found = true;
      break;
    }
  }
  if (found === false) {
    this.groups.push({
      group: group,
      reduction: new Sequence([value])
    });
  }
  return this;
}
module.exports = Group;
