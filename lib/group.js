var Promise = require("bluebird");
var Sequence = require(__dirname+"/sequence.js");

function Group(groups) {
  this.groups = groups || new Sequence(); // {group : <group>, reduction: <reduction>}
  // this.type?
}
module.exports = Group;

var util = require(__dirname+"/utils/main.js");

// Import methods from Sequence
var keys = Object.keys(Sequence.prototype);
for(var i=0; i<keys.length; i++) {
  (function(key) {
    Group.prototype[key] = function() {
      var self = this;
      var args = arguments;
      return Promise.map(this.groups.sequence, function(group) {
        if (group.reduction[key].apply === undefined) {
          console.log('+++', key);
        }
        var p = group.reduction[key].apply(group.reduction, args);
        if (p.then === undefined) {
          console.log('---', key);
        }
        return p.then(function(result) {
          return [group.group, result]
        })
      }, {concurrency: 1}).then(function(results) {
        var groups = new Group();
        for(var i=0; i<results.length; i++) {
          groups.addGroup(results[i][0], results[i][1]);
        }
        return groups;
      });
    }
  })(keys[i]);
}

Group.prototype.toSequence = function() {
  return this;
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
