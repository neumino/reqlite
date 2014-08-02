var helper = require(__dirname+"/helper.js");
var Sequence = require(__dirname+"/sequence.js");

function Group(groups) {
    this.groups = groups || []; // {group : <group>, reduction: <reduction>}
}

// Import methods from Sequence
var keys = Object.keys(Sequence.prototype);
for(var i=0; i<keys.length; i++) {
    (function(key) {
        Group.prototype[key] = function() {
            for(var k=0; k<this.groups.length; k++) {
                this.groups[k].reduction[key].apply(this.groups[k].reduction, arguments);
            }
            return this;
        }
    })(keys[i]);
}

Group.prototype.toDatum = function() {
    var result = [];
    for(var i=0; i<this.groups.length; i++) {
        result.push({
            group: this.groups[i].group,
            reduction: this.groups[i].reduction.toDatum()
        })
    }
    return result;
}
Group.prototype.push = function(group, value) {
    var found = false;
    for(var i=0; i<this.groups.length; i++) {
        if (helper.eq(this.groups[i].group, group)) {
            this.groups[i].reduction.push(value);
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
