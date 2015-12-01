var Promise = require("bluebird");
var Sequence = require(__dirname+"/sequence.js");

function Group(groups) {
  this.groups = groups || new Sequence(); // {group : <group>, reduction: <reduction>}
  // this.type?
}
module.exports = Group;

var util = require(__dirname+"/utils.js");

// Import methods from Sequence
var keys = Object.keys(Sequence.prototype);
for(var i=0; i<keys.length; i++) {
  (function(key) {
    Group.prototype[key] = function() {
      var args = arguments;
      return Promise.map(this.groups.sequence, function(group) {
        var p = group.reduction[key].apply(group.reduction, args);
        return p.then(function(result) {
          return [group.group, result];
        });
      }, {concurrency: 1}).then(function(results) {
        var groups = new Group();
        for(var i=0; i<results.length; i++) {
          groups.addGroup(results[i][0], results[i][1]);
        }
        return groups;
      });
    };
  })(keys[i]);
}

Group.fromSequence = function(sequence, fieldOrFns, options, query, internalOptions) {
  var sequence = sequence.sequence.slice(0);
  var groups = new Group();
  return Promise.map(sequence, function(doc) {
    return Promise.map(fieldOrFns, function(fieldOrFn, index) {
      if (!util.isFunction(fieldOrFn)) {
        query.frames.push(index+1);
        util.assertType(fieldOrFn, "STRING", query);
        fieldOrFn = util.generateFieldFunction(fieldOrFn);
      }
      // fieldOrFn is not a ReQL function
      var varId = util.getVarId(fieldOrFn);
      query.context[varId] = doc;
      query.frames.push(1);
      return query.evaluate(fieldOrFn, internalOptions).then(function(subGroup) {
        query.frames.pop();
        delete query.context[varId];
        return subGroup;
      }).catch(function(err) {
        delete query.context[varId];
        if (err.message.match(/^No attribute `/) !== null) {
          return null;
        }
        throw err;
      });
    }, {concurrency: 1}).then(function(group) {
      if (group.length === 1) {
        group = group[0];
      }
      else {
        group = new Sequence(group);
      }
      groups.push(group, doc);
      return groups;
    });
  }, {concurrency: 1}).then(function() {
    return groups;
  });
};

Group.fromTable = function(table, fieldOrFns, options, query, internalOptions) {
  var self = table;
  var keys = Object.keys(self.documents);
  var documents = []; // Keep a copy of the documents as we asynchronously iterate
  for(var i=0; i<keys.length; i++) {
    documents.push(self.documents[keys[i]]);
  }

  var groups = new Group();
  return Promise.map(documents, function(doc) {
    var promise;
    if (util.isPlainObject(options) && (typeof options.index === 'string')) {
      var fieldOrFn = self.indexes[options.index].fn;

      var varId = util.getVarId(fieldOrFn);
      query.context[varId] = doc;
      promise = query.evaluate(fieldOrFn, internalOptions).bind({}).then(function(subGroup) {
        delete query.context[varId];
        return subGroup;
      }).catch(function(err) {
        // RethinkDB skips these values...
        // Save the error for now
        this.err = err;
        return undefined;
      });
    }
    else {
      promise = Promise.resolve();
    }
    return promise.then(function(indexGroup) {
      if (this.err) {
        return; // The index returned an error, we skip this document
      }
      return Promise.map(fieldOrFns, function(fieldOrFn) {
        if (typeof fieldOrFn === "string") {
          fieldOrFn = util.generateFieldFunction(fieldOrFn);
        }
        // fieldOrFn is not a ReQL function
        var varId = util.getVarId(fieldOrFn);
        query.context[varId] = doc;
        return query.evaluate(fieldOrFn, internalOptions).then(function(subGroup) {
          return subGroup;
        }).catch(function() {
          // We just return the null group in case of an error
          delete query.context[varId];
          return null;
        });
      }, {concurrency: 1}).then(function(group) {
        if (indexGroup !== undefined) {
          group.unshift(indexGroup);
        }
        if (group.length === 1) {
          group = group[0];
        }
        else {
          group = new Sequence(group);
        }
        groups.push(group, doc);
        return groups;
      });
    });
  }, {concurrency: 1}).then(function() {
    return groups;
  });
};

Group.fromChanges = function(changes, fieldOrFns, options, query, internalOptions) {
  // This will throw
  return changes.group(fieldOrFns, options, query, internalOptions);
}

Group.prototype.toSequence = function() {
  return this;
};
Group.prototype.typeOf = function() {
  if ((this.groups.length > 0)
      && (this.groups.get(0).reduction instanceof Sequence)) {
    return "GROUPED_STREAM";
  }
  else {
    return "GROUPED_DATA";
  }
};

Group.prototype.toDatum = function() {
  var result = {
    "$reql_type$": "GROUPED_DATA",
    "data": []
  };
  for(var i=0; i<this.groups.length; i++) {
    result.data.push([
      util.toDatum(this.groups.get(i).group),
      util.toDatum(this.groups.get(i).reduction)
    ]);
  }
  return result;
};
Group.prototype.ungroup = function() {
  var result = new Sequence();
  for(var i=0; i<this.groups.length; i++) {
    result.push(this.groups.get(i));
  }
  return result;
};

Group.prototype.addGroup = function(group, value) {
  this.groups.push({
    group: group,
    reduction: value
  });
};
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
};
