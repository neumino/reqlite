function Selection(selection, table, args) {
  args = args || {};

  this.selection = selection || [];
  this.length = this.selection.length;
  this.table = table;

  if (args.type === undefined) {
    this.type = "SELECTION<STREAM>";
  }
  else {
    this.type = args.type;
  }
  this.operations = [];

  if ((args.operation !== undefined) && (args.index !== undefined)) {
    this.operations.push({
      operation: args.operation,
      index: args.index,
      args: args.args,
      options: args.options
    });
  }
}
module.exports = Selection;

var util = require(__dirname+"/utils.js");
var Sequence = require(__dirname+"/sequence.js");
var Error = require(__dirname+"/error.js");
var Promise = require("bluebird");


// operation = {type: 'between', index: 'foo'}
Selection.prototype.addOperation = function(operation) {
  this.operations.push(operation);
};

// Import methods from Sequence
var keys = Object.keys(Sequence.prototype);
for(var i=0; i<keys.length; i++) {
  (function(key) {
    Selection.prototype[key] = function() {
      var docs = [];
      for(var i=0; i<this.selection.length; i++) {
        docs.push(this.selection[i].doc);
      }
      var sequence = new Sequence(docs, this);
      return sequence[key].apply(sequence, arguments);
    };
  })(keys[i]);
}

Selection.prototype.get = function(index) {
  return this.selection[index];
};

Selection.prototype.setType = function(type) {
  this.type = type;
};
Selection.prototype.typeOf = function() {
  return this.type;
};
Selection.prototype.toSequence = function() {
  var result = new Sequence();
  for(var i=0; i<this.selection.length; i++) {
    result.push(this.selection[i].doc);
  }
  return result;
};
Selection.prototype.toSelection = function() {
  var result = new Selection();
  for(var i=0; i<this.selection.length; i++) {
    result.push(this.selection[i]);
  }
  return result;
};


Selection.prototype.toSelection = function() {
  var result = new Selection();
  for(var i=0; i<this.selection.length; i++) {
    result.push(this.selection[i]);
  }
  return result;
};


Selection.prototype.push = function(doc) {
  this.selection.push(doc);
  this.length++;
};
Selection.prototype.pop = function() {
  this.length--;
  return this.selection.pop();
};

Selection.prototype.filter = function(filter, options, query, internalOptions) {
  var self = this;
  var selection = new Selection([], self.table, {});

  if (options.default === undefined) {
    options.default = false;
  }

  if (util.isFunction(filter)) {
    var varId = util.getVarId(filter);
    query.frames.push(1);
    return Promise.reduce(self.selection, function(result, context) {
      query.context[varId] = context;
      return query.evaluate(filter, query, internalOptions).bind({}).then(function(filterResult) {
        delete query.context[varId];
        if (util.isTrue(filterResult)) {
          result.push(context);
        }
        return result;
      }).catch(function(err) {
        delete query.context[varId];
        if (err.message.match(/^No attribute/)) {
          if (util.isTrue(options.default)) {
            result.push(context);
          }
        }
        else {
          throw err;
        }
        return result;
      });
    }, selection).then(function(result) {
      return result;
    });
  }
  else {
    query.frames.push(1);
    return query.evaluate(filter, query, internalOptions).then(function(filterValue) {
      if (typeof filterValue === 'function') {
        for(var i=0; i<self.selection.length; i++) {
          try {
            var filterResult = filterValue(util.toDatum(self.selection[i]));
          }
          catch(error) {
            throw new Error.ReqlRuntimeError(error.toString(), query.frames);
          }
          util.assertJavaScriptResult(filterResult, query);
          filterResult = util.revertDatum(filterResult);
          if (util.isTrue(filterResult)) {
            selection.push(self.selection[i]);
          }
        }
      }
      else if (util.isPlainObject(filterValue)) {
        for(var i=0; i<self.selection.length; i++) {
          if (util.filter(util.toDatum(self.selection[i]), util.toDatum(filterValue))) {
            selection.push(self.selection[i]);
          }
        }
      }
      else {
        for(var i=0; i<self.selection.length; i++) {
          if ((filterValue !== null) && (filterValue !== false)) {
            selection.push(self.selection[i]);
          }
        }
      }
      query.frames.pop();
      return selection;
    });
  }
};

Selection.prototype.update = function(rawUpdate, options, query, internalOptions) {
  return Promise.map(this.selection, function(doc) {
    return doc.update(rawUpdate, options, query, internalOptions);
  }, {concurrency: 1}).then(function(mergedResults) {
    var result = util.writeResult();
    for(var i=0; i<mergedResults.length; i++) {
      util.mergeWriteResult(result, mergedResults[i]);
    }
    return result;
  });
};

Selection.prototype.replace = function(rawReplace, options, query, internalOptions) {
  return Promise.map(this.selection, function(doc) {
    return doc.replace(rawReplace, options, query, internalOptions);
  }, {concurrency: 1}).then(function(mergedResults) {
    var result = util.writeResult();
    for(var i=0; i<mergedResults.length; i++) {
      util.mergeWriteResult(result, mergedResults[i]);
    }
    return result;
  });

};


Selection.prototype.delete = function(options, query) {
  return Promise.map(this.selection, function(doc) {
    return doc.delete(options, query);
  }, {concurrency: 1}).then(function(mergedResults) {
    var result = util.writeResult();
    for(var i=0; i<mergedResults.length; i++) {
      util.mergeWriteResult(result, mergedResults[i]);
    }
    return result;
  });

};

Selection.prototype.skip = function(skip) {
  var result = new Selection([], this.table, {});
  for(var i=skip; i<this.selection.length; i++) {
    result.push(this.selection[i]);
  }
  return result;
};
Selection.prototype.limit = function(limit) {
  var result;
  if ((this.type === 'TABLE_SLICE')
      && (this.operations.length > 0)
      && (this.operations[this.operations.length-1].operation === 'orderBy')) {
    result = new Selection([], this.table, {
      type: 'TABLE_SLICE'
    });
    for(var i=0; i<this.operations.length; i++) {
      result.operations.push(this.operations[i]);
    }
    if (util.isPlainObject(result.operations[result.operations.length-1].args)) {
      result.operations[result.operations.length-1].args = {};
    }
    result.operations[result.operations.length-1].args.limit = limit;
  }
  else {
    result = new Selection([], this.table, {});
  }
  for(var i=0; i<Math.min(limit,this.length); i++) {
    result.push(this.selection[i]);
  }
  return result;
};

Selection.prototype.orderBy = function(fields, options, query, internalOptions) {
  var self = this;
  var hasIndex = false;
  var index;
  var order;
  if (options.index !== undefined) {
    hasIndex = true;
    index = options.index;
    order = 'ASC';
    if (util.isAsc(index)) {
      order = 'ASC';
      index = options.index.value;
    }
    else if (util.isDesc(index)) {
      order = 'DESC';
      index = options.index.value;
    }
    if (this.type !== 'TABLE_SLICE') {
      return Promise.reject(new Error.ReqlRuntimeError("Cannot use an index on a selection", query.frames));
    }
    else {
      // We have a table slice, so this.operations.length > 0
      if (this.operations[this.operations.length-1].index !== index) {
        query.frames.push(0);
        query.frames.push(0);
        return Promise.reject(new Error.ReqlRuntimeError('Cannot order by index `'+index+'` after calling '+this.operations[this.operations.length-1].operation.toUpperCase()+' on index `'+this.operations[this.operations.length-1].index+'`', query.frames));
      }
    }
    query.frames.push('index');
    util.assertType(index, 'STRING', query);
    query.frames.pop();

    if (this.table.indexes[index] === undefined) {
      return Promise.reject(new Error.ReqlRuntimeError('Index `'+index+'` was not found on table `'+this.db+'.'+this.name+'`', query.frames));
    }

    if (order === 'DESC') {
      fields.unshift([74, [this.table.indexes[index].fn], {}]);
    }
    else {
      fields.unshift(this.table.indexes[index].fn);
    }
  }

  var selection = new Selection([], this.table, {
    type: this.type
  });
  //TODO Move in the constructor
  selection.operations = this.operations.slice(0);

  var mainPromise;
  var restrictedSelection = new Selection([], this.table, {});
  if (hasIndex === true) {
    var varId = util.getVarId(self.table.indexes[index].fn);
    mainPromise = Promise.map(self.selection, function(doc) {
      query.context[varId] = doc;
      return query.evaluate(self.table.indexes[index].fn, query, internalOptions).then(function(indexValue) {
        delete query.context[varId];
        if ((indexValue !== null) || (util.isPlainObject(indexValue) && (indexValue.$reql_type$ === undefined))) {
          restrictedSelection.push(doc);
        }
      }).catch(function(err) {
        if (err.message.match(/^No attribute/)) {
          // We also drop documents where a non existent error was thrown
        }
        else {
          throw err;
        }
      });
    }, {concurrency: 1}).then(function() {
      return restrictedSelection;
    });
  }
  else {
    mainPromise = Promise.resolve(this);
  }

  var sequenceToSort = new Sequence();
  return mainPromise.then(function(restrictedSelection) {
    return Promise.map(restrictedSelection.selection, function(ref) {
      var element = {
        original: ref,
        fields: new Sequence()
      };
      return util.computeFields(element, 0, fields, query, internalOptions);
    }, {concurrency: 1}).then(function(resolved) {
      for(var i=0; i<resolved.length; i++) {
        sequenceToSort.push(resolved[i]);
      }
      sequenceToSort._orderBy();
      for(var i=0; i<sequenceToSort.length; i++) {
        selection.push(sequenceToSort.get(i).original);
      }
      return selection;
    });
  });
};

Selection.prototype._orderBy = function() {
  this.selection.sort(function(left, right) {
    for(var i=0; i<left.fields.length; i++) {
      if (util.gt(left.fields.get(i).value, right.fields.get(i).value)) {
        if (left.fields.get(i).order === "ASC") {
          return 1;
        }
        else {
          return -1;
        }
      }
      if (util.lt(left.fields.get(i).value, right.fields.get(i).value)) {
        if (left.fields.get(i).order === "ASC") {
          return -1;
        }
        else {
          return 1;
        }
      }
    }
    return 0;
  });
  return this;
};

Selection.prototype.distinct = function(options, query, internalOptions) {
  var self = this;
  if (options.index !== undefined) {
    if (this.type !== 'TABLE_SLICE') {
      throw new Error.ReqlRuntimeError("Cannot use an index on a selection", query.frames);
    }

    if (this.operations[this.operations.length-1].index !== options.index) {
      throw new Error.ReqlRuntimeError('Cannot use between index `'+this.operations[this.operations.length-1].index+'` after calling '+this.operations[this.operations.length-1].operation.toUpperCase()+' on index `'+this.index+'`', query.frames);
    }

    var fn = this.table.indexes[options.index].fn;
    var varId = util.getVarId(fn);

    var mapped = new Sequence();
    return Promise.map(this.selection, function(doc) {
      query.context[varId] = doc;
      return query.evaluate(fn, query, internalOptions).then(function(next) {
        delete query.context[varId];
        if (self.table.indexes[options.index].multi === true) {
          for(var j=0; j<next.sequence.length; j++) {
            mapped.push(next.sequence[j]);
          }
        }
        else {
          mapped.push(next);
        }
        return mapped;
      }).catch(function(err) {
        if (err.message.match(/^No attribute `/) === null) {
          throw err;
        }
        return mapped;
        // else we just skip the non existence error
      });
    }, {concurrency: 1}).then(function() {
      // We can't use results here since we could have a multi index
      return mapped.distinct({}, query, internalOptions);
    });
  }
  else {
    return this.toSequence().distinct({}, query, internalOptions);
  }
};

Selection.prototype.between = function(left, right, options, query, internalOptions) {
  var self = this;
  if (this.type !== 'TABLE_SLICE') {
    throw new Error.ReqlRuntimeError("Cannot use an index on a selection", query.frames);
  }

  if ((util.isPlainObject(options) === false) || (typeof options.index !== 'string')) {
    options.index = this.operations[this.operations.length-1].index;
  }

  for(var i=0; i<this.operations.length; i++) {
    if (this.operations[i].operation === 'between') {
      // This is weird, did we miss a frame before?
      query.frames.push(0);
      query.frames.push(0);
      throw new Error.ReqlRuntimeError('Cannot perform multiple BETWEENs on the same table', query.frames);
    }
  }

  // We now have a TABLE_SLICE and therefore this.operations.length > 0
  if (this.operations[this.operations.length-1].index !== options.index) {
    throw new Error.ReqlRuntimeError('Cannot use between index `'+this.operations[this.operations.length-1].index+'` after calling '+this.operations[this.operations.length-1].operation.toUpperCase()+' on index `'+this.index+'`', query.frames);
  }



  var selection = new Selection([], this.table, {
    type: this.type
  });
  selection.operations = this.operations.slice(0);

  var varId = util.getVarId(self.table.indexes[options.index].fn);
  return Promise.reduce(self.selection, function(result, doc) {
    query.context[varId] = doc;
    return query.evaluate(self.table.indexes[options.index].fn, internalOptions).then(function(valueIndex) {
      delete query.context[varId];
      if (self.table.indexes[options.index].multi === true) {
        for(var k=0; k<valueIndex.length; k++) {
          util.between(result, doc, valueIndex.get(k), left, right, options);
        }
      }
      else {
        util.between(result, doc, valueIndex, left, right, options);
      }
      return result;
    }).catch(function(err) {
      //TODO Test this code path
      if (err.message.match(/^No attribute `/) === null) {
        throw err;
      }
    });
  }, selection);
};

Selection.prototype.toDatum = function() {
  var result = [];
  for(var i=0; i<this.selection.length; i++) {
    result.push(util.toDatum(this.selection[i]));
  }
  return result;
};
