var util = require(__dirname+"/utils/main.js");
var Sequence = require(__dirname+"/sequence.js");
var Error = require(__dirname+"/error.js");

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
    })
  }
}

// operation = {type: 'between', index: 'foo'}
Selection.prototype.addOperation = function(operation) {
  this.operations.push(operation)
}

// Import methods from Sequence
var keys = Object.keys(Sequence.prototype);
for(var i=0; i<keys.length; i++) {
  (function(key) {
    Selection.prototype[key] = function() {
      var docs = [];
      for(var i=0; i<this.selection.length; i++) {
        docs.push(this.selection[i].doc)
      }
      var sequence = new Sequence(docs, this);
      return sequence[key].apply(sequence, arguments);
    }
  })(keys[i]);
}

Selection.prototype.get = function(index) {
  return this.selection[index];
}

Selection.prototype.setType = function(type) {
  this.type = type;
}
Selection.prototype.typeOf = function() {
  return this.type;
}
Selection.prototype.toSequence = function() {
  var result = new Sequence();
  for(var i=0; i<this.selection.length; i++) {
    result.push(this.selection[i].doc);
  }
  return result;
}
Selection.prototype.toSelection = function() {
  var result = new Selection();
  for(var i=0; i<this.selection.length; i++) {
    result.push(this.selection[i]);
  }
  return result;
}


Selection.prototype.toSelection = function() {
  var result = new Selection();
  for(var i=0; i<this.selection.length; i++) {
    result.push(this.selection[i]);
  }
  return result;
}


Selection.prototype.push = function(doc) {
  this.selection.push(doc);
  this.length++;
}
Selection.prototype.pop = function(doc) {
  this.length--;
  return this.selection.pop();
}

Selection.prototype.filter = function(filter, options, query, internalOptions) {
  var selection = new Selection([], this.table, {});

  if (options.default === undefined) {
    options.default = false;
  }
  
  if (util.isFunction(filter)) {
    var varId = util.getVarId(filter);
    query.frames.push(1);
    for(var i=0; i<this.selection.length; i++) {
      query.context[varId] = this.selection[i].doc;
      var filterResult;
      try {
        filterResult = query.evaluate(filter, query, internalOptions);
      }
      catch(err) {
        if (err.message.match(/^No attribute/)) {
          filterResult = options.default;
        }
        else {
          throw err;
        }
      }
      if (filterResult) { // TODO Should we check for a strict true here?
        selection.push(this.selection[i]);
      }
      delete query.context[varId];
    }
    query.frames.pop();
  }
  else {
    query.frames.push(1);
    var filterValue = query.evaluate(filter, query, internalOptions);
    if (typeof filterValue === 'function') {
      for(var i=0; i<this.selection.length; i++) {
        try {
          var filterResult = filterValue(util.toDatum(this.selection[i]))
        }
        catch(error) {
          throw new Error.ReqlRuntimeError(error.toString(), query.frames)
        }
        util.assertJavaScriptResult(filterResult, query);
        filterResult = util.revertDatum(filterResult);
        if (util.isTrue(filterResult)) {
          selection.push(this.selection[i]);
        }
      }
    }
    else if (util.isPlainObject(filterValue)) {
      for(var i=0; i<this.selection.length; i++) {
        if (util.filter(util.toDatum(this.selection[i]), util.toDatum(filterValue))) {
          selection.push(this.selection[i]);
        }
      }
    }
    else {
      for(var i=0; i<this.selection.length; i++) {
        if ((filterValue !== null) && (filterValue !== false)) {
          selection.push(this.selection[i]);
        }
      }
    }
    query.frames.pop();
  }
  
  return selection;
}

Selection.prototype.update = function(rawUpdate, options, query) {
  var result = util.writeResult();
  var primaryKey = this.table.options.primaryKey;
  var updateValue;

  var varId;
  for(var i=0; i<this.selection.length; i++) {
    util.mergeWriteResult(result, this.selection[i].update(rawUpdate, options, query))
  }
  return result;
}

Selection.prototype.replace = function(rawUpdate, options, query) {
  var result = util.writeResult();
  var primaryKey = this.table.options.primaryKey;
  var replaceValue;

  for(var i=0; i<this.selection.length; i++) {

    var temp = this.selection[i].replace(rawUpdate, options, query);
    util.mergeWriteResult(result, temp)
    //util.mergeWriteResult(result, this.selection[i].replace(rawUpdate, options, query))
  }
  return result;
}


Selection.prototype.delete = function(options, query) {
  var result = util.writeResult();
  for(var i=0; i<this.selection.length; i++) {
    util.mergeWriteResult(result, this.selection[i].delete(options, query));
  }
  return result;
}

Selection.prototype.skip = function(skip) {
  var result = new Selection([], this.table, {});
  for(var i=skip; i<this.selection.length; i++) {
    // TODO Should we also deep copy this.selection[i]
    result.push(this.selection[i]);
  }
  return result;
}
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
    // TODO Should we also deep copy this.selection[i]
    result.push(this.selection[i]);
  }
  return result;
}

Selection.prototype.orderBy = function(fields, options, query, internalOptions) {
  var hasIndex = false;
  if (typeof options.index === "string") {
    if (this.type !== 'TABLE_SLICE') {
      throw new Error.ReqlRuntimeError("Cannot use an index on a selection", query.frames);
    }
    else {
      // We have a table slice, so this.operations.length > 0
      if (this.operations[this.operations.length-1].index !== options.index) {
        query.frames.push(0);
        query.frames.push(0);
        throw new Error.ReqlRuntimeError('Cannot order by index `'+options.index+'` after calling '+this.operations[this.operations.length-1].operation.toUpperCase()+' on index `'+this.operations[this.operations.length-1].index+'`', query.frames);
      }
    }
  }
  if (options.index !== undefined) {
    var index = options.index;
    var order = 'ASC'
    if (util.isAsc(index)) {
      order= 'ASC';
      index = options.index.value;
    }
    else if (util.isDesc(index)) {
      order= 'DESC';
      index = options.index.value;
    }
    query.frames.push('index')
    util.assertType(index, 'STRING', query);
    query.frames.pop();

    if (this.table.indexes[index] === undefined) {
      throw new Error.ReqlRuntimeError('Index `'+index+'` was not found on table `'+this.db+'.'+this.name+'`', query.frames)
    }
  }


  var selection = new Selection([], this.table, {});
  if ((Array.isArray(options.index) || (typeof options.index === "string"))
      && (fields.length === 0)){
    for(var i=0; i<this.operations; i++) {
      selection.addOperation(this.operations[i]);
    }
    selection.addOperation({
      operation: 'orderBy',
      index: options.index
    });
  }

  if (Array.isArray(options.index) || (typeof options.index === "string")) {
    if (this.table.indexes[options.index] === undefined) {
      throw new Error.ReqlRuntimeError('Index `'+options.index+'` was not found on table `'+this.table.db+'.'+this.table.name+'`', query.frames)
    }
  }

  for(var i=0; i<this.length; i++) {
    if (options.index !== undefined) {
      // We drop null and plain objects
      try {
        var varId = util.getVarId(this.table.indexes[index].fn);
        query.context[varId] = this.get(i);
        var indexValue = query.evaluate(this.table.indexes[index].fn, query, internalOptions);
        delete query.context[varId];

        if ((indexValue !== null) || (util.isPlainObject(indexValue) && (indexValue.$reql_type$ === undefined))) {
          selection.push(this.get(i));
        }
      }
      catch(err) {
        if (err.message.match(/^No attribute/)) {
          // We also drop documents where a non existent error was thrown
        }
        else {
          throw err;
        }
      }
    }
    else {
      selection.push(this.get(i));
    }
  }
  if (options.index !== undefined) {
    hasIndex = true;
    fields.unshift(this.table.indexes[index].fn);
  }
  selection._orderBy(fields, hasIndex, query, internalOptions);
  return selection;
}

Selection.prototype._orderBy = function(fields, hasIndex, query, internalOptions) {
  this.selection.sort(function(left, right) {
    var leftValue, rightValue;

    for(var i=0; i<fields.length; i++) {
      var field = fields[i];
      var order = "ASC";
      if (util.isRawAsc(field)) {
        field = field[1][0];
        order = "ASC";
      }
      else if (util.isRawDesc(field)) {
        field = field[1][0];
        order = "DESC";
      }
      if (util.isFunction(field)) {
        var varId = util.getVarId(field);
        query.context[varId] = left;
        leftValue = query.evaluate(field, internalOptions);
        delete query.context[varId];

        query.context[varId] = right;
        rightValue = query.evaluate(field, internalOptions);
        delete query.context[varId];
      }
      else {
        field = query.evaluate(field, internalOptions);
        if (typeof field === 'function') {
          if ((hasIndex) && (i !== 0)) {
            query.frames.push(i);
          }
          else if (!hasIndex) {
            query.frames.push(i+1);
          }

          leftValue = field(util.toDatum(left));
          util.assertJavaScriptResult(leftValue, query);
          leftValue = util.revertDatum(leftValue);
          rightValue = field(util.toDatum(right));
          util.assertJavaScriptResult(rightValue, query);
          rightValue = util.revertDatum(rightValue);
          if ((!hasIndex) || (i !== 0)) {
            query.frames.pop();
          }
        }
        else {
          if ((hasIndex) && (i !== 0)) {
            query.frames.push(i);
          }
          else if (!hasIndex) {
            query.frames.push(i+1);
          }
          util.assertType(field, "STRING", query);
          if ((!hasIndex) || (i !== 0)) {
            query.frames.pop();
          }
          leftValue = util.getBracket(left, field, query);
          rightValue = util.getBracket(right, field, query);
        }
      }


      if (util.gt(leftValue, rightValue)) {
        if (order === "ASC") {
          return 1;
        }
        else {
          return -1;
        }
      }
      else if (util.lt(leftValue, rightValue)) {
        if (order === "ASC") {
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
}

Selection.prototype.distinct = function(options, query, internalOptions) {
  var result;
  if (options.index !== undefined) {
    result = new Sequence([]);

    if (this.type !== 'TABLE_SLICE') {
      throw new Error.ReqlRuntimeError("Cannot use an index on a selection", query.frames);
    }

    if (this.operations[this.operations.length-1].index !== options.index) {
      throw new Error.ReqlRuntimeError('Cannot use between index `'+this.operations[this.operations.length-1].index+'` after calling '+this.operations[this.operations.length-1].operation.toUpperCase()+' on index `'+this.index+'`', query.frames);
    }

    for(var i=0; i<this.length; i++) {
      var varId = util.getVarId(this.table.indexes[options.index].fn)
      query.context[varId] = this.get(i);

      if (this.table.indexes[options.index].multi === true) {
        //TODO Test
        valuesIndex = query.evaluate(this.table.indexes[options.index].fn, query, internalOptions);
        for(var k=0; k<valuesIndex.length; k++) {
          valueIndex = valuesIndex[k];
          result.push(valueIndex);
        }
      }
      else {
        valueIndex = query.evaluate(this.table.indexes[options.index].fn, query, internalOptions);
        result.push(valueIndex);
      }
    }
  }
  else {
    result = this.toSequence();
  }
  var result = result.distinct();
  return result
}
Selection.prototype.between = function(left, right, options, query, internalOptions) {
  //TODO Plumbing for changes?
  if (this.type !== 'TABLE_SLICE') {
    throw new Error.ReqlRuntimeError("Cannot use an index on a selection", query.frames);
  }

  if ((util.isPlainObject(options) === false) || (typeof options.index !== 'string')) {
    options.index = this.operations[this.operations.length-1].index;
  };

  for(var i=0; i<this.operations.length; i++) {
    if (this.operations[i].operation === 'between') {
      // This is weird, did we miss a frame before?
      query.frames.push(0);
      query.frames.push(0);
      throw new Error.ReqlRuntimeError('Cannot perform multiple BETWEENs on the same table', query.frames)
    }
  }

  // We now have a TABLE_SLICE and therefore this.operations.length > 0
  if (this.operations[this.operations.length-1].index !== options.index) {
    throw new Error.ReqlRuntimeError('Cannot use between index `'+this.operations[this.operations.length-1].index+'` after calling '+this.operations[this.operations.length-1].operation.toUpperCase()+' on index `'+this.index+'`', query.frames);
  }



  var selection = new Selection([], this.table, {
    type: this.type
  });
  for(var i=0; i<this.length; i++) {
    var varId = util.getVarId(this.table.indexes[options.index].fn)
    query.context[varId] = this.get(i);

    if (this.table.indexes[options.index].multi === true) {
      //TODO Test
      valuesIndex = query.evaluate(this.table.indexes[options.index].fn, query, internalOptions);

      for(var k=0; k<valuesIndex.length; k++) {
        valueIndex = valuesIndex[k];
        util.between(selection, this.get(i), valueIndex, left, right, options);
      }
    }
    else {
      valueIndex = query.evaluate(this.table.indexes[options.index].fn, query, internalOptions);
      util.between(selection, this.get(i), valueIndex, left, right, options);
    }
  }
  return selection;
}

Selection.prototype.toDatum = function() {
  var result = [];
  for(var i=0; i<this.selection.length; i++) {
    result.push(util.toDatum(this.selection[i]));
  }
  return result;
}

module.exports = Selection;
