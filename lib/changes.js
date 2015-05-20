var util = require(__dirname+"/utils/main.js");
var Error = require(__dirname+"/error.js");
var protodef = require(__dirname+"/protodef.js");
var Sequence = require(__dirname+"/sequence.js");
var Query = require(__dirname+"/query.js");

function Changes(input, query) {
  if (!util.isDocument(input)) {
    util.toSequence(input, query);
    util.assertType(input, 'TABLE_SLICE', query);
  }

  var self = this;
  self.callbacks = [];
  self.notes = [protodef.Response.ResponseNote.SEQUENCE_FEED]
  self.buffer = []; // buffer of changes
  // Pre and post methods
  self.preMethods = [];
  self.postMethods = [];
  // self.table;
  // self.type;
  self.complete = false;

  // inRange is the full table!
  // self.inRange = [
  //  { doc: ..., valueIndex: ...}
  // ]

  if (util.isTable(input)) {
    self.table = input;
    self.type = 'table';
  }
  else if (util.isDocument(input)) {
    self.table = input.table;
    self.type = 'document';
    var primaryKey = self.table.options.primaryKey;
    if (util.isMissingDoc(input)) {
      self.primaryKeyValue = input.primaryKeyValue;
    }
    else {
      self.primaryKeyValue = input.doc[primaryKey];
    }
    self.preMethods.push(function(sequence) {
      var result = new Sequence();
      for(var i=0; i<sequence.length; i++) {
        if (((sequence.get(i).old_val != null) && (sequence.get(i).old_val[primaryKey] === self.primaryKeyValue))
            || ((sequence.get(i).new_val != null) && (sequence.get(i).new_val[primaryKey] === self.primaryKeyValue))) {
          result.push(sequence.get(i));
        }
      }
      return result;
    });
  }
  else if (util.isSelection(input)) {
    self.table = input.table;
    for(var i=0; i<input.operations.length; i++) {
      var operation = input.operations[i];

      // TODO Check if that's safe? Well, can we actually have more than one operation on a feed?
      if (operation.operation === 'between') {
        self.type = 'between';
      }
      else if (operation.operation === 'orderBy') {
        if (typeof operation.args.limit === 'number') {
          self.type = 'orderByLimited';
          self.limit = operation.args.limit;
        }
        else {
          self.type = 'orderBy';
        }
      }

      if (operation.operation === 'between') {
        self.preMethods.push(Changes.createMethodBetween(self, operation, query))
      }
      else if (operation.operation === 'orderBy') {
        self.preMethods.push(Changes.createMethodOrderBy(self, operation, query))
      }
    }
  }
  else {
    throw new Error.ReqlRuntimeError('not supported type', query.frames)
  }
  self.listener = Changes.createListener(self);
  self.table.on('change', self.listener)
}

// Import methods from Sequence
var keys = Object.keys(Sequence.prototype);
for(var i=0; i<keys.length; i++) {
  (function(key) {
    Changes.prototype[key] = function() {
      var _len = arguments.length;var _args = new Array(_len); for(var _i = 0; _i < _len; _i++) {_args[_i] = arguments[_i];}
      this.postMethods.push(function(sequence) {
        return sequence[key].apply(sequence, _args);
      });
      return this;
    }
  })(keys[i]);
}
Changes.prototype.skip = function(skip, query) {
  var self = this;
  self.skip = 0;
  self.postMethods.push(function(sequence) {
    while ((self.skip < skip) && (sequence.length > 0)) {
      sequence.shift();
      self.skip++;
    }
    return sequence;
  });
  return this;
}

Changes.prototype.limit = function(limit, query) {
  var self = this;
  if (limit === 0) {
    self.complete = 0; // Needed for getInitialResponse
  }
  self.returned = 0;
  self.postMethods.push(function(sequence) {
    var result = new Sequence();
    while ((self.returned < limit) && (sequence.length > 0)) {
      result.push(sequence.get(0));
      sequence.shift();
      self.returned++;
    }
    if (self.returned === limit) {
      self.complete = true;
    }
    return result;
  });
  return this;
}

Changes.prototype.slice = function(start, end, options, query) {
  if (options.left_bound === 'open') {
    this.skip(start+1);
  }
  else {
    this.skip(start);
  }
  if (options.right_bound === 'closed') {
    if (options.left_bound === 'open') {
      this.limit(end-start);
    }
    else {
      this.limit(end-start+1);
    }
  }
  else {
    if (options.left_bound === 'open') {
      this.limit(end-start-1);
    }
    else {
      this.limit(end-start);
    }
  }
  return this;
}

/* Methods not overridden are:
  // Test the one below
  Changes.prototype.slice
  Changes.prototype.map
  Changes.prototype.offsetsOf
  Changes.prototype.concatMap
  Changes.prototype.hasFields = 
  Changes.prototype.merge = 
  Changes.prototype.filter = 
  Changes.prototype.pluck = 
  Changes.prototype.without = 
  Changes.prototype.withFields = 
  Changes.prototype.getField = 
  Changes.prototype.intersects = 
  Changes.prototype.includes = 
  Changes.prototype.getBracket
  Changes.prototype.forEach
  Changes.prototype.join
*/


// Overwrite methods defined in sequence
Changes.prototype.throwTerminal = function(query) {
  this.table.removeListener('change', this.listener);
  throw new Error.ReqlRuntimeError('Cannot call a terminal (`reduce`, `count`, etc.) on an infinite stream (such as a changefeed)', query.frames)
}

Changes.prototype.sum = function(fieldOrFn, query, internalOptions) {
  query.frames.push(0); // No need to pop it, and we can't move that in throwTerminal -_-
  this.throwTerminal(query);
}

Changes.prototype.avg = function(fieldOrFn, query, internalOptions) {
  query.frames.push(0); // No need to pop it, and we can't move that in throwTerminal -_-
  this.throwTerminal(query);
}

Changes.prototype.min = function(fieldOrFn, options, query, internalOptions) {
  query.frames.push(0);
  this.throwTerminal(query);
}

Changes.prototype.max = function(fieldOrFn, options, query, internalOptions) {
  query.frames.push(0);
  this.throwTerminal(query);
}

Changes.prototype.group = function(fieldOrFns, options, query, internalOptions) {
  this.throwTerminal(query);
}

Changes.prototype.insertAt = function(position, value, query) {
  // Technically this should be unreachable,
  query.frames.push(0);
  this.throwTerminal(query);
}

Changes.prototype.changeAt = function(position, value, query) {
  query.frames.push(0);
  this.throwTerminal(query);
}

Changes.prototype.spliceAt = function(position, other, query) {
  query.frames.push(0);
  this.throwTerminal(query);
}

Changes.prototype.deleteAt = function(start, end, query) {
  query.frames.push(0);
  this.throwTerminal(query);
}

Changes.prototype.distinct = function(options, query) {
  query.frames.push(0);
  this.throwTerminal(query);
}

Changes.prototype.reduce = function(fn, query, internalOptions) {
  query.frames.push(0);
  this.throwTerminal(query);
}

Changes.prototype.setInsert = function(other, query) {
  query.frames.push(0);
  this.throwTerminal(query);
}

Changes.prototype.contains = function(predicates, query, internalOptions) {
  query.frames.push(0);
  this.throwTerminal(query);
}

Changes.prototype.setIntersection = function(other, query) {
  query.frames.push(0);
  this.throwTerminal(query);
}

Changes.prototype.setDifference = function(other, query) {
  query.frames.push(0);
  this.throwTerminal(query);
}

Changes.prototype.setUnion = function(other, query) {
  query.frames.push(0);
  this.throwTerminal(query);
}

Changes.prototype.sample = function(sample, query) {
  query.frames.push(0);
  this.throwTerminal(query);
}

Changes.prototype.count = function(predicate, query, internalOptions) {
  query.frames.push(0);
  this.throwTerminal(query);
}

Changes.prototype.nth = function(index, query) {
  query.frames.push(0);
  this.throwTerminal(query);
}

Changes.prototype.orderBy = function(fields, options, query, internalOptions) {
  query.frames.push(0);
  this.throwTerminal(query);
}


//TODO Test, RethinkDB seems to sometimes fail...
Changes.prototype.isEmpty = function(query) {
  return this.getInitialResponse().isEmpty();
}


// Meta method
Changes.prototype.getInitialResponse = function() {
  if (this.type === undefined) {
    throw new Error.ReqlRuntimeError('foo', this.frames)
  }
  if (this.type === 'document') {
    var doc = this.table.get(this.primaryKeyValue);
    if (util.isMissingDoc(doc)) {
      return new Sequence([{
        new_val: null
      }])
    }
    else {
      return new Sequence([{
        new_val: doc.doc
      }])
    }
  }
  else if (this.type === 'orderByLimited') {
    var result = new Sequence();
    for(var i=0; i<Math.min(this.limit, this.inRange.length); i++) {
      result.push({
        new_val: this.inRange.get(i).doc
      });
    }
    return result
  }
  else {
    return [];
  }
}

Changes.prototype.continue = function() {
}
Changes.prototype.stop = function() {
  this.table.removeListener('change', this.listener);
}

Changes.prototype.get = function() {
}

Changes.prototype.onNext = function(callback) {
  if (this.buffer.length > 0) {
    var change = this.buffer.shift();
    callback(change, this.notes);
  }
  else {
    this.callbacks.push(callback);
  }
}

Changes.createMethodBetween = function(self, operation, query) {
  //TODO We should probably not use query here...
  var left = operation.args.left;
  var right = operation.args.right;
  var options = operation.options;

  return function(sequence) {
    var result = new Sequence();
    for(var i=0; i<sequence.length; i++) {
      var change = sequence.get(i);
      var values = [];
      if (change.new_val != null) {
        values.push(change.new_val);
      }
      if (change.old_val != null) {
        values.push(change.old_val);
      }

      var keep = true;
      for(var i=0; i<values.length; i++) {
        var varId = util.getVarId(self.table.indexes[operation.index].fn);
        query.context[varId] = values[i];
        var valueIndex = query.evaluate(self.table.indexes[operation.index].fn, query, {});
        delete query.context[varId]
        if (options.left_bound === "closed") {
          if (util.lt(valueIndex, left)) {
            keep = false;
            break;
          }
        }
        else {
          if (util.le(valueIndex, left)) {
            keep = false;
            break;
          }
        }

        if (options.right_bound === "closed") {
          if (util.gt(valueIndex, right)) {
            keep = false;
            break;
          }
        }
        else {
          if (util.ge(valueIndex, right)) {
            keep = false;
            break;
          }
        }
      }
      if (keep === true) {
        result.push(change);
      }
    }
    return result;
  }
}

Changes.createMethodOrderBy = function(self, operation, query) {
  //TODO Use a tree and keep some references in a hashmap 
  var orderedSequence = self.table.orderBy([], {index: operation.index}, query, {});
  self.inRange = new Sequence();
  for(var i=0; i<orderedSequence.length; i++) {
    var doc = orderedSequence.get(i).doc;
    var varId = util.getVarId(self.table.indexes[operation.index].fn);
    query.context[varId] = doc;
    var valueIndex = query.evaluate(self.table.indexes[operation.index].fn, query, {});
    delete query.context[varId];
    self.inRange.push({
      doc: doc,
      valueIndex: valueIndex
    })
  }
  var limit;
  if (util.isPlainObject(operation.args) && (typeof operation.args.limit === 'number')) {
    limit = operation.args.limit;
    self.inRange.limit(limit);
  }
  return function(sequence) {
    var result = new Sequence();
    for(var i=0; i<sequence.length; i++) {
      var change = sequence.get(i);
      if (change.old_val === null) { // insert
        var varId = util.getVarId(self.table.indexes[operation.index].fn);
        query.context[varId] = change.new_val;
        var valueIndex = query.evaluate(self.table.indexes[operation.index].fn, query, {});
        delete query.context[varId];

        if (operation.args.order === 'DESC') {
          var index = self.inRange.length;
          while ((index > 0) && util.gt(valueIndex, self.inRange.get(index-1).valueIndex)) {
            index--;
          }
        }
        else { // ASC
          var index = self.inRange.length;
          while ((index > 0) && util.lt(valueIndex, self.inRange.get(index-1).valueIndex)) {
            index--;
          }
        }
        if (typeof limit === 'number') {
          if (index < limit) {
            self.inRange._pushAt(index, {
              doc: change.new_val,
              valueIndex: valueIndex
            })
            if (self.inRange.length > limit) {
              result.push({
                new_val: change.new_val,
                old_val: self.inRange.get(limit).doc, // +1 since we just inserted the new result
              });
            }
            else {
              result.push({
                new_val: change.new_val,
                old_val: null
              });
            }
          }
        }
        else {
          self.inRange._pushAt(index, {
            doc: change.new_val,
            valueIndex: valueIndex
          })
          result.push(change);
        }
      }
      else if (change.new_val === null) { // delete
        var primaryKey = self.table.options.primaryKey;
        var oldVal;

        var varId = util.getVarId(self.table.indexes[operation.index].fn);
        query.context[varId] = change.old_val;
        var valueIndex = query.evaluate(self.table.indexes[operation.index].fn, query, {});
        delete query.context[varId];

        if (operation.args.order === 'DESC') {
          var index = self.inRange.length;
          while (index > 0) {
            if (util.gt(valueIndex, self.inRange.get(index-1).valueIndex)) {
              index--;
            }
            else if (util.eq(change.old_val[primaryKey], self.inRange.get(index-1).doc[primaryKey])) {
              self.inRange._deleteAt(index-1);
              index--;
            }
            else {
              break;
            }
          }
        }
        else { // ASC
          var index = self.inRange.length;
          while (index > 0) {
            if (util.lt(valueIndex, self.inRange.get(index-1).valueIndex)) {
              index--;
            }
            else if (util.eq(change.old_val[primaryKey], self.inRange.get(index-1).doc[primaryKey])) {
              self.inRange._deleteAt(index-1);
              index--;
              break;
            }
            else {
              break;
            }
          }
        }
        if (typeof limit === 'number') {
          if (index < limit) {
            if (self.inRange.length >= limit) {
              result.push({
                new_val: self.inRange.get(limit-1).doc,
                old_val: change.old_val
              });
            }
            else {
              result.push({
                old_val: change.old_val
              });
            }
          }
        }
        else {
          result.push(change);
        }
      }
      else { // Update, new_val and old_val are defined
        var varId = util.getVarId(self.table.indexes[operation.index].fn);
        query.context[varId] = change.old_val;
        var valueIndex = query.evaluate(self.table.indexes[operation.index].fn, query, {});
        delete query.context[varId];

        if (operation.args.order === 'DESC') {
          var index = self.inRange.length;
          while (index > 0) {
            if (util.gt(valueIndex, self.inRange.get(index-1).valueIndex)) {
              index--;
            }
            else if (util.eq(change.old_val[primaryKey], self.inRange.get(index-1).doc[primaryKey])) {
              self.inRange._deleteAt(index-1);
              index--;
            }
            else {
              break;
            }
          }
        }
        else { // ASC
          var index = self.inRange.length;
          while (index > 0) {
            if (util.lt(valueIndex, self.inRange.get(index-1).valueIndex)) {
              index--;
            }
            else if (util.eq(change.old_val[primaryKey], self.inRange.get(index-1).doc[primaryKey])) {
              self.inRange._deleteAt(index-1);
              index--;
              break;
            }
            else {
              break;
            }
          }
        }
        var changeResult = {
          old_val: null
        }
        if (typeof limit === 'number') {
          if (index < limit) {
            changeResult = {
              old_val: change.old_val
            }
          }
        }
        else {
          changeResult = {
            old_val: change.old_val
          }
        }

        query.context[varId] = change.new_val;
        var valueIndex = query.evaluate(self.table.indexes[operation.index].fn, query, {});
        delete query.context[varId];

        if (operation.args.order === 'DESC') {
          var index = self.inRange.length;
          while ((index > 0) && util.gt(valueIndex, self.inRange.get(index-1).valueIndex)) {
            index--;
          }
        }
        else { // ASC
          var index = self.inRange.length;
          while ((index > 0) && util.lt(valueIndex, self.inRange.get(index-1).valueIndex)) {
            index--;
          }
        }
        if (typeof limit === 'number') {
          if (index < limit) {
            self.inRange._pushAt(index, {
              doc: change.new_val,
              valueIndex: valueIndex
            })
            changeResult.new_val = change.new_val;
          }
        }
        else {
          self.inRange._pushAt(index, {
            doc: change.new_val,
            valueIndex: valueIndex
          })
          changeResult.new_val = change.new_val;
        }
        result.push(changeResult);
      }
    }

    return result;
  }
}

Changes.createListener = function(self) {
  return function(change) {
    change.old_val = util.deepCopy(change.old_val);
    change.new_val = util.deepCopy(change.new_val);
    // Apply transformation
    try {
      var sequence = new Sequence([change], {})
      for(var i=0; i<self.preMethods.length; i++) {
        sequence = self.preMethods[i](sequence);
      }
      for(var i=0; i<self.postMethods.length; i++) {
        sequence = self.postMethods[i](sequence);
      }
      if (sequence.length > 0) {
        var response;
        if (self.complete === false) {
          response = {
            t: protodef.Response.ResponseType.SUCCESS_PARTIAL,
            r: util.toDatum(sequence),
            n: self.notes
          };
        }
        else {
          response = {
            t: protodef.Response.ResponseType.SUCCESS_SEQUENCE,
            r: util.toDatum(sequence),
            n: self.notes
          };

        }
        if (self.callbacks.length > 0) {
          var callback = self.callbacks.shift();
          callback(response, self.notes);
        }
        else {
          self.buffer.push(response);
        }
      }
    }
    catch(err) {
      var fullError = {
        t: err.type,
        r: [err.message],
        n: [],
        b: [] //TODO Need frame?
      }
      if (self.callbacks.length > 0) {
        var callback = self.callbacks.shift();
        callback(fullError, self.notes);
      }
      else {
        self.buffer.push(fullError);
      }
    }


    // Else nothing to send back
  }
}

// Terms

module.exports = Changes;
