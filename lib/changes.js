var util = require(__dirname+"/utils.js");
var Error = require(__dirname+"/error.js");
var protodef = require(__dirname+"/protodef.js");
var Sequence = require(__dirname+"/sequence.js");
var Promise = require("bluebird");

function Changes(input, query, options) {
  if (!util.isDocument(input)) {
    util.toSequence(input, query);
    util.assertType(input, 'TABLE_SLICE', query);
  }

  var self = this;
  self.callbacks = [];
  self.notes = [protodef.Response.ResponseNote.SEQUENCE_FEED];
  self.buffer = []; // buffer of changes
  self.rawBuffer = []; // buffer of raw changes, not yet processed
  // Pre and post methods
  self.preMethods = [];
  self.postMethods = [];
  // self.table;
  // self.type;
  self.complete = false;
  // Lock on the change, to know if we can analyze the next change
  self.lock = false;
  self.options = options || {};

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
    self.notes = [protodef.Response.ResponseNote.ATOM_FEED];
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
      return Promise.resolve(result);
    });
  }
  else if (util.isSelection(input)) {
    self.table = input.table;
    for(var i=0; i<input.operations.length; i++) {
      var operation = input.operations[i];

      self.index = operation.index;
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
          self.limit = Infinity;
        }
      }

      if (operation.operation === 'between') {
        self.preMethods.push(Changes.createMethodBetween(self, operation, query));
      }
      else if (operation.operation === 'orderBy') {
        self.preMethods.push(Changes.createMethodOrderBy(self, operation, query));
      }
    }
  }
  else {
    throw new Error.ReqlRuntimeError('not supported type', query.frames);
  }
  //self.table.on('change', self.listener)
}

// Start the listener AFTER returning the initial response
Changes.prototype.startListener = function() {
  this.listener = Changes.createListener(this);
  // Start the listener when the query is completely evaluated to avoid leaking listeners
  this.table.on('change', this.listener);
};

// Import methods from Sequence
var keys = Object.keys(Sequence.prototype);
for(var i=0; i<keys.length; i++) {
  (function(key) {
    Changes.prototype[key] = function() {
      var _len = arguments.length;
      var _args = new Array(_len); for(var _i = 0; _i < _len; _i++) {_args[_i] = arguments[_i];}
      this.postMethods.push(function(sequence) {
        return sequence[key].apply(sequence, _args);
      });
      return Promise.resolve(this);
    };
  })(keys[i]);
}

Changes.prototype.skip = function(skip) {
  var self = this;
  var _skip = 0;
  self.postMethods.push(function(sequence) {
    while ((_skip < skip) && (sequence.length > 0)) {
      sequence.shift();
      _skip++;
    }
    return sequence;
  });
  return this;
};

Changes.prototype.limit = function(limit, query) {
  var self = this;
  if (limit === 0) {
    self.complete = 0; // Needed for getInitialResponse
  }
  var returned = 0;
  self.postMethods.push(function(sequence) {
    var result = new Sequence();
    while ((returned < limit) && (sequence.length > 0)) {
      result.push(sequence.get(0));
      sequence.shift();
      returned++;
    }
    if (returned === limit) {
      self.complete = true;
      self.stop();
    }
    return result;
  });
  return this;
};

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
};

Changes.prototype.map = function(fn, query, internalOptions) {
  var self = this;
  self.returned = 0;
  self.postMethods.push(function(sequence) {
    return Sequence.map([sequence], fn, query, internalOptions);
  });
  return this;
};

Changes.prototype.offsetsOf = function(predicate, query) {
  var self = this;
  var offset = 0;
  self.postMethods.push(function(sequence) {
    return sequence.offsetsOf(predicate, query).then(function(result) {
      for(var i=0; i<result.length; i++) {
        result.sequence[i] += offset;
      }
      return result;
    }).finally(function() {
      offset += sequence.length;
    });
  });
  return this;
};

// Overwrite methods defined in sequence
Changes.prototype.throwTerminal = function(query) {
  throw new Error.ReqlRuntimeError('Cannot call a terminal (`reduce`, `count`, etc.) on an infinite stream (such as a changefeed)', query.frames);
};

Changes.prototype.sum = function(fieldOrFn, query, internalOptions) {
  query.frames.push(0); // No need to pop it, and we can't move that in throwTerminal -_-
  this.throwTerminal(query);
};

Changes.prototype.avg = function(fieldOrFn, query, internalOptions) {
  query.frames.push(0); // No need to pop it, and we can't move that in throwTerminal -_-
  this.throwTerminal(query);
};

Changes.prototype.min = function(fieldOrFn, options, query, internalOptions) {
  query.frames.push(0);
  this.throwTerminal(query);
};

Changes.prototype.max = function(fieldOrFn, options, query, internalOptions) {
  query.frames.push(0);
  this.throwTerminal(query);
};

Changes.prototype.group = function(fieldOrFns, options, query, internalOptions) {
  this.throwTerminal(query);
};

Changes.prototype.insertAt = function(position, value, query) {
  // Technically this should be unreachable,
  query.frames.push(0);
  this.throwTerminal(query);
};

Changes.prototype.changeAt = function(position, value, query) {
  query.frames.push(0);
  this.throwTerminal(query);
};

Changes.prototype.spliceAt = function(position, other, query) {
  query.frames.push(0);
  this.throwTerminal(query);
};

Changes.prototype.deleteAt = function(start, end, query) {
  query.frames.push(0);
  this.throwTerminal(query);
};

Changes.prototype.distinct = function(options, query) {
  query.frames.push(0);
  this.throwTerminal(query);
};

Changes.prototype.reduce = function(fn, query, internalOptions) {
  query.frames.push(0);
  this.throwTerminal(query);
};

Changes.prototype.setInsert = function(other, query) {
  query.frames.push(0);
  this.throwTerminal(query);
};

Changes.prototype.contains = function(predicates, query, internalOptions) {
  query.frames.push(0);
  this.throwTerminal(query);
};

Changes.prototype.setIntersection = function(other, query) {
  query.frames.push(0);
  this.throwTerminal(query);
};

Changes.prototype.setDifference = function(other, query) {
  query.frames.push(0);
  this.throwTerminal(query);
};

Changes.prototype.setUnion = function(other, query) {
  query.frames.push(0);
  this.throwTerminal(query);
};

Changes.prototype.sample = function(sample, query) {
  query.frames.push(0);
  this.throwTerminal(query);
};

Changes.prototype.count = function(predicate, query, internalOptions) {
  query.frames.push(0);
  this.throwTerminal(query);
};

Changes.prototype.nth = function(index, query) {
  query.frames.push(0);
  this.throwTerminal(query);
};

Changes.prototype.orderBy = function(fields, options, query, internalOptions) {
  query.frames.push(0);
  this.throwTerminal(query);
};

Changes.prototype.forEach = function(fn, query, internalOptions) {
  query.frames.push(0);
  this.throwTerminal(query);
};

Changes.prototype.toSequence = function(other, query) {
  return this;
};


//TODO Test, RethinkDB seems to sometimes fail...
Changes.prototype.isEmpty = function(query) {
  return this.getInitialResponse().then(function(sequence) {
    return sequence.isEmpty();
  })
};

// Meta method
Changes.prototype.getInitialResponse = function(query) {
  // TODO Add tests around returnStates
  var self = this;
  if (self.type === undefined) {
    throw new Error.ReqlRuntimeError('TODO', self.frames);
  }

  if (self.type === 'document') {
    return self.table.get(self.primaryKeyValue).then(function(doc) {
      var result = new Sequence();
      if (self.options.include_initial === true) {
        if (self.options.include_states === true) {
          result.push({
            state: 'initializing'
          })
        }

        if (util.isMissingDoc(doc)) {
          result.push({
            new_val: null
          });
        }
        else {
          result.push({
            new_val: doc.doc
          });
        }
      }

      if (self.options.include_states === true) {
        result.push({
          state: 'ready'
        })
      }
      return result;
    });
  }
  else if ((self.type === 'orderByLimited') || (self.type === 'orderBy')) {
    return self.table.orderBy([], {index: self.index}, query, {}).then(function(orderedSequence) {
      self.inRange = new Sequence();
      return Promise.reduce(orderedSequence.selection, function(result, doc) {
        var varId = util.getVarId(self.table.indexes[self.index].fn);
        query.context[varId] = doc;
        return query.evaluate(self.table.indexes[self.index].fn, query, {}).then(function(valueIndex) {
          delete query.context[varId];
          result.push({
            doc: doc,
            valueIndex: valueIndex // TODO: Filter the errors
          });
          return result;
        });
      }, self.inRange);
    }).then(function(inRange) {
      var result = new Sequence();
      if ((self.options.include_initial === true) &&
          (self.type === 'orderByLimited')) {

        if (self.options.include_states === true) {
          result.push({
            state: 'initializing'
          })
        }
        for(var i=0; i<Math.min(self.limit, inRange.length); i++) {
          result.push({
            new_val: inRange.get(i).doc
          });
        }
      }
      if (self.options.include_states === true) {
        result.push({
          state: 'ready'
        })
      }
      return result;
    });
  }
  return Promise.resolve(new Sequence());
};

Changes.prototype.continue = function() {
};
Changes.prototype.stop = function() {
  this.table.removeListener('change', this.listener);
};

Changes.prototype.get = function() {
};

Changes.prototype.onNext = function(callback) {
  if (this.buffer.length > 0) {
    var change = this.buffer.shift();
    callback(change, this.notes);
  }
  else {
    this.callbacks.push(callback);
  }
};

Changes.createMethodBetween = function(self, operation, query) {
  //TODO We should probably not use query here...
  var left = operation.args.left;
  var right = operation.args.right;
  var options = operation.options;

  return function(sequence) {
    return Promise.reduce(sequence.sequence, function(result, element) {
      var change = element;
      var values = [];
      if (change.new_val != null) {
        values.push(change.new_val);
      }
      if (change.old_val != null) {
        values.push(change.old_val);
      }

      return Promise.reduce(values, function(keep, value) {
        var varId = util.getVarId(self.table.indexes[operation.index].fn);
        query.context[varId] = value;
        return query.evaluate(self.table.indexes[operation.index].fn, query, {}).then(function(valueIndex) {
          delete query.context[varId];
          if (options.left_bound === "closed") {
            if (util.lt(valueIndex, left)) {
              return false;
            }
          }
          else {
            if (util.le(valueIndex, left)) {
              return false;
            }
          }

          if (options.right_bound === "closed") {
            if (util.gt(valueIndex, right)) {
              return false;
            }
          }
          else {
            if (util.ge(valueIndex, right)) {
              return false;
            }
          }
          return keep;
        });
      }, true).then(function(keep) {
        if (keep === true) {
          result.push(change);
        }
        return result;
      });
    }, new Sequence());
  };
};

Changes.createMethodOrderBy = function(self, operation, query) {
  //TODO Use a tree and keep some references in a hashmap
  //TODO Refactor...
  return function(sequence) {
    return Promise.reduce(sequence.sequence, function(result, change) {
      if (self.inRange == null) {
        // self.inRange can be null if we do not have returnInitial: true
        self.inRange = new Sequence();
      }
      if (change.old_val === null) { // insert
        var varId = util.getVarId(self.table.indexes[operation.index].fn);
        query.context[varId] = change.new_val;
        return query.evaluate(self.table.indexes[operation.index].fn, query, {}).then(function(valueIndex) {
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
          if (isFinite(self.limit)) {
            if (index < self.limit) {
              self.inRange._pushAt(index, {
                doc: change.new_val,
                valueIndex: valueIndex
              });
              if (self.inRange.length > self.limit) {
                result.push({
                  new_val: change.new_val,
                  old_val: self.inRange.get(self.limit).doc, // +1 since we just inserted the new result
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
            });
            result.push(change);
          }
          return result;
        });
      }
      else if (change.new_val === null) { // delete
        var primaryKey = self.table.options.primaryKey;

        var varId = util.getVarId(self.table.indexes[operation.index].fn);
        query.context[varId] = change.old_val;
        return query.evaluate(self.table.indexes[operation.index].fn, query, {}).then(function(valueIndex) {
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
          if (isFinite(self.limit)) {
            if (index < self.limit) {
              if (self.inRange.length >= self.limit) {
                result.push({
                  new_val: self.inRange.get(self.limit-1).doc,
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
          return result;
        });
      }
      else { // Update, new_val and old_val are defined
        var varId = util.getVarId(self.table.indexes[operation.index].fn);
        query.context[varId] = change.old_val;
        return query.evaluate(self.table.indexes[operation.index].fn, query, {}).bind({}).then(function(valueIndex) {
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
          this.changeResult = {
            old_val: null
          };
          if (isFinite(self.limit)) {
            if (index < self.limit) {
              this.changeResult = {
                old_val: change.old_val
              };
            }
          }
          else {
            this.changeResult = {
              old_val: change.old_val
            };
          }

          query.context[varId] = change.new_val;
          return query.evaluate(self.table.indexes[operation.index].fn, query, {});
        }).then(function(valueIndex) {
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
          if (isFinite(self.limit)) {
            if (index < self.limit) {
              self.inRange._pushAt(index, {
                doc: change.new_val,
                valueIndex: valueIndex
              });
              this.changeResult.new_val = change.new_val;
            }
          }
          else {
            self.inRange._pushAt(index, {
              doc: change.new_val,
              valueIndex: valueIndex
            });
            this.changeResult.new_val = change.new_val;
          }
          result.push(this.changeResult);
          return result;
        });
      }
    }, new Sequence());
  };
};

Changes.createListener = function(self) {
  return function(change) {
    if (self.lock === true) {
      if (change !== undefined) {
        self.rawBuffer.push(change);
      }
      return;
    }
    if (change === undefined) {
      change = self.rawBuffer.shift();
    }
    self.lock = true;
    // We have to rebuild the pseudo types here
    change.old_val = util.revertDatum(util.deepCopy(change.old_val));
    change.new_val = util.revertDatum(util.deepCopy(change.new_val));
    // Apply transformation
    Promise.reduce(self.preMethods.concat(self.postMethods), function(result, method) {
      var p = method(result);
      if (p instanceof Promise === false) {
        p = Promise.resolve(p);
      }
      return p.then(function(partial) {
        return partial;
      });
    }, new Sequence([change], {})).then(function(sequence) {
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
    }).catch(function(err) {
      self.stop();
      var fullError = {
        t: err.type,
        r: [err.message],
        n: [],
        b: [] //TODO Need frame?
      };
      if (self.callbacks.length > 0) {
        var callback = self.callbacks.shift();
        callback(fullError, self.notes);
      }
      else {
        self.buffer.push(fullError);
      }
    }).finally(function() {
      self.lock = false;
      if (self.rawBuffer.length > 0) {
        self.listener();
      }
    });
    // Else nothing to send back
  };
};

// Terms

module.exports = Changes;
