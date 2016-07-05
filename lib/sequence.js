function Sequence(sequence, options) {
  options = options || {};

  this.sequence = sequence || [];
  if (options.type === undefined) {
    this.type = 'ARRAY';
  }
  else {
    this.type = options.type;
  }
  this.length = this.sequence.length;
  this.stream = true; // TODO Properly set
  this.infiniteRange = false;
  this.index = 0;
  //TODO Add if it's a stream or not?
}
module.exports = Sequence;

var util = require(__dirname+"/utils.js");
var Error = require(__dirname+"/error.js");

var Minval = require(__dirname+"/minval.js");
var Maxval = require(__dirname+"/maxval.js");
var Promise = require('bluebird');

Sequence.prototype.get = function(i) {
  if (this.infiniteRange === true) {
    return this.index++;
  }
  else {
    return this.sequence[i];
  }
};


//TODO Prefix with an underscore
Sequence.prototype.push = function(element) {
  this.sequence.push(element);
  this.length++;
  return this;
};
Sequence.prototype.shift = function() {
  this.sequence.shift();
  this.length--;
  return this;
};
Sequence.prototype.unshift = function(element) {
  this.sequence.unshift(element);
  this.length++;
  return this;
};
Sequence.prototype._pushAt = function(index, element) {
  this.sequence.splice(index, 0, element);
  this.length++;
  return this;
};
Sequence.prototype._deleteAt = function(index) {
  this.sequence.splice(index, 1);
  this.length--;
  return this;
};
Sequence.prototype.pop = function() {
  this.length--;
  this.sequence.pop();
};


Sequence.prototype.sum = function(fieldOrFn, query, internalOptions) {
  return Promise.reduce(this.sequence, function(left, right) {
    if ((fieldOrFn !== undefined) && !util.isFunction(fieldOrFn)) {
      query.frames.push(1);
      util.assertType(fieldOrFn, "STRING", query);
      query.frames.pop();
      fieldOrFn = util.generateFieldFunction(fieldOrFn);
    }
    if (fieldOrFn === undefined) {
      return left+right;
    }
    else { // fieldOrFn is a function
      var varId = util.getVarId(fieldOrFn);
      query.context[varId] = right;
      query.frames.push(1);
      return new Promise(function(resolve, reject) {
        query.evaluate(fieldOrFn, query, internalOptions).then(function(right) {
          delete query.context[varId];
          query.frames.pop();
          resolve(left+right);
        }).catch(function(err) {
          delete query.context[varId];
          if (err.message.match(/^No attribute `/) === null) {
            reject(err);
          }
          else {
            query.frames.pop();
            resolve(left);
          }
        });
      });
    }
  }, 0);
};

Sequence.prototype.avg = function(fieldOrFn, query, internalOptions) {
  var count = 0;
  return Promise.reduce(this.sequence, function(left, right) {
    if ((fieldOrFn !== undefined) && !util.isFunction(fieldOrFn)) {
      query.frames.push(1);
      util.assertType(fieldOrFn, "STRING", query);
      query.frames.pop();
      fieldOrFn = util.generateFieldFunction(fieldOrFn);
    }
    if (fieldOrFn === undefined) {
      count++;
      return left+right;
    }
    else { // fieldOrFn is a function
      var varId = util.getVarId(fieldOrFn);
      query.context[varId] = right;
      query.frames.push(1);
      return new Promise(function(resolve, reject) {
        query.evaluate(fieldOrFn, query, internalOptions).then(function(right) {
          delete query.context[varId];
          query.frames.pop();
          count++;
          resolve(left+right);
        }).catch(function(err) {
          delete query.context[varId];
          if (err.message.match(/^No attribute `/) === null) {
            reject(err);
          }
          else {
            query.frames.pop();
            resolve(left);
          }
        });
      });
    }
  }, 0).then(function(result) {
    return result/count;
  });
};

Sequence.prototype.min = function(fieldOrFn, options, query, internalOptions) {
  var self = this;
  var original;
  return Promise.reduce(self.sequence, function(left, right, index) {
    if ((fieldOrFn !== undefined) && !util.isFunction(fieldOrFn)) {
      query.frames.push(1);
      util.assertType(fieldOrFn, "STRING", query);
      query.frames.pop();
      fieldOrFn = util.generateFieldFunction(fieldOrFn);
    }
    if (fieldOrFn === undefined) {
      if (util.lt(right, left)) {
        return right;
      }
      else {
        return left;
      }
    }
    else { // fieldOrFn is a function
      var varId = util.getVarId(fieldOrFn);
      query.context[varId] = right;
      query.frames.push(1);
      return new Promise(function(resolve, reject) {
        query.evaluate(fieldOrFn, query, internalOptions).then(function(right) {
          delete query.context[varId];
          query.frames.pop();
          if (util.lt(right, left)) {
            original = self.sequence[index];
            return resolve(right);
          }
          else {
            return resolve(left);
          }

        }).catch(function(err) {
          delete query.context[varId];
          if (err.message.match(/^No attribute `/) === null) {
            return reject(err);
          }
          else {
            query.frames.pop();
            return resolve(left);
          }
        });
      });
    }
    return left;
  }, new Maxval()).then(function(result) {
    if (util.isMaxval(result)) {
      throw new Error.ReqlRuntimeError("Cannot take the min of an empty stream.  (If you passed `min` a field name, it may be that no elements of the stream had that field.)", query.frames);
    }
    if (original !== undefined) {
      return original;
    }
    return result;
  });
};

Sequence.prototype.max = function(fieldOrFn, options, query, internalOptions) {
  var self = this;
  var original;
  return Promise.reduce(self.sequence, function(left, right, index) {
    if ((fieldOrFn !== undefined) && !util.isFunction(fieldOrFn)) {
      query.frames.push(1);
      util.assertType(fieldOrFn, "STRING", query);
      query.frames.pop();
      fieldOrFn = util.generateFieldFunction(fieldOrFn);
    }
    if (fieldOrFn === undefined) {
      if (util.gt(right, left)) {
        return right;
      }
      else {
        return left;
      }
    }
    else { // fieldOrFn is a function
      var varId = util.getVarId(fieldOrFn);
      query.context[varId] = right;
      query.frames.push(1);
      return new Promise(function(resolve, reject) {
        query.evaluate(fieldOrFn, query, internalOptions).then(function(right) {
          delete query.context[varId];
          query.frames.pop();
          if (util.gt(right, left)) {
            original = self.sequence[index];
            return resolve(right);
          }
          else {
            return resolve(left);
          }

        }).catch(function(err) {
          delete query.context[varId];
          if (err.message.match(/^No attribute `/) === null) {
            return reject(err);
          }
          else {
            query.frames.pop();
            return resolve(left);
          }
        });
      });
    }
    return left;
  }, new Minval()).then(function(result) {
    if (util.isMinval(result)) {
      throw new Error.ReqlRuntimeError("Cannot take the min of an empty stream.  (If you passed `min` a field name, it may be that no elements of the stream had that field.)", query.frames);
    }
    if (original !== undefined) {
      return original;
    }
    return result;
  });

};

Sequence.prototype.concat = function(other) {
  var result = new Sequence();
  for(var i=0; i<this.sequence.length; i++) {
    result.push(this.sequence[i]);
  }
  other = other.toSequence();
  for(var i=0; i<other.sequence.length; i++) {
    result.push(other.sequence[i]);
  }
  return result;
};

Sequence.prototype.insertAt = function(position, value, query) {
  if (position < 0) {
    var originalPosition = position;
    position = this.sequence.length+position+1;
    if (position < 0) {
      util.outOfBoundNoSize(originalPosition, query);
    }
  }
  var result = new Sequence();
  for(var i=0; i<this.sequence.length; i++) {
    if (i === position) {
      result.push(value);
    }
    result.push(this.sequence[i]);
  }
  if (position === this.sequence.length) {
    result.push(value);
  }
  return result;
};

Sequence.prototype.changeAt = function(position, value, query) {
  if (position >= this.sequence.length) {
    util.outOfBound(position, this.sequence.length, this);
  }

  if (position < 0) {
    var originalPosition = position;
    position = this.sequence.length+position;
    if (position < 0) {
      util.outOfBoundNoSize(originalPosition, query);
    }
  }
  var result = new Sequence();
  for(var i=0; i<this.sequence.length; i++) {
    if (i === position) {
      result.push(value);
    }
    else {
      result.push(this.sequence[i]);
    }
  }
  if (position === this.sequence.length) {
    result.push(value);
  }
  return result;
};
Sequence.prototype.spliceAt = function(position, other, query) {
  if (position < 0) {
    var originalPosition = position;
    position = this.sequence.length+position+1;
    if (position < 0) {
      util.outOfBoundNoSize(originalPosition, query);
    }
  }

  var result = this.toSequence();
  for(var i=0; i<other.sequence.length; i++) {
    result = result.insertAt(position+i, other.sequence[i]);
  }
  return result;
};
Sequence.prototype.deleteAt = function(start, end, query) {
  if (start > this.sequence.length) {
    util.outOfBound(start, this.sequence.length, this);
  }

  if (start < 0) {
    var originalStart = start;
    start = this.sequence.length+start;
    if (start < 0) {
      util.outOfBoundNoSize(originalStart, query);
    }
  }
  if (end === undefined) {
    end = start+1;
  }
  if (end > this.sequence.length) {
    util.outOfBound(end, this.sequence.length, this);
  }
  else if (end < 0) {
    var originalEnd = end;
    end = this.sequence.length+end-1;
    if (end < 0) {
      util.outOfBoundNoSize(originalEnd, query);
    }
  }

  if (end<start) {
    throw new Error.ReqlRuntimeError("Start index `"+start+"` is greater than end index `"+end+"`", query.frames);
  }

  var result = new Sequence();
  for(var i=0; i<this.sequence.length; i++) {
    if ((i<start) || (end<=i)) {
      result = result.push(this.sequence[i]);
    }
  }
  return result;
};



Sequence.prototype.zip = function(query) {
  var result = new Sequence();
  for(var i=0; i<this.sequence.length; i++) {
    try {
    result.push(util.mergeDatum(
        util.getBracket(this.sequence[i], 'left', query),
        util.getBracket(this.sequence[i], 'right', query),
    query));
    }
    catch(err) {
      if (err.message.match(/^No attribute `/) === null) {
        throw err;
      }
      throw new Error.ReqlRuntimeError('ZIP can only be called on the result of a join', query.frames);
    }
  }
  return result;
};
Sequence.prototype.distinct = function(options, query, internalOptions) {
  // options is undefined for Sequence
  var self = this;
  var copy = self.toSequence();
  return copy.orderBy([[69,[[2,[3]],[13,[]]]]], {}, query, internalOptions).then(function(sorted) {
    var result = new Sequence();
    for(var i=0; i<sorted.sequence.length; i++) {
      if ((result.length === 0)
        || !util.eq(result.get(result.sequence.length-1), sorted.get(i))) {

        result.push(sorted.get(i));
      }
    }
    return result;
  });
};

Sequence.prototype.reduce = function(fn, query, internalOptions) {
  if (this.sequence.length === 0) {
    query.frames.push(1);
    query.frames.push(1);
    throw new Error.ReqlRuntimeError("Cannot reduce over an empty stream", query.frames);
  }
  else if (this.sequence.length === 1) {
    return Promise.resolve(this.sequence[0]);
  }
  else {
    return Promise.reduce(this.sequence.slice(1), function(left, right) {
      if (util.isFunction(fn)) {
        var varLeft, varRight;
        var varIds = util.getVarIds(fn);
        query.frames.push(1);
        util.assertArity(2, fn[1][0][1], query, fn);
        query.frames.pop();

        varLeft = varIds[0];
        varRight = varIds[1];

        query.context[varLeft] = left;
        query.context[varRight] = right;
        query.frames.push(1);
        return query.evaluate(fn, query, internalOptions).then(function(result) {
          query.frames.push(1);
          util.assertType(result, 'DATUM', query);
          query.frames.pop();
          query.frames.pop();
          delete query.context[varLeft];
          delete query.context[varRight];
          return result;
        });
      }
      else {
        query.frames.push(1);
        return query.evaluate(fn, internalOptions).then(function(fnValue) {
          if (typeof fnValue === 'function') {
            try {
              var result = fnValue(left, right);
            }
            catch(error) {
              throw new Error.ReqlRuntimeError(error.toString(), query.frames);
            }
            util.assertJavaScriptResult(result, query);
            result = util.revertDatum(result);
            query.frames.pop();
            return result;
          }
          else {
            // We are going to throw
            util.assertType(fn, 'FUNCTION', query);
          }
        });
      }
    }, this.sequence[0]).then(function(result) {
      return result;
    });
  }
};

Sequence.prototype.fold = function(base, fn, options, query, internalOptions) {
  var emittedResult = new Sequence();
  return Promise.reduce(this.sequence, function(prevResult, arg, index) {
    util.assertType(fn, 'FUNCTION', query);
    var varBase, varArg;
    var varIds = util.getVarIds(fn);
    query.frames.push(1);
    util.assertArity(2, fn[1][0][1], query, fn);
    query.frames.pop();

    varBase = varIds[0];
    varArg = varIds[1];

    query.context[varBase] = prevResult;
    query.context[varArg] = arg;
    query.frames.push(1);
    return query.evaluate(fn, query, internalOptions).then(function(result) {
      query.frames.push(1);
      util.assertType(result, 'DATUM', query);
      query.frames.pop();
      query.frames.pop();
      delete query.context[varBase];
      delete query.context[varArg];
      if (util.isFunction(options.emit)) {
        util.assertArity(3, options.emit[1][0][1], query, options.emit);
        var varIds = util.getVarIds(options.emit);
        varPrevAcc = varIds[0];
        varArg = varIds[1];
        varNewAcc = varIds[2];
        query.context[varPrevAcc] = prevResult;
        query.context[varArg] = arg;
        query.context[varNewAcc] = result;
        return query.evaluate(options.emit, query, internalOptions).then(function(emittedPartial) {
          emittedResult = emittedResult.concat(emittedPartial);
          return result;
        })
      }
      return result;
    });
  }, base).then(function(result) {
    if (util.isFunction(options.emit)) {
      return emittedResult;
    }
    return result;
  });
};

Sequence.prototype.hasFields = function(keys) {
  var result = new Sequence();
  for(var i=0; i<this.length; i++) {
    if (util.hasFields(this.get(i), keys)) {
      result.push(this.get(i));
    }
  }
  return result;
};

//TODO Use a hash? a deterministic JSON.stringify? (for all the set/array operations
Sequence.prototype.difference = function(other, query) {
  var result = new Sequence();
  var found;

  // Remove duplicates
  for(var i=0; i<this.sequence.length; i++) {
    found = false;
    for(var j=0; j<other.sequence.length; j++) {
      if (util.eq(this.sequence[i], other.sequence[j])) {
        found = true;
        break;
      }
    }
    if (found === false) {
      result.push(this.sequence[i]);
    }
  }

  return result;
};
Sequence.prototype.setInsert = function(value, query) {
  var result = new Sequence();
  var found;

  // Remove duplicates
  for(var i=0; i<this.sequence.length; i++) {
    found = false;
    for(var j=0; j<result.sequence.length; j++) {
      if (util.eq(this.sequence[i], result.sequence[j])) {
        found = true;
        break;
      }
    }
    if (found === false) {
      result.push(this.sequence[i]);
    }
  }

  // Try to add value
  found = false;
  for(var j=0; j<result.sequence.length; j++) {
    if (util.eq(value, result.sequence[j])) {
      found = true;
      break;
    }
  }
  if (found === false) {
    result.push(value);
  }

  return result;
};

Sequence.prototype.contains = function(predicates, query, internalOptions) {
  var self = this;
  var founded = [];
  for(var i=0; i<predicates.length; i++) {
    founded.push(false);
  }

  return Promise.reduce(predicates, function(result, predicate, position) {
    if (result === false) {
      return false;
    }

    return Promise.reduce(self.sequence, function(alreadyFound, value) {
      if (alreadyFound === true) {
        return true;
      }
      if (util.isFunction(predicate)) {
        var varId = util.getVarId(predicate);
        query.context[varId] = value;
        query.frames.push(position+1); // +1 because we already evaluated the sequence
        return query.evaluate(predicate, query, internalOptions).then(function(predicateResult) {
          query.frames.push(1);
          util.assertType(predicateResult, 'DATUM', query);
          query.frames.pop();
          query.frames.pop();
          delete query.context[varId];
          return util.isTrue(predicateResult);
        });
      }
      else {
        return query.evaluate(predicate, query, internalOptions).then(function(predicateValue) {
          // We could get a function from r.js
          if (typeof predicateValue === 'function') {
            try {
              var predicateResult = predicateValue(util.toDatum(value));
            }
            catch(error) {
              query.frames.push(1);
              throw new Error.ReqlRuntimeError(error.toString(), query.frames);
            }
            return util.isTrue(predicateResult);
          }
          else {
            query.frames.push(1);
            util.assertType(predicateValue, 'DATUM', query);
            query.frames.pop();
            return util.eq(value, predicateValue);
          }
        });
      }
    }, false);
  }, true);
};

Sequence.prototype.setIntersection = function(other, query) {
  var result = new Sequence();
  var found;

  for(var i=0; i<this.sequence.length; i++) {
    found = false;
    for(var j=0; j<other.sequence.length; j++) {
      if (util.eq(this.sequence[i], other.sequence[j])) {
        found = true;
        break;
      }
    }
    if (found === true) {
      result.push(this.sequence[i]);
    }
  }
  return result;
};
Sequence.prototype.setDifference = function(other, query) {
  var result = new Sequence();
  var found;

  for(var i=0; i<this.sequence.length; i++) {
    found = false;
    for(var j=0; j<other.sequence.length; j++) {
      if (util.eq(this.sequence[i], other.sequence[j])) {
        found = true;
        break;
      }
    }
    if (found === false) {
      for(var j=0; j<result.sequence.length; j++) {
        if (util.eq(this.sequence[i], result.sequence[j])) {
          found = true;
          break;
        }
      }
    }

    if (found === false) {
      result.push(this.sequence[i]);
    }
  }
  return result;
};

Sequence.prototype.setUnion = function(other, query) {
  // TODO This is a really not efficient now...
  var result = this;
  for(var i=0; i<other.sequence.length; i++) {
    result = result.setInsert(other.sequence[i]);
  }
  return result;
};

Sequence.prototype.toSequence = function() {
  // Returns a new sequence
  // NOT a deep copy
  if (this.infiniteRange === true) {
    return Sequence.range();
  }
  else {
    var result = new Sequence();
    for(var i=0; i<this.sequence.length; i++) {
      result.push(this.sequence[i]);
    }
    return result;
  }
};

Sequence.prototype.sample = function(sample, query) {
  var result = new Sequence();
  while ((sample > 0) && (this.sequence.length > 0)) {
    var index = Math.floor(Math.random()*this.sequence.length);
    result.push(this.sequence.splice(index, 1));
    sample--;
  }
  return result;
};

Sequence.prototype.merge = function(toMerge, query, internalOptions) {
  return Promise.reduce(this.sequence, function(result, element) {
    return util.merge(element, toMerge, query, internalOptions).then(function(resultMerge) {
      result.push(resultMerge);
      return result;
    });
  }, new Sequence());
};


Sequence.prototype.eqJoin = function(leftField, other, options, query, internalOptions) {
  // other is a table slice since eqJoin requires an index
  // If leftField is a string, replace it with a function
  if (typeof leftField === "string") {
    leftField = util.generateFieldFunction(leftField);
  }

  return Promise.reduce(this.sequence, function(result, doc) {
    var varId = util.getVarId(leftField);

    query.context[varId] = doc;
    return query.evaluate(leftField, internalOptions).then(function(leftFieldValue) {
      return other.getAll([leftFieldValue], options, query);
    }).then(function(partial) {
      partial = partial.toSequence();
      for(var k=0; k<partial.sequence.length; k++) {
        result.push({
          left: doc,
          right: partial.get(k)
        });
      }
      return result;
    }).catch(function(err) {
      if (err.message.match(/^No attribute `/) === null) {
        throw err;
      }
      // else we just skip the non existence error
      return result;
    }).finally(function() {
      delete query.context[varId];
    });
  }, new Sequence());
};

Sequence.prototype.join = function(type, other, predicate, query, internalOptions) {
  return Promise.reduce(this.sequence, function(result, doc) {
    var returned = false;
    return Promise.reduce(other.sequence, function(result, otherDoc) {
      if (util.isFunction(predicate)) {
        var varIds = util.getVarIds(predicate);
        query.context[varIds[0]] = doc;
        query.context[varIds[1]] = otherDoc;

        if (Array.isArray(predicate[1][0][1]) && (predicate[1][0][1].length > 0)) {
          try {
            util.assertArity(2, predicate[1][0][1], query);
          }
          catch(err) {
            // RethinkDB bug, see issue 4189
            //TODO Once the bug is fixed, we should remove this try/catch
            var found = 2;
            var expected = varIds.length;
            util.arityError(expected, found, query, predicate);
          }
        }

        return query.evaluate(predicate, internalOptions).then(function(predicateResult) {
          delete query.context[varIds[0]];
          delete query.context[varIds[1]];
          if (util.isTrue(predicateResult)) {
            returned = true;
            result.push({
              left: doc,
              right: otherDoc
            });
          }
          return result;
        });
      }
      else {
        return query.evaluate(predicate, internalOptions).then(function(predicateResult) {
          if (typeof predicateResult === 'function') {
            try{
              predicateResult = predicateResult(doc, otherDoc);
            }
            catch(error) {
              query.frames.push(2);
              throw new Error.ReqlRuntimeError(error.toString(), query.frames);
            }

            if (util.isTrue(predicateResult)) {
              returned = true;
              result.push({
                left: doc,
                right: otherDoc
              });
            }
          }
          else if (util.isTrue(predicateResult)) {
            returned = true;
            result.push({
              left: doc,
              right: otherDoc
            });
          }
          return result;
        });
      }
    }, result).then(function() {
      if (returned === false && type === 'outer') {
        result.push({
          left: doc
        });
      }
      return result;
    });
  }, new Sequence());
};

Sequence.prototype.filter = function(filter, options, query, internalOptions) {
  var self = this;
  var sequence = new Sequence();

  if (options.default === undefined) {
    options.default = false;
  }

  if (util.isFunction(filter)) {
    var varId = util.getVarId(filter);
    query.frames.push(1);
    return Promise.reduce(self.sequence, function(result, context) {
      query.context[varId] = context;
      return query.evaluate(filter, query, internalOptions).bind({}).then(function(filterResult) {
        if (util.isTrue(filterResult)) {
          result.push(context);
        }
        return result;
      }).catch(function(err) {
        if (err.message.match(/^No attribute/)) {
          if (util.isTrue(options.default)) {
            result.push(context);
          }
        }
        else {
          throw err;
        }
        return result;
      }).finally(function() {
        delete query.context[varId];
        return result;
      });
    }, sequence);
  }
  else {
    query.frames.push(1);
    return query.evaluate(filter, query, internalOptions).then(function(filterValue) {
      if (typeof filterValue === 'function') {
        for(var i=0; i<self.sequence.length; i++) {
          try {
            var filterResult = filterValue(util.toDatum(self.get(i)));
          }
          catch(error) {
            throw new Error.ReqlRuntimeError(error.toString(), query.frames);
          }
          util.assertJavaScriptResult(filterResult, query);
          filterResult = util.revertDatum(filterResult);
          if (util.isTrue(filterResult)) {
            sequence.push(self.sequence[i]);
          }
        }
      }
      else if (util.isPlainObject(filterValue)) {
        for(var i=0; i<self.sequence.length; i++) {
          if (util.filter(util.toDatum(self.sequence[i]), util.toDatum(filterValue))) {
            sequence.push(self.sequence[i]);
          }
        }
      }
      else {
        for(var i=0; i<self.sequence.length; i++) {
          if ((filterValue !== null) && (filterValue !== false)) {
            sequence.push(self.sequence[i]);
          }
        }
      }
      query.frames.pop();
      return sequence;
    });
  }
};



Sequence.prototype.count = function(predicate, query, internalOptions) {
  if (predicate !== undefined) {
    return Promise.map(this.sequence, function(element) {
      if (util.isFunction(predicate)) {
        var varId = util.getVarId(predicate);
        query.context[varId] = element;
        return query.evaluate(predicate, internalOptions).then(function(predicateResult) {
          delete query.context[varId];
          util.assertType(predicateResult, 'DATUM', query);
          if (util.isTrue(predicateResult)) {
            return 1;
          }
          return 0;
        });
      }
      else { // predicate is a value
        return query.evaluate(predicate).then(function(predicateResult) {
          if (typeof predicateResult === 'function') {
            if (util.isTrue(predicateResult(element))) {
              return 1;
            }
          }
          else {
            if (util.eq(element, predicateResult)) {
              return 1;
            }
          }
          return 0;
        });
      }
    }, {concurrency: 1}).then(function(results) {
      var total = 0;
      for(var i=0; i<results.length; i++) {
        total += results[i];
      }
      return total;
    });
  }
  else {
    return Promise.resolve(this.length);
  }
};

Sequence.prototype.skip = function(skip) {
  var result = new Sequence();
  if (skip < 0) {
    skip = Math.max(0, this.sequence.length+skip);
  }
  for(var i=skip; i<this.sequence.length; i++) {
    result.push(this.sequence[i]);
  }
  return result;
};

Sequence.prototype.limit = function(limit) {
  var result = new Sequence();
  for(var i=0; i<Math.min(limit,this.length); i++) {
    result.push(this.get(i));
  }
  return result;
};

Sequence.prototype.pluck = function(keys) {
  var result = new Sequence();
  for(var i=0; i<this.sequence.length; i++) {
    result.push(util.pluck(this.sequence[i], keys));
  }
  return result;
};
Sequence.prototype.without = function(keys) {
  var result = new Sequence();
  for(var i=0; i<this.length; i++) {
    result.push(util.without(this.get(i), keys));
  }
  return result;
};


Sequence.prototype.slice = function(start, end, options, query) {
  var result = new Sequence();

  if (options.left_bound === undefined) {
    options.left_bound = "closed";
  }
  else if ((options.left_bound !== 'closed') && (options.left_bound !== 'open')) {
    throw new Error.ReqlRuntimeError('Expected `open` or `closed` for optarg `left_bound` (got `"'+options.left_bound+'"`)', query.frames);
  }
  if (options.right_bound === undefined) {
    options.right_bound = "open";
  }
  else if ((options.right_bound !== 'closed') && (options.right_bound !== 'open')) {
    throw new Error.ReqlRuntimeError('Expected `open` or `closed` for optarg `right_bound` (got `"'+options.left_bound+'"`)', query.frames);
  }

  if (start < 0) {
    start = Math.max(0, this.sequence.length+start);
  }

  if (end === undefined) {
    end = this.sequence.length;
  }
  else if ((typeof end === 'number') && (end < 0)) {
    end = this.sequence.length+end;
  }

  if (options.left_bound === "open") {
    start++;
  }
  if (options.right_bound === "closed") {
    end++;
  }

  for(var i=start; i<end; i++) {
    if (i >=this.sequence.length) { break; }
    result.push(this.sequence[i]);
  }
  return result;
};

Sequence.prototype.nth = function(index, query) {
  var originalIndex = index;
  if (index < 0) {
    index = this.sequence.length+index;
  }
  if ((index >= this.sequence.length) || (index < 0)) {
    throw new Error.ReqlRuntimeError("Index out of bounds: "+originalIndex, query.frames);
  }
  return this.sequence[index];
};

Sequence.prototype.offsetsOf = function(predicate, query) {
  return Promise.reduce(this.sequence, function(result, doc, index) {
    if (util.isFunction(predicate)) {
      var varId = util.getVarId(predicate);
      query.context[varId] = doc;
      query.frames.push(1);
      query.frames.push(1);
      return query.evaluate(predicate, query).then(function(predicateResult) {
        util.assertType(predicateResult, 'DATUM', query);
        query.frames.pop();
        query.frames.pop();
        if (util.isTrue(predicateResult)) {
          result.push(index);
        }
        delete query.context[varId];
        return result;
      });
    }
    else {
      query.frames.push(1);
      return query.evaluate(predicate, query).then(function(predicateResult) {
        if (typeof predicateResult === 'function') {
          predicateResult = predicateResult(doc);
          util.assertJavaScriptResult(result, query);
        }
        else {
          util.assertType(predicateResult, 'DATUM', query);
        }
        query.frames.pop();
        if (util.eq(doc, predicateResult)) {
          result.push(index);
        }
        return result;
      });
    }
  }, new Sequence());

};

Sequence.prototype.isEmpty = function() {
  return this.length === 0;
};

Sequence.prototype.map = function(fn, query, internalOptions) {
  return Sequence.map([this], fn, query, internalOptions);
};

// map is variadic, hence this method.
Sequence.map = function(sequences, fn, query, internalOptions) {
  return Promise.map(sequences[0].sequence, function(element, index) {
    element = sequences[0].sequence[index];
    for(var j=0; j<sequences.length; j++) {
      if (sequences[j].sequence[index] === undefined) {
        return;
      }
    }
    if (util.isFunction(fn)) {
      var varIds = util.getVarIds(fn);
      try {
        util.assertArity(sequences.length, fn[1][0][1], query);
      }
      catch(err) {
        // RethinkDB throws a special error here...
        if (varIds.length === 1) {
          if (sequences.length !== 1) {
            throw new Error.ReqlRuntimeError('The function passed to `map` expects '+varIds.length+' argument, but '+sequences.length+' sequences were found', query.frames);
          }
        }
        else if (varIds.length !== 0) {
          if (sequences.length === 1) {
            throw new Error.ReqlRuntimeError('The function passed to `map` expects '+varIds.length+' arguments, but '+sequences.length+' sequence was found', query.frames);
          }
        }
      }
      for(var k=0; k<varIds.length; k++) {
        query.context[varIds[k]] = sequences[k].sequence[index];
      }
      // What is this frame?
      query.frames.push(sequences.length);
      return query.evaluate(fn, internalOptions).then(function(resultFn) {
        // What is this frame?
        query.frames.push(1);
        util.assertType(resultFn, 'DATUM', query);
        query.frames.pop();
        query.frames.pop();
        for(var k=0; k<varIds.length; k++) {
          delete query.context[varIds[k]];
        }
        return resultFn;
      });
    }
    else {
      query.frames.push(sequences.length);
      return query.evaluate(fn, internalOptions).then(function(fnValue) {
        if (typeof fnValue === 'function') {
          var args = [];
          for(var k=0; k<sequences.length; k++) {
            args.push(util.toDatum(sequences[k].sequence[index]));
          }
          var fnResult = fnValue.apply({}, args);
          util.assertJavaScriptResult(fnResult, query);
          fnResult = util.revertDatum(fnResult);
          query.frames.pop();
          return fnResult;
        }
        else {
          query.frames.pop();
          return fnValue;
        }
      });
    }
  }, {concurrency: 1}).then(function(rawResult) {
    // TODO Test cleaning
    var result = new Sequence();
    for(var i=0; i<rawResult.length; i++) {
      if (rawResult[i] === undefined) {
        break;
      }
      result.push(rawResult[i]);
    }
    return result;
  });
};

Sequence.prototype.concatMap = function(fn, query, internalOptions) {
  var self = this;
  return Promise.reduce(this.sequence, function(result, doc) {
    if (util.isFunction(fn)) {
      var varId = util.getVarId(fn);
      query.context[varId] = doc;
      query.frames.push(1);
      return query.evaluate(fn, internalOptions).then(function(partial) {
        query.frames.push(1);
        util.assertType(partial, 'SEQUENCE', self);
        query.frames.pop();
        partial = util.toSequence(partial, query);
        query.frames.pop();
        for(var k=0; k<partial.length; k++) {
          result.push(partial.get(k));
        }
        delete query.context[varId];
        return result;
      });
    }
    else {
      query.frames.push(1);
      return query.evaluate(fn, internalOptions).then(function(resultFn) {
        query.frames.pop();
        if (typeof resultFn === 'function') {
          var partial = resultFn(doc);
          partial = util.toSequence(partial, query);
          util.assertType(partial, 'SEQUENCE', self);
          for(var k=0; k<partial.length; k++) {
            result.push(partial[k]);
          }
        }
        // If resultFn is a sequence?
        else {
          query.frames.push(1); // We will throw
          util.assertType(resultFn, 'FUNCTION', query);
        }
        return result;
      });
    }

  }, new Sequence());
};
Sequence.prototype.withFields = function(fields) {
  // fields is a Sequence
  var result = new Sequence();
  var element;
  for(var i=0; i<this.length; i++) {
    if (util.hasFields(this.get(i), fields)) {
      element = {};
      for(var j=0; j<fields.length; j++) {
        if (typeof fields.get(j) === 'string') {
          element[fields.get(j)] = this.get(i)[fields.get(j)];
        }
        else if (util.isPlainObject(fields)) {
          var fieldsToCopy = Object.keys(fields.get(j));
          for(var k=0; k<fieldsToCopy.length; k++) {
            element[fieldsToCopy[k]] = this.get(i)[fieldsToCopy[k]];
          }
        }
      }
      result.push(element);
    }
  }
  return result;
};

Sequence.prototype.orderBy = function(fields, options, query, internalOptions) {
  var self = this;
  if (options.index !== undefined) {
    throw new Error.ReqlRuntimeError("Indexed order_by can only be performed on a TABLE or TABLE_SLICE", query.frames);
  }
  var sequenceToSort = new Sequence();
  return Promise.map(self.sequence, function(ref) {
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
    var result = new Sequence();
    for(var i=0; i<sequenceToSort.length; i++) {
      result.push(sequenceToSort.get(i).original);
    }
    return result;
  });
};

Sequence.prototype._orderBy = function() {
  this.sequence.sort(function(left, right) {
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


Sequence.prototype.getField = function(field) {
  var result = new Sequence();
  for(var i=0; i<this.length; i++) {

    if (this.get(i)[field] !== undefined) {
      result.push(this.get(i)[field]);
    }
    // else we just skip those values
  }
  return result;
};

Sequence.prototype.toDatum = function() {
  var result = [];
  for(var i=0; i<this.sequence.length; i++) {
    //TODO Check for null
    if ((this.sequence[i] != null) && (typeof this.sequence[i].toDatum === "function")) {
      result.push(this.sequence[i].toDatum());
    }
    else {
      result.push(util.toDatum(this.sequence[i]));
    }
  }
  return result;
};

Sequence.prototype.getBracket = function(key, query) {
  if (typeof key === 'number') {
    return this.sequence[key];
  }
  else {
    var result = new Sequence();
    for(var i=0; i<this.sequence.length; i++) {
      if (util.isSequence(this.get(i))) {
        throw new Error.ReqlRuntimeError('Cannot perform bracket on a sequence of sequences', query.frames);
      }
      result.push(util.getBracket(this.get(i), key, query));
    }
    return result;
  }
};

Sequence.prototype.clone = function() {
  var result = new Sequence();
  for(var i=0; i<this.sequence.length; i++) {
    result.push(this.sequence[i]);
  }
  return result;
};


Sequence.prototype.forEach = function(fn, query, internalOptions) {
  var self = this;
  return Promise.reduce(self.sequence, function(result, right) {
    if (util.isFunction(fn)) {
      var varId = util.getVarId(fn);
      query.context[varId] = right;
      query.frames.push(1);
      return query.evaluate(fn, self, internalOptions).then(function(writeResult) {
        util.assertType(writeResult, 'DATUM', query);
        util.mergeWriteResult(result, writeResult);
        query.frames.pop();
        delete query.context[varId];
        return result;
      });
    }
    else {
      query.frames.push(1);
      return query.evaluate(fn, self, internalOptions).then(function(writeResult) {
        query.frames.pop();
        if (typeof writeResult === 'function') {
          try {
            writeResult = writeResult(util.toDatum(right));
          }
          catch(error) {
            query.frames.push(1);
            throw new Error.ReqlRuntimeError(error.toString(), query.frames);
          }
          util.mergeWriteResult(result, writeResult);
        }
        else {
          util.mergeWriteResult(result, writeResult);
        }
        return result;
      });
    }

  }, util.writeResult());

};

Sequence.prototype.intersects = function(geometry, query) {
  var result = new Sequence();
  for(var i=0; i<this.length; i++) {
    if (!util.isGeometry(this.get(i))) {
      throw new Error.ReqlRuntimeError('Cannot perform intersects on a non-object non-sequence `'+JSON.stringify(this.get(i), null, 4)+'`', query.frames);
    }
    if (this.get(i).intersects(geometry)) {
      result.push(this.get(i));
    }
  }
  return result;
};

Sequence.prototype.includes = function(geometry, query) {
  var result = new Sequence();
  for(var i=0; i<this.length; i++) {
    if (!util.isGeometry(this.get(i))) {
      throw new Error.ReqlRuntimeError('Cannot perform includes on a non-object non-sequence `'+JSON.stringify(this.get(i), null, 4)+'`', query.frames);
    }
    if (this.get(i).includes(geometry)) {
      result.push(this.get(i));
    }
  }
  return result;
};


Sequence.prototype.setInfiniteRange = function() {
  this.infiniteRange = true;
  this.stream = true;
  this.length = Infinity;
};

Sequence.range = function(start, end) {
  var sequence = new Sequence([], {
    type: 'STREAM'
  });
  if (start === undefined) {
    sequence.setInfiniteRange();
  }
  else if (end === undefined) {
    for(var i=0; i<start; i++) {
      sequence.push(i);
    }
  }
  else {
    for(var i=start; i<end; i++) {
      sequence.push(i);
    }
  }
  return sequence;
};
