var util = require(__dirname+"/utils/main.js");
var Error = require(__dirname+"/error.js");

var Minval = require(__dirname+"/minval.js");
var Maxval = require(__dirname+"/maxval.js");
var Range = require(__dirname+"/range.js");

function Sequence(sequence, options) {
  options = options || {};

  this.sequence = sequence || [];
  if (options.type === undefined) {
    this.type = 'ARRAY'
  }
  else {
    this.type = options.type;
  }
  this.length = this.sequence.length;;
  this.stream = true; // TODO Properly set
  this.infiniteRange = false;
  this.index = 0;
  //TODO Add if it's a stream or not?
}

Sequence.prototype.get = function(i) {
  if (this.infiniteRange === true) {
    return this.index++;
  }
  else {
    return this.sequence[i];
  }
}


//TODO Prefix with an underscore
Sequence.prototype.push = function(element) {
  this.sequence.push(element);
  this.length++;
  return this;
}
Sequence.prototype.shift = function() {
  this.sequence.shift();
  this.length--;
  return this;
}
Sequence.prototype.unshift = function(element) {
  this.sequence.unshift(element);
  this.length++;
  return this;
}
Sequence.prototype._pushAt = function(index, element) {
  this.sequence.splice(index, 0, element);
  this.length++;
  return this;
}
Sequence.prototype._deleteAt = function(index) {
  this.sequence.splice(index, 1);
  this.length--;
  return this;
}
Sequence.prototype.pop = function(element) {
  this.length--;
  this.sequence.pop();
}


Sequence.prototype.sum = function(fieldOrFn, query, internalOptions) {
  if ((fieldOrFn !== undefined) && !util.isFunction(fieldOrFn)) {
    query.frames.push(1);
    util.assertType(fieldOrFn, "STRING", query);
    query.frames.pop();
    fieldOrFn = util.generateFieldFunction(fieldOrFn);
  }

  var result = 0;
  for(var i=0; i<this.sequence.length; i++) {
    if (fieldOrFn === undefined) {
      result += this.sequence[i];
    }
    else { // fieldOrFn is a function
      var varId = util.getVarId(fieldOrFn);
      query.context[varId] = this.sequence[i];
      try{
        query.frames.push(1);
        result += query.evaluate(fieldOrFn, query, internalOptions);
        query.frames.pop();
      }
      catch(err) {
        if (err.message.match(/^No attribute `/) === null) {
          throw err;
        }
        // else we just skip the non existence error
        query.frames.pop();
      }
      delete query.context[varId];
    }
  }
  return result;
}

Sequence.prototype.avg = function(fieldOrFn, query, internalOptions) {
  if ((fieldOrFn !== undefined) && !util.isFunction(fieldOrFn)) {
    query.frames.push(1);
    util.assertType(fieldOrFn, "STRING", query);
    query.frames.pop();
    fieldOrFn = util.generateFieldFunction(fieldOrFn);
  }

  var result = 0;
  var count = 0;
  for(var i=0; i<this.sequence.length; i++) {
    if (fieldOrFn === undefined) {
      result += this.sequence[i];
      count += 1;
    }
    else { // fieldOrFn is a function
      var varId = util.getVarId(fieldOrFn);
      query.context[varId] = this.sequence[i];
      try{
        query.frames.push(1);
        result += query.evaluate(fieldOrFn, query, internalOptions);
        query.frames.pop();
        count += 1;
      }
      catch(err) {
        if (err.message.match(/^No attribute `/) === null) {
          throw err;
        }
        // else we just skip the non existence error
        query.frames.pop();
      }
      delete query.context[varId];
    }
  }
  return result/count;
}

Sequence.prototype.min = function(fieldOrFn, options, query, internalOptions) {
  // options is actually not use here, we keep to have the same signature as on a table,
  // though we do not have to...
  if ((fieldOrFn !== undefined) && !util.isFunction(fieldOrFn)) {
    query.frames.push(1);
    util.assertType(fieldOrFn, "STRING", query);
    query.frames.pop();
    fieldOrFn = util.generateFieldFunction(fieldOrFn);
  }

  var result; // the result (not the minimum if a field/fn is provided)
  var min; // the minimum found so far
  var next;
  for(var i=0; i<this.sequence.length; i++) {
    if (fieldOrFn === undefined) {
      next = this.sequence[i];
      if ((min === undefined) || (util.lt(next, min))) {
        min = next;
        result = min;
      }
    }
    else { // fieldOrFn is a function
      var varId = util.getVarId(fieldOrFn);
      query.context[varId] = this.sequence[i];
      try{
        next = query.evaluate(fieldOrFn, query, internalOptions);
        if ((min === undefined) || (util.lt(next, min))) {
          min = next;
          result = this.sequence[i];
        }
      }
      catch(err) {
        if (err.message.match(/^No attribute `/) === null) {
          throw err;
        }
        // else we just skip the non existence error
      }
      delete query.context[varId];
    }
  }
  return result;
}

Sequence.prototype.max = function(fieldOrFn, options, query, internalOptions) {
  if ((fieldOrFn !== undefined) && !util.isFunction(fieldOrFn)) {
    query.frames.push(1);
    util.assertType(fieldOrFn, "STRING", query);
    query.frames.pop();
    fieldOrFn = util.generateFieldFunction(fieldOrFn);
  }

  var result; // the result (not the maximum if a field/fn is provided)
  var max; // the maximum found so far
  var next;
  for(var i=0; i<this.sequence.length; i++) {
    if (fieldOrFn === undefined) {
      next = this.sequence[i];
      if ((max === undefined) || (util.gt(next, max))) {
        max = next;
        result = max;
      }
    }
    else { // fieldOrFn is a function
      var varId = util.getVarId(fieldOrFn);
      query.context[varId] = this.sequence[i];
      try{
        query.frames.push(1);
        next = query.evaluate(fieldOrFn, query, internalOptions);
        query.frames.pop();
        if ((max === undefined) || (util.gt(next, max))) {
          max = next;
          result = this.sequence[i];
        }
      }
      catch(err) {
        if (err.message.match(/^No attribute `/) === null) {
          throw err;
        }
        // else we just skip the non existence error
        query.frames.pop();
      }
      delete query.context[varId];
    }
  }
  return result;
}


Sequence.prototype.group = function(fieldOrFns, options, query, internalOptions) {
  var Group = require(__dirname+"/group.js");
  var groups = new Group();

  var fieldOrFn, group, subGroup;
  for(var i=0; i<fieldOrFns.length; i++) {
    if (!util.isFunction(fieldOrFns[i])) {
      query.frames.push(i+1);
      util.assertType(fieldOrFns[i], "STRING", query);
      query.frames.pop();
    }
  }

  for(var i=0; i<this.sequence.length; i++) { // Iterate on all the documents of the table, pay attention to what browserify does here
    group = new Sequence();
    for(var j=0; j<fieldOrFns.length; j++) {
      fieldOrFn = fieldOrFns[j];

      if (typeof fieldOrFn === "string") {
        fieldOrFn = util.generateFieldFunction(fieldOrFn);
      }

      var varId = util.getVarId(fieldOrFn);
      query.context[varId] = this.sequence[i];
      try {
        query.frames.push(j+1);
        subGroup = query.evaluate(fieldOrFn, internalOptions)
        query.frames.pop();
      }
      catch(err) {
        if (err.message.match(/^No attribute `/) === null) {
          throw err;
        }
        subGroup = null
      }
      group.push(subGroup);
      delete query.context[varId];
    }

    if (group.length === 1) {
      group = group.get(0);
    }
    groups.push(group, this.sequence[i])
  }
  return groups;
}

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
}
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
}

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
}
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
    result = result.insertAt(position+i, other.sequence[i])
  }
  return result;
}
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
    throw new Error.ReqlRuntimeError("Start index `"+start+"` is greater than end index `"+end+"`", query.frames)
  }

  var result = new Sequence();
  for(var i=0; i<this.sequence.length; i++) {
    if ((i<start) || (end<=i)) {
      result = result.push(this.sequence[i])
    }
  }
  return result;
}



Sequence.prototype.zip = function(query) {
  var result = new Sequence();
  for(var i=0; i<this.sequence.length; i++) {
    if ((this.sequence[i].left === undefined) || (this.sequence[i].right, query)) {
      throw new Error.ReqlRuntimeError('ZIP can only be called on the result of a join', query.frames)
    }
    result.push(util.mergeDatum(this.sequence[i].left, this.sequence[i].right, query));
  }
  return result;
}
Sequence.prototype.distinct = function(options, query) {
  // options is undefined for Sequence
  var copy = this.toSequence();
  copy.sequence.sort(function(left, right) {
    if (util.lt(left, right)) {
      return -1;
    }
    else if (util.eq(left, right)) {
      return 0;
    }
    else {
      return 1
    }
  });

  var result = new Sequence();
  for(var i=0; i<copy.sequence.length; i++) {
    if ((result.sequence.length === 0)
      || !util.eq(result.sequence[result.sequence.length-1], copy.sequence[i])) {

      result.push(copy.sequence[i]);
    }
  }
  return result;
}

Sequence.prototype.reduce = function(fn, query, internalOptions) {
  if (this.sequence.length === 0) {
    query.frames.push(1);
    throw new Error.ReqlRuntimeError("Cannot reduce over an empty stream", query.frames)
  }
  else if (this.sequence.length === 1) {
    return this.sequence[0];
  }
  else {
    if (util.isFunction(fn)) {
      var result = this.sequence[0];
      var varLeft, varRight;
      for(var i=1; i<this.sequence.length; i++) {
        var varIds = util.getVarIds(fn);
        query.frames.push(1);
        try {
          util.assertArity(2, fn[1][0][1], query);
        }
        catch(err) {
          // RethinkDB bug, see issue 4189
          //TODO Once the bug is fixed, we should remove this try/catch
          var found = 2;
          var expected = varIds.length;
          util.arityError(expected, found, query);
        }
        query.frames.pop();

        varLeft = varIds[0];
        varRight = varIds[1];

        query.context[varLeft] = result;
        query.context[varRight] = this.get(i);
        query.frames.push(1);
        result = query.evaluate(fn, query, internalOptions);
        query.frames.push(1);
        util.assertType(result, 'DATUM', query);
        query.frames.pop();
        query.frames.pop();
        delete query.context[varLeft];
        delete query.context[varRight];
      }
    }
    else {
      query.frames.push(1);
      var fnValue = query.evaluate(fn, internalOptions);
      if (typeof fnValue === 'function') {
        var result = this.get(0);
        for(var i=1; i<this.length; i++) {
          try {
            var result = fnValue(result, this.get(i))
          }
          catch(error) {
            throw new Error.ReqlRuntimeError(error.toString(), query.frames)
          }
          util.assertJavaScriptResult(result, query);
          result = util.revertDatum(result);
        }
      }
      else {
        util.assertType(fn, 'FUNCTION', query);
      }
      query.frames.pop();
    }
    return result;
  }
}
Sequence.prototype.hasFields = function(keys) {
  var result = new Sequence();
  for(var i=0; i<this.length; i++) {
    if (util.hasFields(this.get(i), keys)) {
      result.push(this.get(i));
    }
  }
  return result;
}

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
}
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
}
Sequence.prototype.contains = function(predicates, query, internalOptions) {
  var numFound = 0;
  var founded = [];
  for(var i=0; i<predicates.length; i++) {
    founded.push(false);
  }

  for(var i=0; i<this.sequence.length; i++) {
    for(var j=0; j<predicates.length; j++) {
      var predicate = predicates[j];
      var predicateResult;
      if (util.isFunction(predicate)) {
        var varId = util.getVarId(predicate);
        query.context[varId] = this.sequence[i];
        query.frames.push(j+1) // +1 because we already evaluated the sequence
        predicateResult = query.evaluate(predicate, query, internalOptions);
        query.frames.push(1);
        util.assertType(predicateResult, 'DATUM', query);
        query.frames.pop();
        query.frames.pop()
        delete query.context[varId];

        if ((founded[j] === false) && (util.isTrue(predicateResult))) {
          founded[j] = true;
          numFound++;
          if (numFound === predicates.length) {
            return true;
          }
        }
      }
      else {
        if (founded[j] === false) {
          var predicateValue = query.evaluate(predicate, query, internalOptions);
          // We could get a function from r.js
          if (typeof predicateValue === 'function') {
            try {
              var predicateResult = predicateValue(util.toDatum(this.sequence[i]));
            }
            catch(error) {
              query.frames.push(1);
              throw new Error.ReqlRuntimeError(error.toString(), query.frames)
            }
            if (util.isTrue(predicateResult)) {
              founded[j] = true;
              numFound++;
              if (numFound === predicates.length) {
                return true;
              }
            }
          }
          else {
            query.frames.push(1);
            util.assertType(predicateValue, 'DATUM', query);
            query.frames.pop();
            if (this.sequence[i] === predicateValue) {
              founded[j] = true;
              numFound++;
              if (numFound === predicates.length) {
                return true;
              }
            }
          }
        }
      }
    }
  }
  for(var i=0; i<founded.length; i++) {
    if (founded[i] === false) {
      return false;
    }
  }
  return true;
}
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
}
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
}

Sequence.prototype.setUnion = function(other, query) {
  // TODO This is a really not efficient now...
  var result = this;
  for(var i=0; i<other.sequence.length; i++) {
    result = result.setInsert(other.sequence[i]);
  }
  return result;
}

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
}

Sequence.prototype.sample = function(sample, query) {
  var result = new Sequence();
  while ((sample > 0) && (this.sequence.length > 0)) {
    var index = Math.floor(Math.random()*this.sequence.length);
    result.push(this.sequence.splice(index, 1));
    sample--;
  }
  return result;
}

Sequence.prototype.merge = function(toMerge, query, internalOptions) {
  var result = new Sequence();

  for(var i=0; i<this.sequence.length; i++) {
    result.push(util.merge(this.sequence[i], toMerge, query, internalOptions))
  }

  return result;
}


Sequence.prototype.eqJoin = function(leftField, other, options, query, internalOptions) {
  // other is a table since eqJoin requires an index

  var result = new Sequence();

  // If leftFiend is a string, replace it with a function
  if (typeof leftField === "string") {
    var uuid = util.uuid();
    leftField = [ 69, [ [ 2, [ uuid ] ], [ 31, [ [ 10, [ uuid ] ], leftField ] ] ] ]
  }

  var varId, leftFieldValue, partial;
  for(var i=0; i<this.sequence.length; i++) {
    varId = leftField[1][0][1][0]; //TODO Refactor

    query.context[varId] = this.sequence[i];
    var leftFieldValue = undefined;
    try{
      leftFieldValue = query.evaluate(leftField, internalOptions);
    }
    catch(err) {
      if (err.message.match(/^No attribute `/) === null) {
        throw err;
      }
      // else we just skip the non existence error
    }
    delete query.context[varId];
    if (leftFieldValue !== undefined) {
      partial = other.getAll([leftFieldValue], options, query).toSequence();

      for(var k=0; k<partial.sequence.length; k++) {
        result.push({
          left: this.sequence[i],
          right: partial.sequence[k]
        })
      }
    }
  }
  return result;
}

Sequence.prototype.join = function(type, other, predicate, query, internalOptions) {
  var result = new Sequence();
  var varIds, predicateResult, returned;


  if (typeof other.toSequence === "function") {
    other = other.toSequence();
  }

  for(var i=0; i<this.sequence.length; i++) {
    returned = false; 
    for(var j=0; j<other.sequence.length; j++) {
      if (util.isFunction(predicate)) {
        varIds = util.getVarIds(predicate)
        query.context[varIds[0]] = this.get(i);
        query.context[varIds[1]] = other.get(j);

        try {
          util.assertArity(2, predicate[1][0][1], query);
        }
        catch(err) {
          // RethinkDB bug, see issue 4189
          //TODO Once the bug is fixed, we should remove this try/catch
          var found = 2;
          var expected = varIds.length;
          util.arityError(expected, found, query);
        }

        predicateResult = query.evaluate(predicate, internalOptions);
        delete query.context[varIds[0]];
        delete query.context[varIds[1]];

        if (util.isTrue(predicateResult)) {
          returned = true;
          result.push({
            left: this.sequence[i],
            right: other.sequence[j]
          });
        }
      }
      else {
        predicateResult = query.evaluate(predicate, internalOptions);
        if (typeof predicateResult === 'function') {
          try{
            predicateResult = predicateResult(this.get(i), other.get(j))
          }
          catch(error) {
            query.frames.push(2);
            throw new Error.ReqlRuntimeError(error.toString(), query.frames)
          }

          if (util.isTrue(predicateResult)) {
            returned = true;
            result.push({
              left: this.sequence[i],
              right: other.sequence[j]
            });
          }
        }
        else if (util.isTrue(predicateResult)) {
          returned = true;
          result.push({
            left: this.sequence[i],
            right: other.sequence[j]
          });
        }
      }
    }
    if ((type === 'outer') && (returned === false)) {
      result.push({
        left: this.sequence[i]
      });
    }
  }
  return result;
}

Sequence.prototype.filter = function(filter, options, query, internalOptions) {
  var sequence = new Sequence();

  if (options.default === undefined) {
    options.default = false;
  }
  
  if (util.isFunction(filter)) {
    var varId = util.getVarId(filter);
    query.frames.push(1);
    util.assertArity(1, util.getVarIds(filter), query);
    for(var i=0; i<this.sequence.length; i++) {
      query.context[varId] = this.sequence[i];
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
      util.assertType(filterResult, 'DATUM', query);
      if (filterResult) { // TODO Should we check for a strict true here?
        sequence.push(this.sequence[i]);
      }
      delete query.context[varId];
    }
    query.frames.pop();
  }
  else {
    query.frames.push(1);
    var filterValue = query.evaluate(filter, query, internalOptions);
    if (typeof filterValue === 'function') {
      for(var i=0; i<this.length; i++) {
        try {
          var filterResult = filterValue(util.toDatum(this.get(i)));
        }
        catch(error) {
          throw new Error.ReqlRuntimeError(error.toString(), query.frames)
        }
        util.assertJavaScriptResult(filterResult, query);
        filterResult = util.revertDatum(filterResult);
        if (util.isTrue(filterResult)) {
          sequence.push(this.get(i));
        }
      }
    }
    else if (util.isPlainObject(filterValue)) {
      for(var i=0; i<this.length; i++) {
        if (util.filter(util.toDatum(this.get(i)), util.toDatum(filterValue))) {
          sequence.push(this.get(i));
        }
      }
    }
    else {
      for(var i=0; i<this.length; i++) {
        if ((filter !== null) && (filter !== false)) {
          sequence.push(this.get(i));
        }
      }
    }
    query.frames.pop();
  }
  return sequence;
}



Sequence.prototype.count = function(predicate, query, internalOptions) {
  if (predicate !== undefined) {
    var result = 0;
    if (util.isFunction(predicate)) {
      for(var i=0; i<this.sequence.length; i++) {
        var varId = util.getVarId(predicate);
        query.context[varId] = this.sequence[i];
        var predicateResult = query.evaluate(predicate, internalOptions);
        util.assertType(predicateResult, 'DATUM', query);
        if (util.isTrue(predicateResult)) {
          result++;
        }
        delete query.context[varId];
      }
    }
    else { // predicate is a value
      predicateResult = query.evaluate(predicate);
      if (typeof predicateResult === 'function') {
        for(var i=0; i<this.length; i++) {
          if (util.isTrue(predicateResult(this.get(i)))) {
            result++;
          }
        }
      }
      else {
        for(var i=0; i<this.length; i++) {
          if (util.eq(util.toDatum(this.get(i)), util.toDatum(predicateResult))) {
            result++;
          }
        }
      }
    }
    return result;
  }
  else {
    return this.sequence.length;
  }
}

Sequence.prototype.skip = function(skip, query) {
  var result = new Sequence();
  if (skip < 0) {
    skip = Math.max(0, this.sequence.length+skip);
  }
  for(var i=skip; i<this.sequence.length; i++) {
    // TODO Should we also deep copy this.selection[i]
    result.push(this.sequence[i]);
  }
  return result;
}

Sequence.prototype.limit = function(limit) {
  // TODO Should we make deep copies of this.get(i)
  var result = new Sequence();
  for(var i=0; i<Math.min(limit,this.length); i++) {
    result.push(this.get(i));
  }
  return result;
}

Sequence.prototype.pluck = function(keys) {
  var result = new Sequence();
  for(var i=0; i<this.sequence.length; i++) {
    result.push(util.pluck(this.sequence[i], keys));
  }
  return result;
}
Sequence.prototype.without = function(keys) {
  var result = new Sequence();
  for(var i=0; i<this.length; i++) {
    result.push(util.without(this.get(i), keys));
  }
  return result;
}


Sequence.prototype.slice = function(start, end, options, query) {
  var result = new Sequence();

  if (options.left_bound === undefined) {
    options.left_bound = "closed";
  }
  else if ((options.left_bound !== 'closed') && (options.left_bound !== 'open')) {
    throw new Error.ReqlRuntimeError('Expected `open` or `closed` for optarg `left_bound` (got `"'+options.left_bound+'"`)', query.frames)
  }
  if (options.right_bound === undefined) {
    options.right_bound = "open";
  }
  else if ((options.right_bound !== 'closed') && (options.right_bound !== 'open')) {
    throw new Error.ReqlRuntimeError('Expected `open` or `closed` for optarg `right_bound` (got `"'+options.left_bound+'"`)', query.frames)
  }



  // TODO Check arguments
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
    if (i >=this.sequence.length) { break }
    // TODO Should we also deep copy this.selection[i]
    result.push(this.sequence[i]);
  }
  return result;
}

Sequence.prototype.nth = function(index, query) {
  var originalIndex = index;
  if (index < 0) {
    index = this.sequence.length+index;
  }
  if ((index >= this.sequence.length) || (index < 0)) {
    throw new Error.ReqlRuntimeError("Index out of bounds: "+originalIndex, query.frames)
  }
  return this.sequence[index]
}

Sequence.prototype.offsetsOf = function(predicate, query) {
  var result = new Sequence();

  var predicateResult;
  if (!util.isFunction(predicate)) {
    predicateResult = query.evaluate(predicate, query);
    query.frames.push(1);
    util.assertType(predicateResult, 'DATUM', query);
    query.frames.pop();
  }
  for(var i=0; i<this.sequence.length; i++) {
    if (util.isFunction(predicate)) {
      var varId = util.getVarId(predicate);
      query.context[varId] = this.get(i);
      query.frames.push(1);
      query.frames.push(1);
      predicateResult = query.evaluate(predicate, query);
      util.assertType(predicateResult, 'DATUM', query);
      query.frames.pop();
      query.frames.pop();
      // We don't have to check that predicateResult is a DATUM
      // as this check will be perfomed with FUNC

      if (util.isTrue(predicateResult)) {
        result.push(this.get(i));
      }
      delete query.context[varId];
    }
    else {
      if (util.eq(this.sequence[i], predicateResult)) {
        result.push(i);
      }
    }
  }
  return result;
}

Sequence.prototype.isEmpty = function(query) {
  return this.length === 0;
}

// map is variadic, hence this method.
Sequence.map = function(sequences, fn, query, internalOptions) {
  var result = new Sequence();

  for(var i=0; i<sequences[0].sequence.length; i++) {
    // We stop at the shortest sequence
    for(var j=0; j<sequences.length; j++) {
      if (sequences[j].sequence[i] === undefined) {
        return result;
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
            throw new Error.ReqlRuntimeError('The function passed to `map` expects '+varIds.length+' argument, but '+sequences.length+' sequences were found', query.frames)
          }
        }
        else {
          if (sequences.length === 1) {
            throw new Error.ReqlRuntimeError('The function passed to `map` expects '+varIds.length+' arguments, but '+sequences.length+' sequence was found', query.frames)
          }
        }
      }
      for(var k=0; k<varIds.length; k++) {
        query.context[varIds[k]] = sequences[k].sequence[i];
      }
      query.frames.push(sequences.length);
      var resultFn = query.evaluate(fn, internalOptions);
      result.push(resultFn)
      query.frames.push(1);
      util.assertType(resultFn, 'DATUM', query);
      query.frames.pop();
      query.frames.pop();
      for(var k=0; k<varIds.length; k++) {
        delete query.context[varIds[k]];
      }
    }
    else {
      query.frames.push(sequences.length);
      var fnValue = query.evaluate(fn, internalOptions);
      if (typeof fnValue === 'function') {
        var args = [];
        for(var k=0; k<sequences.length; k++) {
          args.push(util.toDatum(sequences[k].sequence[i]))
        }
        var fnResult = fnValue.apply({}, args);
        util.assertJavaScriptResult(fnResult, query);
        fnResult = util.revertDatum(fnResult);
        result.push(fnResult);
      }
      else {
        util.assertType(fn, 'FUNCTION', query);
      }
      query.frames.pop();
    }
  }
  return result;
}

Sequence.prototype.concatMap = function(fn, query, internalOptions) {
  //TODO Check that fn is a function
  var result = new Sequence();
  var partial;
  for(var i=0; i<this.length; i++) {
    if (util.isFunction(fn)) {
      var varId = util.getVarId(fn);
      query.context[varId] = this.get(i);
      query.frames.push(1);
      partial = query.evaluate(fn, internalOptions);
      query.frames.push(1);
      util.assertType(partial, 'SEQUENCE', this);
      query.frames.pop();
      query.frames.pop();
      partial = util.toSequence(partial, query);
      for(var k=0; k<partial.length; k++) {
        result.push(partial.get(k));
      }
      delete query.context[varId]
    }
    else {
      query.frames.push(1);
      var resultFn = query.evaluate(fn);
      query.frames.pop();
      if (typeof resultFn === 'function') {
        partial = resultFn(this.get(i))
        partial = util.toSequence(partial, query);
        //util.assertType(partial, 'SEQUENCE', this);
        for(var k=0; k<partial.length; k++) {
          result.push(partial[k]);
        }
      }
      else {
        query.frames.push(1); // We will throw
        util.assertType(resultFn, 'FUNCTION', query);
      }
    }
  }

  return result;
}
Sequence.prototype.withFields = function(fields) {
  // fields is a Sequence

  var result = new Sequence();
  var valid, element;
  for(var i=0; i<this.length; i++) {
    /*
    valid = true;
    for(var j=0; j<fields.length; j++) {
      if (this.get(i)[fields.get(j)] === undefined) {
        valid = false;
        break;
      }
    }
    */
    //if (valid === true) {
    if (util.hasFields(this.get(i), fields)) {
      element = {};
      for(var j=0; j<fields.length; j++) {
        if (typeof fields.get(j) === 'string') {
          element[fields.get(j)] = this.get(i)[fields.get(j)]
        }
        else if (util.isPlainObject(fields)) {
          var fieldsToCopy = Object.keys(fields.get(j));
          for(var k=0; k<fieldsToCopy.length; k++) {
            element[fieldsToCopy[k]] = this.get(i)[fieldsToCopy[k]];
          }
        }
      }
      result.push(element)
    }
  }
  return result;
}

Sequence.prototype.orderBy = function(fields, options, query, internalOptions) {
  if (options.index !== undefined) {
    //TODO Send the appropriate message
    throw new Error.ReqlRuntimeError("Indexed order_by can only be performed on a TABLE or TABLE_SLICE", query.frames);
  }

  var result = new Sequence(this.sequence);
  result._orderBy(fields, query, internalOptions);
  return result;
}

Sequence.prototype._orderBy = function(fields, query, internalOptions) {
  this.sequence.sort(function(left, right) {
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
        try {
          leftValue = query.evaluate(field, internalOptions);
        }
        catch(err) {
          leftValue = err;
        }
        delete query.context[varId];

        query.context[varId] = right;
        try {
          rightValue = query.evaluate(field, internalOptions);
        }
        catch(err) {
          rightValue = err;
        }
        delete query.context[varId];
      }
      else {
        field = query.evaluate(field, internalOptions);
        if (typeof field === 'function') {
          query.frames.push(i+1);
          try {
            leftValue = field(util.toDatum(left));
            util.assertJavaScriptResult(leftValue, query);
          }
          catch(err) {
            if (err.message.match(/^No attribute/)) {
              leftValue = err;
            }
            else {
              throw err;
            }
          }
          leftValue = util.revertDatum(leftValue);
          try {
            rightValue = field(util.toDatum(right));
            util.assertJavaScriptResult(rightValue, query);
          }
          catch(err) {
            if (err.message.match(/^No attribute/)) {
              rightValue = err;
            }
            else {
              throw err;
            }
          }
          rightValue = util.revertDatum(rightValue);
          query.frames.pop();
        }
        else {
          query.frames.push(i+1);
          util.assertType(field, "STRING", query);
          query.frames.pop();
          try {
            leftValue = util.getBracket(left, field, query);
          }
          catch(err) {
            leftValue = err;
          }
          try {
            rightValue = util.getBracket(right, field, query);
          }
          catch(err) {
            rightValue = err;
          }

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


Sequence.prototype.getField = function(field) {
  var result = new Sequence();
  for(var i=0; i<this.length; i++) {

    if (this.get(i)[field] !== undefined) {
      result.push(this.get(i)[field]);
    }
    // else we just skip those values
  }
  return result;
}

Sequence.prototype.toDatum = function() {
  var result = [];
  for(var i=0; i<this.sequence.length; i++) {
    //TODO Check for null
    if (typeof this.sequence[i].toDatum === "function") {
      result.push(this.sequence[i].toDatum());
    }
    else {
      result.push(util.toDatum(this.sequence[i]));
    }
  }
  return result;
}

Sequence.prototype.getBracket = function(key, query) {
  if (typeof key === 'number') {
    return this.sequence[key];
  }
  else {
    var result = new Sequence();
    for(var i=0; i<this.sequence.length; i++) {
      if (util.isSequence(this.get(i))) {
        throw new Error.ReqlRuntimeError('Cannot perform bracket on a sequence of sequences', query.frames)
      }
      result.push(util.getBracket(this.get(i), key, query));
    }
    return result;
  }
}

Sequence.prototype.clone = function() {
  var result = new Sequence();
  for(var i=0; i<this.sequence.length; i++) {
    result.push(this.sequence[i]);
  }
  return result;
}


Sequence.prototype.forEach = function(fn, query, internalOptions) {
  var result = util.writeResult();
  for(var i=0; i<this.sequence.length; i++) {
    if (util.isFunction(fn)) {
      var varId = util.getVarId(fn);
      query.context[varId] = this.sequence[i];
      writeResult = query.evaluate(fn, this, internalOptions);
      util.assertType(writeResult, 'DATUM', query);
      util.mergeWriteResult(result, writeResult);

      delete query.context[varId];
    }
    else {
      writeResult = query.evaluate(fn, this, internalOptions);
      if (typeof writeResult === 'function') {
        try {
          writeResult = writeResult(util.toDatum(this.sequence[i]));
        }
        catch(error) {
          query.frames.push(1);
          throw new Error.ReqlRuntimeError(error.toString(), query.frames)
        }
        util.mergeWriteResult(result, writeResult);
      }
      else {
        util.mergeWriteResult(result, writeResult);
      }
    }
  }
  return result;

}

Sequence.prototype.intersects = function(geometry, query) {
  var result = new Sequence();
  for(var i=0; i<this.length; i++) {
    if (!util.isGeometry(this.get(i))) {
      throw new Error.ReqlRuntimeError('Cannot perform intersects on a non-object non-sequence `'+JSON.stringify(this.get(i), null, 4)+'`', query.frames)
    }
    if (this.get(i).intersects(geometry)) {
      result.push(this.get(i));
    }
  }
  return result;
}

Sequence.prototype.includes = function(geometry, query) {
  var result = new Sequence();
  for(var i=0; i<this.length; i++) {
    if (!util.isGeometry(this.get(i))) {
      throw new Error.ReqlRuntimeError('Cannot perform includes on a non-object non-sequence `'+JSON.stringify(this.get(i), null, 4)+'`', query.frames)
    }
    if (this.get(i).includes(geometry)) {
      result.push(this.get(i));
    }
  }
  return result;
}


Sequence.prototype.setInfiniteRange = function() {
  this.infiniteRange = true;
  this.stream = true;
  this.length = Infinity;
}

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
}

module.exports = Sequence
