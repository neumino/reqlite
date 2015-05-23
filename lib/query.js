// The main logic goes here parsing/executing queries

var protodef = require(__dirname+"/protodef.js");
var termTypes = protodef.Term.TermType;
var util = require(__dirname+"/utils/main.js");
var Promise = require("bluebird");

var Database = require(__dirname+"/database.js");
var Table = require(__dirname+"/table.js");
var Sequence = require(__dirname+"/sequence.js");
var Error = require(__dirname+"/error.js");

var Document = require(__dirname+"/document.js");
var Selection = require(__dirname+"/selection.js");
var Group = require(__dirname+"/group.js");
var Changes = require(__dirname+"/changes.js");

var Minval = require(__dirname+"/minval.js");
var Maxval = require(__dirname+"/maxval.js");
var Asc = require(__dirname+"/asc.js");
var Desc = require(__dirname+"/desc.js");
var Literal = require(__dirname+"/literal.js");
var ReqlDate = require(__dirname+"/date.js");
var ReqlGeometry = require(__dirname+"/geo.js");

var constants = require(__dirname+"/constants.js");

//TODO Make sure that we test for NaN, Infinity when running a JS function
// Keep things in a function in case we somehow decide to implement lazy cursor

//TODO Check more options like in insert

//TODO Populate clustering config

//TODO Replace "= []" with "= new Sequence"

//TODO Revert datum after JS function
function Query(server, query, options) {
  this.server = server;
  this.query = query;
  this.options = {};
  this.context = {};
  this.frames = [];
  this.now = ReqlDate.now();
  this.complete = false;
  //this.token = token;

}
//TODO Make a deep copy of the results for the local browser?

// Reqlite doesn't evaluate things in a lazy way. The only thing that can be lazily
// returned is actually an infinite range.
// Return a promise
Query.prototype.continue = function(query) {
  var self = this;
  var response = new Promise(function(resolve, reject) {
    if ((self.result instanceof Sequence) && (self.result.infiniteRange === true)) {
      toReturn = new Sequence();
      for(var i=0; i<constants.ROW_PER_BATCH; i++) {
        //TODO Handle undefined
        toReturn.push(self.result.get());
      }
      toReturn = util.toDatum(toReturn, self);
      resolve({
        t: protodef.Response.ResponseType.SUCCESS_PARTIAL,
        r: toReturn,
        n: []
      });
    }
    else if (util.isChanges(self.result)) {
      self.result.onNext(function(data, notes) {
        // The feed is responsible to build the response
        resolve(data);
      });
    }
  })
  return response
}
//TODO Implement query.stop

//TODO do we actually need toDatum?

Query.prototype.stop = function(query) {
  if (util.isChanges(this.result)) {
    this.result.stop();
  }
}
//TODO Make sure there are no stray arrays going around
Query.prototype.run = function(query) {
  query = query || this.query;

  var queryType = query[0];

  try {
    this.options = query[2];
    var result = this.evaluate(query[1], {});
    var toReturn;

    // TODO Check more types
    if (result instanceof Database) {
      throw new Error.ReqlRuntimeError("Query result must be of type DATUM, GROUPED_DATA, or STREAM (got DATABASE)", query.frames);
    }
    //TODO switch to result.stream === true instead of infiniteRange
    if ((result instanceof Sequence) && (result.infiniteRange === true)) {
      this.result = result;
      this.complete = false;
      toReturn = new Sequence();
      for(var i=0; i<constants.ROW_PER_BATCH; i++) {
        toReturn.push(result.get());
      }
      toReturn = util.toDatum(toReturn, this);
    }
    else if (util.isChanges(result)) {
      this.result = result;
      this.result.startListener();
      toReturn = util.toDatum(this.result.getInitialResponse());
      this.complete = this.result.complete;
      //TODO Pass notes
    }
    else {
      this.complete = true;
      toReturn = util.toDatum(result, this);
      toReturn = [toReturn]; // Why do we need that?
    }

    var response;
    if (this.complete === true) {
      response = {
        t: protodef.Response.ResponseType.SUCCESS_ATOM,
        r: toReturn
      }
    }
    else {
      response = {
        t: protodef.Response.ResponseType.SUCCESS_PARTIAL,
        r: toReturn,
        n: []
      }
    }
    return response
  }
  catch(err) {
    return {
      t: err.type,
      r: [err.message],
      b: err.frames || [],
      debug: err.stack
    }
  }
}

Query.prototype.evaluate = function(term, internalOptions) {
  internalOptions = internalOptions || {};
  // Check if one of the arguments is `r.args`, in which case we reinject the
  // arguments in term
  if (Array.isArray(term) && (Array.isArray(term[1]))) {
    for(var i=0; i<term[1].length; i++) {
      if (util.isRawArgs(term[1][i])) {
        // 1 -- args
        // i -- current arg
        // 1 -- arguments of r.args
        // 0 -- first argument of r.args
        // 0 -- MAKE_ARRAY
        if ((term[1][i][1] === undefined) || (Array.isArray(term[1][i][1]) && (term[1][i][1].length === 0))) {
          this.frames.push(i);
          throw new Error.ReqlRuntimeError('Expected 1 argument but found 0', this.frames)
          this.frames.pop();
        }
        else {
          // This is kind of weird, we should use ArityRange if RethinkDB didn't sent the wrong error
          this.frames.push(i);
          util.assertArity(1, term[1][i][1], this);
          this.frames.pop();
          if (term[1][i][1][0][0] === termTypes.MAKE_ARRAY) {
            if (term[1][i][1].length > 1) {
              this.frames.push(0);
              util.arityError(1, 2, this);
              // Not poping as util.arityError is throwing an error.
            }
            term[1].splice.apply(term[1], [i, 1].concat(term[1][i][1][0][1]));
            i--;
          }
          else {
            var value = this.evaluate(term[1][i][1][0], internalOptions);
            this.frames.push(0);
            util.assertType(value, 'ARRAY', this);
            this.frames.pop();
          }
        }
      }
    }
  }

  if ((Array.isArray(term) === false) && (util.isPlainObject(term) === false)) {
    // Primtiive
    return term;
  }
  else if (util.isPlainObject(term)) {
    // Plain object
    var keys = Object.keys(term);
    var result = {};
    for(var i=0; i<keys.length; i++) {
      this.frames.push(keys[i])
      result[keys[i]] = this.evaluate(term[keys[i]], internalOptions);
      this.frames.pop()
    }
    return result;
  }

  var termType = term[0];
  var options = this.evaluate(term[2], internalOptions) || {};

  //TODO Check arity
  switch(termType) {
    case termTypes.MAKE_ARRAY: // 2
      var ar = new Sequence();
      for(var i=0; i<term[1].length; i++) {
        this.frames.push(i);
        var element = this.evaluate(term[1][i], internalOptions);
        util.assertType(element, 'DATUM', this);
        ar.push(element);
        this.frames.pop();
      }
      return ar;
    case termTypes.MAKE_OBJ:
      // This is deprecated code, the current driver cannot reach this code.
      // options are already evaluated
      util.assertArity(0, term[1], this);
      return options;
    case termTypes.VAR: // 10
      util.assertArity(1, term[1], this);
      this.frames.push(0)
      var varId = this.evaluate(term[1][0], internalOptions);
      this.frames.pop()
      if (this.context[varId] === undefined) {
        throw new Error.ReqlRuntimeError("The server is buggy, context not found")
      }
      return this.context[varId];
    case termTypes.JAVASCRIPT:
      if (internalOptions.deterministic === true) { util.nonDeterministicOp(this); }
      util.assertArity(1, term[1], this);
      if (constants.ENABLE_JS === true) {
        // This is not safe, we CANNOT contain eval
        var result;
        function evalInContext(term) {
          result = eval(term);
          return result;
        }
        result = evalInContext.call({}, term[1][0]);
        //TODO test
        if ((result === Infinity) || (result === -Infinity)) {
          throw new Error.ReqlRuntimeError('Number return value is not finite', this.frames)
        }
      }
      else {
        throw new Error.ReqlRuntimeError("`r.js was disabled. Update ENABLE_JS in `constants.js` if you want to enable `r.js`", this.frames)
      }
      return result;
    case termTypes.UUID:
      util.assertArity(0, term[1], false, this);
      return util.uuid();
    case termTypes.HTTP:
      //TODO This is the only? asynchronous call...
      util.notAvailable(null, this);
    case termTypes.ERROR:
      util.assertArityRange(0, 1, term[1], this);
      this.frames.push(0);
      var message = this.evaluate(term[1][0], internalOptions);
      util.assertType(message, 'STRING', this);
      this.frames.pop();

      throw new Error.ReqlRuntimeError(message, this.frames);
    case termTypes.IMPLICIT_VAR: // 13
      util.assertArity(0, term[1], this);
      if (Object.keys(this.context).length > 1) {
        // This should happen only if there is a bug in the driver or in the server
        throw new Error.ReqlRuntimeError("Ambiguous implicit var");
      }
      return this.context[Object.keys(this.context)[0]];
    case termTypes.DB: // 14
      if (internalOptions.deterministic === true) { util.nonDeterministicOp(this); }
      util.assertArity(1, term[1], this);
      this.frames.push(0)
      var arg = this.evaluate(term[1][0], internalOptions);
      util.assertType(arg, 'STRING', this);
      util.assertNoSpecialChar(arg, 'Database', this);
      this.frames.pop();

      if (this.server.databases[arg] == null) {
        throw new Error.ReqlRuntimeError("Database `"+arg+"` does not exist", this.frames)
      }
      return this.server.databases[arg];
    case termTypes.TABLE: // 15
      util.assertArityRange(1, 2, term[1], this); // Enforced by the driver
      var db, tableName;
      if (term[1].length === 1) {
        db = this.evaluate(this.options.db, internalOptions) || this.server.databases['test'];
        this.frames.push(0);
        tableName = this.evaluate(term[1][0], internalOptions);
        util.assertType(tableName, 'STRING', this);
        util.assertNoSpecialChar(tableName, 'Table', this);
        this.frames.pop();
      }
      else if (term[1].length === 2) {
        this.frames.push(0);
        db = this.evaluate(term[1][0], internalOptions);
        util.assertType(db, 'DATABASE', this);
        this.frames.pop();
        this.frames.push(1);
        tableName = this.evaluate(term[1][1], internalOptions);
        util.assertType(tableName, 'STRING', this);
        util.assertNoSpecialChar(tableName, 'Table', this);
        this.frames.pop();
      }

      if (db.tables[tableName] == null) {
        throw new Error.ReqlRuntimeError("Table `"+db.name+'.'+tableName+"` does not exist", this.frames)
      }
      return db.tables[tableName];
    case termTypes.GET:
      util.assertArity(2, term[1], this);
      this.frames.push(0);
      var table = this.evaluate(term[1][0], internalOptions);
      util.assertType(table, 'TABLE', this);
      this.frames.pop();

      this.frames.push(1);
      var value = this.evaluate(term[1][1], internalOptions);
      util.assertType(value, 'DATUM', this);
      this.frames.pop();

      return table.get(value);
    case termTypes.GET_ALL:
      util.assertArityRange(2, Infinity, term[1], this);
      this.frames.push(0);
      var table = this.evaluate(term[1][0], internalOptions)
      util.assertType(table, 'TABLE', this);
      this.frames.pop();

      var values = [];
      for(var i=1; i<term[1].length; i++) {
        this.frames.push(i);
        values.push(this.evaluate(term[1][i], internalOptions))
        this.frames.pop();
      }
      return table.getAll.call(table, values, options, this, internalOptions);
    case termTypes.EQ:
      util.assertArityRange(2, Infinity, term[1], this);
      this.frames.push(0);
      var left = this.evaluate(term[1][0], internalOptions);
      util.assertType(left, 'DATUM', this);
      this.frames.pop()

      for(var i=1; i<term[1].length; i++) {
        this.frames.push(i);
        right = this.evaluate(term[1][i], internalOptions);
        util.assertType(right, 'DATUM', this);
        this.frames.pop();
        if (util.eq(left, right) === false) {
          return false;
        }
      }
      return true;
    case termTypes.NE:
      util.assertArityRange(2, Infinity, term[1], this);
      this.frames.push(0);
      var left = this.evaluate(term[1][0], internalOptions);
      util.assertType(left, 'DATUM', this);
      this.frames.pop();

      for(var i=0; i<term[1].length; i++) {
        this.frames.push(i);
        right = this.evaluate(term[1][i], internalOptions);
        util.assertType(right, 'DATUM', this);
        this.frames.pop();
        if (util.eq(left, right) === false) {
          return true;
        }
      }
      return false;
    case termTypes.LT:
      util.assertArityRange(2, Infinity, term[1], this);
      this.frames.push(0);
      var left = this.evaluate(term[1][0], internalOptions);
      util.assertType(left, 'DATUM', this);
      this.frames.pop();

      for(var i=1; i<term[1].length; i++) {
        this.frames.push(i);
        right = this.evaluate(term[1][i], internalOptions);
        util.assertType(right, 'DATUM', this);
        this.frames.pop();
        if (util.lt(left, right) === false) {
          return false;
        }
      }
      return true;
    case termTypes.LE:
      util.assertArityRange(2, Infinity, term[1], this);
      this.frames.push(0);
      var left = this.evaluate(term[1][0], internalOptions);
      util.assertType(left, 'DATUM', this);
      this.frames.pop();

      for(var i=1; i<term[1].length; i++) {
        this.frames.push(i);
        right = this.evaluate(term[1][i], internalOptions);
        util.assertType(right, 'DATUM', this);
        this.frames.pop();
        if ((util.lt(left, right) === false) && (util.eq(left, right) === false)) {
          return false;
        }
      }
      return true;
    case termTypes.GT:
      util.assertArityRange(2, Infinity, term[1], this);
      this.frames.push(0);
      var left = this.evaluate(term[1][0], internalOptions);
      util.assertType(left, 'DATUM', this);
      this.frames.pop();

      for(var i=1; i<term[1].length; i++) {
        this.frames.push(i);
        right = this.evaluate(term[1][i], internalOptions);
        util.assertType(right, 'DATUM', this);
        this.frames.pop();
        if (util.gt(left, right) === false) {
          return false;
        }
      }
      return true;
    case termTypes.GE:
      util.assertArityRange(2, Infinity, term[1], this);
      this.frames.push(0);
      var left = this.evaluate(term[1][0], internalOptions);
      util.assertType(left, 'DATUM', this);
      this.frames.pop();

      for(var i=1; i<term[1].length; i++) {
        this.frames.push(i);
        right = this.evaluate(term[1][i], internalOptions);
        util.assertType(right, 'DATUM', this);
        this.frames.pop();
        if ((util.gt(left, right) === false) && (util.eq(left, right) === false)) {
          return false;
        }
      }
      return true;
    case termTypes.NOT:
      util.assertArity(1, term[1], this);
      this.frames.push(0);
      var value = this.evaluate(term[1][0], internalOptions);
      util.assertType(value, 'DATUM', this);
      this.frames.pop();
      return !util.toBool(value);
    case termTypes.ADD:
      util.assertArityRange(1, Infinity, term[1], this);
      this.frames.push(0);
      var result = this.evaluate(term[1][0], internalOptions);
      util.assertType(result, 'DATUM', this);
      this.frames.pop();

      var valueToAdd;
      for(var i=1; i<term[1].length; i++) {
        this.frames.push(i);
        valueToAdd = this.evaluate(term[1][i], internalOptions);
        util.assertType(valueToAdd, 'DATUM', this);
        this.frames.pop();

        if (util.isDate(result)) {
          util.assertType(valueToAdd, "NUMBER", this);
          result.epoch_time += valueToAdd;
        }
        else if (util.isSequence(result)) {
          result = result.concat(valueToAdd);
        }
        else {
          util.assertType(valueToAdd, util.getType(result), this);
          result += valueToAdd;
        }
      }
      return result;
    case termTypes.SUB:
      util.assertArityRange(1, Infinity, term[1], this);
      this.frames.push(0);
      var result = this.evaluate(term[1][0], internalOptions);
      util.assertType(result, 'DATUM', this);
      this.frames.pop();

      var valueToSubtract;
      for(var i=1; i<term[1].length; i++) {
        this.frames.push(i);
        valueToSubtract = this.evaluate(term[1][i], internalOptions);
        util.assertType(valueToSubtract, 'DATUM', this);
        this.frames.pop();

        if (util.isDate(result)) {
          try {
            util.assertType(valueToSubtract, "PTYPE<TIME>", this);
          }
          catch(err) {
            util.assertType(valueToSubtract, "NUMBER", this);
          }
          if (util.getType(valueToSubtract) === 'NUMBER') {
            result.epoch_time -= valueToSubtract;
          }
          else {
            result = result.epoch_time - valueToSubtract.epoch_time
          }
        }
        else {
          util.assertType(result, "NUMBER", this);
          util.assertType(valueToSubtract, "NUMBER", this);
          result -= valueToSubtract;
        }
      }
      return result;
    case termTypes.MUL:
      util.assertArityRange(1, Infinity, term[1], this);
      this.frames.push(0);
      var result = this.evaluate(term[1][0], internalOptions);
      util.assertType(result, 'DATUM', this);
      this.frames.pop();
      try {
        util.assertType(result, "ARRAY", this);
      }
      catch(err) {
        util.assertType(result, "NUMBER", this);
      }

      if (util.isSequence(result)) {
        // Keep a reference of the sequence to concat
        var toConcat = result.clone();
      }

      for(var i=1; i<term[1].length; i++) {
        this.frames.push(i);
        valueToMul = this.evaluate(term[1][i], internalOptions);
        util.assertType(valueToMul, 'DATUM', this);
        this.frames.pop();
        util.assertType(valueToMul, 'NUMBER', this);

        if (result instanceof Sequence) {
          for(var j=0; j<valueToMul-1; j++) {
            for(var k=0; k<toConcat.length; k++) {
              result.push(toConcat.get(k))
            }
          }
        }
        else {
          result *= valueToMul;
        }
      }
      return result;
    case termTypes.DIV:
      util.assertArityRange(1, Infinity, term[1], this);
      this.frames.push(0);
      var result = this.evaluate(term[1][0], internalOptions);
      util.assertType(result, 'DATUM', this);
      this.frames.pop();
      util.assertType(result, 'NUMBER', this);

      for(var i=1; i<term[1].length; i++) {
        this.frames.push(i);
        valueToDiv = this.evaluate(term[1][i], internalOptions);
        util.assertType(valueToDiv, 'DATUM', this);
        this.frames.pop();
        util.assertType(valueToDiv, 'NUMBER', this);

        util.assertType(valueToDiv, "NUMBER", this);
        if (valueToDiv === 0) {
          throw new Error.ReqlRuntimeError('Cannot divide by zero', this.frames)
        }
        result /= valueToDiv;
      }
      return result;
    case termTypes.MOD:
      util.assertArity(2, term[1], this);
      this.frames.push(0);
      var numerator = this.evaluate(term[1][0], internalOptions);
      util.assertType(numerator, 'DATUM', this);
      util.assertType(numerator, "NUMBER", this);
      util.assertType(numerator, "INT", this);
      this.frames.pop();

      this.frames.push(1);
      var denominator = this.evaluate(term[1][1], internalOptions);
      util.assertType(denominator, 'DATUM', this);
      util.assertType(denominator, "NUMBER", this);
      util.assertType(denominator, "INT", this);
      this.frames.pop();
      
      if (denominator === 0) {
        throw new Error.ReqlRuntimeError('Cannot take a number modulo 0', this.frames)
      }

      var remainder = numerator%denominator;
      if ((remainder < 0) && (numerator >= 0)) {
        remainder += denominator;
      }
      return remainder;
    case termTypes.APPEND:
      util.assertArity(2, term[1], this);
      this.frames.push(0);
      var sequence = this.evaluate(term[1][0], internalOptions);
      util.assertType(sequence, 'DATUM', this);
      this.frames.pop();

      this.frames.push(1);
      var element = this.evaluate(term[1][1], internalOptions);
      util.assertType(element, 'DATUM', this);
      this.frames.pop();

      // Well, RethinkDB doesn't properly frame that...
      util.assertType(sequence, 'ARRAY', this);
      sequence.push(element);
      return sequence;
    case termTypes.PREPEND:
      util.assertArity(2, term[1], this);
      this.frames.push(0);
      var sequence = this.evaluate(term[1][0], internalOptions);
      util.assertType(sequence, 'DATUM', this);
      this.frames.pop();

      this.frames.push(1);
      var element = this.evaluate(term[1][1], internalOptions);
      util.assertType(element, 'DATUM', this);
      this.frames.pop();

      // Well, RethinkDB doesn't properly frame that...
      util.assertType(sequence, 'ARRAY', this);
      sequence.unshift(element);
      return sequence;
    case termTypes.DIFFERENCE:
      // Non datum are allowed here (typically tables)
      util.assertArity(2, term[1], this);
      this.frames.push(0);
      var sequence = this.evaluate(term[1][0], internalOptions);
      this.frames.pop();

      this.frames.push(1);
      var other = this.evaluate(term[1][1], internalOptions);
      this.frames.pop();

      util.toSequence(sequence, this);
      other = util.toSequence(other, this);
      return sequence.difference(other, this);
    case termTypes.SET_INSERT:
      util.assertArity(2, term[1], this);
      this.frames.push(0);
      var sequence = this.evaluate(term[1][0], internalOptions);
      util.assertType(sequence, 'DATUM', this);
      this.frames.pop();

      this.frames.push(1);
      var value = this.evaluate(term[1][1], internalOptions);
      util.assertType(value, 'DATUM', this);
      this.frames.pop();

      util.assertType(sequence, 'ARRAY', this);
      return sequence.setInsert(value, this);
    case termTypes.SET_INTERSECTION:
      util.assertArity(2, term[1], this);
      this.frames.push(0);
      var sequence = this.evaluate(term[1][0], internalOptions);
      util.assertType(sequence, 'DATUM', this);
      this.frames.pop();

      this.frames.push(1);
      var other = this.evaluate(term[1][1], internalOptions);
      util.assertType(other, 'DATUM', this);
      this.frames.pop();

      util.assertType(sequence, 'ARRAY', this);
      util.assertType(other, 'ARRAY', this);

      return sequence.setIntersection(other, this);
    case termTypes.SET_UNION:
      util.assertArity(2, term[1], this);
      this.frames.push(0);
      var sequence = this.evaluate(term[1][0], internalOptions);
      util.assertType(sequence, 'DATUM', this);
      this.frames.pop();

      this.frames.push(1);
      var other = this.evaluate(term[1][1], internalOptions);
      util.assertType(other, 'DATUM', this);
      this.frames.pop();

      util.assertType(sequence, 'ARRAY', this);
      util.assertType(other, 'ARRAY', this);

      return sequence.setUnion(other, this);
    case termTypes.SET_DIFFERENCE:
      util.assertArity(2, term[1], this);
      this.frames.push(0);
      var sequence = this.evaluate(term[1][0], internalOptions);
      util.assertType(sequence, 'DATUM', this);
      this.frames.pop();

      this.frames.push(1);
      var other = this.evaluate(term[1][1], internalOptions);
      util.assertType(other, 'DATUM', this);
      this.frames.pop();

      util.assertType(sequence, 'ARRAY', this);
      util.assertType(other, 'ARRAY', this);

      return sequence.setDifference(other);
    case termTypes.SLICE:
      util.assertArityRange(2, 3, term[1], this);
      this.frames.push(0);
      var sequence = this.evaluate(term[1][0], internalOptions)
      util.assertType(sequence, ["ARRAY", "PTYPE<BINARY>"], this);
      this.frames.pop();

      if (term[1].length > 1) {
        this.frames.push(1);
        var start = this.evaluate(term[1][1], internalOptions)
        util.assertType(start, 'NUMBER', this);
        this.frames.pop();
      }

      var end;
      if (term[1].length > 2) {
        this.frames.push(2);
        end = this.evaluate(term[1][2], internalOptions)
        util.assertType(end, 'NUMBER', this);
        this.frames.pop();
      }
      return sequence.slice(start, end, options, this);
    case termTypes.SKIP:
      util.assertArity(2, term[1], this);
      this.frames.push(0);
      var sequenceOrBin = this.evaluate(term[1][0], internalOptions)
      util.assertType(sequenceOrBin, ['ARRAY', 'PTYPE<BINARY>'], this);
      this.frames.pop();

      this.frames.push(1);
      var skip = this.evaluate(term[1][1], internalOptions)
      util.assertType(skip, "NUMBER", this);
      this.frames.pop();

      if (util.isBinary(sequenceOrBin)) {
        var buffer = new Buffer(sequenceOrBin.data, 'base64');
        buffer = buffer.slice(skip);
        sequenceOrBin.data = buffer.toString('base64');
        return sequenceOrBin;
      }
      else {
        return sequenceOrBin.skip(skip, this);
      }
    case termTypes.LIMIT:
      util.assertArity(2, term[1], this);
      this.frames.push(0);
      var sequence = this.evaluate(term[1][0], internalOptions)
      this.frames.pop();

      // This will enforce the sequence type
      if (!util.isSelection(sequence)) {
        util.toSequence(sequence, this);
      }

      this.frames.push(1);
      var limit = this.evaluate(term[1][1], internalOptions)
      util.assertType(limit, "NUMBER", this);
      this.frames.pop();
      if (limit < 0) {
        throw new Error.ReqlRuntimeError('LIMIT takes a non-negative argument (got '+limit+')', this.frames)
      }
      return sequence.limit(limit, this);
    case termTypes.OFFSETS_OF:
      util.assertArity(2, term[1], this);
      this.frames.push(0);
      var sequence = this.evaluate(term[1][0], internalOptions)
      this.frames.pop();

      util.toSequence(sequence, this);
      return sequence.offsetsOf(term[1][1], this);
    case termTypes.CONTAINS:
      //util.assertArityRange(0, Infinity, term[1], this);
      this.frames.push(0);
      var sequence = this.evaluate(term[1][0], internalOptions)
      this.frames.pop();

      util.toSequence(sequence, this);

      var predicates = [];
      for(var i=1; i<term[1].length; i++) {
        predicates.push(term[1][i]);
      }
      return sequence.contains(predicates, this, internalOptions);
    case termTypes.GET_FIELD:
      util.assertArity(2, term[1], this);
      this.frames.push(0);
      var sequenceOrObject = this.evaluate(term[1][0], internalOptions)
      try {
        util.assertType(sequenceOrObject, 'OBJECT', this);
      }
      catch(err) {
        util.cannotPerformOp('get_field', sequenceOrObject, this);
      }
      this.frames.pop();
      this.frames.push(1)
      var field = this.evaluate(term[1][1], internalOptions)
      util.assertType(field, 'DATUM', this);
      this.frames.pop();

      if (sequenceOrObject instanceof Document) {
        if (sequenceOrObject.doc[field] === undefined) {
          throw new Error.ReqlRuntimeError("No attribute `"+field+"` in object:\n"+JSON.stringify(util.toDatum(sequenceOrObject), null, 2), this.frames)
        }
        return sequenceOrObject.doc[field];
      }
      else if (util.isSequence(sequenceOrObject)) {
        return sequenceOrObject.getField(field);
      }
      else {
        if (sequenceOrObject[field] === undefined) {
          throw new Error.ReqlRuntimeError("No attribute `"+field+"` in object:\n"+JSON.stringify(util.toDatum(sequenceOrObject), null, 2), this.frames)
        }
        return sequenceOrObject[field];
      }
    case termTypes.KEYS:
      util.assertArity(1, term[1], this);
      this.frames.push(0);
      var obj = this.evaluate(term[1][0], internalOptions)
      try {
        util.assertType(obj, 'OBJECT', this);
      }
      catch(err) {
        throw new Error.ReqlRuntimeError('Cannot call `keys` on objects of type `'+util.typeOf(obj)+'`', this.frames)
      }
      this.frames.pop();

      if (typeof obj.keys === 'function') {
        return obj.keys();
      }
      else {
        var result = new Sequence();
        var keys = Object.keys(obj);
        for(var i=0; i<keys.length; i++) {
          result.push(keys[i]);
        }
        return result;
      }
    case termTypes.OBJECT:
      //util.assertArityRange(0, Infinity, term[1], this);
      if (term[1].length%2 === 1) {
        throw new Error.ReqlRuntimeError("OBJECT expects an even number of arguments (but found "+term[1].length+")")
      }
      var result = {};
      var key, value;
      var i=0;
      while (i<term[1].length) {
        this.frames.push(i);
        key = this.evaluate(term[1][i], internalOptions);
        util.assertType(key, "STRING", this);
        this.frames.pop();

        this.frames.push(i+1);
        value = this.evaluate(term[1][i+1], internalOptions)
        this.frames.pop();

        if (result[key] !== undefined) {
          throw new Error.ReqlRuntimeError('Duplicate key `'+key+'` in object.  (got `'+JSON.stringify(result[key], null, 4)+'` and `'+JSON.stringify(value, null, 4)+'` as values)', this.frames)
        }


        result[key] = value;
        i += 2;
      }
      return result;
    case termTypes.HAS_FIELDS:
      util.assertArityRange(1, Infinity, term[1], this);
      this.frames.push(0);
      var sequenceOrObject = this.evaluate(term[1][0], internalOptions);
      if (!util.isObject(sequenceOrObject) && !util.isSequence(sequenceOrObject)) {
        util.cannotPerformOp('has_fields', util.toDatum(sequenceOrObject), this);
      }
      this.frames.pop();

      var keys = new Sequence();
      for(var i=1; i<term[1].length; i++) {
        this.frames.push(i);
        var field = this.evaluate(term[1][i], internalOptions);
        util.assertType(field, 'DATUM', this);
        keys.push(field);
        this.frames.pop();
      }

      if (typeof sequenceOrObject.hasFields === 'function') {
        return sequenceOrObject.hasFields(keys);
      }
      else {
        return util.hasFields(sequenceOrObject, keys);
      }

    case termTypes.WITH_FIELDS:
      util.assertArityRange(1, Infinity, term[1], this);
      this.frames.push(0);
      var sequence = this.evaluate(term[1][0], internalOptions);
      if (!util.isSequence(sequence)) {
        // While we will mention a non-object/non-sequence value, with_fields
        // works only on sequence, not on objects
        // See rethinkdb/rethinkdb/issues/4177 for why it's has_fields and not with_fields
        util.cannotPerformOp('has_fields', util.toDatum(sequence), this);
      }
      this.frames.pop();

      var fields = new Sequence();
      for(var i=1; i<term[1].length; i++) {
        this.frames.push(i);
        var field = this.evaluate(term[1][i], internalOptions);
        fields.push(field);
        util.assertType(field, 'DATUM', this);
        this.frames.pop();
      }
      return sequence.withFields(fields, this);
    case termTypes.PLUCK:
      util.assertArityRange(1, Infinity, term[1], this);
      this.frames.push(0);
      var sequenceOrObject = this.evaluate(term[1][0], internalOptions);
      if (!util.isObject(sequenceOrObject) && !util.isSequence(sequenceOrObject)) {
        util.cannotPerformOp('pluck', util.toDatum(sequenceOrObject), this);
      }
      this.frames.pop();

      var keys = new Sequence();
      for(var i=1; i<term[1].length; i++) {
        this.frames.push(i);
        var field = this.evaluate(term[1][i], internalOptions);
        util.assertType(field, 'DATUM', this);
        keys.push(field);
        this.frames.pop();
      }
      for(var i=0; i<keys.length; i++) {
        util.assertPath(keys.get(i), this);
      }
      if (typeof sequenceOrObject.pluck === 'function') {
        return sequenceOrObject.pluck(keys);
      }
      else {
        return util.pluck(sequenceOrObject, keys);
      }
    case termTypes.WITHOUT:
      util.assertArityRange(1, Infinity, term[1], this);
      this.frames.push(0);
      var sequenceOrObject = this.evaluate(term[1][0], internalOptions);
      if (!util.isObject(sequenceOrObject) && !util.isSequence(sequenceOrObject)) {
        util.cannotPerformOp('without', util.toDatum(sequenceOrObject), this);
      }
      this.frames.pop();

      var keys = [];
      for(var i=1; i<term[1].length; i++) {
        this.frames.push(i);
        var field = this.evaluate(term[1][i], internalOptions);
        util.assertType(field, 'DATUM', this);
        keys.push(field);
        this.frames.pop();
      }

      for(var i=0; i<keys.length; i++) {
        util.assertPath(keys[i], this);
      }

      if (typeof sequenceOrObject.without === 'function') {
        return sequenceOrObject.without(keys);
      }
      else {
        return util.without(sequenceOrObject, keys);
      }
    case termTypes.MERGE:
      //TODO Test with Documents
      util.assertArityRange(1, Infinity, term[1], this);
      this.frames.push(0);
      var sequenceOrObject = this.evaluate(term[1][0], internalOptions);
      if (!util.isObject(sequenceOrObject) && !util.isSequence(sequenceOrObject)) {
        util.cannotPerformOp('merge', util.toDatum(sequenceOrObject), this);
      }
      this.frames.pop();

      for(var i=1; i<term[1].length; i++) {
        if (typeof sequenceOrObject.merge === 'function') {
          sequenceOrObject = sequenceOrObject.merge(term[1][i], this);
        }
        else {
          sequenceOrObject = util.merge(sequenceOrObject, term[1][i], this, internalOptions);
        }
      }
      return sequenceOrObject;
    case termTypes.BETWEEN:
      util.assertArity(3, term[1], this);
      if (options.left_bound === undefined) {
        options.left_bound = "closed";
      }
      else if ((options.left_bound !== 'closed') && (options.left_bound !== 'open')) {
        throw new Error.ReqlRuntimeError('Expected `open` or `closed` for optarg `left_bound` (got `"'+options.left_bound+'"`)', this.frames)
      }
      if (options.right_bound === undefined) {
        options.right_bound = "open";
      }
      else if ((options.right_bound !== 'closed') && (options.right_bound !== 'open')) {
        throw new Error.ReqlRuntimeError('Expected `open` or `closed` for optarg `right_bound` (got `"'+options.right_bound+'"`)', this.frames)
      }

      this.frames.push(0);
      var table = this.evaluate(term[1][0], internalOptions)
      util.assertType(table, 'TABLE_SLICE', this);
      this.frames.pop();

      this.frames.push(1);
      var left = this.evaluate(term[1][1], internalOptions)
      util.assertType(left, 'DATUM', this);
      this.frames.pop();

      this.frames.push(2);
      var right = this.evaluate(term[1][2], internalOptions)
      util.assertType(right, 'DATUM', this);
      this.frames.pop();
      return table.between(left, right, options, this, internalOptions);
    case termTypes.MINVAL:
      util.assertArity(0, term[1], this);
      return new Minval();
    case termTypes.MAXVAL:
      util.assertArity(0, term[1], this);
      return new Maxval();
    case termTypes.REDUCE:
      util.assertArity(2, term[1], this);
      this.frames.push(0);
      var sequence = this.evaluate(term[1][0], internalOptions)
      this.frames.pop();
      util.toSequence(sequence, this);

      return sequence.reduce(term[1][1], this, internalOptions);
    case termTypes.MAP:
      util.assertArityRange(2, Infinity, term[1], this);
      var sequences = []
      for(var i=0; i<term[1].length-1; i++) {
        this.frames.push(i);
        var sequenceOrSelection = this.evaluate(term[1][i], internalOptions);
        sequences.push(sequenceOrSelection);
        this.frames.pop();
      }
      for(var i=0; i<sequences.length; i++) {
        sequences[i] = util.toSequence(sequences[i], this);
      }
      // RethinkDB seem to currently accept only one change max
      // See rethinkdb/rethinkdb/issues/4242
      if ((sequences.length === 1) && util.isChanges(sequences[0])) {
        return sequences[0].map(term[1][term[1].length-1], this, internalOptions)
      }
      return Sequence.map(sequences, term[1][term[1].length-1], this, internalOptions);
    case termTypes.FILTER:
      util.assertArity(2, term[1], this);
      this.frames.push(0);
      var sequence = this.evaluate(term[1][0], internalOptions);
      this.frames.pop();
      // Do not reassign here as we want to keep selections
      util.toSequence(sequence, this);

      return sequence.filter(term[1][1], options, this, internalOptions);
    case termTypes.CONCAT_MAP: // 40
      util.assertArity(2, term[1], this);
      var sequence = this.evaluate(term[1][0], internalOptions);
      util.toSequence(sequence, this);
      return sequence.concatMap(term[1][1], this, internalOptions);
    case termTypes.ORDER_BY:
      // There's a special arity error for orderBy...
      if ((term[1].length === 1) && (options.index === undefined)) {
        throw new Error.ReqlRuntimeError('Must specify something to order by', this.frames)
      }
      this.frames.push(0);
      var sequence = this.evaluate(term[1][0], internalOptions);
      // Do not copy as we want to keep the selection type
      this.frames.pop();
      util.toSequence(sequence, this);

      var fields = [];
      for(var i=1; i<term[1].length; i++) {
        fields.push(term[1][i]);
      }
      return sequence.orderBy(fields, options, this, internalOptions);
    case termTypes.DISTINCT:
      util.assertArity(1, term[1], this);
      this.frames.push(0);
      var sequenceOrSelection = this.evaluate(term[1][0], internalOptions)
      this.frames.pop();
      util.toSequence(sequenceOrSelection, this);
      return sequenceOrSelection.distinct(options, this, internalOptions);
    case termTypes.COUNT:
      util.assertArityRange(1, 2, term[1], this);
      this.frames.push(0);
      var sequenceOrBin = this.evaluate(term[1][0], internalOptions);
      this.frames.pop();

      if (util.isBinary(sequenceOrBin) && (term[1].length === 1)) {
        // See rethinkdb/rethinkdb#2804
        // This is equivalent to: new Buffer(sequenceOrBin.data, 'base64').length
        var length = sequenceOrBin.data.length;
        var blocksOf78 = Math.floor(length/78);
        var remainderOf78 = length%78;
        var base64Digits = blocksOf78*76+remainderOf78
        var blocksOf4 = Math.floor(base64Digits/4)
        var remainderOf4 = base64Digits%4;
        var numberOfEquals2;
        if (/==$/.test(sequenceOrBin.data)) {
          numberOfEquals2 = 2;
        }
        else if (/=$/.test(sequenceOrBin.data)) {
          numberOfEquals2 = 1;
        }
        else {
          numberOfEquals2 = 0;
        }
        return 3*blocksOf4-numberOfEquals2;
      }
      sequenceOrBin = util.toSequence(sequenceOrBin, this);
      return sequenceOrBin.count(term[1][1], this, internalOptions);
    case termTypes.IS_EMPTY:
      util.assertArity(1, term[1], this);
      this.frames.push(0);
      var sequence = this.evaluate(term[1][0], internalOptions)
      this.frames.pop();
      util.toSequence(sequence, this);
      return sequence.isEmpty(this);
    case termTypes.UNION:
      util.assertArityRange(1, Infinity, term[1], this);
      this.frames.push(0);
      var result = this.evaluate(term[1][0], internalOptions)
      this.frames.pop();
      result = util.toSequence(result, this);

      for(var i=1; i<term[1].length; i++) {
        this.frames.push(i);
        var other = this.evaluate(term[1][i], internalOptions)
        this.frames.pop();
        other = util.toSequence(other, this);
        result = result.concat(other);
      }
      return result;
    case termTypes.NTH:
      util.assertArity(2, term[1], this);
      this.frames.push(0);
      var sequence = this.evaluate(term[1][0], internalOptions)
      this.frames.pop();
      util.toSequence(sequence, this);
      this.frames.push(1);
      var index = this.evaluate(term[1][1], internalOptions)
      this.frames.pop();
      util.assertType(index, "NUMBER", this);
      return sequence.nth(index, this);
    case termTypes.BRACKET:
      util.assertArity(2, term[1], this);
      this.frames.push(0);
      var sequenceOrObject = this.evaluate(term[1][0], internalOptions);
      if (!util.isObject(sequenceOrObject) && !util.isSequence(sequenceOrObject)) {
        util.cannotPerformOp('bracket', sequenceOrObject, this);
      }
      this.frames.pop()

      this.frames.push(1);
      var key = this.evaluate(term[1][1], internalOptions);
      this.frames.pop()
      if (typeof key === 'number') {
        sequenceOrObject = util.toSequence(sequenceOrObject, this);
      }

      var result = util.getBracket(sequenceOrObject, key, this);
      return result;
    case termTypes.INNER_JOIN:
      util.assertArity(3, term[1], this);
      this.frames.push(0);
      var sequence = this.evaluate(term[1][0], internalOptions)
      this.frames.pop();
      util.toSequence(sequence, this);

      this.frames.push(1);
      var otherSequence = this.evaluate(term[1][1], internalOptions)
      this.frames.pop();
      otherSequence = util.toSequence(otherSequence, this);

      return sequence.join('inner', otherSequence, term[1][2], this, internalOptions);
    case termTypes.OUTER_JOIN:
      util.assertArity(3, term[1], this);
      this.frames.push(0);
      var sequence = this.evaluate(term[1][0], internalOptions)
      this.frames.pop();
      util.toSequence(sequence, this);

      this.frames.push(1);
      var otherSequence = this.evaluate(term[1][1], internalOptions)
      this.frames.pop();
      otherSequence = util.toSequence(otherSequence, this);

      return sequence.join('outer', otherSequence, term[1][2], this, internalOptions);
    case termTypes.EQ_JOIN:
      util.assertArity(3, term[1], this);
      this.frames.push(0);
      var sequence = this.evaluate(term[1][0], internalOptions)
      this.frames.pop();
      util.toSequence(sequence, this);

      this.frames.push(2);
      var rightTable = this.evaluate(term[1][2], internalOptions)
      this.frames.pop();
      util.assertType(rightTable, 'TABLE', this);

      return sequence.eqJoin(term[1][1], rightTable, options, this, internalOptions);
    case termTypes.ZIP:
      util.assertArity(1, term[1], this);
      this.frames.push(0);
      var sequence = this.evaluate(term[1][0], internalOptions)
      this.frames.pop();
      util.toSequence(sequence, this);

      return sequence.zip(this);
    case termTypes.RANGE:
      util.assertArityRange(0, 2, term[1], this);
      if (term[1].length === 0) {
        return Sequence.range();
      }
      else if (term[1].length === 1) {
        this.frames.push(0);
        var end = this.evaluate(term[1][0], internalOptions)
        util.assertType(end, "NUMBER", this);
        this.frames.pop();
        return Sequence.range(end);
      }
      else if (term[1].length === 2) {
        this.frames.push(0);
        var start = this.evaluate(term[1][0], internalOptions)
        util.assertType(start, "NUMBER", this);
        this.frames.pop();

        this.frames.push(1);
        var end = this.evaluate(term[1][1], internalOptions)
        util.assertType(end, "NUMBER", this);
        this.frames.pop();

        return Sequence.range(start, end);
      }
    case termTypes.INSERT_AT:
      util.assertArity(3, term[1], this);
      this.frames.push(0);
      var sequence = this.evaluate(term[1][0], internalOptions)
      util.assertType(sequence, 'DATUM', this);
      this.frames.pop();
      util.assertType(sequence, 'ARRAY', this);
      
      this.frames.push(1);
      var position = this.evaluate(term[1][1], internalOptions)
      this.frames.pop();
      util.assertType(position, 'NUMBER', this);
      if (!util.isChanges(sequence) && (position > sequence.sequence.length)) {
        throw new Error.ReqlRuntimeError("Index `"+position+"` out of bounds for array of size: `"+sequence.sequence.length+"`", this.frames)
      }

      this.frames.push(2);
      var value = this.evaluate(term[1][2], internalOptions)
      this.frames.pop();

      return sequence.insertAt(position, value, this);
    case termTypes.DELETE_AT:
      util.assertArityRange(2, 3, term[1], this);
      this.frames.push(0);
      var sequence = this.evaluate(term[1][0], internalOptions)
      util.assertType(sequence, 'DATUM', this);
      this.frames.pop();
      util.assertType(sequence, 'ARRAY', this);

      this.frames.push(1);
      var start = this.evaluate(term[1][1], internalOptions)
      util.assertType(start, 'NUMBER', this);
      this.frames.pop();

      var end;
      if (term[1].length > 1) {
        this.frames.push(2);
        end = this.evaluate(term[1][2], internalOptions)
        util.assertType(start, 'NUMBER', this);
        this.frames.pop();
      }

      return sequence.deleteAt(start, end, this);
    case termTypes.CHANGE_AT:
      util.assertArity(3, term[1], this);
      this.frames.push(0);
      var sequence = this.evaluate(term[1][0], internalOptions)
      util.assertType(sequence, 'DATUM', this);
      this.frames.pop();
      util.assertType(sequence, 'ARRAY', this);
      
      this.frames.push(1);
      var position = this.evaluate(term[1][1], internalOptions)
      util.assertType(position, 'NUMBER', this);
      this.frames.pop();

      this.frames.push(2);
      var value = this.evaluate(term[1][2], internalOptions)
      util.assertType(value, 'DATUM', this);
      this.frames.pop();

      return sequence.changeAt(position, value, this);
    case termTypes.SPLICE_AT:
      util.assertArity(3, term[1], this);
      this.frames.push(0);
      var sequence = this.evaluate(term[1][0], internalOptions)
      util.assertType(sequence, 'DATUM', this);
      this.frames.pop();
      util.assertType(sequence, 'ARRAY', this);
      
      this.frames.push(1);
      var position = this.evaluate(term[1][1], internalOptions)
      util.assertType(position, 'NUMBER', this);
      this.frames.pop();
      if (position > sequence.sequence.length) {
        util.outOfBound(position, sequence.sequence.length, this);
      }

      this.frames.push(2);
      var other = this.evaluate(term[1][2], internalOptions)
      util.assertType(other, 'DATUM', this);
      this.frames.pop();
      util.assertType(other, 'ARRAY', this);

      return sequence.spliceAt(position, other, this);
    case termTypes.COERCE_TO:
      util.assertArity(2, term[1], this);
      this.frames.push(0);
      var value = this.evaluate(term[1][0], internalOptions)
      this.frames.pop();

      this.frames.push(1);
      var newType = this.evaluate(term[1][1], internalOptions);
      util.assertType(newType, 'STRING', this);
      this.frames.pop();
      newType = newType.toUpperCase();

      var currentType = util.typeOf(value);
      
      if (newType === "NUMBER") {
        return parseFloat(value);
      }
      else if (newType === "STRING") {
        if (value === null) {
          return 'null';
        }
        else if (typeof value === 'string') {
          return value;
        }
        else if (typeof value === 'number') {
          return JSON.stringify(value);
        }
        else if (util.isSequence(value)) {
          return JSON.stringify(util.toDatum(value));
        }
        else if (util.isBinary(value)) {
          return new Buffer(value.data, 'base64').toString();
        }
        else if (util.isPlainObject(value)) {
          return JSON.stringify(util.toDatum(value));
        }
        this.frames.push(0);
        throw new Error.ReqlRuntimeError("Cannot coerce "+currentType+" to STRING", this.frames)
      }
      else if (newType === "ARRAY") {
        if (util.isSequence(value)) {
          return value.toSequence();
        }
        else if (util.isBinary(value)) {
          this.frames.push(0);
          throw new Error.ReqlRuntimeError("Cannot coerce BINARY to ARRAY", this.frames)
        }
        else if (util.isPlainObject(value)) {
          var result = new Sequence();
          var keys = Object.keys(value);
          for(var i=0; i<keys.length; i++) {
            var pair = new Sequence();
            pair.push(keys[i]);
            pair.push(value[keys[i]]);
            result.push(pair);
          }
          result.sequence.sort(function(a, b) {
            if (a.get(0) > b.get(0)) {
              return 1;
            }
            else if (a.get(0) > b.get(0)) {
              return -1;
            }
            else {
              return 0;
            }
          });
          return result;
        }
        else {
          this.frames.push(0);
          throw new Error.ReqlRuntimeError("Cannot coerce "+currentType+" to ARRAY", this.frames)
        }
      }
      else if (newType === "OBJECT") {
        if (util.isSequence(value)) {
          var result = {};
          var key, keyValue;
          var i=0;
          for(var i=0; i<value.length; i++) {
            pair = value.get(i);
            // WHy don't we push a frame here? Blame RethinkDB...
            if (!util.isSequence(pair)) {
              throw new Error.ReqlRuntimeError('Expected type ARRAY but found '+util.typeOf(pair), this.frames)
            }
            result[pair.get(0)] = pair.get(1);
          }
          return result;
        }
        else if (util.isBinary(value)) {
          this.frames.push(0);
          throw new Error.ReqlRuntimeError("Cannot coerce BINARY to OBJECT", this.frames)
        }
        else if (util.isPlainObject(value)) {
          return value;
        }
        this.frames.push(0);
        throw new Error.ReqlRuntimeError("Cannot coerce "+currentType+" to OBJECT", this.frames)
      }
      else if (newType === "BINARY") {
        if (typeof value === 'string') {
          return {
            $reql_type$: 'BINARY',
            data: new Buffer(value).toString('base64')
          }
        }
        else if (util.isBinary(value)) {
          return value;
        }
        this.frames.push(0);
        throw new Error.ReqlRuntimeError("Cannot coerce "+currentType+" to BINARY", this.frames)
      }
      else {
        util.notAvailable(null, this);
      }
    case termTypes.TYPE_OF:
      util.assertArity(1, term[1], this);
      this.frames.push(0);
      var value = this.evaluate(term[1][0], internalOptions)
      // r.js(...).typeOf() is a valid query
      util.assertJavaScriptResult(value, this);
      value = util.revertDatum(value);
      this.frames.pop();

      return util.typeOf(value, this);
    case termTypes.UPDATE:
      util.assertArity(2, term[1], this);
      this.frames.push(0);
      var selection = this.evaluate(term[1][0], internalOptions)
      util.assertType(selection, 'SELECTION', this);
      this.frames.pop();
      return selection.update(term[1][1], options, this, internalOptions)
    case termTypes.DELETE:
      util.assertArity(1, term[1], this);
      this.frames.push(0);
      var selection = this.evaluate(term[1][0], internalOptions)
      util.assertType(selection, 'SELECTION', this);
      this.frames.pop();
      return selection.delete(options, this)
    case termTypes.REPLACE:
      util.assertArity(2, term[1], this);
      this.frames.push(0);
      var selection = this.evaluate(term[1][0], internalOptions)
      util.assertType(selection, 'SELECTION', this);
      this.frames.pop();

      return selection.replace(term[1][1], options, this, internalOptions);
    case termTypes.INSERT:
      util.assertArity(2, term[1], this);
      this.frames.push(0);
      var table = this.evaluate(term[1][0], internalOptions);
      util.assertType(table, 'TABLE', this);
      this.frames.pop();

      this.frames.push(0);
      var docs = this.evaluate(term[1][1], internalOptions)
      this.frames.pop();

      util.assertOptions(options, ['durability', 'return_changes', 'conflict'], this);
      return table.insert(docs, options, this)
    case termTypes.DB_CREATE:
      util.assertArity(1, term[1], this);
      this.frames.push(0);
      var dbName = this.evaluate(term[1][0], internalOptions);
      util.assertType(dbName, 'STRING', this);
      util.assertNoSpecialChar(dbName, 'Database', this);
      this.frames.pop();

      if (this.server.databases[dbName] != null) {
        throw new Error.ReqlRuntimeError("Database `"+dbName+"` already exists", this.frames)
      }
      this.server.databases[dbName] = new Database(dbName)
      return {
        config_changes: [{
          new_val: {
            id: this.server.databases[dbName].id,
            name: dbName
          },
          old_val: null
        }],
        dbs_created: 1,
      }
    case termTypes.DB_DROP:
      util.assertArity(1, term[1], this);
      this.frames.push(0)
      var dbName = this.evaluate(term[1][0], internalOptions);
      util.assertType(dbName, 'STRING', this);
      util.assertNoSpecialChar(dbName, 'Database', this);
      this.frames.pop();

      if (this.server.databases[dbName] == null) {
        throw new Error.ReqlRuntimeError("Database `"+dbName+"` does not exist", this.frames)
      }
      var db = this.server.databases[dbName];
      delete this.server.databases[dbName];
      return {
        config_changes: [{
          new_val: null,
          old_val: {
            id: db.id,
            name: dbName
          }
        }],
        dbs_dropped: 1,
        tables_dropped: Object.keys(db.tables).length
      }
    case termTypes.DB_LIST:
      util.assertArity(0, term[1], this);
      return Object.keys(this.server.databases)
    case termTypes.TABLE_CREATE:
      util.assertArityRange(1, 2, term[1], this);
      util.assertOptions(options, ['primary_key', 'durability', 'shards', 'replicas', 'primary_replica_tag'], this);
      var db, tableName;
      if (term[1].length === 1) {
        if (this.options.db !== undefined) {
          db = this.evaluate(this.options.db, internalOptions)
        }
        else {
          db = this.server.databases['test']
        }
        util.assertType(db, 'DATABASE', this);
        this.frames.push(0);
        tableName = this.evaluate(term[1][0], internalOptions)
        util.assertType(tableName, 'STRING', this);
        util.assertNoSpecialChar(tableName, 'Table', this);
        this.frames.pop();
        if (db.tables[tableName] != null) {
          throw new Error.ReqlRuntimeError('Table `'+db.name+'.'+tableName+'` already exists', this.frames);
        }
      }
      else if (term[1].length === 2) {
        this.frames.push(0);
        db = this.evaluate(term[1][0], internalOptions)
        util.assertType(db, 'DATABASE', this);
        this.frames.pop();
        this.frames.push(1);
        tableName = this.evaluate(term[1][1], internalOptions)
        util.assertType(tableName, 'STRING', this);
        util.assertNoSpecialChar(tableName, 'Table', this);
        this.frames.pop();
        if (db.tables[tableName] != null) {
          throw new Error.ReqlRuntimeError("Table `"+tableName+"` already exists", this.frames)
        }
      }

      var table = new Table(tableName, db.name, options);
      db.tables[tableName] = table;
      return {
        config_changes: [{
          new_val: {
            db: db.name,
            durability: "hard", //TODO Handle optarg
            write_acks: "majority", //TODO Handle optarg
            id: table.id,
            name: table.name,
            primary_key: table.options.primaryKey,
            shards: [{
              primary_replica: "reqlite",
              replicas: ["reqlite"]
            }]
          },
          old_val: null
        }],
        tables_created: 1
      }
    case termTypes.TABLE_DROP:
      util.assertArityRange(1, 2, term[1], this);
      var db, tableName;
      if (term[1].length === 1) {
        if (this.options.db !== undefined) {
          db = this.evaluate(this.options.db, internalOptions)
        }
        else {
          db = this.server.databases['test']
        }
        util.assertType(db, 'DATABASE', this);
        this.frames.push(0);
        tableName = this.evaluate(term[1][0], internalOptions)
        util.assertType(tableName, 'STRING', this);
        util.assertNoSpecialChar(tableName, 'Table', this);
        this.frames.pop();
        if (db.tables[tableName] == null) {
          throw new Error.ReqlRuntimeError('Table `'+db.name+'.'+tableName+'` does not exist', this.frames);
        }
      }
      else if (term[1].length === 2) {
        this.frames.push(0);
        db = this.evaluate(term[1][0], internalOptions)
        util.assertType(db, 'DATABASE', this);
        this.frames.pop();

        this.frames.push(1);
        tableName = this.evaluate(term[1][1], internalOptions)
        util.assertType(tableName, 'STRING', this);
        util.assertNoSpecialChar(tableName, 'Table', this);
        this.frames.pop();

        if (db.tables[tableName] == null) {
          throw new Error.ReqlRuntimeError('Table `'+db.name+'.'+tableName+'` does not exist', this.frames);
        }
      }

      var table = db.tables[tableName];
      delete db.tables[tableName];
      return {
        config_changes: [{
          old_val: {
            db: db.name,
            durability: "hard", //TODO Handle optarg
            write_acks: "majority", //TODO Handle optarg
            id: table.id,
            name: table.name,
            primary_key: table.options.primaryKey,
            shards: [{
              primary_replica: "reqlite",
              replicas: ["reqlite"]
            }]
          },
          new_val: null
        }],
        tables_dropped: 1
      }

    case termTypes.TABLE_LIST:
      util.assertArityRange(0, 1, term[1], this);
      if (term[1].length === 0) {
        if (this.options.db !== undefined) {
          db = this.evaluate(this.options.db, internalOptions)
        }
        else {
          db = this.server.databases['test']
        }
      }
      else {
        this.frames.push(0);
        var db = this.evaluate(term[1][0], internalOptions);
        this.frames.pop();
      }
      return Object.keys(db.tables)
    case termTypes.SYNC:
      util.assertArity(1, term[1], this);
      this.frames.push(0);
      var table = this.evaluate(term[1][0], internalOptions);
      util.assertType(table, 'TABLE', this);
      this.frames.pop();
      return {synced: 1}
    case termTypes.INDEX_CREATE:
      util.assertArityRange(2, 3, term[1], this);
      this.frames.push(0);
      var table = this.evaluate(term[1][0], internalOptions);
      util.assertType(table, 'TABLE', this);
      this.frames.pop();
      this.frames.push(1);
      var name = this.evaluate(term[1][1], internalOptions); 
      this.frames.pop();
      // Err, not sure why there's no frame here again...
      util.assertType(name, 'STRING', this);
      util.assertOptions(options, ['multi', 'geo'], this);
      table.indexCreate(name, term[1][2], options, this);
      return {created: 1}
    case termTypes.INDEX_DROP:
      util.assertArity(2, term[1], this);
      this.frames.push(0);
      var table = this.evaluate(term[1][0], internalOptions);
      util.assertType(table, 'TABLE', this);
      this.frames.pop();
      this.frames.push(1);
      var index = this.evaluate(term[1][1], internalOptions);
      util.assertType(index, 'STRING', this);
      this.frames.pop();
      return table.indexDrop(index, this);
    case termTypes.INDEX_LIST:
      util.assertArity(1, term[1], this);
      this.frames.push(0);
      var table = this.evaluate(term[1][0], internalOptions);
      util.assertType(table, 'TABLE', this);
      this.frames.pop();
      return table.indexList();
    case termTypes.INDEX_RENAME:
      util.assertArity(3, term[1], this);
      this.frames.push(0);
      var table = this.evaluate(term[1][0], internalOptions);
      util.assertType(table, 'TABLE', this);
      this.frames.pop();
      this.frames.push(1);
      var oldIndex = this.evaluate(term[1][1], internalOptions);
      util.assertType(oldIndex, 'STRING', this);
      this.frames.pop();
      this.frames.push(2);
      var newIndex = this.evaluate(term[1][2], internalOptions);
      util.assertType(newIndex, 'STRING', this);
      this.frames.pop();

      util.assertOptions(options, ['overwrite'], this);
      var options = term[2] || {};
      return table.indexRename(oldIndex, newIndex, options, this);
    case termTypes.INDEX_STATUS:
      util.assertArityRange(1, Infinity, term[1], this);
      this.frames.push(0);
      var table = this.evaluate(term[1][0], internalOptions);
      util.assertType(table, 'TABLE', this);
      this.frames.pop();

      var indexes = [];
      for(var i=1; i<term[1].length; i++) {
        this.frames.push(i);
        var index = this.evaluate(term[1][i], internalOptions);
        util.assertType(index, 'STRING', this);
        indexes.push(index);
        this.frames.pop();
      }
      try {
        return table.indexWait.apply(table, indexes);
      }
      catch(err) {
        throw new Error.ReqlRuntimeError(err.message, this.frames)
      }
    case termTypes.INDEX_WAIT:
      util.assertArityRange(1, Infinity, term[1], this);
      this.frames.push(0);
      var table = this.evaluate(term[1][0], internalOptions);
      util.assertType(table, 'TABLE', this);
      this.frames.pop();

      var indexes = [];
      for(var i=1; i<term[1].length; i++) {
        this.frames.push(i);
        var index = this.evaluate(term[1][i], internalOptions);
        util.assertType(index, 'STRING', this);
        indexes.push(index);
        this.frames.pop();
      }
      try {
        return table.indexWait.apply(table, indexes);
      }
      catch(err) {
        throw new Error.ReqlRuntimeError(err.message, this.frames)
      }
    case termTypes.FUNCALL:
      // FUNCALL, FN [ AR[], BODY]
      //  0       1  [ 0 [ ...], 
      util.assertArityRange(1, Infinity, term[1], this);
      var fn = term[1][0];
      var argsFn = fn[1][0][1] || [];
      var result;
      for(var i=1; i<term[1].length; i++) {
        this.frames.push(i);
        util.assertPreDatum(term[1][i], this);
        var context = this.evaluate(term[1][i], internalOptions);
        util.assertType(context, 'DATUM', this);
        this.context[argsFn[i-1]] = context; // RethinkDB use the internal syntax r.do(fn, args...) -_-
        this.frames.pop();
      }
      result = this.evaluate(term[1][0], internalOptions);
      for(var i=0; i<argsFn.length; i++) {
        delete this.context[argsFn[i]];
      }
      if (argsFn.length > term[1].length-1) {
        this.frames.push(0);
        util.assertArity(argsFn.length, term[1].slice(1), this);
        this.frames.pop();
      }

      return result;
    case termTypes.BRANCH:
      util.assertArityRange(3, Infinity, term[1], this);
      this.frames.push(0);
      util.assertPreDatum(term[1][0], this);
      var condition = this.evaluate(term[1][0], internalOptions);
      util.assertType(condition, 'DATUM', this);
      this.frames.pop();

      var result
      if (condition === false || condition === null) {
        if (util.isFunction(term[1][2])) {
          throw new Error.ReqlRuntimeError("Query result must be of type DATUM, GROUPED_DATA, or STREAM (got FUNCTION)", this.frames)
        }
        this.frames.push(2);
        result = this.evaluate(term[1][2], internalOptions);
        this.frames.pop();
      }
      else {
        if (util.isFunction(term[1][1])) {
          throw new Error.ReqlRuntimeError("Query result must be of type DATUM, GROUPED_DATA, or STREAM (got FUNCTION)", this.frames)
        }
        this.frames.push(1);
        result = this.evaluate(term[1][1], internalOptions);
        this.frames.pop();
      }
      return result;
    case termTypes.OR:
      util.assertArityRange(1, Infinity, term[1], this);
      var bool;
      for(var i=0; i<term[1].length; i++) {
        this.frames.push(i);
        util.assertPreDatum(term[1][i], this);
        bool = this.evaluate(term[1][i], internalOptions);
        util.assertType(bool, 'DATUM', this);
        this.frames.pop();
        if (util.isTrue(bool)) {
          return bool;
        }
      }
      return false;
    case termTypes.AND:
      util.assertArityRange(1, Infinity, term[1], this);
      for(var i=0; i<term[1].length; i++) {
        this.frames.push(i);
        util.assertPreDatum(term[1][i], this);
        bool = this.evaluate(term[1][i], internalOptions);
        util.assertType(bool, 'DATUM', this);
        this.frames.pop();
        if (bool === false) {
          return false;
        }
        else if (bool === null) {
          return null;
        }
      }
      return bool;
    case termTypes.FOR_EACH:
      util.assertArity(2, term[1], this);
      this.frames.push(0);
      var sequence = this.evaluate(term[1][0], internalOptions);
      this.frames.pop();
      util.toSequence(sequence, this);

      return sequence.forEach(term[1][1], this, internalOptions);
    case termTypes.FUNC: // 69
      util.assertArity(2, term[1], this);
      var fnArgs = term[1][0];
      var body = term[1][1];
      this.frames.push(1);
      var result = this.evaluate(body, internalOptions);
      this.frames.pop();
      return result;
    case termTypes.ASC:
      util.assertArity(1, term[1], this);
      return new Asc(term[1][0]);
    case termTypes.DESC:
      util.assertArity(1, term[1], this);
      return new Desc(term[1][0]);
    case termTypes.INFO:
      util.assertArity(1, term[1], this);
      if (util.isFunction(term[1][0])) {
        //TODO Return the protobuf version
        return {
          source_code: 'function (captures = [(no implicit)]) (args = [])...'
        }
      }
      this.frames.push(0);
      var element = this.evaluate(term[1][0], internalOptions);
      this.frames.pop();

      if (util.isTable(element)) {
        return {
          db: {
            id: element.db.id,
            name: element.db.name,
            type: 'DB'
          },
          doc_count_estimates: [ Object.keys(element.documents).length ],
          id: element.id,
          indexes: Object.keys(element.indexes).sort(),
          name: element.name,
          primary_key: element.options.primaryKey,
          type: 'TABLE'
        }
      }
      else if (util.isBinary(element)) {
        var result = util.toDatum(element);

        //TODO Refactor with count
        var length = element.data.length;
        var blocksOf78 = Math.floor(length/78);
        var remainderOf78 = length%78;
        var base64Digits = blocksOf78*76+remainderOf78
        var blocksOf4 = Math.floor(base64Digits/4)
        var remainderOf4 = base64Digits%4;
        var numberOfEquals2;
        if (/==$/.test(element.data)) {
          numberOfEquals2 = 2;
        }
        else if (/=$/.test(element.data)) {
          numberOfEquals2 = 1;
        }
        else {
          numberOfEquals2 = 0;
        }
        var count = 3*blocksOf4-numberOfEquals2;

        return {
          type: util.typeOf(element),
          value: JSON.stringify(util.toDatum(element)),
          count: count
        }

      }
      else if (util.isDatum(element)) {
        return {
          type: util.typeOf(element),
          value: JSON.stringify(util.toDatum(element))
        }
      }
      return "STRING"
    case termTypes.MATCH:
      util.assertArity(2, term[1], this);
      this.frames.push(0);
      var str = this.evaluate(term[1][0], internalOptions)
      util.assertType(str, "STRING", this);
      this.frames.pop();

      this.frames.push(1);
      var regexRaw = this.evaluate(term[1][1], internalOptions)
      util.assertType(regexRaw, "STRING", this);
      this.frames.pop();

      var flags;
      var components = regexRaw.match(/^\(\?([a-z]*)\)(.*)/);
      if ((Array.isArray(components)) && (components.length > 2)) {
        regexRaw = components[2];
        flags = components[1].split('');
        for(var i=0; i<flags.length; i++) {
          if ((flags[i] !== 'i') && (flags[i] !== 'm')) {
            throw new Error.ReqlRuntimeError('Reqlite support only the flags `i` and `m`, found `'+flags[i]+'`')
          }
        }
      }
      else {
        flags = [];
      }
      var regex = new RegExp(regexRaw, flags.join(''));
      var resultRegex = regex.exec(str);
      if (resultRegex === null) {
        return null;
      }
      var result = {};
      result.start = resultRegex.index;
      result.end = resultRegex.index+resultRegex[0].length;
      result.str = resultRegex[0]
      result.groups = resultRegex.splice(1);
      return result;
    case termTypes.UPCASE:
      util.assertArity(1, term[1], this);
      this.frames.push(0);
      var str = this.evaluate(term[1][0], internalOptions)
      util.assertType(str, 'STRING', this);
      this.frames.pop();
      return str.toUpperCase();
    case termTypes.DOWNCASE:
      util.assertArity(1, term[1], this);
      this.frames.push(0);
      var str = this.evaluate(term[1][0], internalOptions)
      util.assertType(str, 'STRING', this);
      this.frames.pop();
      return str.toLowerCase();
    case termTypes.SAMPLE:
      util.assertArity(2, term[1], this);
      this.frames.push(0);
      var sequence = this.evaluate(term[1][0], internalOptions)
      this.frames.pop();
      util.toSequence(sequence, this);

      this.frames.push(1);
      var sample = this.evaluate(term[1][1], internalOptions)
      util.assertType(sample, 'NUMBER', this);
      this.frames.pop()
      return sequence.sample(sample, this);
    case termTypes.DEFAULT:
      util.assertArity(2, term[1], this);
      var value;
      try {
        this.frames.push(0);
        value = this.evaluate(term[1][0], internalOptions)
        this.frames.pop();
      }
      catch(err) {
        //TODO Create a new class of error for missing attribute
        if (err.message.match(/^No attribute `/)) {
          // Pop the frames 0
          this.frames.pop();
          this.frames.push(1);
          value = this.evaluate(term[1][1], internalOptions)
          this.frames.pop();
        }
        else {
          throw err;
        }
      }
      if (value === null) {
        this.frames.push(1);
        value = this.evaluate(term[1][1], internalOptions)
        this.frames.pop();
      }
      return value;
    case termTypes.JSON:
      util.assertArity(1, term[1], this);
      this.frames.push(0);
      var str = this.evaluate(term[1][0], internalOptions)
      util.assertType(str, "STRING", this);
      this.frames.pop();
      try {
        var value = JSON.parse(str);
      }
      catch(err) {
        throw new Error.ReqlRuntimeError("Failed to parse \""+str+"\" as JSON", this.frames)
      }
      var result = util.revertDatum(value);
      return result;
    case termTypes.TO_JSON_STRING:
      util.assertArity(1, term[1], this);
      this.frames.push(0);
      var value = this.evaluate(term[1][0], internalOptions)
      util.assertType(value, 'DATUM', this);
      this.frames.pop();
      return JSON.stringify(util.toDatum(value));
    case termTypes.ISO8601: // 99
      util.assertArity(1, term[1], this);
      this.frames.push(0);
      var date = this.evaluate(term[1][0], internalOptions);
      util.assertType(date, 'STRING', this);
      this.frames.pop();

      var timezone = ReqlDate.getTimezone(date, options);
      return ReqlDate.iso8601(date, timezone, this);
    case termTypes.TO_ISO8601: // 99
      util.assertArity(1, term[1], this);
      this.frames.push(0);
      var date = this.evaluate(term[1][0], internalOptions)
      util.assertTime(date, this, true);
      this.frames.pop();

      momentDate = date.toMoment();
      if (momentDate.milliseconds() === 0) {
        return momentDate.utcOffset(momentDate.utcOffset()).format('YYYY-MM-DDTHH:mm:ssZ');
      }
      else {
        return momentDate.utcOffset(momentDate.utcOffset()).format('YYYY-MM-DDTHH:mm:ss.SSSZ');
      }
    case termTypes.EPOCH_TIME:
      util.assertArity(1, term[1], this);
      this.frames.push(0);
      var epochTime = this.evaluate(term[1][0], internalOptions);
      util.assertType(epochTime, "NUMBER", this);
      this.frames.pop();
      return new ReqlDate(epochTime)
    case termTypes.TO_EPOCH_TIME:
      util.assertArity(1, term[1], this);
      this.frames.push(0);
      var date = this.evaluate(term[1][0], internalOptions);
      util.assertTime(date, this, true);
      this.frames.pop();
      return date.epoch_time;
    case termTypes.NOW: // 103
      util.assertArity(0, term[1], this);
      return this.now;
    case termTypes.IN_TIMEZONE:
      util.assertArity(2, term[1], this);
      this.frames.push(0);
      var date = this.evaluate(term[1][0], internalOptions);
      this.frames.pop();
      util.assertTime(date, this);

      var timezoneStr = this.evaluate(term[1][1], internalOptions)
      util.assertType(timezoneStr, "STRING", this);
      var timezone = ReqlDate.convertTimezone(timezoneStr, this);
      return date.inTimezone(timezone);
    case termTypes.DURING:
      util.assertArity(3, term[1], this);
      this.frames.push(0);
      var date = this.evaluate(term[1][0], internalOptions);
      util.assertTime(date, this, true);
      date = date.toMoment();
      this.frames.pop();
      this.frames.push(1);
      var left = this.evaluate(term[1][1], internalOptions);
      util.assertTime(left, this, true);
      left = left.toMoment();
      this.frames.pop();
      this.frames.push(2);
      var right = this.evaluate(term[1][2], internalOptions);
      util.assertTime(right, this, true);
      right = right.toMoment();
      this.frames.pop();

      util.assertOptions(options, ['left_bound', 'right_bound'], this);
      var result = date.isBetween(left, right);

      options = options || {};
      options.left_bound = options.left_bound || 'closed';
      options.right_bound = options.right_bound || 'open';
      if (options.left_bound === "closed") {
        result = result || date.isSame(left);
      }
      if (options.right_bound === "closed") {
        result = result || date.isSame(right);
      }
      return result;
    case termTypes.DATE:
      util.assertArity(1, term[1], this);
      this.frames.push(0);
      var date = this.evaluate(term[1][0], internalOptions)
      this.frames.pop();
      util.assertTime(date, this);

      momentDate = date.toMoment();
      // We get the date for the offset '+00:00'...
      momentDate.subtract(momentDate.utcOffset('+00:00').hour(), 'hours')
      momentDate.subtract(momentDate.utcOffset('+00:00').minute(), 'minutes')
      momentDate.subtract(momentDate.utcOffset('+00:00').second(), 'seconds')
      momentDate.subtract(momentDate.utcOffset('+00:00').millisecond(), 'milliseconds')

      return ReqlDate.fromMoment(momentDate, date.timezone, this);
    case termTypes.TIME_OF_DAY:
      util.assertArity(1, term[1], this);
      this.frames.push(0);
      var date = this.evaluate(term[1][0], internalOptions)
      util.assertTime(date, this, true);
      this.frames.pop();

      momentDate = date.toMoment();
      var result = momentDate.utcOffset('+00:00').hour()*60*60;
      result += momentDate.utcOffset('+00:00').minute()*60;
      result += momentDate.utcOffset('+00:00').second();
      result += momentDate.utcOffset('+00:00').millisecond()/1000;

      return result;
    case termTypes.TIMEZONE:
      util.assertArity(1, term[1], this);
      this.frames.push(0);
      var date = this.evaluate(term[1][0], internalOptions)
      util.assertTime(date, this, true);
      this.frames.pop();

      return date.timezone;
    case termTypes.YEAR:
      util.assertArity(1, term[1], this);
      this.frames.push(0);
      var date = this.evaluate(term[1][0], internalOptions)
      util.assertTime(date, this, true);
      this.frames.pop();

      momentDate = date.toMoment();
      return momentDate.year();
    case termTypes.MONTH:
      util.assertArity(1, term[1], this);
      this.frames.push(0);
      var date = this.evaluate(term[1][0], internalOptions)
      util.assertTime(date, this, true);
      this.frames.pop();

      var dateStr = util.dateToString(date);
      return util.monthToInt(dateStr.match(/[^\s]* ([^\s]*)/)[1]);
    case termTypes.DAY:
      util.assertArity(1, term[1], this);
      this.frames.push(0);
      var date = this.evaluate(term[1][0], internalOptions)
      util.assertTime(date, this, true);
      this.frames.pop();

      momentDate = date.toMoment();
      return momentDate.date();
    case termTypes.DAY_OF_WEEK:
      util.assertArity(1, term[1], this);
      this.frames.push(0);
      var date = this.evaluate(term[1][0], internalOptions)
      util.assertTime(date, this, true);
      this.frames.pop();

      momentDate = date.toMoment();
      return momentDate.day();
    case termTypes.DAY_OF_YEAR:
      util.assertArity(1, term[1], this);
      this.frames.push(0);
      var date = this.evaluate(term[1][0], internalOptions)
      util.assertTime(date, this, true);
      this.frames.pop();

      momentDate = date.toMoment();
      return momentDate.dayOfYear();
    case termTypes.HOURS:
      util.assertArity(1, term[1], this);
      this.frames.push(0);
      var date = this.evaluate(term[1][0], internalOptions)
      util.assertTime(date, this, true);
      this.frames.pop();

      momentDate = date.toMoment();
      return momentDate.hours();
    case termTypes.MINUTES:
      util.assertArity(1, term[1], this);
      this.frames.push(0);
      var date = this.evaluate(term[1][0], internalOptions)
      util.assertTime(date, this, true);
      this.frames.pop();

      momentDate = date.toMoment();
      return momentDate.minutes();
    case termTypes.SECONDS:
      util.assertArity(1, term[1], this);
      this.frames.push(0);
      var date = this.evaluate(term[1][0], internalOptions)
      util.assertTime(date, this, true);
      this.frames.pop();

      momentDate = date.toMoment();
      return momentDate.seconds()+momentDate.milliseconds()/1000;
    case termTypes.TIME: // 136
      this.frames.push(0);
      var year = this.evaluate(term[1][0], internalOptions)
      util.assertType(year, 'NUMBER', this);
      this.frames.pop();
      this.frames.push(1);
      var month = this.evaluate(term[1][1], internalOptions)
      util.assertType(month, 'NUMBER', this);
      this.frames.pop();
      this.frames.push(2);
      var day = this.evaluate(term[1][2], internalOptions)
      util.assertType(day, 'NUMBER', this);
      this.frames.pop();

      var hours, minutes, seconds, milliseconds, timezone;
      if (term[1].length === 4) {
        hours = 0;
        minutes = 0;
        seconds = 0;
        milliseconds = 0;
        this.frames.push(3);
        timezone = this.evaluate(term[1][3], internalOptions);
        util.assertType(timezone, 'STRING', this);
        this.frames.pop();
      }
      else if (term[1].length === 7) {
        this.frames.push(3);
        hours = this.evaluate(term[1][3], internalOptions)
        util.assertType(hours, 'NUMBER', this);
        this.frames.pop();
        this.frames.push(4);
        minutes = this.evaluate(term[1][4], internalOptions)
        util.assertType(minutes, 'NUMBER', this);
        this.frames.pop();
        this.frames.push(5);
        seconds = this.evaluate(term[1][5], internalOptions)
        util.assertType(seconds, 'NUMBER', this);
        this.frames.pop();
        milliseconds = seconds - Math.floor(seconds);
        seconds = Math.floor(seconds);
        this.frames.push(6);
        timezone = this.evaluate(term[1][6], internalOptions);
        util.assertType(timezone, 'STRING', this);
        this.frames.pop();
      }
      else {
        throw new Error.ReqlRuntimeError('Expected between 4 and 7 arguments but found '+term[1].length, this.frames);
      }
      return ReqlDate.time(year, month, day, hours, minutes, seconds, milliseconds, timezone, this);
    case termTypes.MONDAY:
      util.assertArity(0, term[1], this);
      return 1;
    case termTypes.TUESDAY:
      util.assertArity(0, term[1], this);
      return 2;
    case termTypes.WEDNESDAY:
      util.assertArity(0, term[1], this);
      return 3;
    case termTypes.THURSDAY:
      util.assertArity(0, term[1], this);
      return 4;
    case termTypes.FRIDAY:
      util.assertArity(0, term[1], this);
      return 5;
    case termTypes.SATURDAY:
      util.assertArity(0, term[1], this);
      return 6;
    case termTypes.SUNDAY:
      util.assertArity(0, term[1], this);
      return 7;
    case termTypes.JANUARY:
      util.assertArity(0, term[1], this);
      return 1;
    case termTypes.FEBRUARY:
      util.assertArity(0, term[1], this);
      return 2;
    case termTypes.MARCH:
      util.assertArity(0, term[1], this);
      return 3;
    case termTypes.APRIL:
      util.assertArity(0, term[1], this);
      return 4;
    case termTypes.MAY:
      util.assertArity(0, term[1], this);
      return 5;
    case termTypes.JUNE:
      util.assertArity(0, term[1], this);
      return 6;
    case termTypes.JULY:
      util.assertArity(0, term[1], this);
      return 7;
    case termTypes.AUGUST:
      util.assertArity(0, term[1], this);
      return 8;
    case termTypes.SEPTEMBER:
      util.assertArity(0, term[1], this);
      return 9;
    case termTypes.OCTOBER:
      util.assertArity(0, term[1], this);
      return 10;
    case termTypes.NOVEMBER:
      util.assertArity(0, term[1], this);
      return 11;
    case termTypes.DECEMBER:
      util.assertArity(0, term[1], this);
      return 12;
    case termTypes.LITERAL:
      util.assertArityRange(0, 1, term[1], this);
      this.frames.push(0);
      var value = this.evaluate(term[1][0], internalOptions)
      this.frames.pop();
      return new Literal(value);
    case termTypes.GROUP:
      // Arity is handle inside the group method
      this.frames.push(0);
      var sequence = this.evaluate(term[1][0], internalOptions)
      this.frames.pop();
      util.toSequence(sequence, this);

      if ((term[1].length < 2) && (typeof options.index !== 'string')) {
        throw new Error.ReqlRuntimeError('Cannot group by nothing', this.frames)
      }

      var fieldOrFns = [];
      for(var i=1; i<term[1].length; i++) {
        fieldOrFns.push(term[1][i]);
      }
      util.assertOptions(options, ['index'], this);
      return sequence.group(fieldOrFns, options, this, internalOptions);
    case termTypes.SUM:
      util.assertArityRange(1, 2, term[1], this);
      this.frames.push(0);
      var sequence = this.evaluate(term[1][0], internalOptions)
      this.frames.pop();

      return sequence.sum(term[1][1], this, internalOptions);
    case termTypes.AVG:
      util.assertArityRange(1, 2, term[1], this);
      this.frames.push(0);
      var sequence = this.evaluate(term[1][0], internalOptions)
      this.frames.pop();

      return sequence.avg(term[1][1], this, internalOptions);
    case termTypes.MIN:
      var extra = (typeof options.index === 'string') ? [options] : [];
      util.assertArityRange(1, 2, term[1].concat(extra), this);
      this.frames.push(0);
      var sequence = this.evaluate(term[1][0], internalOptions)
      this.frames.pop();

      return sequence.min(term[1][1], options, this, internalOptions);
    case termTypes.MAX:
      var extra = (typeof options.index === 'string') ? [options] : [];
      util.assertArityRange(1, 2, term[1].concat(extra), this);
      this.frames.push(0);
      var sequence = this.evaluate(term[1][0], internalOptions)
      this.frames.pop();

      return sequence.max(term[1][1], options, this);
    case termTypes.SPLIT:
      util.assertArityRange(1, 3, term[1], this);
      this.frames.push(0);
      var str = this.evaluate(term[1][0], internalOptions)
      util.assertType(str, "STRING", this);
      this.frames.pop();

      var separator;
      if (term[1].length > 1) {
        this.frames.push(1);
        separator = this.evaluate(term[1][1], internalOptions)
        this.frames.pop();
        if (separator !== null) { // RethinkDB doesn't frame that...
          util.assertType(separator, "STRING", this);
        }
      }
      else {
        separator = /\s+/;
      }
      if (separator === null) {
        separator = /\s+/;
      }

      var limit;
      if (term[1].length > 2) {
        this.frames.push(2);
        limit = this.evaluate(term[1][2], internalOptions)
        util.assertType(limit, "NUMBER", this);
        this.frames.pop();
      }

      var result = str.split(separator);
      if (limit !== undefined) {
        if (limit < result.length) {
          // We can't just join with a RegExp as we need to handle repetitions...
          if (separator instanceof RegExp) {
            var remaining = str;
            result = result.slice(0, limit);
            for(var i=0; i<result.length; i++) {
              remaining = remaining.slice(result[i].length);
              remaining = remaining.slice(remaining.match(separator)[0].length);
            }
            result = result.concat(remaining);
          }
          else {
            result = result.slice(0, limit).concat(result.slice(limit).join(separator))
          }
        }
      }
      return result;
    case termTypes.UNGROUP:
      util.assertArity(1, term[1], this);
      this.frames.push(0);
      var groups = this.evaluate(term[1][0], internalOptions)
      util.assertType(groups, "GROUP", this);
      this.frames.pop();

      return groups.ungroup();
    case termTypes.RANDOM:
      util.assertArityRange(0, 2, term[1], this);
      util.assertOptions(options, ['float'], this);

      if (internalOptions.deterministic === true) { util.nonDeterministicOp(this); }
      if ((!Array.isArray(term)) || (term[1].length === 0)) {
        return Math.random();
      }
      else {
        if (term[1].length === 1) {
          var min = 0;
          this.frames.push(1);
          var max = this.evaluate(term[1][0], internalOptions);
          this.frames.pop();

          util.assertType(max, "NUMBER", this);
          if (options.float !== true) {
            try {
              util.assertType(max, "INT", this);
            }
            catch(err) {
              throw new Error.ReqlRuntimeError("Upper bound ("+max+") could not be safely converted to an integer");
            }
          }
        }
        else if (term[1].length === 2) {
          this.frames.push(1);
          var min = this.evaluate(term[1][0], internalOptions);
          this.frames.pop();

          this.frames.push(2);
          var max = this.evaluate(term[1][1], internalOptions);
          this.frames.pop();

          util.assertType(min, "NUMBER", this);
          util.assertType(max, "NUMBER", this);
          if (options.float !== true) {
            try {
              util.assertType(min, "INT", this);
            }
            catch(err) {
              throw new Error.ReqlRuntimeError("Lower bound ("+min+") could not be safely converted to an integer");
            }

            try {
              util.assertType(max, "INT", this);
            }
            catch(err) {
              throw new Error.ReqlRuntimeError("Upper bound ("+max+") could not be safely converted to an integer");
            }

          }
        }

        if (min > max) {
          if (options.float !== true) {
            var temp = max;
            max = min;
            min = temp;
          }
          else {
            throw new Error.ReqlRuntimeError("Lower bound ("+min+") is not less than upper bound ("+max+")")
          }
        }

        if (options.float === true) {
          return min+Math.random()*(max-min);
        }
        else {
          return Math.floor(min+Math.random()*(max-min));
        }
      }
    case termTypes.CHANGES:
      util.assertArity(1, term[1], this);
      this.frames.push(0);
      var tableSlice = this.evaluate(term[1][0], internalOptions)
      this.frames.pop();
      return new Changes(tableSlice, this)
    case termTypes.ARGS:
      util.isBuggy(this);
    case termTypes.POINT:
      util.assertArity(2, term[1], this);
      this.frames.push(0);
      var longitude = this.evaluate(term[1][0], internalOptions)
      util.assertType(longitude, 'NUMBER', this);
      this.frames.pop();

      this.frames.push(1);
      var latitude = this.evaluate(term[1][1], internalOptions)
      util.assertType(latitude, 'NUMBER', this);
      this.frames.pop();
      return new ReqlGeometry('Point', [longitude, latitude], this);
    case termTypes.CIRCLE:
      util.assertArity(2, term[1], this);
      this.frames.push(0);
      var center = this.evaluate(term[1][0], internalOptions)
      this.frames.pop();
      if (util.isGeometry(center)) {
        center = new Sequence(center.coordinates, {})
      }
      else {
        util.assertPointCoordinates(center, this);
      }

      this.frames.push(1);
      var radius = this.evaluate(term[1][1], internalOptions)
      util.assertType(radius, 'NUMBER', this);
      this.frames.pop();
      var coordinates = util.generateCircle(center, radius, 32);
      return new ReqlGeometry('Polygon', coordinates, this);
    case termTypes.LINE:
      util.assertArityRange(2, Infinity, term[1], this);
      var coordinates = [];
      for(var i=0; i<term[1].length; i++) {
        this.frames.push(i);
        var coordinate = this.evaluate(term[1][i], internalOptions);
        this.frames.pop();
        if (util.isGeometry(coordinate)) {
          coordinates.push(coordinate.coordinates);
        }
        else {
          util.assertPointCoordinates(coordinate, this);
          coordinates.push(util.toDatum(coordinate));
        }
      }
      return new ReqlGeometry('LineString', coordinates, this);
    case termTypes.FILL:
      util.assertArity(1, term[1], this);
      this.frames.push(0);
      var line = this.evaluate(term[1][0], internalOptions)
      this.frames.pop();
      var coordinates = line.coordinates;
      if ((coordinates[0][0] !== coordinates[coordinates.length-1][0])
          || (coordinates[0][1] !== coordinates[coordinates.length-1][1])) {
        coordinates.push(coordinates[0]);
      }
      return new ReqlGeometry('Polygon', [coordinates], this);
    case termTypes.GEOJSON:
      util.assertArity(1, term[1], this);
      this.frames.push(0);
      var geojson = this.evaluate(term[1][0], internalOptions)
      this.frames.pop();
      return ReqlGeometry.geojson(geojson, this);
    case termTypes.TO_GEOJSON:
      util.assertArity(1, term[1], this);
      this.frames.push(0);
      var geometry = this.evaluate(term[1][0], internalOptions)
      this.frames.pop();
      return geometry.toGeojson(this);
    case termTypes.GET_INTERSECTING:
      util.assertArity(2, term[1], this);
      this.frames.push(0);
      var table = this.evaluate(term[1][0], internalOptions)
      util.assertType(table, 'TABLE', this);
      this.frames.pop();

      this.frames.push(1);
      var geometry = this.evaluate(term[1][1], internalOptions)
      util.assertType(geometry, 'GEOMETRY', this);
      this.frames.pop();

      return table.getIntersecting(geometry, options, this, internalOptions);
    case termTypes.GET_NEAREST:
      util.assertArity(2, term[1], this);
      this.frames.push(0);
      var table = this.evaluate(term[1][0], internalOptions)
      util.assertType(table, 'TABLE', this);
      this.frames.pop();

      this.frames.push(1);
      var geometry = this.evaluate(term[1][1], internalOptions)
      this.frames.pop();
      // Grumble grumble
      if (!util.isGeometry(geometry)) {
        util.assertType(geometry, 'ARRAY', this);
      }

      return table.getNearest(geometry, options, this, internalOptions);
    case termTypes.INCLUDES:
      util.assertArity(2, term[1], this);
      this.frames.push(0);
      var geometry1 = this.evaluate(term[1][0], internalOptions)
      if (!util.isGeometry(geometry1) && !util.isSequence(geometry1)) {
        util.cannotPerformOp('includes', geometry1, this);
      }
      this.frames.pop();

      this.frames.push(1);
      var geometry2 = this.evaluate(term[1][1], internalOptions)
      util.assertType(geometry2, 'GEOMETRY', this);
      this.frames.pop();
      return geometry1.includes(geometry2, this);
    case termTypes.INTERSECTS:
      util.assertArity(2, term[1], this);
      this.frames.push(0);
      var geometry1 = this.evaluate(term[1][0], internalOptions)
      if (!util.isGeometry(geometry1) && !util.isSequence(geometry1)) {
        util.cannotPerformOp('intersects', geometry1, this);
      }
      this.frames.pop();

      this.frames.push(1);
      var geometry2 = this.evaluate(term[1][1], internalOptions)
      util.assertType(geometry2, 'GEOMETRY', this);
      this.frames.pop();
      return geometry1.intersects(geometry2, this);
    case termTypes.POLYGON:
      var coordinates = [];
      for(var i=0; i<term[1].length; i++) {
        this.frames.push(i);
        var polygon = this.evaluate(term[1][i], internalOptions);
        coordinates.push(util.toDatum(polygon));
        this.frames.pop();
      }
      if ((coordinates[0][0] !== coordinates[coordinates.length-1][0])
          || (coordinates[0][1] !== coordinates[coordinates.length-1][1])) {
        coordinates.push(coordinates[0]);
      }
      return new ReqlGeometry('Polygon', [coordinates], this);
    case termTypes.POLYGON_SUB:
      util.assertArity(2, term[1], this);
      this.frames.push(0);
      var outerPolygon = this.evaluate(term[1][0], internalOptions)
      util.assertType(outerPolygon, 'GEOMETRY', this);
      if (outerPolygon.type !== 'Polygon') {
        throw new Error.ReqlRuntimeError("Expected a Polygon but found a "+outerPolygon.type, this.frames)
      }
      if (outerPolygon.coordinates.length > 1) {
        throw new Error.ReqlRuntimeError("Expected a Polygon with only an outer shell.  This one has holes", this.frames)
      }
      this.frames.pop();

      this.frames.push(1);
      var innerPolygon = this.evaluate(term[1][1], internalOptions)
      util.assertType(innerPolygon, 'GEOMETRY', this);
      if (innerPolygon.type !== 'Polygon') {
        throw new Error.ReqlRuntimeError("Expected a Polygon but found a "+innerPolygon.type, this.frames)
      }
      this.frames.pop();

      return new ReqlGeometry('Polygon', [outerPolygon.coordinates[0], innerPolygon.coordinates[0]], this);
    case termTypes.DISTANCE:
      util.assertArity(2, term[1], this);
      this.frames.push(0);
      var from = this.evaluate(term[1][0], internalOptions)
      util.assertType(from, 'GEOMETRY', this);
      this.frames.pop();

      this.frames.push(1);
      var to = this.evaluate(term[1][1], internalOptions)
      util.assertType(to, 'GEOMETRY', this);
      this.frames.pop();

      return from.distance(to, options);
    case termTypes.CONFIG:
      util.notAvailable(null, this);
    case termTypes.RECONFIGURE:
      util.notAvailable(null, this);
    case termTypes.REBALANCE:
      util.notAvailable(null, this);
    default:
      throw new Error.ReqlRuntimeError("Unkown term", this.frames)
  }
}

Query.prototype.isComplete = function() {
  return this.complete;
}

module.exports = Query;
