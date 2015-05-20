var util = {};
var Error = require(__dirname+"/../error.js");

var Minval = require(__dirname+"/../minval.js");
var Maxval = require(__dirname+"/../maxval.js");
var Asc = require(__dirname+"/../asc.js");
var Desc = require(__dirname+"/../desc.js");
var Literal = require(__dirname+"/../literal.js");
var ReqlDate = require(__dirname+"/../date.js");
var protodef = require(__dirname+"/../protodef.js");
var constants = require(__dirname+"/../constants.js");
var GeographicLib = require(__dirname+"/../../external/geographiclib.js");
var termTypes = protodef.Term.TermType;
// Lazily loaded
// TODO Fix the dependencies
var Sequence;
var Table;
var Database;
var Selection;
var Document;
var MissingDoc;
var Group;
var Changes;


util.log = function(message) {
  if (constants.DEBUG === true) {
    console.log.apply(console.log, Array.prototype.slice.call(arguments, 0));
  }
}

util.isBuggy = function(query) {
  throw new Error.ReqlRuntimeError("Reqlite is buggy, this code should not be reached", query.frames)
}
util.notAvailable = function(message, query) {
  if (message == null) {
    throw new Error.ReqlRuntimeError("This method has not been implemented (yet) in Reqlite", query.frames)
  }
  else {
    throw new Error.ReqlRuntimeError(message, query.frames)
  }
}

util.isPlainObject = function isPlainObject(value) {
  return Object.prototype.toString.call(value) === '[object Object]';
}

util.typeOf = function(value, query) {
  //TODO Properly handle circular references
  if (Sequence === undefined) { Sequence = require(__dirname+"/../sequence.js"); }

  if (typeof value === "number") {
    return "NUMBER";
  }
  else if (typeof value === "string") {
    return "STRING";
  }
  else if (typeof value === "boolean") {
    return "BOOL";
  }
  else if (value === null) {
    return "NULL";
  }
  else if (typeof value.typeOf === "function") {
    // Covers TABLE, GROUP
    return value.typeOf();
  }
  else if (util.isDate(value)) {
    return "PTYPE<TIME>";
  }
  else if (util.isBinary(value)) {
    return "PTYPE<BINARY>";
  }
  else if (util.isSequence(value)) {
    return value.type; // Can be "ARRAY", "STREAM", etc.
  }
  else if (util.isDocument(value)) {
    return "SELECTION<OBJECT>";
  }
  else if (util.isObject(value)) {
    return "OBJECT";
  }
  else if (typeof value === 'function') {
    return "FUNCTION";
  }
  throw new Error.ReqlRuntimeError("Server is buggy, unknown type", query.frames)

}

util.assertJavaScriptResult = function(value, query) {
  if ((value !== undefined) && ((value === null)
      || util.isTable(value)
      || util.isSequence(value)
      || util.isSelection(value)
      || util.isDate(value)
      || util.isDocument(value)
      || util.isBinary(value)
      || util.isGeometry(value)
      || util.isAsc(value)
      || util.isDesc(value))) {
    return;
  }
  else if (util.isPlainObject(value)) {
    var keys = Object.keys(value);
    for(var i=0; i<keys.length; i++) {
     util.assertJavaScriptResult(value[keys[i]], query);
    }
  }
  else if (Array.isArray(value)) {
    for(var i=0; i<value.length; i++) {
     util.assertJavaScriptResult(value[i], query);
    }
  }
  else {
    util.assertFinite(value, query);
    util.assertDefined(value, query);
  }
}

util.assertFinite = function(value, query) {
  if ((value === Infinity)
      || (value === -Infinity)
      || (value !== value)) {
    throw new Error.ReqlRuntimeError('Number return value is not finite', query.frames)
  }
}

util.assertDefined = function(value, query) {
  if (value === undefined) {
    throw new Error.ReqlRuntimeError('Cannot convert javascript `undefined` to ql::datum_t', query.frames)
  }
  else if (value instanceof RegExp) {
    throw new Error.ReqlRuntimeError('Cannot convert RegExp to ql::datum_t', query.frames)
  }
}


util.nestedWriteForbidden = function(query) {
  throw new Error.ReqlRuntimeError('Cannot nest writes or meta ops in stream operations.  Use FOR_EACH instead', query.frames)
}

util.nonDeterministicOp = function(query) {
  // RethinkDB doesn't seem to keep track of the frames here..
  throw new Error.ReqlRuntimeError('Could not prove function deterministic.  Maybe you want to use the non_atomic flag?', [])
}

util.assertType = function assertType(value, type, query) {
  var typeValue;
  var finalError;

  typeValue = util.getType(value);
  if (Array.isArray(type)) {
    var valid = false;
    for(var i=0; i<type.length; i++) {
      var _type = type[i];
      try {
        util.assertTypeutil(_type, value, typeValue, query);
        valid = true;
        break;
      }
      catch(err) {
        finalError = err;
        // Ignore error, we will throw at the end of this if statement.
      }
    }
    if (valid !== true) {
      //TODO Hack for date.sub, are there other cases?
      //throw finalError;

      for(var i=0; i<type.length; i++) {
        if (type[i] === 'PTYPE<BINARY>') {
          type[i] = 'BINARY';
        }
      }
      throw new Error.ReqlRuntimeError('Expected '+type.join(' or ')+', but found '+typeValue, query.frames)
    }
  }
  else {
    util.assertTypeutil(type, value, typeValue, query)
  }
}

util.assertTime = function(date, query, backquote) {
  if (!(date instanceof ReqlDate)) {
    if (backquote === true) { // RethinkDB doesn't have consistent errors -_-
      throw new Error.ReqlRuntimeError('Not a TIME pseudotype: `'+JSON.stringify(date, null, 4)+'`', query.frames);
    }
    else {
      throw new Error.ReqlRuntimeError('Not a TIME pseudotype: '+JSON.stringify(date, null, 4), query.frames);
    }
  }
}

util.assertAttributes = function(value, attributes, query) {
  for(var i=0; i<attributes.length; i++) {
    if (value[attributes[i]] === undefined) {
      throw new Error.ReqlRuntimeError("No attribute `"+attributes[i]+"` in object:\n"+JSON.stringify(value, null, 4), query.frames)
    }
  }
}
util.assertCoordinatesLength = function(coordinates, query) {
  if (coordinates.length < 2) {
    throw new Error.ReqlRuntimeError('Too few coordinates.  Need at least longitude and latitude', query.frames)
  }
  else if (coordinates.length > 2) {
    throw new Error.ReqlRuntimeError('Too many coordinates.  GeoJSON position should have no more than three coordinates, but got '+coordinates.length, query.frames)
  }
}
util.assertPointCoordinates = function(value, query) {
  util.assertType(value, 'ARRAY', this);
  if (value.length !== 2) {
    throw new Error.ReqlRuntimeError("Expected point coordinate pair.  Got "+value.length+" element array instead of a 2 element one", query.frames)
  }
  util.assertType(value.get(0), 'NUMBER', query);
  util.assertType(value.get(1), 'NUMBER', query);
}

util.assertArity = function(expected, args, query) {
  var len;
  if (Array.isArray(args)) {
    len = args.length;
  }
  else {
    len = 0;
  }
  if (expected !== len) {
    util.arityError(expected, len, query);
  }
}
util.assertArityRange = function(min, max, args, query) {
  var len;
  if (Array.isArray(args)) {
    len = args.length;
  }
  else {
    len = 0;
  }

  if ((len < min) || (len > max)) {
    util.arityRangeError(min, max, len, query);
  }
}

util.arityError = function(expected, found, query) {
  if (expected === 1) {
    throw new Error.ReqlRuntimeError('Expected '+expected+' argument but found '+found, query.frames);
  }
  else {
    throw new Error.ReqlRuntimeError('Expected '+expected+' arguments but found '+found, query.frames);
  }
}

util.arityRangeError = function(min, max, found, query) {
  if (max === Infinity) {
    throw new Error.ReqlRuntimeError('Expected '+min+' or more arguments but found '+found, query.frames);
  }
  else {
    throw new Error.ReqlRuntimeError('Expected between '+min+' and '+max+' arguments but found '+found, query.frames);
  }
}

util.cannotPerformOp = function(operation, value, query) {
  throw new Error.ReqlRuntimeError('Cannot perform '+operation+' on a non-object non-sequence `'+JSON.stringify(value, null, 4)+'`', query.frames)
}


util.assertPreDatum = function(value, query) {
  if (util.isFunction(value)) {
    throw new Error.ReqlRuntimeError("Expected type DATUM but found FUNCTION:\nVALUE FUNCTION", query.frames)
  }
}
util.assertTypeutil = function(type, value, typeValue, query) {
  //TODO use util.getType
  if (type === 'DATUM') {
    if (!util.isDatum(value)) {
      if (util.isTable(value)) {
        throw new Error.ReqlRuntimeError('Expected type DATUM but found TABLE:\ntable("'+value.name+'")', query.frames)
      }
      else if (util.isSelection(value)) {
        throw new Error.ReqlRuntimeError('Expected type DATUM but found SELECTION:\nSELECTION ON table('+value.table.name+')', query.frames)
      }
      else if (util.isChanges(value)) {
        throw new Error.ReqlRuntimeError('Expected type DATUM but found SEQUENCE:\nVALUE SEQUENCE', query.frames)
      }
      else {
        throw new Error.ReqlRuntimeError("Expected type DATUM but found "+typeValue, query.frames)
      }

    }
  }
  else if (type === 'STRING') {
    if (typeof value !== 'string') {
      throw new Error.ReqlRuntimeError("Expected type STRING but found "+typeValue, query.frames)
    }
  }
  else if (type === 'NUMBER') {
    if (typeof value !== 'number') {
      throw new Error.ReqlRuntimeError("Expected type NUMBER but found "+typeValue, query.frames)
    }
  }
  else if (type === 'BOOL') {
    if (typeof value !== 'boolean') {
      throw new Error.ReqlRuntimeError("Expected type BOOL but found "+typeValue, query.frames)
    }
  }
  else if (type === "PTYPE<TIME>") {
    if (!util.isDate(value)) {
      throw new Error.ReqlRuntimeError("Expected type PTYPE<TIME> but found "+typeValue, query.frames)
    }
  }
  else if (type === "GEOMETRY") {
    if (!util.isGeometry(value)) {
      throw new Error.ReqlRuntimeError("Not a GEOMETRY pseudotype: `"+JSON.stringify(value, null, 4)+"`", query.frames)
    }
  }
  else if (type === "POLYGON") {
    if (!util.isGeometry(value)) {
      throw new Error.ReqlRuntimeError("Not a GEOMETRY pseudotype: `"+JSON.stringify(value, null, 4)+"`", query.frames)
    }
    else if (value.type !== 'Polygon') {
      throw new Error.ReqlRuntimeError("Expected geometry of type `Polygon` but found `"+value.type+"`", query.frames)
    }
  }
  else if (type === 'FUNCTION') {
    if (!util.isFunction(value)) {
      if (util.isDatum(value)) {
        typeValue = 'DATUM';
      }
      throw new Error.ReqlRuntimeError("Expected type FUNCTION but found "+typeValue+":\n"+JSON.stringify(util.toDatum(value)), query.frames)
    }
  }
  else if (type === 'GROUP') {
    if (!util.isGroup(value)) {
      if (util.isDatum(value)) {
        typeValue = 'DATUM';
      }
      throw new Error.ReqlRuntimeError("Expected type GROUPED_DATA but found "+typeValue+":\n"+JSON.stringify(util.toDatum(value), null, 4), query.frames)
    }
  }
  else if (type === "DATABASE") {
    if (Database === undefined) { Database = require(__dirname+"/../database.js"); }
    if (!(value instanceof Database)) {
      if (util.isDatum(value)) {
        typeValue = 'DATUM';
      }
      throw new Error.ReqlRuntimeError("Expected type DATABASE but found "+typeValue+":\n"+JSON.stringify(util.toDatum(value)), query.frames)
    }
  }
  else if (type === "TABLE_SLICE") {
    if (Table === undefined) { Table = require(__dirname+"/../table.js"); }
    if (Selection === undefined) { Selection = require(__dirname+"/../selection.js"); }
    if (!util.isTable(value) && !util.isSelection(value)) {
      if (util.isDatum(value)) {
        typeValue = 'DATUM';
      }
      throw new Error.ReqlRuntimeError("Expected type TABLE_SLICE but found "+typeValue+":\n"+JSON.stringify(util.toDatum(value)), query.frames)
    }

  }
  else if (type === "TABLE") {
    if (!util.isTable(value)) {
      if (util.isDatum(value)) {
        typeValue = 'DATUM';
      }
      throw new Error.ReqlRuntimeError("Expected type TABLE but found "+typeValue+":\n"+JSON.stringify(util.toDatum(value)), query.frames)
    }
  }
  else if (type === "SELECTION") {
    if (!util.isTable(value) && !util.isSelection(value) && !util.isDocument(value)) {
      if (util.isDatum(value)) {
        typeValue = 'DATUM';
      }
      throw new Error.ReqlRuntimeError("Expected type SELECTION but found "+typeValue+":\n"+JSON.stringify(util.toDatum(value), null, 4), query.frames)
    }
  }
  else if (type === "ARRAY") {
    if (!util.isSequence(value)) {
      throw new Error.ReqlRuntimeError("Expected type ARRAY but found "+typeValue, query.frames)
    }
  }
  else if (type === "PTYPE<BINARY>") {
    if (!util.isBinary(value)) {
      throw new Error.ReqlRuntimeError("Expected type BINARY but found "+typeValue, query.frames)
    }
  }
  else if (type === "INT") {
    var floored = Math.floor(value);
    if (floored !== value) {
      throw new Error.ReqlRuntimeError("Number not an integer: "+value, query.frames)
    }
  }
  else if (type === "OBJECT") {
    if (!util.isObject(value)) {
      throw new Error.ReqlRuntimeError("Expected type OBJECT but found "+typeValue, query.frames)
    }
  }
}

util.getType = function(value) {
  if (Sequence === undefined) { Sequence = require(__dirname+"/../sequence.js"); }
  if (Table === undefined) { Table = require(__dirname+"/../table.js"); }

  if (value instanceof Table) {
    return "TABLE";
  }
  else if (value instanceof Sequence) {
    return "ARRAY";
  }
  else if (value === null) {
    return "NULL"
  }
  else if (util.isDate(value)) {
    return "PTYPE<TIME>";
  }
  else if (util.isBinary(value)) {
    return "PTYPE<BINARY>";
  }
  else if (value instanceof Maxval) {
    return "MAXVAL";
  }
  else if (value instanceof Minval) {
    return "MINVAL";
  }
  else if (util.isPlainObject(value)) {
    return "OBJECT";
  }
  else {
    return (typeof value).toUpperCase();
  }
}

util.assertOptions = function(options, allowedKeys, query) {
  var isAllowedKey = {};
  for(var i=0; i<allowedKeys.length; i++) {
    isAllowedKey[allowedKeys[i]] = true;
  }

  var keys = Object.keys(options);
  for(var i=0; i<keys.length; i++) {
    if (isAllowedKey[keys[i]] !== true) {
      throw new Error.ReqlRuntimeError('Unrecognized optional argument `'+keys[i]+'`', query.frames)
    }
  }
}

util.assertNoSpecialChar = function assertNoSpecialChar(value, type, query) {
  if (!value.match(/^[0-9a-zA-Z]+$/)) {
    throw new Error.ReqlRuntimeError(type+" name `"+value+"` invalid (Use A-Za-z0-9_ only)", query.frames)
  }
}

util.s4 = function s4() {
  return Math.floor((1+Math.random())*0x10000).toString(16).substring(1);
};

util.uuid = function uuid() {
  return util.s4()+util.s4()+"-"+util.s4()+"-"+util.s4()+"-"+util.s4()+"-"+util.s4()+util.s4()+util.s4();
}

util.writeResult = function writeResult() {
  return {
    deleted: 0,
    errors: 0,
    inserted: 0,
    replaced: 0,
    skipped: 0,
    unchanged: 0
  }
}

// Merging write result in place
util.mergeWriteResult = function(left, right) {
  left.deleted += right.deleted;
  left.errors += right.errors;
  left.inserted += right.inserted;
  left.replaced += right.replaced;
  left.skipped += right.skipped;
  left.unchanged += right.unchanged;
  if (left.generated_keys !== undefined || right.generated_keys !== undefined) {
    if (Array.isArray(left.generated_keys) && Array.isArray(right.generated_keys)) {
      left.generated_keys = left.generated_keys.concat(right.generated_keys);
    }
    else if (Array.isArray(right.generated_keys)) {
      left.generated_keys = right.generated_keys;
    }
  }
  if ((left.first_error === undefined) && (right.first_error !== undefined)) {
    left.first_error = right.first_error;
  }
  if (util.isSequence(right.changes) && right.changes.length > 0) {
    if (!util.isSequence(left.changes)) {
      left.changes = new Sequence();
    }
    for(var i=0; i<right.changes.length; i++) {
      left.changes.push(right.changes.get(i))
    }
  }
}

util.merge = function(self, toMerge, query, internalOptions) {
  var varId;
  if (util.isFunction(toMerge)) {
    varId = util.getVarId(toMerge);
    query.context[varId] = self;
  }

  toMerge = query.evaluate(toMerge, internalOptions);

  if (varId !== undefined) {
    delete query.context[varId];
  }
  return util.mergeDatum(self, toMerge);
}
util.mergeDatum = function merge(self, obj) {
  if (!util.isPlainObject(obj)) {
    return obj;
  }
  if (!util.isPlainObject(self)) {
    return obj;
  }

  //A non in place merge, used for the ReQL `merge` command
  var result = util.deepCopy(self);
  for(var key in obj) {
    // Recursively merge only if both fields are objects, else we'll overwrite the field
    if (obj[key] instanceof Literal) {
      result[key] = obj[key].value;
    }
    else if (util.isPlainObject(obj[key]) && util.isPlainObject(result[key])) {
      result[key] = util.mergeDatum(result[key], obj[key]);
    }
    else {
      result[key] = obj[key];
    }
  }
  return result;
}
util._merge = function _merge(self, obj) {
  // Inplace merge (behave like the `update` command.
  // Return whether `self` has been changed
  var changed = false;
  for(var key in obj) {
    // Recursively merge only if both fields are objects, else we'll overwrite the field
    if (obj[key] instanceof Literal) {
      changed = true;
      if (util.eq(self[key], obj[key].value)) {
        changed = false;
      }
      self[key] = obj[key].value;
    }
    else if (util.isPlainObject(obj[key]) && util.isPlainObject(self[key])) {
      changed = util._merge.call(self[key], obj[key]) || changed;
    }
    else {
      if (Array.isArray(self[key])) {
        if (Array.isArray(obj[key])) {
          if (self[key].length !== obj[key].length) {
            changed = true;
          }
          else {
            for(var i=0; i<self[key].length; i++) {
              if ((util.isPlainObject(self[key][i])) && (util.isPlainObject(obj[key][i]))) {
                changed = util._merge(self[key][i], obj[key][i]) || changed;
              }
              else {
                changed = true;
                self[key][i] = util.deepCopy(obj[key][i]);
              }
            }
          }
        }
        else {
          changed = true;
        }
      }
      else if (self[key] !== obj[key]) {
        changed = true;
      }
      self[key] = obj[key];
    }
  }
  return changed
}

util._replace = function replace(self, obj) {
  // Inplace replace (behave like the `replace` command.
  // Return whether `self` has been changed
  var changed = false;
  for(var key in self) {
    if (obj[key] === undefined) {
      delete self[key];
      changed = true;
    }
  }
  for(var key in obj) {
    // Recursively merge only if both fields are objects, else we'll overwrite the field
    if (util.isPlainObject(obj[key]) && util.isPlainObject(self[key])) {
      changed = util.replaceecall(self[key], obj[key]) || changed;
    }
    else {
      if (Array.isArray(self[key])) {
        if (Array.isArray(obj[key])) {
          if (self[key].length !== obj[key].length) {
            changed = true;
          }
          else {
            for(var i=0; i<self[key].length; i++) {
              //TODO If self[ley][i] is not an object, it will break
              if ((util.isPlainObject(self[key][i])) && (util.isPlainObject(obj[key][i]))) {
                changed = util.replace(self[key][i], obj[key][i]) || changed;
              }
              else if (util.isPlainObject(self[key][i])) {
                changed = true;
                self[key][i];
              }
            }
          }
        }
        else {
          changed = true;
        }
      }
      else if (self[key] !== obj[key]) {
        changed = true;
      }
      self[key] = obj[key];
    }
  }
  return changed
}

util.makeInternalPk = function(value) {
  // Build the internal prinary key we use in the hash table
  if (typeof value === 'string') {
    return "string_"+value
  }
  else if (typeof value === 'number') {
    return "number_"+value
  }
  else if (typeof value === 'boolaen') {
    return "boolean_"+value
  }
  else if (util.isDate(value)) {
    util.validDate(value);
    return "date_"+value.epoch_time+"_"+value.timezone;
  }
  else if (util.isBinary(value)) {
    return "binary_"+value.data
  }
  else {
    var type = util.typeOf(value);
    throw new Error.ReqlRuntimeError("Primary keys must be either a number, string, bool, pseudotype or array (got type "+type+"):\n"+JSON.stringify(value, null, 2));
  }

}
util.validDate = function(date) {
  //TODO use moment
  // `date` should be an object with the field $reql_type$ mapping to "TIME"
  if (typeof date.epoch_time !== "number") {
    throw new Error.ReqlRuntimeError("RqlRuntimeError: Invalid time object constructed (no field `epoch_time`):\n"+JSON.stringify(date, null, 2))
  }
  else if (date.timezone.match(/^(?:Z|[+-](?:2[0-3]|[01][0-9]):[0-5][0-9])$/) == null) {
    throw new Error.ReqlRuntimeError("RqlRuntimeError: Invalid time object constructed (no field `timezone`):\n"+JSON.stringify(date, null, 2))
  }
}
//TODO CamelCase the thing
util.dateToString = function(date) {
  var timezone = date.timezone;
  
  // Extract data from the timezone
  var timezone_array = date.timezone.split(':');
  var sign = timezone_array[0][0]; // Keep the sign
  timezone_array[0] = timezone_array[0].slice(1); // Remove the sign

  // Save the timezone in minutes
  var timezone_int = (parseInt(timezone_array[0], 10)*60+parseInt(timezone_array[1], 10))*60;
  if (sign === '-') {
    timezone_int = -1*timezone_int;
  }

  // d = real date with user's timezone
  var d = new Date(date.epoch_time*1000);

  // Add the user local timezone
  timezone_int += d.getTimezoneOffset()*60;

  // d_shifted = date shifted with the difference between the two timezones
  // (user's one and the one in the ReQL object)
  var d_shifted = new Date((date.epoch_time+timezone_int)*1000);

  // If the timezone between the two dates is not the same,
  // it means that we changed time between (e.g because of daylight savings)
  if (d.getTimezoneOffset() !== d_shifted.getTimezoneOffset()) {
    // d_shifted_bis = date shifted with the timezone of d_shifted and not d
    var d_shifted_bis = new Date((date.epoch_time+timezone_int-(d.getTimezoneOffset()-d_shifted.getTimezoneOffset())*60)*1000);

    var raw_data_str;
    if (d_shifted.getTimezoneOffset() !== d_shifted_bis.getTimezoneOffset()) {
      // We moved the clock forward -- and therefore cannot generate the appropriate time with JS
      // Let's create the date outselves...
      var str_pieces = d_shifted_bis.toString().match(/([^ ]* )([^ ]* )([^ ]* )([^ ]* )(\d{2})(.*)/);
      var hours = parseInt(str_pieces[5], 10);
      hours++;
      if (hours.toString().length === 1) {
        hours = "0"+hours.toString()
      }
      else {
        hours = hours.toString()
      }
      // Note str_pieces[0] is the whole string
      raw_date_str = str_pieces[1]+" "+str_pieces[2]+" "+str_pieces[3]+" "+str_pieces[4]+" "+hours+str_pieces[6]
    }
    else {
      raw_date_str = d_shifted_bis.toString();
    }
  }
  else {
    raw_date_str = d_shifted.toString()
  }

  // Remove the timezone and replace it with the good one
  return raw_date_str.slice(0, raw_date_str.indexOf('GMT')+3)+timezone
}

util.getHours = function(date) {
  //TODO
}

util.isDate = function(date) {
  return date instanceof ReqlDate;
}

// Obviously not a "full" deep copy...
util.deepCopy = function(value) {
  var result;
  if (util.isSequence(value)) {
    result = new Sequence();
    for(var i=0; i<value.length; i++) {
      result.push(value.get(i));
    }
    return result;
  }
  else if (util.isPlainObject(value)) {
    result = {};
    var keys = Object.keys(value);
    for(var i=0; i<keys.length; i++) {
      result[keys[i]] = util.deepCopy(value[keys[i]]);
    }
    return result;
  }
  else if (Array.isArray(value)) {
    result = [];
    for(var i=0; i<value.length; i++) {
      result.push(util.deepCopy(value[i]));
    }
    return result;
  }
  else {
    return value;
  }
}

util.monthToInt = function(month) {
  switch(month) {
    case "Jan":
      return 1
    case "Feb":
      return 2
    case "Mar":
      return 3
    case "Apr":
      return 4
    case "May":
      return 5
    case "Jun":
      return 6
    case "Jul":
      return 7
    case "Aug":
      return 8
    case "Sep":
      return 9
    case "Oct":
      return 10
    case "Nov":
      return 11
    case "Dec":
      return 12
    default:
      throw new Error("Non valid month")
  }
}

util.eq = function(left, right) {
  //TODO Sequence?
  if ((Array.isArray(left)) && (Array.isArray(right))) {
    if (left.length !== right.length) {
      return false;
    }
    for(var i=0; i<left.length; i++) {
      if (util.eq(left[i], right[i]) === false) {
        return false;
      }
    }
    return true;
  }
  else if (left instanceof Minval) {
    if (right instanceof Minval) {
      return true;
    }
    return false;
  }
  else if (left instanceof Maxval) {
    if (righ instanceof Maxval) {
      return true;
    }
    return false;
  }
  else if (right instanceof Minval) {
    return false; // left is not minval
  }
  else if (right instanceof Maxval) {
    return false; // left is not maxval
  }

  else if (util.isPlainObject(left) && (util.isPlainObject(right))) {
    var leftKeys = Object.keys(left);
    var rightKeys = Object.keys(right);
    if (leftKeys.length !== rightKeys.length) {
      return false;
    }
    for(var i=0; i<leftKeys.length; i++) {
      if (util.eq(left[leftKeys[i]], right[leftKeys[i]]) === false) {
        return false;
      }
    }
    // If the keys are not the same, we will eventually compare undefined with something
    // and we will return false, so we won't reach this part of the code
    return true;
  }
  else {
    return left === right;
  }
}

//TODO Add geometry
util.lt = function(left, right) {
  // array < bool < null < number < object < string < time

  if (left instanceof Error.ReqlRuntimeError) {
    if (right instanceof Error.ReqlRuntimeError) {
      return left.message < right.message;
    }
    else {
      return true
    }
  }
  if (left instanceof Minval) {
    if (right instanceof Error.ReqlRuntimeError) {
      return false;
    }
    if (right instanceof Minval) {
      return false;
    }
    return true;
  }
  if (right instanceof Minval) {
    // No need to check for errors
    return false; // left is not an instance of Minval
  }
  if (right instanceof Maxval) {
    if (left instanceof Maxval) {
      return false;
    }
    return true;
  }
  if (left instanceof Maxval) {
    return false; // right is not an instance of Maxval
  }


  if (util.isSequence(left)) {
    if (right instanceof Error.ReqlRuntimeError) {
      return false;
    }
    if (util.isSequence(right)) {
      left = util.toDatum(left);
      right = util.toDatum(right);
      for(var i=0; i<left.length; i++) {
        if (right[i] === undefined) {
          return false;
        }
        if (util.eq(left[i], right[i]) === false) {
          return util.lt(left[i], right[i]);
        }
      }
      return left.length < right.length;
    }
    else {
      return true;
    }
  }
  else if (typeof left === "boolean") {
    if (right instanceof Error.ReqlRuntimeError) {
      return false;
    }
    if (util.isSequence(right)) {
      return false;
    }
    else if (typeof right === "boolean") {
      return (left === false) && (right === true)
    }
    else {
      return true;
    }
  }
  else if (left === null) {
    if (right instanceof Error.ReqlRuntimeError) {
      return false;
    }
    if (util.isSequence(right)
        || (typeof right === "boolean")) {
      return false;
    }
    else {
      return true;
    }
  }
  else if (typeof left === "number") {
    if (right instanceof Error.ReqlRuntimeError) {
      return false;
    }
    if (util.isSequence(right)
        || (typeof right === "boolean")
        || (right === null)) {
      return false;
    }
    else if (typeof right === "number") {
      return left < right;
    }
    else {
      return true;
    }

  }
  else if (util.isPlainObject(left) && (left.$reql_type$ === undefined)) {
    if (right instanceof Error.ReqlRuntimeError) {
      return false;
    }
    // left is just an object
    if (util.isDate(right)) {
      return true;
    }
    else if (util.isBinary(right)) {
      return true;
    }
    else if (typeof right === "string") {
      return true;
    }
    else if (util.isPlainObject(right)) {
      var leftKeys = Object.keys(left);
      var rightKeys = Object.keys(right);
      leftKeys.sort();
      rightKeys.sort();
      for(var i=0; i<leftKeys.length; i++) {
        if (leftKeys[i] !== rightKeys[i]) {
          return leftKeys[i] < rightKeys[i]
        }
        else if (right[leftKeys[i]] === undefined) {
          return false;
        }
        else if (util.eq(left[leftKeys[i]], right[leftKeys[i]]) === true) {
          continue;
        }
        else {
          return (util.lt(left[leftKeys[i]], right[leftKeys[i]]) === true)
        }
        /*
        else if (util.lt(left[leftKeys[i]], right[leftKeys[i]]) === true) {
          return true;
        }
        */
      }
      return false;
    }
    else {
      return false;
    }
  }
  else if (util.isBinary(left)) {
    if (right instanceof Error.ReqlRuntimeError) {
      return false;
    }
    if ((typeof right === 'string')
      || (util.isDate(right))) {
      return true;
    }
    return false;
  }
  else if (util.isDate(left)) {
    if (right instanceof Error.ReqlRuntimeError) {
      return false;
    }
    if (typeof right === 'string') {
      return true;
    }
    return false;
  }
  else if (typeof left === "string") {
    if (right instanceof Error.ReqlRuntimeError) {
      return false;
    }
    if (typeof right === "string") {
      return left < right;
    }
    else {
      return false;
    }
  }
}

util.gt = function(left, right) {
  return !(util.lt(left, right) || util.eq(left, right))
}
util.le = function(left, right) {
  return util.lt(left, right) || util.eq(left, right)
}
util.ge = function(left, right) {
  return util.gt(left, right) || util.eq(left, right)
}


util.filter = function(doc, filter) {
  if (util.isPlainObject(filter)) {
    if (util.isPlainObject(doc)) {
      for(var key in filter) {
        if (doc[key] === undefined) {
          throw new Error.ReqlRuntimeError("No attribute `"+field+"` in object:\n"+JSON.stringify(doc.toDatum(), null, 2), this.frames)
        }
        if (util.filter(doc[key], filter[key]) === false) {
          return false;
        }
      }
    }
    else {
      return false;
    }
  }
  else if (Array.isArray(filter)) {
    if (Array.isArray(doc)) {
      if (filter.length !== doc.length) {
        return false;
      }
      for(var i=0; i<filter.length; i++) {
        if (util.filter(doc[i], filter[i]) === false) {
          return false;
        }
      }
    }
    else {
      return false;
    }
  }
  else {
    return filter === doc;
  }
  return true;
}

util.toDatum = function(doc, query) {
  var result;
  if (Array.isArray(doc)) {
    result = [];
    for(var i=0; i<doc.length; i++) {
      if (typeof doc[i].toDatum === "function") {
        result.push(doc[i].toDatum(query));
      }
      else {
        result.push(util.toDatum(doc[i], query));
      }
    }
    return result;
  }
  else if (util.isPlainObject(doc)) {
    if (typeof doc.toDatum === "function") {
      return doc.toDatum(query);
    }

    result = {};
    for(var key in doc) {
      result[key] = util.toDatum(doc[key], query);
    }
    return result;
  }
  else {
    return doc;
  }
}

// Take a datum and replace arrays with instances of Sequence
util.revertDatum = function(value) {
  if (value === null) {
    return value;
  }
  else if (util.isTable(value)) {
    return value
  }
  else if (util.isSequence(value)) {
    return value;
  }
  else if (util.isSelection(value)) {
    return value
  }
  else if (util.isDate(value)) {
    return value;
  }
  else if (util.isDocument(value)) {
    return value
  }
  else if (util.isBinary(value)) {
    return value
  }
  else if (util.isGeometry(value)) {
    return value
  }
  else if (util.isAsc(value)) {
    return value
  }
  else if (util.isDesc(value)) {
    return value;
  }
  else if (Array.isArray(value)) {
    if (Sequence === undefined) {
      Sequence = require(__dirname+"/../sequence.js");
    }

    var sequence = new Sequence();
    for(var i=0; i<value.length; i++) {
      sequence.push(util.revertDatum(value[i]));
    }
    return sequence;
  }
  else if (util.isPlainObject(value)) {
    var obj = {};
    for(var key in value) {
      obj[key] = util.revertDatum(value[key]);
    }
    return obj;
  }
  else {
    return value
  }
}

util.isTrue = function(value) {
  return !(value === false || value === null)
}

util.assertPath = function(path, query) {
  if (typeof path === 'string') {
    return;
  }
  else if (util.isSequence(path)) {
    for(var i=0; i<path.length; i++) {
      util.assertPath(path.get(i), query);
    }
  }
  else if (util.isPlainObject(path)) {
    var keys = Object.keys(path);
    for(var i=0; i<keys.length; i++) {
      if (path[keys[i]] !== true) {
        util.assertPath(path[keys[i]], query);
      }
    }
  }
  else {
    throw new Error.ReqlRuntimeError('Invalid path argument `'+JSON.stringify(path)+'`', query.frames)
  }
}
// keys:
// - Array of strings
// - Array of object where a field maps to true or to an array of strings
// obj the object to pluck field from
util.pluck = function(obj, keys) {
  //TODO: Refactor this method now that I know how path spec work...
  var result = {}
  var keyValue;
  //TODO Need a deepcopy?
  obj = util.toDatum(obj); // Need to convert to datum for Document
  keys = util.toDatum(keys); // Need to convert sequences to datum
  for(var i=0; i<keys.length; i++) {
    if (typeof keys[i] === 'string') {
      if ((obj != null) && (obj.hasOwnProperty(keys[i]))) {
        result[keys[i]] = obj[keys[i]];
      }
    }
    else if (util.isPlainObject(keys[i])) {
      for(var key in keys[i]) {
        if (keys[i][key] === true) {
          keyValue = util.pluck(obj, [key])
          for(var newKey in keyValue) {
            result[newKey] = keyValue[newKey];
          }
        }
        else if (Array.isArray(keys[i][key])) {
          if (obj[key] != null) {
            keyValue = util.pluck(obj[key], keys[i][key])
            result[key] = keyValue;
          }
        }
        else if (util.isPlainObject(keys[i][key])) {
          if (util.isPlainObject(obj[key])) {
            keyValue = util.pluck(obj[key], [keys[i][key]])
            result[key] = keyValue;
          }

        }
      }
    }
    else if (Array.isArray(keys[i])) {
      if ((obj != null) && (obj.hasOwnProperty(keys[i]))) {
        util.pluck(obj[keys[i]], keys[i])
      }
    }
  }
  return result;
}

util.hasFields = function(obj, keys) {
  obj = util.toDatum(obj); // Need to convert to datum for Document
  keys = util.toDatum(keys); // Need to convert sequences to datum
  var result;
  for(var i=0; i<keys.length; i++) {
    if (typeof keys[i] === 'string') {
      if ((obj.hasOwnProperty(keys[i]) === false) || (obj[keys[i]] === undefined)) {
        return false;
      }
    }
    else if (util.isPlainObject(keys[i])) {
      for(var key in keys[i]) {
        if (keys[i][key] === true) {
          result = util.hasFields(obj, [key])
          if (result === false) { return false }
        }
        else if (Array.isArray(keys[i][key])) {
          if (obj[key] != null) {
            if (Array.isArray(obj[key])) {
              return false;
            }
            result = util.hasFields(obj[key], keys[i][key])
            if (result === false) { return false }
          }
          else {
            return false;
          }
        }
        else if (util.isPlainObject(keys[i][key])) {
          if (obj[key] != null) {
            if (Array.isArray(obj[key])) {
              return false;
            }
            result = util.hasFields(obj[key], [keys[i][key]])
            if (result === false) { return false }
          }
          else {
            return false;
          }
        }
      }
    }
    else if (Array.isArray(keys[i])) {
      if ((obj != null) && (obj.hasOwnProperty(keys[i]))) {
        if (Array.isArray(obj[keys[i]])) {
          return false;
        }
        result = util.hasFields(obj[keys[i]], keys[i])
        if (result === false) { return false }
      }
    }
  }
  return true;
}


util.without = function(obj, keys) {
  var result = util.deepCopy(obj);
  util._without(result, keys);
  return result;
}

// Mutate obj
util._without = function(obj, keys) {
  keys = util.toDatum(keys); // Need to convert sequences to datum
  if (Array.isArray(keys)) {
    for(var i=0; i<keys.length; i++) {
      util._without(obj, keys[i]);
    }
  }
  else if (util.isPlainObject(keys)) {
    for(var key in keys) {
      if (keys[key] === true) {
        delete obj[key];
      }
      else { // keys[key] is an array or an object
        if (obj[key] != null) {
          util._without(obj[key], keys[key])
        }
      }
    }
  }
  else if (typeof keys === 'string') {
    delete obj[keys];
  }
}

util.toBool = function(val) {
  if ((val === false) || (val === null)) {
    return false;
  }
  else {
    return true;
  }
}
util.between = function(result, doc, valueIndex, left, right, options) {
  keep = true;
  if (options.left_bound === "closed") {
    if (util.lt(valueIndex, left)) {
      keep = false;
    }
  }
  else {
    if (util.le(valueIndex, left)) {
      keep = false;
    }
  }

  if (options.right_bound === "closed") {
    if (util.gt(valueIndex, right)) {
      keep = false;
    }
  }
  else {
    if (util.ge(valueIndex, right)) {
      keep = false;
    }
  }

  if (keep === true) {
    result.push(doc);
  }
}

util.isFunction = function(term) {
  return Array.isArray(term) && (term[0] === termTypes.FUNC)
}
util.getVarId = function(term) {
  return term[1][0][1][0]
}
util.getVarIds = function(term) {
  return term[1][0][1]
}

util.toSequence = function(seq, query) {
  if (Array.isArray(seq)) {
    return seq;
  }
  try {
    return seq.toSequence();
  }
  catch(err) {
    throw new Error.ReqlRuntimeError('Cannot convert '+util.typeOf(seq)+' to SEQUENCE', query.frames)
  }
}

util.generateFieldFunction = function(field) {
  var varName = util.uuid(); // Queries use number, so this is relatively safe
  return [ termTypes.FUNC, [ [ termTypes.MAKE_ARRAY, [ varName ] ], [ termTypes.GET_FIELD, [ [ termTypes.VAR, [ varName ] ], field ] ] ] ]
}

util.outOfBound = function(index, size, query) {
  throw new Error.ReqlRuntimeError("Index `"+index+"` out of bounds for array of size: `"+size+"`", query.frames)
}

util.outOfBoundNoSize = function(index, query) {
  throw new Error.ReqlRuntimeError("Index out of bounds: "+index, this.frames)
}

util.isDocument = function(doc) {
  if (Document === undefined) { Document = require(__dirname+"/../document.js"); }
  if (MissingDoc === undefined) { MissingDoc = require(__dirname+"/../missing_doc.js"); }
  return doc instanceof Document || doc instanceof MissingDoc;
}

util.isMissingDoc = function(doc) {
  if (MissingDoc === undefined) { MissingDoc = require(__dirname+"/../missing_doc.js"); }
  return doc instanceof MissingDoc;
}

util.isObject = function(doc) {
  return util.isDocument(doc)
    || (util.isPlainObject(doc) && (util.$reql_type$ === undefined));
}

util.isSequence = function(seq) {
  //TODO Do not monkey patch..., use instanceof
  if ((seq != null) && (typeof seq.toSequence === 'function')) {
    return true;
  }
  return false;
}

util.isBinary = function(bin) {
  return util.isPlainObject(bin) && (bin.$reql_type$ === 'BINARY')
}

util.isGeometry = function(geometry) {
  return util.isPlainObject(geometry) && (geometry.$reql_type$ === 'GEOMETRY')
}

util.isDatum = function(datum) {
  return (typeof datum === 'string')
    || (typeof datum === 'number')
    || (typeof datum === 'boolean')
    || (datum === null)
    || (typeof datum === 'number')
    || ((util.isPlainObject(datum))
        && !util.isTable(datum)
        && !util.isSelection(datum)
        && !util.isChanges(datum))
}

util.isRawArgs = function(term) {
  return Array.isArray(term) && term[0] === termTypes.ARGS;
}

util.isTable = function(table) {
  if (Table === undefined) { Table = require(__dirname+"/../table.js"); }
  return table instanceof Table;
}

util.isSelection = function(selection) {
  return (selection != null) && (typeof selection.toSelection === 'function');
}

util.isAsc = function(field) {
  return field instanceof Asc;
}

util.isDesc = function(field) {
  return field instanceof Desc;
}

util.isRawAsc = function(field) {
  return Array.isArray(field) && (field[0] === 73)
}
util.isRawDesc = function(field) {
  return Array.isArray(field) && (field[0] === 74)
}

util.isChanges = function(field) {
  if (Changes === undefined) { Changes = require(__dirname+"/../changes.js"); }
  return field instanceof Changes;
}

util.isGroup = function(group) {
  if (Group === undefined) { Group = require(__dirname+"/../group.js"); }
  return group instanceof Group;
}

util.generateCircle = function(center, radius, points) {
  var coordinates = [];
  for(var i=0; i<points+1; i++) {
    angle = 180+((i)/(points)*360)%360
    coordinates.push(util.generateCirclePoint(center, radius, angle));
  }
  return [coordinates];
}

util.generateCirclePoint = function(center, radius, angle) {
  var point = GeographicLib.Geodesic.WGS84.Direct(
    center.get(1),
    center.get(0),
    angle,
    radius);
  var coordinates = [point.lon2, point.lat2];
  return coordinates;
}

util.getBracket = function(obj, key, query) {
  if (typeof obj.getBracket === 'function') {
    return obj.getBracket(key, query);
  }
  else if (obj.hasOwnProperty(key)) {
    return obj[key];
  }
  else {
    throw new Error.ReqlRuntimeError("No attribute `"+key+"` in object:"+JSON.stringify(obj, null, 4), query.frames)
  }

}

module.exports = util;
