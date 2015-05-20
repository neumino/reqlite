var _util = require('util');
var EventEmitter = require('events').EventEmitter;
var util = require(__dirname+"/utils/main.js");
var Error = require(__dirname+"/error.js");
var Sequence = require(__dirname+"/sequence.js");

function Document(doc, table) {
  this.doc = doc;
  this.table = table;
}
_util.inherits(Document, EventEmitter);

Document.prototype.typeOf = function() {
  return "SELECTION<OBJECT>";
}

//TODO Forbid nested writes
// TODO Refactor with replace?
Document.prototype.update = function(newValue, options, query, internalOptions) {
  var result = util.writeResult();
  var primaryKey = this.table.options.primaryKey;

  // Update context
  var updateValue;
  if (util.isFunction(newValue)) {
    var varId = util.getVarId(newValue);
    query.context[varId] = this;
    if (options.non_atomic === true) {
      try {
        updateValue = query.evaluate(newValue, internalOptions)
      }
      catch(err) {
        result.errors++;
        result.first_error = err.message+'.'; // WHAT?!?
        return result;
      }
    }
    else {
      try {
        updateValue = query.evaluate(newValue, {deterministic: true})
      }
      catch(err) {
        if (err.message.match(/^Could not prove function deterministic.  Maybe you want to use the non_atomic flag?/)) {
          throw err;
        }
        else {
          result.errors++;
          result.first_error = err.message+'.';
          return result;
        }
      }
    }
    delete query.context[varId]
  }
  else {
    if (options.non_atomic === true) {
      try {
        updateValue = query.evaluate(newValue, internalOptions)
      }
      catch(err) {
        result.errors++;
        result.first_error = err.message+'.';
        return result;
      }
    }
    else {
      try {
        updateValue = query.evaluate(newValue, {deterministic: true})
      }
      catch(err) {
        if (err.message.match(/^Could not prove function deterministic.  Maybe you want to use the non_atomic flag?/)) {
          throw err;
        }
        result.errors++;
        result.first_error = err.message+'.';
        return result;
      }
    }
    if (typeof updateValue === 'function') {
      try {
        updateValue = updateValue(util.toDatum(this));
        util.assertJavaScriptResult(updateValue, query);
      }
      catch(err) {
        result.errors++;
        result.first_error = err.message+'.';
        return result;
      }
      updateValue = util.revertDatum(updateValue);
    }
  }

  try {
    util.assertType(updateValue, 'DATUM', this);
  }
  catch(err) {
    result.errors++;
    result.first_error = err.message;
    return result;
  }
  if ((updateValue[primaryKey] !== undefined) 
      && (this.doc[primaryKey] !== updateValue[primaryKey])) {
    result.errors++;
    result.first_error = "Primary key `id` cannot be changed(`"+
      JSON.stringify(util.toDatum(updateValue), null, 2)+"` -> `"+
      JSON.stringify(this.doc, null, 2)+"`)"
  }
  else {
    var old_val = util.deepCopy(this.doc);
    if (updateValue instanceof Document) {
      updateValue = updateValue.doc;
    }
    var changed = util._merge(this.doc, updateValue);
    if (changed === true) {
      result.replaced++;
      this.emit('change', {new_val: this.doc, old_val: old_val});
      if (options.return_changes === true) {
        result.changes = new Sequence([{
          new_val: util.deepCopy(this.doc),
          old_val: old_val
        }])
      }
    }
    else {
      result.unchanged++;
    }
  }
  if (result.errors === 0) {
    this.table._saveOriginal(this.doc[primaryKey], util.deepCopy(this.doc));
  }
  return result;
}

Document.prototype.merge = function(obj, query) {
  return util.merge(this.doc, obj, query);
}

Document.prototype.without = function(keys) {
  return util.without(this.doc, keys);
}

Document.prototype.replace = function(newValue, options, query, internalOptions) {
  var result = util.writeResult();
  var primaryKey = this.table.options.primaryKey;

  // replace context
  var replaceValue;
  if (util.isFunction(newValue)) {
    var varId = util.getVarId(newValue);
    query.context[varId] = this;
    if (options.non_atomic === true) {
      try {
        replaceValue = query.evaluate(newValue, internalOptions)
      }
      catch(err) {
        result.errors++;
        result.first_error = err.message+'.'; // WHAT?!?
        if (options.return_changes === true) {
          this.emit('change', {new_val: this.doc, old_val: old_val});
          result.changes = new Sequence([{
            new_val: util.deepCopy(this.doc),
            old_val: util.deepCopy(this.doc)
          }])
        }
        return result;
      }
    }
    else {
      try {
        replaceValue = query.evaluate(newValue, {deterministic: true})
      }
      catch(err) {
        if (err.message.match(/^Could not prove function deterministic.  Maybe you want to use the non_atomic flag?/)) {
          throw err;
        }
        else {
          result.errors++;
          result.first_error = err.message+'.';

          if (options.return_changes === true) {
            this.emit('change', {new_val: this.doc, old_val: this.doc});
            result.changes = new Sequence([{
              new_val: util.deepCopy(this.doc),
              old_val: util.deepCopy(this.doc)
            }])
          }
          return result;
        }
      }
    }
    delete query.context[varId]
  }
  else {
    if (options.non_atomic === true) {
      try {
        replaceValue = query.evaluate(newValue, internalOptions)
      }
      catch(err) {
        result.errors++;
        result.first_error = err.message+'.';
        return result;
      }
    }
    else {
      try {
        replaceValue = query.evaluate(newValue, {deterministic: true})
      }
      catch(err) {
        if (err.message.match(/^Could not prove function deterministic.  Maybe you want to use the non_atomic flag?/)) {
          throw err;
        }
        result.errors++;
        result.first_error = err.message+'.';
        return result;
      }
    }
    if (typeof replaceValue === 'function') {
      try {
        replaceValue = replaceValue(util.toDatum(this));
        util.assertJavaScriptResult(replaceValue, query);
      }
      catch(err) {
        result.errors++;
        result.first_error = err.message+'.';
        return result;
      }
      replaceValue = util.revertDatum(replaceValue);
    }
  }

  try {
    util.assertType(replaceValue, 'DATUM', this);
  }
  catch(err) {
    throw err;
    result.errors++;
    result.first_error = err.message;
    return result;
  }
  if ((replaceValue[primaryKey] !== undefined) 
      && (this.doc[primaryKey] !== replaceValue[primaryKey])) {
    result.errors++;
    result.first_error = "Primary key `id` cannot be changed(`"+
      JSON.stringify(util.toDatum(replaceValue), null, 2)+"` -> `"+
      JSON.stringify(this.doc, null, 2)+"`)"
    if (options.return_changes === true) {
      this.emit('change', {new_val: this.doc, old_val: this.doc});
      result.changes = new Sequence([{
        new_val: util.deepCopy(this.doc),
        old_val: util.deepCopy(this.doc)
      }])
    }
  }
  else {
    // We need this for changes
    var old_val = util.deepCopy(this.doc);
    if (replaceValue instanceof Document) {
      replaceValue = replaceValue.doc;
    }
    var changed = util._replace(this.doc, replaceValue);
    if (changed === true) {
      result.replaced++;
      this.emit('change', {new_val: this.doc, old_val: old_val});
      if (options.return_changes === true) {
        result.changes = new Sequence([{
          new_val: util.deepCopy(this.doc),
          old_val: old_val
        }])
      }
    }
    else {
      result.unchanged++;
    }
  }
  if (result.errors === 0) {
    this.table._saveOriginal(this.doc[primaryKey], util.deepCopy(this.doc));
  }
  return result;
}

Document.prototype.delete = function(options, query) {
  return this.table._delete(this, options, query);

}

Document.prototype.toDatum = function() {
  var result = {};
  var keys = Object.keys(this.doc);
  for(var i=0; i<keys.length; i++) {
    var key = keys[i];
    if ((this.doc[key] != null) && (typeof this.doc[key].toDatum === "function")) {
      result[key] = this.doc[key].toDatum();
    }
    else {
      result[key] = util.toDatum(this.doc[key]);
    }
  }
  return result;
}

Document.prototype.getField = function(field) {
  return this.doc[field];
}

Document.prototype.getBracket = function(field, query) {
  if (this.doc.hasOwnProperty(field)) {
    return this.doc[field];
  }
  throw new Error.ReqlRuntimeError("No attribute `"+field+"` in object:"+JSON.stringify(this.doc, null, 4), query.frames)
}

Document.prototype.keys = function() {
  var result = new Sequence();
  var keys = Object.keys(this.doc);
  for(var i=0; i<keys.length; i++) {
    result.push(keys[i]);
  }
  return result;
}

module.exports = Document;
