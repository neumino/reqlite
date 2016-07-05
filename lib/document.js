var _util = require('util');
var EventEmitter = require('events').EventEmitter;

function Document(doc, table) {
  this.doc = doc;
  this.table = table;
}
_util.inherits(Document, EventEmitter);
module.exports = Document;

var util = require(__dirname+"/utils.js");
var Error = require(__dirname+"/error.js");
var Sequence = require(__dirname+"/sequence.js");

Document.prototype.typeOf = function() {
  return "SELECTION<OBJECT>";
};

Document.prototype.pluck = function(keys) {
  return util.pluck(this.doc, keys);
};
Document.prototype.hasFields = function(keys) {
  return util.hasFields(this.doc, keys);
};

//TODO Forbid nested writes
// TODO Refactor with replace?
Document.prototype.update = function(newValue, options, query, internalOptions) {
  var self = this;
  var primaryKey = self.table.options.primaryKey;

  return new Promise(function(resolve, reject) {
    // Update context
    if (options.non_atomic !== true) {
      internalOptions = {deterministic: true};
    }

    if (util.isFunction(newValue)) {
      var varId = util.getVarId(newValue);
      query.context[varId] = self;
      query.evaluate(newValue, internalOptions).bind({}).then(function(updateValue) {
        util.assertType(updateValue, 'DATUM', query);
        delete query.context[varId];
        resolve(Document.handleUpdateValue(self, updateValue, primaryKey, options));
      }).catch(function(err) {
        if (err.message.match(/^Could not prove argument deterministic.  Maybe you want to use the non_atomic flag?/)) {
          reject(err);
        }
        var result = util.writeResult();
        result.errors++;
        if (err.message.match(/^Expected type DATUM but found/)) {
          result.first_error = err.message;
        }
        else {
          result.first_error = err.message+'.'; //WTF?
        }
        resolve(result);
      });
    }
    else {
      query.evaluate(newValue, internalOptions).bind({}).then(function(updateValue) {
        if (typeof updateValue === 'function') {
          updateValue = updateValue(util.toDatum(self));
          util.assertJavaScriptResult(updateValue, query);
          updateValue = util.revertDatum(updateValue);
        }
        util.assertType(updateValue, 'DATUM', self);
        resolve(Document.handleUpdateValue(self, updateValue, primaryKey, options));
      }).catch(function(err) {
        if (err.message.match(/^Could not prove argument deterministic.  Maybe you want to use the non_atomic flag?/)) {
          reject(err);
        }
        var result = util.writeResult();
        result.errors++;
        result.first_error = err.message+'.';
        resolve(result);
      });
    }
  });
};

Document.handleUpdateValue = function(self, updateValue, primaryKey, options) {
  var result = util.writeResult();
  if (updateValue === null) {
    updateValue = {};
  }
  if ((updateValue[primaryKey] !== undefined)
      && (self.doc[primaryKey] !== updateValue[primaryKey])) {
    result.errors++;
    result.first_error = "Primary key `id` cannot be changed(`"+
      JSON.stringify(util.toDatum(updateValue), null, 2)+"` -> `"+
      JSON.stringify(self.doc, null, 2)+"`)";
    if (options.return_changes === true) {
      result.changes = new Sequence();
    }
    if (options.return_changes === 'always') {
      var change = {
        new_val: util.deepCopy(self.doc),
        old_val: util.deepCopy(self.doc),
      }
      if (result.first_error) {
        change.error = result.first_error;
      }
      result.changes = new Sequence([change]);
    }
  }
  else {
    var old_val = util.deepCopy(self.doc);
    if (updateValue instanceof Document) {
      updateValue = updateValue.doc;
    }
    var changed = util._merge(self.doc, updateValue);
    if (options.return_changes === 'always') {
      result.changes = new Sequence([{
        new_val: util.deepCopy(self.doc),
        old_val: old_val
      }]);
    }
    if (changed === true) {
      result.replaced++;
      self.emit('change', {new_val: self.doc, old_val: old_val});
      if (options.return_changes === true) {
        result.changes = new Sequence([{
          new_val: util.deepCopy(self.doc),
          old_val: old_val
        }]);
      }
    }
    else {
      result.unchanged++;
      if (options.return_changes === true) {
        result.changes = new Sequence();
      }
      if (options.return_changes === 'always') {
        result.changes = new Sequence([{
          new_val: util.deepCopy(self.doc),
          old_val: util.deepCopy(self.doc),
        }]);
      }

    }
  }
  return result;
};

Document.prototype.merge = function(obj, query) {
  return util.merge(this.doc, obj, query);
};

Document.prototype.without = function(keys) {
  return util.without(this.doc, keys);
};

Document.prototype.replace = function(newValue, options, query, internalOptions) {
  //TODO Throw if we are missing the primary key
  var self = this;
  var primaryKey = self.table.options.primaryKey;

  return new Promise(function(resolve, reject) {
    // Update context
    if (options.non_atomic !== true) {
      internalOptions = {deterministic: true};
    }

    if (util.isFunction(newValue)) {
      var varId = util.getVarId(newValue);
      query.context[varId] = self;
      query.evaluate(newValue, internalOptions).bind({}).then(function(updateValue) {
        delete query.context[varId];
        resolve(Document.handleReplaceValue(self, updateValue, primaryKey, options));
      }).catch(function(err) {
        if (err.message.match(/^Could not prove argument deterministic.  Maybe you want to use the non_atomic flag?/)) {
          reject(err);
        }
        var result = util.writeResult();
        result.errors++;
        if (err.message.match(/^Expected type DATUM but found/)) {
          result.first_error = err.message;
        }
        else {
          result.first_error = err.message+'.'; //WTF?
        }
        resolve(result);
      });
    }
    else {
      query.evaluate(newValue, internalOptions).bind({}).then(function(updateValue) {
        if (typeof updateValue === 'function') {
          updateValue = updateValue(util.toDatum(self));
          util.assertJavaScriptResult(updateValue, query);
          updateValue = util.revertDatum(updateValue);
        }
        util.assertType(updateValue, 'DATUM', self);
        resolve(Document.handleReplaceValue(self, updateValue, primaryKey, options));
      }).catch(function(err) {
        if (err.message.match(/^Could not prove argument deterministic.  Maybe you want to use the non_atomic flag?/)) {
          reject(err);
        }
        var result = util.writeResult();
        result.errors++;
        result.first_error = err.message+'.';
        resolve(result);
      });
    }
  });
};

Document.handleReplaceValue = function(self, replaceValue, primaryKey, options) {
  var result = util.writeResult();
  if (replaceValue === null) {
    self.delete({});
    result.deleted++;
  }
  else if ((replaceValue[primaryKey] !== undefined)
      && (self.doc[primaryKey] !== replaceValue[primaryKey])) {
    result.errors++;
    result.first_error = "Primary key `id` cannot be changed(`"+
      JSON.stringify(util.toDatum(replaceValue), null, 2)+"` -> `"+
      JSON.stringify(self.doc, null, 2)+"`)";
    if (options.return_changes === true) {
      result.changes = new Sequence();
    }
    if (options.return_changes === 'always') {
      var change = {
        new_val: util.deepCopy(self.doc),
        old_val: util.deepCopy(self.doc),
      }
      if (result.first_error) {
        change.error = result.first_error;
      }
      result.changes = new Sequence([change]);
    }
  }
  else {
    var old_val = util.deepCopy(self.doc);
    if (replaceValue instanceof Document) {
      replaceValue = replaceValue.doc;
    }
    var changed = util._replace(self.doc, replaceValue);
    if (options.return_changes === 'always') {
      result.changes = new Sequence([{
        new_val: util.deepCopy(self.doc),
        old_val: old_val
      }]);
    }
    if (changed === true) {
      result.replaced++;
      self.emit('change', {new_val: self.doc, old_val: old_val});
      if (options.return_changes === true) {
        result.changes = new Sequence([{
          new_val: util.deepCopy(self.doc),
          old_val: old_val
        }]);
      }
    }
    else {
      result.unchanged++;
      if (options.return_changes === true) {
        result.changes = new Sequence();
      }
      if (options.return_changes === 'always') {
        result.changes = new Sequence([{
          new_val: util.deepCopy(self.doc),
          old_val: util.deepCopy(self.doc),
        }]);
      }
    }
  }
  return result;
};

Document.prototype.delete = function(options, query) {
  return this.table._delete(this, options);

};

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
};

Document.prototype.getField = function(field) {
  return this.doc[field];
};

Document.prototype.getBracket = function(field, query) {
  if (this.doc.hasOwnProperty(field)) {
    return this.doc[field];
  }
  throw new Error.ReqlRuntimeError("No attribute `"+field+"` in object:"+JSON.stringify(this.doc, null, 4), query.frames);
};

Document.prototype.keys = function() {
  var result = new Sequence();
  var keys = Object.keys(this.doc);
  for(var i=0; i<keys.length; i++) {
    result.push(keys[i]);
  }
  return result;
};
