var _util = require('util');
var EventEmitter = require('events').EventEmitter;

//TODO Handle table_slice everywhere
function Table(name, db, options) {
  this.name = name;
  this.db = db; // the name of the database
  this.options = options || {};
  this.options.primaryKey = this.options.primary_key || "id";
  this.documents = {};
  this.indexes = {}; // String -> FUNC terms
  this.indexes[this.options.primaryKey] = {
    //TODO Make me use TERMS
    //fn: function(doc) { return doc[name] },
    // 69=FUNC, 2=MAKE_ARRAY, 31=GET_FIELD, 10=VAR, 1=ARGUMENT_INDEX
    //TODO Use a uuid to avoid collision
    fn: [ 69, [ [ 2, [ 1 ] ], [ 31, [ [ 10, [ 1 ] ], this.options.primaryKey ] ] ] ],
    function: { /* TODO */ },
    geo: false,
    outdated: false,
    ready: true,
    multi: false
  };
  this.id = util.uuid();
  if (options.meta === true) {
    // meta table
    this.meta = true;
  }
}
_util.inherits(Table, EventEmitter);

module.exports = Table;

var Selection = require(__dirname+"/selection.js");
var Sequence = require(__dirname+"/sequence.js");
var Document = require(__dirname+"/document.js");
var MissingDoc = require(__dirname+"/missing_doc.js");
var util = require(__dirname+"/utils.js");
var Error = require(__dirname+"/error.js");
var Promise = require("bluebird");

// Import methods from Selection
var _keys = Object.keys(Selection.prototype);
for(var i=0; i<_keys.length; i++) {
  (function(key) {
    Table.prototype[key] = function() {
      var docs = [];
      for(var internalPk in this.documents) {
        docs.push(this.documents[internalPk]);
      }
      var selection = new Selection(docs, this, {});
      return selection[key].apply(selection, arguments);
    };
  })(_keys[i]);
}

Table.prototype.typeOf = function() {
  return "TABLE";
};

Table.prototype.get = function(primaryKeyValue) {
  var internalPk = util.makeInternalPk(primaryKeyValue);
  if (this.documents[internalPk] === undefined) {
    return Promise.resolve(new MissingDoc(primaryKeyValue, this));
  }
  else {
    return Promise.resolve(this.documents[internalPk]);
  }
};

// Single delete
Table.prototype._delete = function(doc, options) {
  var pk = this.options.primaryKey;
  var internalPk = util.makeInternalPk(doc.doc[pk]);
  var result;

  // The document does exist, if it doesn't, the selection is empty, or we are
  // deleting a missing doc
  var oldValue = this.documents[internalPk];
  if (oldValue === undefined) {
    result = util.writeResult();
    result.skipped++;
    return result;
  }
  delete this.documents[internalPk];
  this.emit('change', {
    new_val: null,
    old_val: oldValue.doc
  });

  result = util.writeResult();
  result.deleted++;
  if (options.return_changes === true || options.return_changes === 'always') {
    result.changes = new Sequence([{
      new_val: null,
      old_val: util.deepCopy(doc.doc)
    }]);
  }
  return Promise.resolve(result);
};

Table.prototype.insert = function(docs, options, query) {
  options = options || {};

  try {
    util.assertType(docs, ['OBJECT', 'ARRAY'], query);
  }
  catch(err) {
    var typeValue = util.getType(docs);
    throw new Error.ReqlRuntimeError("Cannot convert "+typeValue+" to SEQUENCE", this.frames);
  }

  var result = util.writeResult();
  var newDoc;
  if (util.isSequence(docs)) {
    for(var i=0; i<docs.length; i++) {
      newDoc = this._singleInsert(docs.get(i), options);
      util.mergeWriteResult(result, newDoc);
    }
  }
  else {
    newDoc = this._singleInsert(docs, options);
    util.mergeWriteResult(result, newDoc);
  }
  return result;
};

Table.prototype._singleInsert = function(doc, options) {
  var self = this;
  var result = util.writeResult();
  doc = new Document(doc, this);

  if (self.meta === true && options.meta !== true) {
    result.errors++;
    result.first_error = "It's illegal to insert new rows into the `"+self.db+"."+self.name+"` table.";
    if (options.return_changes === true || options.return_changes === 'always') {
      result.changes = new Sequence();
    }
    if (doc.doc[pk] === undefined) { // RethinkDB seems to generate a key no matter what.
      result.generated_keys = new Sequence([util.uuid()]);
    }
    return result;
  }

  // Forward events
  doc.on('change', function(change) {
    self.emit('change', change);
  });

  var pk = this.options.primaryKey;

  if (doc.doc[pk] === undefined) {
    // Keep generating a uuid until one is free...
    var uuid;
    while (true) {
      uuid = util.uuid();
      internalPk = util.makeInternalPk(uuid);
      if (this.documents[internalPk] === undefined) {
        doc.doc[pk] = uuid;
        this.documents[internalPk] = doc;
        this.emit('change', {new_val: doc.doc, old_val: null});
        if (!util.isSequence(result.generated_keys)) {
          result.generated_keys = new Sequence();
        }
        result.generated_keys.push(uuid);
        result.inserted++;
        if (options.return_changes === true || options.return_changes === 'always') {
          result.changes = new Sequence([{
            new_val: util.deepCopy(doc.doc),
            old_val: null
          }]);
        }
        break;
      }
    }
  }
  else {
    // Can throw, `insert` will catch it
    try {
      var internalPk = util.makeInternalPk(doc.doc[pk]);
    }
    catch(err) {
      result.errors++;
      result.first_error = err.message;
      if (options.return_changes === true) {
        result.changes = new Sequence();
      }
      return result;
    }

    if (this.documents[internalPk] === undefined) {
      this.documents[internalPk] = doc;
      this.emit('change', {new_val: doc.doc, old_val: null});
      result.inserted++;
      if (options.return_changes === true || options.return_changes === 'always') {
        result.changes = new Sequence([{
          new_val: util.deepCopy(doc.doc),
          old_val: null
        }]);
      }
    }
    else {
      if (options.conflict === 'replace') {
        var oldValue = util.deepCopy(this.documents[internalPk].doc);
        this.documents[internalPk] = doc;
        result.replaced++;
        if (options.return_changes === true || options.return_changes === 'always') {
          result.changes = new Sequence([{
            new_val: util.deepCopy(doc.doc),
            old_val: oldValue
          }]);
        }
        this.emit('change', {new_val: doc.doc, old_val: oldValue});
      }
      else if (options.conflict === 'update') {
        var oldValue = util.deepCopy(this.documents[internalPk].doc);
        var changed = util._merge(this.documents[internalPk].doc, doc.doc);
        if (changed === true) {
          result.replaced++;
          this.emit('change', {new_val: this.documents[internalPk].doc, old_val: oldValue});
          if (options.return_changes === true || options.return_changes === 'always') {
            result.changes = new Sequence([{
              new_val: util.deepCopy(this.documents[internalPk].doc),
              old_val: oldValue
            }]);
          }
        }

      }
      else {
        result.first_error = 'Duplicate primary key `'+pk+'`:'+JSON.stringify(this.documents[internalPk].toDatum(), null, 4);
        result.errors++;
        if (options.return_changes === true) {
          result.changes = new Sequence();
        }
        if (options.return_changes === 'always') {
          var oldValue = util.deepCopy(this.documents[internalPk].doc);
          var change = {
            new_val: oldValue,
            old_val: oldValue
          };
          if (result.first_error) {
            change.error = 'Duplicate primary key `'+pk+'`:'+JSON.stringify(this.documents[internalPk].toDatum(), null, 4)
              +JSON.stringify(doc.toDatum(), null, 4);
          }
          result.changes = new Sequence([change]);
        }
      }
    }
  }
  return result;
};

Table.prototype.getAll = function(args, options, query, internalOptions) {
  // This work only on a TABLE, not on a TABLE_SLICE
  var self = this;
  //TODO Implement frames
  var selection = new Selection([], this, {});

  // If no secondary index is provided, we are dealing with the primary key
  if (!util.isPlainObject(options)) {
    options = {};
  }
  if (typeof options.index !== 'string') {
    options.index = self.options.primaryKey;
  }


  var keys = Object.keys(self.documents);
  var documents = [];
  for(var i=0; i<keys.length; i++) {
    documents.push(self.documents[keys[i]]);
  }
  //TODO Throw if the index does not exist
  var varId = util.getVarId(self.indexes[options.index].fn);
  return Promise.reduce(documents, function(result, doc) {
    query.context[varId] = doc;
    return query.evaluate(self.indexes[options.index].fn, internalOptions).then(function(valueIndex) {
      if (self.indexes[options.index].multi === true) {
        for(var k=0; k<valueIndex.length; k++) {
          for(var j=0; j<args.length; j++) {
            if (util.eq(valueIndex.get(k), args[j])) {
              result.push(doc);
              break;
            }
          }
        }
      }
      else {
        for(var j=0; j<args.length; j++) {
          if (util.eq(valueIndex, args[j])) {
            result.push(doc);
            break;
          }
        }
      }
      return result;
    }).catch(function(err) {
      //TODO Test this code path
      if (err.message.match(/^No attribute `/) === null) {
        throw err;
      }
      return result;
    }).finally(function() {
      delete query.context[varId];
    });
  }, selection);
};

Table.prototype.between = function(left, right, options, query, internalOptions) {
  var self = this;
  //TODO Implement frames
  // If no secondary index is provided, we are dealing with the primary key
  if ((util.isPlainObject(options) === false) || (typeof options.index !== 'string')) {
    options.index = this.options.primaryKey;
  }

  var selection = new Selection([], this, {
    type: 'TABLE_SLICE',
    operation: 'between',
    index: options.index,
    args: {
      left: left,
      right: right,
    },
    options: {
      left_bound: options.left_bound || 'closed',
      right_bound: options.right_bound || 'open'
    }
  });

  var keys = Object.keys(this.documents);
  var documents = [];
  for(var i=0; i<keys.length; i++) {
    documents.push(self.documents[keys[i]]);
  }
  //TODO Throw if the index does not exist
  var varId = util.getVarId(self.indexes[options.index].fn);
  return Promise.reduce(documents, function(result, doc) {
    query.context[varId] = doc;
    return query.evaluate(self.indexes[options.index].fn, internalOptions).then(function(valueIndex) {
      delete query.context[varId];
      if (self.indexes[options.index].multi === true) {
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


Table.prototype.indexCreate = function(name, fn, options, query) {
  if (this.indexes[name] != null) {
    throw new Error.ReqlRuntimeError("Index `"+name+"` already exists on table `"+this.db+"."+this.name+"`", query.frames);
  }

  if (fn === undefined) {
    fn = util.generateFieldFunction(name);
  }
  else {
    query.frames.push(2);
    util.assertType(fn, 'FUNCTION', this);
    query.frames.pop();

  }
  this.indexes[name] = {
    fn: fn,
    function: { /* TODO */ },
    geo: options.geo || false,
    multi: options.multi || false,
    outdated: false,
    ready: true
  };
  return {created: 1};
};

Table.prototype.indexDrop = function(name, query) {
  if (this.indexes[name] == null) {
    throw new Error.ReqlRuntimeError("Index `"+name+"` does not exist on table `"+this.db+"."+this.name+"`", query.frames);
  }
  delete this.indexes[name];
  return {dropped: 1};
};

Table.prototype.indexWait = function(indexes, query) {
  var result = new Sequence();

  if (indexes.length === 0) {
    indexes = Object.keys(this.indexes);
    for(var i=0; i<indexes.length; i++) {
      var index = indexes[i];
      if (index !== this.options.primaryKey) {
        result.push({
          index: index,
          function: this.indexes[index].function,
          geo: this.indexes[index].geo,
          multi: this.indexes[index].multi,
          outdated: this.indexes[index].outdated,
          ready: this.indexes[index].ready
        });
      }
    }
  }
  else {
    for(var i=0; i<indexes.length; i++) {
      var index = indexes[i];
      if (this.indexes[index] == null) {
        throw new Error.ReqlRuntimeError("Index `"+index+"` was not found on table `"+this.db+"."+this.table+"`", query.frames);
      }

      if (index !== this.options.primaryKey) {
        result.push({
          index: index,
          function: this.indexes[index].function,
          geo: this.indexes[index].geo,
          multi: this.indexes[index].multi,
          outdated: this.indexes[index].outdated,
          ready: this.indexes[index].ready
        });
      }
    }

  }
  return result;
};


// Table can use an index
Table.prototype.orderBy = function(fields, options, query, internalOptions) {
  var self = this;

  var hasIndex = false;
  if (options.index !== undefined) {
    hasIndex = true;
    // Blame RethinkDB
    //query.frames.push('index');
    var index = options.index;
    var order = 'ASC';
    if (util.isAsc(index)) {
      order = 'ASC';
      index = options.index.value;
    }
    else if (util.isDesc(index)) {
      order = 'DESC';
      index = options.index.value;
    }
    query.frames.push('index');
    util.assertType(index, 'STRING', query);
    query.frames.pop();

    if (this.indexes[index] === undefined) {
      return Promise.reject(new Error.ReqlRuntimeError('Index `'+index+'` was not found on table `'+this.db+'.'+this.name+'`', query.frames));
    }

    if (order === 'DESC') {
      fields.unshift([74, [this.indexes[index].fn], {}]);
    }
    else {
      fields.unshift(this.indexes[index].fn);
    }
  }

  var selection;
  // Check fields.length === 1 since we just added the index there
  if ((typeof options.index === "string") && (fields.length === 1)) {
    selection = new Selection([], this, {
      type: 'TABLE_SLICE',
      operation: 'orderBy',
      index: options.index,
      args: {
        order: order
      }
    });
  }
  else {
    selection = new Selection([], this, {});
  }

  var mainPromise;
  if (hasIndex === true) {
    var internalKeys = Object.keys(self.documents);
    var varId = util.getVarId(self.indexes[index].fn);
    mainPromise = Promise.map(internalKeys, function(internalKey) {
      query.context[varId] = self.documents[internalKey];
      return query.evaluate(self.indexes[index].fn, query, internalOptions).then(function(indexValue) {
        delete query.context[varId];
        if ((indexValue !== null) || (util.isPlainObject(indexValue) && (indexValue.$reql_type$ === undefined))) {
          selection.push(self.documents[internalKey]);
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
      return selection;
    });
  }
  else {
    for(var internalKey in this.documents) {
      selection.push(self.documents[internalKey]);
    }
    mainPromise = Promise.resolve(selection);
  }
  return mainPromise.then(function(selection) {
    return selection.orderBy(fields, {}, query, internalOptions);
  });
};


Table.prototype.indexList = function() {
  // Remove the primary key
  var indexes = Object.keys(this.indexes);
  for(var i=0; i<indexes.length; i++) {
    if (indexes[i] === this.options.primaryKey) {
      indexes.splice(i, 1);
      break;
    }
  }
  return new Sequence(indexes, {});
};


Table.prototype.indexRename = function(oldIndex, newIndex, options, query) {
  if (newIndex === oldIndex) {
    if (this.indexes[oldIndex] !== undefined) {
      return {renamed: 0};
    }
    throw new Error.ReqlRuntimeError('Index `'+oldIndex+'` does not exist or index `'+newIndex+'` already exists on table `'+this.db+'.'+this.name+'`', query.frames);
  }
  if ((this.indexes[newIndex] != null) && (options.overwrite !== true)) {
    throw new Error.ReqlRuntimeError('Index `'+oldIndex+'` does not exist or index `'+newIndex+'` already exists on table `'+this.db+'.'+this.name+'`', query.frames);
  }
  if (this.indexes[oldIndex] === undefined) {
    throw new Error.ReqlRuntimeError('Index `'+oldIndex+'` does not exist on table `'+this.db+'.'+this.name+'`', query.frames);
  }

  this.indexes[newIndex] = this.indexes[oldIndex];
  delete this.indexes[oldIndex];
  return {renamed: 1};
};

Table.prototype.min = function(fieldOrFn, options, query, internalOptions) {
  var self = this;
  if ((fieldOrFn !== undefined) && !util.isFunction(fieldOrFn)) {
    query.frames.push(1);
    util.assertType(fieldOrFn, "STRING", query);
    query.frames.pop();
    fieldOrFn = util.generateFieldFunction(fieldOrFn);
  }

  if (util.isPlainObject(options) && (typeof options.index === 'string')) {
    if (this.indexes[options.index] == null) {
      throw new Error.ReqlRuntimeError("Index `"+options.index+"` does not exist on table `"+this.db+"."+this.name+"`", this.frames);
    }
    fieldOrFn = this.indexes[options.index].fn;
    return this.toSequence().min(fieldOrFn, options, query).then(function(result) {
      return self.get(result[self.options.primaryKey]);
    });
  }
  else {
    return self.toSequence().min(fieldOrFn, options, query, internalOptions);
  }
};

Table.prototype.max = function(fieldOrFn, options, query, internalOptions) {
  var self = this;
  if ((fieldOrFn !== undefined) && !util.isFunction(fieldOrFn)) {
    query.frames.push(1);
    util.assertType(fieldOrFn, "STRING", query);
    query.frames.pop();
    fieldOrFn = util.generateFieldFunction(fieldOrFn);
  }

  if (util.isPlainObject(options) && (typeof options.index === 'string')) {
    if (this.indexes[options.index] == null) {
      throw new Error.ReqlRuntimeError("Index `"+options.index+"` does not exist on table `"+this.db+"."+this.name+"`", this.frames);
    }
    fieldOrFn = this.indexes[options.index].fn;
    return this.toSequence().max(fieldOrFn, options, query).then(function(result) {
      return self.get(result[self.options.primaryKey]);
    });
  }
  else {
    return self.toSequence().max(fieldOrFn, options, query, internalOptions);
  }
};

Table.prototype.distinct = function(options, query, internalOptions) {
  var self = this;
  if (util.isPlainObject(options) && (options.index !== undefined)) {
    query.frames.push('index');
    util.assertType(options.index, 'STRING', query);
    query.frames.pop();
    if (this.indexes[options.index] == null) {
      throw new Error.ReqlRuntimeError("Index `"+options.index+"` was not found on table `"+this.db+"."+this.name+"`", query.frames);
    }
    var fn = this.indexes[options.index].fn;
    var varId = util.getVarId(fn);

    var keys = Object.keys(this.documents);
    var documents = []; // Keep a copy of the documents as we asynchronously iterate
    for(var i=0; i<keys.length; i++) {
      documents.push(self.documents[keys[i]]);
    }

    var mapped = new Sequence();
    return Promise.map(documents, function(doc) {
      query.context[varId] = doc;
      return query.evaluate(fn, query, internalOptions).then(function(next) {
        delete query.context[varId];
        if (self.indexes[options.index].multi === true) {
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
    return Promise.resolve(this.toSequence());
  }
};


Table.prototype.getIntersecting = function(geometry, options, query, internalOptions) {
  var self = this;
  if (typeof options.index !== 'string') {
    //TODO
  }
  //TODO Test if geo index
  var keys = Object.keys(self.documents);
  var documents = [];
  for(var i=0; i<keys.length; i++) {
    documents.push(self.documents[keys[i]]);
  }

  var varId = util.getVarId(self.indexes[options.index].fn);
  return Promise.reduce(documents, function(result, doc) {
    query.context[varId] = doc;
    return query.evaluate(self.indexes[options.index].fn, query, internalOptions).then(function(valueIndex) {
      delete query.context[varId];
      if (valueIndex.intersects(geometry)) {
        result.push(doc);
      }
      return result;
    });
  }, new Selection());
};

Table.prototype.getNearest = function(geometry, options, query, internalOptions) {
  var self = this;
  options = options || {};
  options.max_dist = options.max_dist || 100000; // 100km
  if (typeof options.index !== 'string') {
    //TODO
  }
  //TODO Test if geo index

  var keys = Object.keys(self.documents);
  var documents = [];
  for(var i=0; i<keys.length; i++) {
    documents.push(self.documents[keys[i]]);
  }

  var varId = util.getVarId(self.indexes[options.index].fn);
  return Promise.reduce(documents, function(result, doc) {
    query.context[varId] = doc;
    return query.evaluate(self.indexes[options.index].fn, query, internalOptions).then(function(loc) {
      delete query.context[varId];
      var distance = loc.distance(geometry, {unit: 'm'});
      // TODO Make the document immutable
      if (distance < options.max_dist) {
        result.push({
          doc: doc.doc,
          dist: distance
        });
      }
      return result;
    });
  }, new Sequence()).then(function(result) {
    result.orderBy(['dist'], {}, query);
    return result;
  });
};
