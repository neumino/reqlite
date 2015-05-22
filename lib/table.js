var _util = require('util');
var EventEmitter = require('events').EventEmitter;

//TODO Handle table_slice everywhere
function Table(name, db, options) {
  this.name = name;
  this.db = db;
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
  }
  this.id = util.uuid();
}
_util.inherits(Table, EventEmitter);

module.exports = Table;

var Selection = require(__dirname+"/selection.js");
var Sequence = require(__dirname+"/sequence.js");
var Document = require(__dirname+"/document.js");
var MissingDoc = require(__dirname+"/missing_doc.js");
var util = require(__dirname+"/utils/main.js");
var Error = require(__dirname+"/error.js");
var Group = require(__dirname+"/group.js");


// Import methods from Selection
var keys = Object.keys(Selection.prototype);
for(var i=0; i<keys.length; i++) {
  (function(key) {
    Table.prototype[key] = function() {
      var docs = [];
      for(var internalPk in this.documents) {
        docs.push(this.documents[internalPk]);
      }
      var selection = new Selection(docs, this, {});
      return selection[key].apply(selection, arguments);
    }
  })(keys[i]);
}

Table.prototype._saveOriginal = function(id, oldDoc) {
}

Table.prototype.typeOf = function() {
  return "TABLE";
}

Table.prototype.get = function(primaryKeyValue) {
  var pk = this.options.primaryKey;
  var internalPk = util.makeInternalPk(primaryKeyValue);
  if (this.documents[internalPk] === undefined) {
    return new MissingDoc(primaryKeyValue, this);
  }
  else {
    return this.documents[internalPk];
  }
}

// Single delete
Table.prototype._delete = function(doc, options, query) {
  var pk = this.options.primaryKey;
  var internalPk = util.makeInternalPk(doc.doc[pk]);
  var result;

  // The document does exist, if it doesn't, the selection is empty, or we are
  // deleting a missing doc
  var oldValue = this.documents[internalPk];
  delete this.documents[internalPk]
  this.emit('change', {
    new_val: null,
    old_val: oldValue.doc
  });
  result = {
    deleted: 1,
    errors: 0,
    inserted: 0,
    replaced: 0,
    skipped: 0,
    unchanged: 0 
  }
  if (options.return_changes === true) {
    result.changes = new Sequence([{
      new_val: null,
      old_val: util.deepCopy(doc.doc)
    }])
  }
  if (result.errors === 0) {
    this._saveOriginal(doc.doc[pk], oldValue.doc);
  }
  return result;
}

Table.prototype.insert = function(docs, options, query) {
  options = options || {};

  try {
    util.assertType(docs, ['OBJECT', 'ARRAY'], query);
  }
  catch(err) {
    typeValue = util.getType(docs);
    throw new Error.ReqlRuntimeError("Cannot convert "+typeValue+" to SEQUENCE", this.frames)
  }

  var result = util.writeResult();
  var generatedKeys = [];
  var newDoc;
  if (docs instanceof Sequence) {
    for(var i=0; i<docs.length; i++) {
      newResult = this._singleInsert(docs.get(i), options, query);
      util.mergeWriteResult(result, newResult);
    }
  }
  else {
    newResult = this._singleInsert(docs, options, query);
    util.mergeWriteResult(result, newResult);
  }
  return result;
}

Table.prototype._singleInsert = function(doc, options, query) {
  var self = this;
  var result = util.writeResult();
  doc = new Document(doc, this);
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
      if (this.documents[uuid] === undefined) {
        doc.doc[pk] = uuid;
        this.documents[util.makeInternalPk(uuid)] = doc;
        this.emit('change', {new_val: doc.doc, old_val: null})
        if (!util.isSequence(result.generated_keys)) {
          result.generated_keys = new Sequence();
        }
        result.generated_keys.push(uuid);
        result.inserted++;
      }
    }
  }
  else {
    // Can throw, `insert` will catch it
    var internalPk = util.makeInternalPk(doc.doc[pk]);

    if (this.documents[internalPk] === undefined) {
      this.documents[internalPk] = doc;
      this.emit('change', {new_val: doc.doc, old_val: null})
      result.inserted++;
      if (options.return_changes === true) {
        result.changes = new Sequence([{
          new_val: util.deepCopy(doc.doc),
          old_val: null
        }])
      }
    }
    else {
      if (options.conflict === 'replace') {
        var oldValue = util.deepCopy(this.documents[internalPk].doc)
        this.documents[internalPk] = doc;
        result.replaced++;
        if (options.return_changes === true) {
          result.changes = new Sequence([{
            new_val: util.deepCopy(doc.doc),
            old_val: oldValue
          }])
        }
        this.emit('change', {new_val: doc.doc, old_val: oldValue})
      }
      else if (options.conflict === 'update') {
        var oldValue = util.deepCopy(this.documents[internalPk].doc)
        var changed = util._merge(this.documents[internalPk].doc, doc.doc);
        if (changed === true) {
          result.replaced++;
          this.emit('change', {new_val: this.documents[internalPk].doc, old_val: oldValue})
          if (options.return_changes === true) {
            result.changes = new Sequence([{
              new_val: util.deepCopy(this.documents[internalPk].doc),
              old_val: oldValue
            }])
          }
        }

      }
      else {
        result.first_error = 'Duplicate primary key `'+pk+'`:'+JSON.stringify(this.documents[internalPk].toDatum(), null, 4)
        result.errors++;
        if (options.return_changes === true) {
          result.changes = new Sequence();
        }
      }
    }
  }
  if (result.errors === 0) {
    this._saveOriginal(doc.doc[pk], this.documents[internalPk].doc);
  }
  return result;
}
Table.prototype.getAll = function(args, options, query, internalOptions) {
  // This work only on a TABLE, not on a TABLE_SLICE
  //TODO Implement frames
  var selection = new Selection([], this, {});

  // If no secondary index is provided, we are dealing with the primary key
  if (!util.isPlainObject(options)) {
    options = {};
  }
  if (typeof options.index !== 'string') {
    options.index = this.options.primaryKey; 
  };

    
  var keys = Object.keys(this.documents);
  //TODO Throw if the index does not exist
  var varId = util.getVarId(this.indexes[options.index].fn)

  for(var i=0; i<keys.length; i++) { // Iterate on all the documents of the tablepay attention to what browserify does here
    query.context[varId] = this.documents[keys[i]].doc;

    if (this.indexes[options.index].multi === true) {
      var valuesIndex = undefined;
      try {
        valuesIndex = query.evaluate(this.indexes[options.index].fn, internalOptions);
      }
      catch(err) {
        if (err.message.match(/^No attribute `/) === null) {
          throw err;
        }
      }
      if (valuesIndex !== undefined) {
        for(var j=0; j<args.length; j++) {
          for(var k=0; k<valuesIndex.length; k++) {
            var valueIndex = valuesIndex.get(k);
            if (util.eq(util.toDatum(valueIndex), util.toDatum(args[j]))) {
              selection.push(this.documents[keys[i]]);
              break;
            }
          }
        }
      }
    }
    else {
      var valueIndex = undefined;
      try {
        valueIndex = query.evaluate(this.indexes[options.index].fn, query, internalOptions);
      }
      catch(err) {
        if (err.message.match(/^No attribute `/) === null) {
          throw err;
        }
      }


      if (valueIndex !== undefined) {
        for(var j=0; j<args.length; j++) {
          if (util.eq(util.toDatum(valueIndex), util.toDatum(args[j]))) {
            selection.push(this.documents[keys[i]]);
            break;
          }
        }
      }
    }
    delete query.context[varId];
  }

  return selection;
}

Table.prototype.between = function(left, right, options, query, internalOptions) {
  //TODO Implement frames

  // If no secondary index is provided, we are dealing with the primary key
  if ((util.isPlainObject(options) === false) || (typeof options.index !== 'string')) {
    options.index = this.options.primaryKey; 
  };

  // TODO Mimick the server's by returning a table hack to enable chaining between and orderBy
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

  var valueIndex, valuesIndex, keep;

  var keys = Object.keys(this.documents);

  for(var i=0; i<keys.length; i++) { // Iterate on all the documents of the tablepay attention to what browserify does here
    var varId = util.getVarId(this.indexes[options.index].fn)
    query.context[varId] = this.documents[keys[i]].doc;

    if (this.indexes[options.index].multi === true) {
      valuesIndex = query.evaluate(this.indexes[options.index].fn, query, internalOptions);

      for(var k=0; k<valuesIndex.length; k++) {
        valueIndex = valuesIndex[k];
        util.between(selection, this.documents[keys[i]], valueIndex, left, right, options);
      }
    }
    else {
      valueIndex = query.evaluate(this.indexes[options.index].fn, query, internalOptions);
      util.between(selection, this.documents[keys[i]], valueIndex, left, right, options);
    }
  }
  delete query.context[varId];
  
  return selection;
}


Table.prototype.indexCreate = function(name, fn, options, query) {
  if (this.indexes[name] != null) {
    throw new Error.ReqlRuntimeError("Index `"+name+"` already exists on table `"+this.db+"."+this.name+"`", query.frames)
  }

  if (fn === undefined) {
    fn = util.generateFieldFunction(name)
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
  }

}

Table.prototype.indexDrop = function(name, query) {
  if (this.indexes[name] == null) {
    throw new Error.ReqlRuntimeError("Index `"+name+"` does not exist on table `"+this.db+"."+this.name+"`", query.frames)
  }
  delete this.indexes[name];
  return {dropped: 1};
}

Table.prototype.indexWait = function() {
  var args = Array.prototype.slice.apply(arguments);
  var result = [];

  if (args.length === 0) {
    var indexes = Object.keys(this.indexes);
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
    for(var i=0; i<args.length; i++) {
      var index = args[i];
      if (this.indexes[index] == null) {
        throw new Error("Index `"+name+"` was not found on table `"+this.db+"."+this.table+"`")
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
}


// Table can use an index
Table.prototype.orderBy = function(fields, options, query, internalOptions) {
  var selection;
  var hasIndex = false;

  if (options.index !== undefined) {
    // Blame RethinkDB
    //query.frames.push('index');
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

    if (this.indexes[index] === undefined) {
      throw new Error.ReqlRuntimeError('Index `'+index+'` was not found on table `'+this.db+'.'+this.name+'`', query.frames)
    }
  }

  if ((typeof options.index === "string") && (fields.length === 0)) {
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

  if (options.index !== undefined) {
    hasIndex = true;
    for(var internalKey in this.documents) {
      // We drop null and plain objects
      try {
        var varId = util.getVarId(this.indexes[index].fn);
        query.context[varId] = this.documents[internalKey];
        var indexValue = query.evaluate(this.indexes[index].fn, query, internalOptions);
        delete query.context[varId];

        if ((indexValue !== null) || (util.isPlainObject(indexValue) && (indexValue.$reql_type$ === undefined))) {
          selection.push(this.documents[internalKey]);
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
    if (order === 'DESC') {
      fields.unshift([74, [this.indexes[index].fn], {}]);
    }
    else {
      fields.unshift(this.indexes[index].fn);
    }
  }
  else {
    for(var internalKey in this.documents) {
      selection.push(this.documents[internalKey]);
    }
  }
  selection._orderBy(fields, hasIndex, query);
  return selection;
}


Table.prototype.indexList = function() {
  // Remove the primary key
  var indexes = Object.keys(this.indexes);
  for(var i=0; i<indexes.length; i++) {
    if (indexes[i] === this.options.primaryKey) {
      indexes.splice(i, 1)
      break;
    }
  }
  return new Sequence(indexes, {});
}


Table.prototype.indexRename = function(oldIndex, newIndex, options, query) {
  if (this.indexes[oldIndex] === undefined) {
    query.frames.push(1);
    throw new Error.ReqlRuntimeError('Index `'+oldIndex+'` does not exist on table `'+this.db+'.'+this.name+'`', query.frames)
  }
  if (newIndex === oldIndex) {
    return {renamed: 0}
  }
  if ((this.indexes[newIndex] != null) && (options.overwrite !== true)) {
    query.frames.push(2);
    throw new Error.ReqlRuntimeError('Index `'+newIndex+'` already exists on table `'+this.db+'.'+this.name+'`', query.frames)
  }



  this.indexes[newIndex] = this.indexes[oldIndex];
  delete this.indexes[oldIndex];
  return {renamed: 1}
}

Table.prototype.group = function(fieldOrFns, options, query, internalOptions) {
  var groups = new Group();

  var fieldOrFn, group, subGroup;

  var keys = Object.keys(this.documents);
  for(var i=0; i<keys.length; i++) { // Iterate on all the documents of the tablepay attention to what browserify does here
    group = [];
    if (util.isPlainObject(options) && (typeof options.index === 'string')) {
      fieldOrFn = this.indexes[options.index].fn;

      var varId = util.getVarId(fieldOrFn);
      query.context[varId] = util.toDatum(this.documents[keys[i]]);
      try {
        subGroup = query.evaluate(fieldOrFn, internalOptions)
      }
      catch(err) {
        //TODO Catch all errors?
        continue; // RethinkDB skips these values...
      }
      group.push(subGroup);
      delete query.context[varId];
    }

    for(var j=0; j<fieldOrFns.length; j++) {
      fieldOrFn = fieldOrFns[j];

      if (typeof fieldOrFn === "string") {
        fieldOrFn = util.generateFieldFunction(fieldOrFn);
      }

      var varId = util.getVarId(fieldOrFn);
      query.context[varId] = util.toDatum(this.documents[keys[i]]);
      try {
        subGroup = query.evaluate(fieldOrFn, internalOptions)
      }
      catch(err) {
        //TODO Catch all errors?
        subGroup = null
      }
      group.push(subGroup);
      delete query.context[varId];
    }

    if (group.length === 1) {
      group = group[0];
    }
    groups.push(group, this.documents[keys[i]])
  }
  return groups;
}

Table.prototype.min = function(fieldOrFn, options, query) {
  if ((fieldOrFn !== undefined) && !util.isFunction(fieldOrFn)) {
    query.frames.push(1);
    util.assertType(fieldOrFn, "STRING", query);
    query.frames.pop();
    fieldOrFn = util.generateFieldFunction(fieldOrFn);
  }

  if (util.isPlainObject(options) && (typeof options.index === 'string')) {
    if (this.indexes[options.index] == null) {
      throw new Error.ReqlRuntimeError("Index `"+options.index+"` does not exist on table `"+this.db+"."+this.name+"`", this.frames)
    }
    fieldOrFn = this.indexes[options.index].fn;
    var result = this.toSequence().min(fieldOrFn, options, query);
    return this.get(result[this.options.primaryKey])
  }
  else {
    return this.toSequence().min(fieldOrFn, options, query);
  }
}

Table.prototype.max = function(fieldOrFn, options, query) {
  if ((fieldOrFn !== undefined) && !util.isFunction(fieldOrFn)) {
    query.frames.push(1);
    util.assertType(fieldOrFn, "STRING", query);
    query.frames.pop();
    fieldOrFn = util.generateFieldFunction(fieldOrFn);
  }

  if (util.isPlainObject(options) && (typeof options.index === 'string')) {
    if (this.indexes[options.index] == null) {
      throw new Error.ReqlRuntimeError("Index `"+options.index+"` does not exist on table `"+this.db+"."+this.name+"`", this.frames)
    }
    fieldOrFn = this.indexes[options.index].fn;
    var result = this.toSequence().max(fieldOrFn, options, query);
    return this.get(result[this.options.primaryKey])
  }
  else {
    return this.toSequence().max(fieldOrFn, options, query);
  }
}

Table.prototype.distinct = function(options, query, internalOptions) {
  var copy;

  if (util.isPlainObject(options) && (options.index !== undefined)) {
    query.frames.push('index');
    util.assertType(options.index, 'STRING', query);
    query.frames.pop();
    copy = new Sequence();
    if (this.indexes[options.index] == null) {
      throw new Error.ReqlRuntimeError("Index `"+options.index+"` was not found on table `"+this.db+"."+this.name+"`", query.frames)
    }

    fn = this.indexes[options.index].fn;
    var varId = util.getVarId(fn);

    var keys = Object.keys(this.documents);
    for(var i=0; i<keys.length; i++) { // Iterate on all the documents of the tablepay attention to what browserify does here
      query.context[varId] = this.documents[keys[i]];
      try{
        next = query.evaluate(fn, query, internalOptions);
        if (this.indexes[options.index].multi === true) {
          for(var j=0; j<next.sequence.length; j++) {
            copy.push(next.sequence[j]);
          }
        }
        else {
          copy.push(next);
        }
      }
      catch(err) {
        if (err.message.match(/^No attribute `/) === null) {
          throw err;
        }
        // else we just skip the non existence error
      }
    }
    delete query.context[varId];

  }
  else {
    copy = this.toSequence();
  }

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


Table.prototype.getIntersecting = function(geometry, options, query, internalOptions) {
  if (typeof options.index !== 'string') {
    //TODO
  }
  //TODO Test if geo index
  
  var keys = Object.keys(this.documents);
  var valueIndex, valuesIndex;
  var result = new Selection();

  var varId = util.getVarId(this.indexes[options.index].fn);
  for(var i=0; i<keys.length; i++) {
    query.context[varId] = this.documents[keys[i]].doc;

    valueIndex = query.evaluate(this.indexes[options.index].fn, query, internalOptions);

    if (valueIndex.intersects(geometry)) {
      result.push(this.documents[keys[i]])
    }
    delete query.context[varId];
  }
  return result;
}

Table.prototype.getNearest = function(geometry, options, query, internalOptions) {
  options = options || {};
  options.max_dist = options.max_dist || 100000 // 100km
  if (typeof options.index !== 'string') {
    //TODO
  }
  //TODO Test if geo index
  
  var keys = Object.keys(this.documents);
  var result = new Sequence();

  var varId = util.getVarId(this.indexes[options.index].fn);
  for(var i=0; i<keys.length; i++) {
    query.context[varId] = this.documents[keys[i]].doc;

    var loc = query.evaluate(this.indexes[options.index].fn, query, internalOptions);
    var distance = loc.distance(geometry, {unit: 'm'});

    // TODO Make the document immutable
    if (distance < options.max_dist) {
      result.push({
        doc: this.documents[keys[i]],
        dist: distance
      })
    }
    delete query.context[varId];
  }

  result.orderBy(['dist'], {}, query)
  return result;
}
