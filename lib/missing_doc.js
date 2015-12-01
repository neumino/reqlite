function MissingDoc(primaryKeyValue, table) {
  this.primaryKeyValue = primaryKeyValue;
  this.table = table;
}
module.exports = MissingDoc;

var Sequence = require(__dirname+"/sequence.js");

MissingDoc.prototype.delete = function(options, query) {
  var result = {
    deleted: 0,
    errors: 0,
    inserted: 0,
    replaced: 0,
    skipped: 1,
    unchanged: 0
  };
  if (options.return_changes === true) {
    result.changes = new Sequence();
  }
  return result;
};

MissingDoc.prototype.replace = function(newValue, options, query, internalOptions) {
  var result = {
    deleted: 0,
    errors: 0,
    inserted: 0,
    replaced: 0,
    skipped: 1,
    unchanged: 0
  };
  if (options.return_changes === true) {
    result.changes = new Sequence();
  }
  return result;
};

MissingDoc.prototype.update = function(newValue, options, query, internalOptions) {
  var result = {
    deleted: 0,
    errors: 0,
    inserted: 0,
    replaced: 0,
    skipped: 1,
    unchanged: 0
  };
  if (options.return_changes === true) {
    result.changes = new Sequence();
  }
  return result;

};

MissingDoc.prototype.toDatum = function() {
  return null;
};

