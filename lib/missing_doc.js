function MissingDoc(primaryKeyValue, table) {
  this.primaryKeyValue = primaryKeyValue;
  this.table = table;
}
module.exports = MissingDoc;

MissingDoc.prototype.delete = function() {
  return {
    deleted: 0,
    errors: 0,
    inserted: 0,
    replaced: 0,
    skipped: 1,
    unchanged: 0 
  }
}

MissingDoc.prototype.replace = function() {
  return {
    deleted: 0,
    errors: 0,
    inserted: 0,
    replaced: 0,
    skipped: 1,
    unchanged: 0 
  }
}

MissingDoc.prototype.update = function() {
  return {
    deleted: 0,
    errors: 0,
    inserted: 0,
    replaced: 0,
    skipped: 1,
    unchanged: 0 
  }
}

MissingDoc.prototype.toDatum = function() {
  return null;
}

