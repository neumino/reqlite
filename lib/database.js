function Database(name) {
  this.name = name;
  this.tables = {};
  this.id = util.uuid();
}
module.exports = Database;

var Table = require(__dirname+"/table.js");
var util = require(__dirname+"/utils.js");

Database.prototype.table = function(name) {
  return this.tables[name];
};

Database.prototype.tableDrop = function(name) {
  delete this.tables[name];
};

Database.prototype.tableCreate = function(name, options) {
  var table = new Table(name, this.name, options);
  this.tables[name] = table;
  return {
    config_changes: [{
      new_val: {
        db: this.name,
        durability: "hard", //TODO Handle optarg
        write_acks: "majority", //TODO Handle optarg
        id: table.id,
        indexes: [],
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
  };

};

Database.prototype.typeOf = function() {
  return "DB";
};

