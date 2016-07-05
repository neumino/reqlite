// The main logic goes here parsing/executing queries

var protodef = require(__dirname+"/protodef.js");
var termTypes = protodef.Term.TermType;
var util = require(__dirname+"/utils.js");
var Promise = require("bluebird");
var request = require('request');

var Database = require(__dirname+"/database.js");
var Group = require(__dirname+"/group.js");
var Sequence = require(__dirname+"/sequence.js");
var Error = require(__dirname+"/error.js");

var Document = require(__dirname+"/document.js");
var Changes = require(__dirname+"/changes.js");

var Minval = require(__dirname+"/minval.js");
var Maxval = require(__dirname+"/maxval.js");
var Asc = require(__dirname+"/asc.js");
var Desc = require(__dirname+"/desc.js");
var Literal = require(__dirname+"/literal.js");
var ReqlDate = require(__dirname+"/date.js");
var ReqlGeometry = require(__dirname+"/geo.js");

var constants = require("./constants.js");

//TODO Make sure that we test for NaN, Infinity when running a JS function
// Keep things in a function in case we somehow decide to implement lazy cursor

//TODO Check more options like in insert

//TODO Populate clustering config

//TODO Replace "= []" with "= new Sequence"

//TODO Revert datum after JS function

//TODO Check for "Cannot perform bracket on..." errors too (like with "No attribute..."

//TODO Ensure that args is an array before trying to access an element

//TODO Before calling util.cannotPerformOp, check for MissingDoc
function Query(server, query, options, token) {
  this.server = server;
  this.query = query;
  // Used to track the presence of r.args (workaround)
  this.originalQuery = util.deepCopy(query);
  this.options = {};
  this.context = {};
  this.frames = [];
  this.nowValue = ReqlDate.now();
  this.complete = false;
  this.token = token;

}
//TODO Make a deep copy of the results for the local browser?

// Reqlite doesn't evaluate things in a lazy way. The only thing that can be lazily
// returned is actually an infinite range.
// Return a promise
Query.prototype.continue = function(query) {
  var self = this;
  var response = new Promise(function(resolve, reject) {
    var toReturn;
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
  });
  return response;
};
//TODO Implement query.stop

//TODO do we actually need toDatum?

Query.prototype.stop = function() {
  if (util.isChanges(this.result)) {
    this.result.stop();
  }
};
//TODO Make sure there are no stray arrays going around
Query.prototype.run = function(query) {
  var self = this;
  query = query || self.query;

  //TODO Assert that query[0] is START?
  try {
    self.options = query[2];
    var queryResult = self.evaluate(query[1], {});
    var toReturn;
    return Promise.resolve(queryResult).then(function(result) {
      // TODO Check more types
      if (result instanceof Database) {
        throw new Error.ReqlRuntimeError("Query result must be of type DATUM, GROUPED_DATA, or STREAM (got DATABASE)", query.frames);
      }
      //TODO switch to result.stream === true instead of infiniteRange
      if ((result instanceof Sequence) && (result.infiniteRange === true)) {
        self.result = result;
        self.complete = false;
        toReturn = new Sequence();
        for(var i=0; i<constants.ROW_PER_BATCH; i++) {
          toReturn.push(result.get());
        }
        toReturn = util.toDatum(toReturn, self);
      }
      else if (util.isChanges(result)) {
        self.result = result;
        return self.result.getInitialResponse(self).then(function(toReturn) {
          self.result.startListener();
          toReturn = util.toDatum(toReturn);
          self.complete = self.result.complete;

          var response;
          if (self.complete === true) {
            response = {
              t: protodef.Response.ResponseType.SUCCESS_ATOM,
              r: toReturn
            };
          }
          else {
            response = {
              t: protodef.Response.ResponseType.SUCCESS_PARTIAL,
              r: toReturn,
              n: result.notes
            };
          }
          return response;
        });
        //TODO Pass notes
      }
      else {
        self.complete = true;
        toReturn = util.toDatum(result, self);
        toReturn = [toReturn]; // Why do we need that?
      }

      var response;
      if (self.complete === true) {
        response = {
          t: protodef.Response.ResponseType.SUCCESS_ATOM,
          r: toReturn
        };
      }
      else {
        response = {
          t: protodef.Response.ResponseType.SUCCESS_PARTIAL,
          r: toReturn,
          n: []
        };
      }
      return response;
    }).catch(function(err) {
      return {
        t: err.type,
        r: [err.message],
        b: err.frames || [],
        debug: err.stack
      };
    });
  }
  catch(err) {
    return {
      t: err.type,
      r: [err.message],
      b: err.frames || [],
      debug: err.stack
    };
  }
};

Query.prototype.evaluate = function(term, internalOptions) {
  internalOptions = internalOptions || {};
  var self = this;
  // Check if one of the arguments is `r.args`, in which case we reinject the
  // arguments in term
  if (util.isRawArgs(term)) {
    util.assertArity(1, term[1], self);
    term = term[1][0];
  }
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
          throw new Error.ReqlRuntimeError('Expected 1 argument but found 0', self.frames);
        }
        else {
          // This is kind of weird, we should use ArityRange if RethinkDB didn't sent the wrong error
          this.frames.push(i);
          util.assertArity(1, term[1][i][1], this);
          this.frames.pop();
          if (term[1][i][1][0][0] === termTypes.MAKE_ARRAY) {
            if (term[1][i][1].length > 1) {
              this.frames.push(0);
              util.arityError(1, 2, this, term);
              // Not poping as util.arityError is throwing an error.
            }
            term[1].splice.apply(term[1], [i, 1].concat(term[1][i][1][0][1]));
            i--;
          }
          else {
            self.frames.push(0);
            return self.evaluate(term[1][i][1][0], internalOptions).then(function(value) {
              // This is going to throw
              util.assertType(value, 'ARRAY', self);
              term[1][i][1][0] = value;
              term[1].splice.apply(term[1], [i, 1].concat(value.toDatum()));
              return self.evaluate(term, internalOptions);
            });
          }
        }
      }
    }
  }

  if ((Array.isArray(term) === false) && (util.isPlainObject(term) === false)) {
    // Primtiive
    return Promise.resolve(term);
  }
  else if (util.isPlainObject(term)) {
    // r.args could produce non plain object (like a sequence).
    // In this case we just return it.
    if (term.constructor !== Object) {
      return Promise.resolve(term);
    }
    // Plain object
    var keys = Object.keys(term);
    return Promise.reduce(keys, function(result, key) {
      self.frames.push(key);
      return self.evaluate(term[key], internalOptions).then(function(value) {
        self.frames.pop();
        result[key] = value;
        return result;
      });
    }, {});
  }

  var termType = term[0];
  //TODO Check for a promise? Move this evaluation in each code?
  //var options = this.evaluate(term[2], internalOptions) || {};
  var options = term[2] || {};

  switch(termType) {
    case termTypes.MAKE_ARRAY: // 2
      return self.makeArray(term[1], options, internalOptions);
    case termTypes.MAKE_OBJ:
      return self.makeObject(term[1], options, internalOptions);
    case termTypes.VAR: // 10
      return self.varId(term[1], options, internalOptions);
    case termTypes.JAVASCRIPT:
      return self.javascript(term[1], options, internalOptions);
    case termTypes.UUID:
      return self.uuid(term[1], options, internalOptions);
    case termTypes.HTTP:
      return self.http(term[1], options, internalOptions);
    case termTypes.ERROR:
      return self.error(term[1], options, internalOptions);
    case termTypes.IMPLICIT_VAR: // 13
      return self.implicitVar(term[1], options, internalOptions);
    case termTypes.DB: // 14
      return self.db(term[1], options, internalOptions);
    case termTypes.TABLE: // 15
      return self.table(term[1], options, internalOptions);
    case termTypes.GET:
      return self.get(term[1], options, internalOptions);
    case termTypes.GET_ALL:
      return self.getAll(term[1], options, internalOptions);
    case termTypes.EQ:
      return self.eq(term[1], options, internalOptions);
    case termTypes.NE:
      return self.ne(term[1], options, internalOptions);
    case termTypes.LT:
      return self.comparator('lt', term[1], options, internalOptions);
    case termTypes.LE:
      return self.comparator('le', term[1], options, internalOptions);
    case termTypes.GT:
      return self.comparator('gt', term[1], options, internalOptions);
    case termTypes.GE:
      return self.comparator('ge', term[1], options, internalOptions);
    case termTypes.NOT:
      return self.not(term[1], options, internalOptions);
    case termTypes.ADD:
      return self.add(term[1], options, internalOptions);
    case termTypes.SUB:
      return self.sub(term[1], options, internalOptions);
    case termTypes.MUL:
      return self.mul(term[1], options, internalOptions);
    case termTypes.DIV:
      return self.div(term[1], options, internalOptions);
    case termTypes.MOD:
      return self.mod(term[1], options, internalOptions);
    case termTypes.FLOOR:
      return self.floor(term[1], options, internalOptions);
    case termTypes.CEIL:
      return self.ceil(term[1], options, internalOptions);
    case termTypes.ROUND:
      return self.round(term[1], options, internalOptions);
    case termTypes.APPEND:
      return self.append(term[1], options, internalOptions);
    case termTypes.PREPEND:
      return self.prepend(term[1], options, internalOptions);
    case termTypes.DIFFERENCE:
      return self.difference(term[1], options, internalOptions);
    case termTypes.SET_INSERT:
      return self.setInsert(term[1], options, internalOptions);
    case termTypes.SET_INTERSECTION:
      return self.setIntersection(term[1], options, internalOptions);
    case termTypes.SET_UNION:
      return self.setUnion(term[1], options, internalOptions);
    case termTypes.SET_DIFFERENCE:
      return self.setDifference(term[1], options, internalOptions);
    case termTypes.SLICE:
      return self.slice(term[1], options, internalOptions);
    case termTypes.SKIP:
      return self.skip(term[1], options, internalOptions);
    case termTypes.LIMIT:
      return self.limit(term[1], options, internalOptions);
    case termTypes.OFFSETS_OF:
      return self.offsetsOf(term[1], options, internalOptions);
    case termTypes.CONTAINS:
      return self.contains(term[1], options, internalOptions);
    case termTypes.GET_FIELD:
      return self.getField(term[1], options, internalOptions);
    case termTypes.KEYS:
      return self.keys(term[1], options, internalOptions);
    case termTypes.VALUES:
      return self.values(term[1], options, internalOptions);
    case termTypes.OBJECT:
      return self.object(term[1], options, internalOptions);
    case termTypes.HAS_FIELDS:
      return self.hasFields(term[1], options, internalOptions);
    case termTypes.WITH_FIELDS:
      return self.withFields(term[1], options, internalOptions);
    case termTypes.PLUCK:
      return self.pluck(term[1], options, internalOptions);
    case termTypes.WITHOUT:
      return self.without(term[1], options, internalOptions);
    case termTypes.MERGE:
      return self.merge(term[1], options, internalOptions);
    case termTypes.BETWEEN:
      return self.between(term[1], options, internalOptions);
    case termTypes.MINVAL:
      return self.minval(term[1], options, internalOptions);
    case termTypes.MAXVAL:
      return self.maxval(term[1], options, internalOptions);
    case termTypes.FOLD:
      return self.fold(term[1], options, internalOptions);
    case termTypes.REDUCE:
      return self.reduce(term[1], options, internalOptions);
    case termTypes.MAP:
      return self.map(term[1], options, internalOptions);
    case termTypes.FILTER:
      return self.filter(term[1], options, internalOptions);
    case termTypes.CONCAT_MAP: // 40
      return self.concatMap(term[1], options, internalOptions);
    case termTypes.ORDER_BY:
      return self.orderBy(term[1], options, internalOptions);
    case termTypes.DISTINCT:
      return self.distinct(term[1], options, internalOptions);
    case termTypes.COUNT:
      return self.count(term[1], options, internalOptions);
    case termTypes.IS_EMPTY:
      return self.isEmpty(term[1], options, internalOptions);
    case termTypes.UNION:
      return self.union(term[1], options, internalOptions);
    case termTypes.NTH:
      return self.nth(term[1], options, internalOptions);
    case termTypes.BRACKET:
      return self.bracket(term[1], options, internalOptions);
    case termTypes.INNER_JOIN:
      return self.join('inner', term[1], options, internalOptions);
    case termTypes.OUTER_JOIN:
      return self.join('outer', term[1], options, internalOptions);
    case termTypes.EQ_JOIN:
      return self.eqJoin(term[1], options, internalOptions);
    case termTypes.ZIP:
      return self.zip(term[1], options, internalOptions);
    case termTypes.RANGE:
      return self.range(term[1], options, internalOptions);
    case termTypes.INSERT_AT:
      return self.insertAt(term[1], options, internalOptions);
    case termTypes.DELETE_AT:
      return self.deleteAt(term[1], options, internalOptions);
    case termTypes.CHANGE_AT:
      return self.changeAt(term[1], options, internalOptions);
    case termTypes.SPLICE_AT:
      return self.spliceAt(term[1], options, internalOptions);
    case termTypes.COERCE_TO:
      return self.coerceTo(term[1], options, internalOptions);
    case termTypes.TYPE_OF:
      return self.typeOf(term[1], options, internalOptions);
    case termTypes.UPDATE:
      return self.update(term[1], options, internalOptions);
    case termTypes.DELETE:
      return self.delete(term[1], options, internalOptions);
    case termTypes.REPLACE:
      return self.replace(term[1], options, internalOptions);
    case termTypes.INSERT:
      return self.insert(term[1], options, internalOptions);
    case termTypes.DB_CREATE:
      return self.dbCreate(term[1], options, internalOptions);
    case termTypes.DB_DROP:
      return self.dbDrop(term[1], options, internalOptions);
    case termTypes.DB_LIST:
      return self.dbList(term[1], options, internalOptions);
    case termTypes.TABLE_CREATE:
      return self.tableCreate(term[1], options, internalOptions);
    case termTypes.TABLE_DROP:
      return self.tableDrop(term[1], options, internalOptions);
    case termTypes.TABLE_LIST:
      return self.tableList(term[1], options, internalOptions);
    case termTypes.SYNC:
      return self.sync(term[1], options, internalOptions);
    case termTypes.GRANT:
      return self.grant(term[1], options, internalOptions);
    case termTypes.INDEX_CREATE:
      return self.indexCreate(term[1], options, internalOptions);
    case termTypes.INDEX_DROP:
      return self.indexDrop(term[1], options, internalOptions);
    case termTypes.INDEX_LIST:
      return self.indexList(term[1], options, internalOptions);
    case termTypes.INDEX_RENAME:
      return self.indexRename(term[1], options, internalOptions);
    case termTypes.INDEX_STATUS:
      return self.indexStatus(term[1], options, internalOptions);
    case termTypes.INDEX_WAIT:
      return self.indexWait(term[1], options, internalOptions);
    case termTypes.FUNCALL:
      return self.funcall(term[1], options, internalOptions);
    case termTypes.BRANCH:
      return self.branch(term[1], options, internalOptions);
    case termTypes.OR:
      return self.or(term[1], options, internalOptions);
    case termTypes.AND:
      return self.and(term[1], options, internalOptions);
    case termTypes.FOR_EACH:
      return self.forEach(term[1], options, internalOptions);
    case termTypes.FUNC: // 69
      return self.func(term[1], options, internalOptions);
    case termTypes.ASC:
      return self.asc(term[1], options, internalOptions);
    case termTypes.DESC:
      return self.desc(term[1], options, internalOptions);
    case termTypes.INFO:
      return self.info(term[1], options, internalOptions);
    case termTypes.MATCH:
      return self.match(term[1], options, internalOptions);
    case termTypes.UPCASE:
      return self.upcase(term[1], options, internalOptions);
    case termTypes.DOWNCASE:
      return self.downcase(term[1], options, internalOptions);
    case termTypes.SAMPLE:
      return self.sample(term[1], options, internalOptions);
    case termTypes.DEFAULT:
      return self.default(term[1], options, internalOptions);
    case termTypes.JSON:
      return self.json(term[1], options, internalOptions);
    case termTypes.TO_JSON_STRING:
      return self.toJsonString(term[1], options, internalOptions);
    case termTypes.ISO8601: // 99
      return self.ISO8601(term[1], options, internalOptions);
    case termTypes.TO_ISO8601: // 99
      return self.toISO8601(term[1], options, internalOptions);
    case termTypes.EPOCH_TIME:
      return self.epochTime(term[1], options, internalOptions);
    case termTypes.TO_EPOCH_TIME:
      return self.toEpochTime(term[1], options, internalOptions);
    case termTypes.NOW: // 103
      return self.now(term[1], options, internalOptions);
    case termTypes.IN_TIMEZONE:
      return self.inTimezone(term[1], options, internalOptions);
    case termTypes.DURING:
      return self.during(term[1], options, internalOptions);
    case termTypes.DATE:
      return self.date(term[1], options, internalOptions);
    case termTypes.TIME_OF_DAY:
      return self.timeOfDay(term[1], options, internalOptions);
    case termTypes.TIMEZONE:
      return self.timezone(term[1], options, internalOptions);
    case termTypes.YEAR:
      return self.year(term[1], options, internalOptions);
    case termTypes.MONTH:
      return self.month(term[1], options, internalOptions);
    case termTypes.DAY:
      return self.day(term[1], options, internalOptions);
    case termTypes.DAY_OF_WEEK:
      return self.dayOfWeek(term[1], options, internalOptions);
    case termTypes.DAY_OF_YEAR:
      return self.dayOfYear(term[1], options, internalOptions);
    case termTypes.HOURS:
      return self.hours(term[1], options, internalOptions);
    case termTypes.MINUTES:
      return self.minutes(term[1], options, internalOptions);
    case termTypes.SECONDS:
      return self.seconds(term[1], options, internalOptions);
    case termTypes.TIME: // 136
      return self.time(term[1], options, internalOptions);
    case termTypes.MONDAY:
      return self.monday(term[1], options, internalOptions);
    case termTypes.TUESDAY:
      return self.tuesday(term[1], options, internalOptions);
    case termTypes.WEDNESDAY:
      return self.wednesday(term[1], options, internalOptions);
    case termTypes.THURSDAY:
      return self.thursday(term[1], options, internalOptions);
    case termTypes.FRIDAY:
      return self.friday(term[1], options, internalOptions);
    case termTypes.SATURDAY:
      return self.saturday(term[1], options, internalOptions);
    case termTypes.SUNDAY:
      return self.sunday(term[1], options, internalOptions);
    case termTypes.JANUARY:
      return self.january(term[1], options, internalOptions);
    case termTypes.FEBRUARY:
      return self.february(term[1], options, internalOptions);
    case termTypes.MARCH:
      return self.march(term[1], options, internalOptions);
    case termTypes.APRIL:
      return self.april(term[1], options, internalOptions);
    case termTypes.MAY:
      return self.may(term[1], options, internalOptions);
    case termTypes.JUNE:
      return self.june(term[1], options, internalOptions);
    case termTypes.JULY:
      return self.july(term[1], options, internalOptions);
    case termTypes.AUGUST:
      return self.august(term[1], options, internalOptions);
    case termTypes.SEPTEMBER:
      return self.september(term[1], options, internalOptions);
    case termTypes.OCTOBER:
      return self.october(term[1], options, internalOptions);
    case termTypes.NOVEMBER:
      return self.november(term[1], options, internalOptions);
    case termTypes.DECEMBER:
      return self.december(term[1], options, internalOptions);
    case termTypes.LITERAL:
      return self.literal(term[1], options, internalOptions);
    case termTypes.GROUP:
      return self.group(term[1], options, internalOptions);
    case termTypes.SUM:
      return self.sum(term[1], options, internalOptions);
    case termTypes.AVG:
      return self.avg(term[1], options, internalOptions);
    case termTypes.MIN:
      return self.min(term[1], options, internalOptions);
    case termTypes.MAX:
      return self.max(term[1], options, internalOptions);
    case termTypes.SPLIT:
      return self.split(term[1], options, internalOptions);
    case termTypes.UNGROUP:
      return self.ungroup(term[1], options, internalOptions);
    case termTypes.RANDOM:
      return self.random(term[1], options, internalOptions);
    case termTypes.CHANGES:
      return self.changes(term[1], options, internalOptions);
    case termTypes.ARGS:
      // Throws
      return util.isBuggy(this);
    case termTypes.POINT:
      return self.point(term[1], options, internalOptions);
    case termTypes.CIRCLE:
      return self.circle(term[1], options, internalOptions);
    case termTypes.LINE:
      return self.line(term[1], options, internalOptions);
    case termTypes.FILL:
      return self.fill(term[1], options, internalOptions);
    case termTypes.GEOJSON:
      return self.geojson(term[1], options, internalOptions);
    case termTypes.TO_GEOJSON:
      return self.toGeojson(term[1], options, internalOptions);
    case termTypes.GET_INTERSECTING:
      return self.getIntersecting(term[1], options, internalOptions);
    case termTypes.GET_NEAREST:
      return self.getNearest(term[1], options, internalOptions);
    case termTypes.INCLUDES:
      return self.includes(term[1], options, internalOptions);
    case termTypes.INTERSECTS:
      return self.intersects(term[1], options, internalOptions);
    case termTypes.POLYGON:
      return self.polygon(term[1], options, internalOptions);
    case termTypes.POLYGON_SUB:
      return self.polygonSub(term[1], options, internalOptions);
    case termTypes.DISTANCE:
      return self.distance(term[1], options, internalOptions);
    case termTypes.CONFIG:
      // Throws
      return util.notAvailable(null, this);
    case termTypes.RECONFIGURE:
      // Throws
      return util.notAvailable(null, this);
    case termTypes.REBALANCE:
      // Throws
      return util.notAvailable(null, this);
    default:
      throw new Ereor.ReqlRuntimeError("Unknown term", this.frames);
  }
};

Query.prototype.isComplete = function() {
  return this.complete;
};

// All the functions below should never be given a promise.
Query.prototype.makeArray = function(args, options, internalOptions) {
  var self = this;
  return Promise.map(args, function(arg, index) {
    self.frames.push(index);
    return self.evaluate(arg, internalOptions).then(function(result) {
      util.assertType(result, 'DATUM', self);
      self.frames.pop();
      return result;
    });
  }, {concurrency: 1}).then(function(array) {
    return new Sequence(array);
  });
};

Query.prototype.makeObject = function(args, options, internalOptions) {
  // This is deprecated code, the current driver cannot reach this code.
  // options are already evaluated
  var self = this;
  util.assertArity(0, args, self);
  return self.evaluate(options, internalOptions).then(function(options) {
    return options;
  });
};


Query.prototype.javascript = function(args, options, internalOptions) {
  var self = this;
  if (internalOptions.deterministic === true) {
    util.nonDeterministicOp(self);
  }
  util.assertArity(1, args, self);
  if (constants.ENABLE_JS === true) {
    // This is not safe, we CANNOT contain eval
    var result;
    // Move this in its own file?
    function evalInContext(term) {
      result = eval(term);
      return result;
    }
    return self.evaluate(args[0], internalOptions).then(function(value) {
      var result = evalInContext.call({}, value);
      util.assertJavaScriptResult(result, self);
      return result;
    });
  }
  else {
    return Promise.reject(new Error.ReqlRuntimeError("`r.js was disabled. Update ENABLE_JS in `constants.js` if you want to enable `r.js`", this.frames));
  }
  return Promise.resolve(result);

};

Query.prototype.uuid = function(args, options, internalOptions) {
  util.assertArityRange(0, 1, args, this);
  if (args.length === 1) {
    return this.evaluate(args[0], internalOptions).then(function(name) {
      return util.uuidV5(constants.UUID_NAMESPACE, name);
    });
  }
  return Promise.resolve(util.uuid());
};

Query.prototype.varId = function(args, options, internalOptions) {
  var self = this;
  util.assertArity(1, args, self);
  self.frames.push(0);
  return self.evaluate(args[0], internalOptions).then(function(varId) {
    self.frames.pop();
    if (self.context[varId] === undefined) {
      throw new Error.ReqlRuntimeError("The server is buggy, context not found");
    }
    return self.context[varId];

  });

};

Query.prototype.error = function(args, options, internalOptions) {
  var self = this;
  //TODO Check that it's inside a default block
  util.assertArityRange(0, 1, args, self);
  self.frames.push(0);
  if (args.length === 0) {
    //TODO Add test
    throw new Error.ReqlRuntimeError('Query.prototype.error!', self.frames);
  }

  return self.evaluate(args[0], internalOptions).then(function(message) {
    util.assertType(message, 'STRING', self);
    self.frames.pop();
    throw new Error.ReqlRuntimeError(message, self.frames);
  });
};

Query.prototype.implicitVar = function(args, options, internalOptions) {
  var self = this;
  util.assertArity(0, args, self);
  if (Object.keys(self.context).length > 1) {
    // This should happen only if there is a bug in the driver or in the server
    //throw new Error.ReqlRuntimeError("Ambiguous implicit var");
    throw new Error.ReqlRuntimeError("Ambiguous implicit var:"+Object.keys(self.context).join(', '));
  }
  return Promise.resolve(self.context[Object.keys(self.context)[0]]);
};


Query.prototype.db = function(args, options, internalOptions) {
  var self = this;
  if (internalOptions.deterministic === true) {
    // Uh, RethinkDB highlight the whole write query
    self.frames.pop();
    self.frames.pop();
    self.frames.pop();
    util.nonDeterministicOp(self);
  }
  util.assertArity(1, args, this);
  this.frames.push(0);
  var dbName = this.evaluate(args[0], internalOptions);
  return Promise.resolve(dbName).then(function(dbName) {
    util.assertType(dbName, 'STRING', self);
    util.assertNoSpecialChar(dbName, 'Database', self);
    self.frames.pop();
    if (self.server.databases[dbName] == null) {
      throw new Error.ReqlRuntimeError("Database `"+dbName+"` does not exist", self.frames);
    }
    return self.server.databases[dbName];
  });
};

Query.prototype.table = function(args, options, internalOptions) {
  var self = this;
  util.assertArityRange(1, 2, args, this); // Enforced by the driver
  return Promise.resolve(options).bind({}).then(function(options) {
    //TODO Use options/check them
    this.options = options;
    var db;
    if (args.length === 1) {
      this.dbProvided = false;
      if (internalOptions.deterministic === true) {
        // Uh, RethinkDB highlight the whole write query
        self.frames.pop();
        util.nonDeterministicOp(self);
      }
      if (self.options.db !== undefined) {
        db = self.evaluate(self.options.db, internalOptions);
      }
      else {
        db = self.server.databases['test'];
      }
    }
    else if (args.length === 2) {
      this.dbProvided = true;
      self.frames.push(0);
      db = self.evaluate(args[0], internalOptions);
    }
    return Promise.resolve(db);
  }).then(function(db) {
    this.db = db;
    util.assertType(db, 'DATABASE', self);
    if (this.dbProvided === true) {
      self.frames.pop();
      self.frames.push(1);
      return self.evaluate(args[1], internalOptions);
    }
    else {
      self.frames.push(0);
      return self.evaluate(args[0], internalOptions);
    }
  }).then(function(tableName) {
    util.assertType(tableName, 'STRING', self);
    util.assertNoSpecialChar(tableName, 'Table', self);
    self.frames.pop();
    if (this.db.tables[tableName] == null) {
      throw new Error.ReqlRuntimeError("Table `"+this.db.name+'.'+tableName+"` does not exist", this.frames);
    }
    return this.db.tables[tableName];
  });
};

Query.prototype.get = function(args, options, internalOptions) {
  var self = this;
  util.assertArity(2, args, self);
  self.frames.push(0);
  return self.evaluate(args[0], internalOptions).bind({}).then(function(table) {
    util.assertType(table, 'TABLE', self);
    this.table = table;
    self.frames.pop();
    self.frames.push(1);
    return self.evaluate(args[1], internalOptions);
  }).then(function(key) {
    util.assertType(key, 'DATUM', self);
    self.frames.pop();
    return this.table.get(key);
  });
};

Query.prototype.getAll = function(args, options, internalOptions) {
  var self = this;
  util.assertArityRange(1, Infinity, args, self);
  self.frames.push(0);
  return self.evaluate(options, internalOptions).bind({}).then(function(options) {
    this.options = options;
    return self.evaluate(args[0], internalOptions);
  }).then(function(table) {
    util.assertType(table, 'TABLE', self);
    self.frames.pop();
    this.table = table;
    return Promise.map(args.slice(1), function(arg, index) {
      self.frames.push(index+1);
      return self.evaluate(arg, internalOptions).then(function(result) {
        self.frames.pop();
        return result;
      });
    }, {concurrency: 1});
  }).then(function(values) {
    return this.table.getAll(values, options, self, internalOptions);
  });
};

Query.prototype.eq = function(args, options, internalOptions) {
  var self = this;
  // We must keep the synchronous behavior to return the first error.
  // And to not alter other data
  util.assertArityRange(2, Infinity, args, this);
  self.frames.push(0);
  return self.evaluate(args[0], internalOptions).then(function(reference) {
    util.assertType(reference, 'DATUM', self);
    self.frames.pop();
    var result = true;
    return Promise.reduce(args.slice(1), function(reference, right, index) {
      if (result === false) {
        return false;
      }
      self.frames.push(index+1);
      return self.evaluate(right, internalOptions).then(function(right) {
        util.assertType(right, 'DATUM', self);
        self.frames.pop();
        if (util.eq(reference, right) === false) {
          result = false;
          return false;
        }
        else {
          return reference;
        }
      });
    }, reference).then(function(reference) {
      if (result === true) {
        return true;
      }
      return reference;
    });
  });
};

Query.prototype.ne = function(args, options, internalOptions) {
  var self = this;
  // We must keep the synchronous behavior to return the first error.
  // And to not alter other data
  util.assertArityRange(2, Infinity, args, this);
  self.frames.push(0);
  return self.evaluate(args[0], internalOptions).then(function(reference) {
    util.assertType(reference, 'DATUM', self);
    self.frames.pop();
    var result = false;
    return Promise.reduce(args.slice(1), function(reference, right, index) {
      if (result === true) {
        return true;
      }
      self.frames.push(index+1);
      return self.evaluate(right, internalOptions).then(function(right) {
        util.assertType(right, 'DATUM', self);
        self.frames.pop();
        if (util.eq(reference, right) === false) {
          result = true;
          return false;
        }
        else {
          return reference;
        }
      });
    }, reference).then(function() {
      return result;
    });
  });
};

// For lt, gt, ge, le
Query.prototype.comparator = function(comparator, args, options, internalOptions) {
  var self = this;

  util.assertArityRange(2, Infinity, args, self);
  self.frames.push(0);
  return self.evaluate(args[0], internalOptions).then(function(left) {
    util.assertType(left, 'DATUM', self);
    self.frames.pop();
    var keepGoing = true;
    return Promise.reduce(args.slice(1), function(left, right, index) {
      if (keepGoing === false) { return; }
      self.frames.push(index+1);
      return self.evaluate(right, internalOptions).then(function(right) {
        util.assertType(right, 'DATUM', self);
        self.frames.pop();
        if (util[comparator](left, right) === false) {
          keepGoing = false;
        }
        return right;
      });
    }, left).then(function() {
      if (keepGoing === true) {
        return true;
      }
      else {
        return false;
      }
    });
  });
};

Query.prototype.not = function(args, options, internalOptions) {
  var self = this;
  util.assertArity(1, args, self);
  self.frames.push(0);
  return self.evaluate(args[0], internalOptions).then(function(value) {
    util.assertType(value, 'DATUM', self);
    self.frames.pop();
    return !util.toBool(value);
  });
};

Query.prototype.add = function(args, options, internalOptions) {
  var self = this;
  util.assertArityRange(1, Infinity, args, self);
  self.frames.push(0);
  return self.evaluate(args[0], internalOptions).then(function(result) {
    util.assertType(result, 'DATUM', self);
    self.frames.pop();
    return Promise.reduce(args.slice(1), function(result, value, index) {
      self.frames.push(index+1);
      return self.evaluate(value, internalOptions).then(function(value) {
        util.assertType(value, 'DATUM', self);
        self.frames.pop();
        if (util.isDate(result)) {
          util.assertType(value, "NUMBER", self);
          result = new ReqlDate(result.epoch_time + value, result.timezone);
        }
        else if (util.isSequence(result)) {
          result = result.concat(value);
        }
        else {
          util.assertType(value, util.getType(result), self);
          result += value;
        }
        return result;
      });
    }, result);
  });
};

Query.prototype.sub = function(args, options, internalOptions) {
  var self = this;
  util.assertArityRange(1, Infinity, args, self);
  self.frames.push(0);
  return self.evaluate(args[0], internalOptions).then(function(result) {
    util.assertType(result, 'DATUM', self);
    self.frames.pop();
    return Promise.reduce(args.slice(1), function(result, value, index) {
      self.frames.push(index+1);
      return self.evaluate(value, internalOptions).then(function(value) {
        util.assertType(value, 'DATUM', self);
        self.frames.pop();
        if (util.isDate(result)) {
          try {
            util.assertType(value, "PTYPE<TIME>", this);
          }
          catch(err) {
            util.assertType(value, "NUMBER", this);
          }
          if (util.getType(value) === 'NUMBER') {
            result = new ReqlDate(result.epoch_time - value, result.timezone);
          }
          else {
            result = result.epoch_time - value.epoch_time;
          }
        }
        else {
          util.assertType(result, "NUMBER", this);
          util.assertType(value, "NUMBER", this);
          result -= value;
        }
        return result;
      });
    }, result);
  });
};

Query.prototype.mul = function(args, options, internalOptions) {
  var self = this;
  util.assertArityRange(1, Infinity, args, self);
  self.frames.push(0);
  return self.evaluate(args[0], internalOptions).then(function(result) {
    util.assertType(result, 'DATUM', self);
    try {
      util.assertType(result, "ARRAY", this);
    }
    catch(err) {
      util.assertType(result, "NUMBER", this);
    }

    self.frames.pop();
    return Promise.reduce(args.slice(1), function(result, value, index) {
      self.frames.push(index+1);
      return self.evaluate(value, internalOptions).then(function(value) {
        util.assertType(value, 'DATUM', self);
        util.assertType(value, 'NUMBER', this);
        self.frames.pop();
        if (util.isSequence(result)) {
          // Keep a reference of the sequence to concat
          var reference = result.clone(); //TODO use Sequence.concat
          for(var j=0; j<value-1; j++) {
            for(var k=0; k<reference.length; k++) {
              result.push(reference.get(k));
            }
          }
        }
        else {
          result *= value;
        }
        return result;
      });
    }, result);
  });
};

Query.prototype.div = function(args, options, internalOptions) {
  var self = this;
  util.assertArityRange(1, Infinity, args, self);
  self.frames.push(0);
  return self.evaluate(args[0], internalOptions).then(function(result) {
    util.assertType(result, 'DATUM', self);
    util.assertType(result, "NUMBER", this);

    self.frames.pop();
    return Promise.reduce(args.slice(1), function(result, value, index) {
      self.frames.push(index+1);
      return self.evaluate(value, internalOptions).then(function(value) {
        util.assertType(value, 'DATUM', self);
        util.assertType(value, 'NUMBER', this);
        self.frames.pop();
        if (value === 0) {
          throw new Error.ReqlRuntimeError('Cannot divide by zero', this.frames);
        }
        result /= value;
        return result;
      });
    }, result);
  });
};

Query.prototype.mod = function(args, options, internalOptions) {
  var self = this;
  util.assertArity(2, args, self);
  self.frames.push(0);
  return self.evaluate(args[0], internalOptions).then(function(numerator) {
    util.assertType(numerator, 'DATUM', self);
    util.assertType(numerator, "NUMBER", self);
    util.assertType(numerator, "INT", self);
    this.numerator = numerator;
    self.frames.pop();
    self.frames.push(1);
    return self.evaluate(args[1], internalOptions);
  }).then(function(denominator) {
    util.assertType(denominator, 'DATUM', self);
    util.assertType(denominator, "NUMBER", self);
    util.assertType(denominator, "INT", self);
    self.frames.pop();
    if (denominator === 0) {
      throw new Error.ReqlRuntimeError('Cannot take a number modulo 0', this.frames);
    }
    var remainder = this.numerator%denominator;
    if ((remainder < 0) && (this.numerator >= 0)) {
      remainder += denominator;
    }
    return remainder;
  });
};

Query.prototype.floor = function(args, options, internalOptions) {
  var self = this;
  util.assertArity(1, args, self);
  self.frames.push(0);
  return self.evaluate(args[0], internalOptions).then(function(num) {
    util.assertType(num, 'DATUM', self);
    util.assertType(num, "NUMBER", self);
    self.frames.pop();
    return Math.floor(num);
  });
};

Query.prototype.ceil = function(args, options, internalOptions) {
  var self = this;
  util.assertArity(1, args, self);
  self.frames.push(0);
  return self.evaluate(args[0], internalOptions).then(function(num) {
    util.assertType(num, 'DATUM', self);
    util.assertType(num, "NUMBER", self);
    self.frames.pop();
    return Math.ceil(num);
  });
};

Query.prototype.round = function(args, options, internalOptions) {
  var self = this;
  util.assertArity(1, args, self);
  self.frames.push(0);
  return self.evaluate(args[0], internalOptions).then(function(num) {
    util.assertType(num, 'DATUM', self);
    util.assertType(num, "NUMBER", self);
    self.frames.pop();
    return Math.round(num);
  });
};

Query.prototype.append = function(args, options, internalOptions) {
  var self = this;
  util.assertArity(2, args, self);
  self.frames.push(0);
  return self.evaluate(args[0], internalOptions).then(function(sequence) {
    util.assertType(sequence, 'DATUM', self);
    self.frames.pop();
    this.sequence = sequence;
    self.frames.push(1);
    return self.evaluate(args[1], internalOptions);
  }).then(function(element) {
    util.assertType(element, 'DATUM', self);
    self.frames.pop();
    util.assertType(this.sequence, 'ARRAY', self); // Gosh...
    var result = this.sequence.toSequence();
    result.push(element);
    return result;
  });
};

Query.prototype.prepend = function(args, options, internalOptions) {
  var self = this;
  util.assertArity(2, args, self);
  self.frames.push(0);
  return self.evaluate(args[0], internalOptions).then(function(sequence) {
    util.assertType(sequence, 'DATUM', self);
    self.frames.pop();
    this.sequence = sequence;
    self.frames.push(1);
    return self.evaluate(args[1], internalOptions);
  }).then(function(element) {
    util.assertType(element, 'DATUM', self);
    self.frames.pop();
    util.assertType(this.sequence, 'ARRAY', self); // Gosh...
    this.sequence.unshift(element);
    return this.sequence;
  });
};

Query.prototype.difference = function(args, options, internalOptions) {
  var self = this;
  util.assertArity(2, args, this);
  self.frames.push(0);
  return self.evaluate(args[0], internalOptions).bind({}).then(function(sequence) {
    self.frames.pop();
    this.sequence = util.toSequence(sequence, self);
    self.frames.push(1);
    return self.evaluate(args[1], internalOptions);
  }).then(function(other) {
    self.frames.pop();
    other = util.toSequence(other, self);
    return this.sequence.difference(other, self);
  });
};

Query.prototype.setInsert = function(args, options, internalOptions) {
  var self = this;
  util.assertArity(2, args, self);
  self.frames.push(0);
  return self.evaluate(args[0], internalOptions).bind({}).then(function(sequence) {
    util.assertType(sequence, 'DATUM', self);
    self.frames.pop();
    this.sequence = sequence;
    self.frames.push(1);
    return self.evaluate(args[1], internalOptions);
  }).then(function(value) {
    util.assertType(value, 'DATUM', self);
    self.frames.pop();
    util.assertType(this.sequence, 'ARRAY', self);
    return this.sequence.setInsert(value, self);
  });
};

Query.prototype.setIntersection = function(args, options, internalOptions) {
  var self = this;
  util.assertArity(2, args, self);
  self.frames.push(0);
  return self.evaluate(args[0], internalOptions).bind({}).then(function(sequence) {
    util.assertType(sequence, 'DATUM', self);
    self.frames.pop();
    this.sequence = sequence;
    self.frames.push(1);
    return self.evaluate(args[1], internalOptions);
  }).then(function(other) {
    util.assertType(other, 'DATUM', self);
    self.frames.pop();
    util.assertType(this.sequence, 'ARRAY', self);
    util.assertType(other, 'ARRAY', self);
    return this.sequence.setIntersection(other, self);
  });
};

Query.prototype.setUnion = function(args, options, internalOptions) {
  var self = this;
  util.assertArity(2, args, self);
  self.frames.push(0);
  return self.evaluate(args[0], internalOptions).bind({}).then(function(sequence) {
    util.assertType(sequence, 'DATUM', self);
    self.frames.pop();
    this.sequence = sequence;
    self.frames.push(1);
    return self.evaluate(args[1], internalOptions);
  }).then(function(other) {
    util.assertType(other, 'DATUM', self);
    self.frames.pop();
    util.assertType(this.sequence, 'ARRAY', self);
    util.assertType(other, 'ARRAY', self);
    return this.sequence.setUnion(other, self);
  });
};

Query.prototype.setDifference = function(args, options, internalOptions) {
  var self = this;
  util.assertArity(2, args, self);
  self.frames.push(0);
  return self.evaluate(args[0], internalOptions).bind({}).then(function(sequence) {
    util.assertType(sequence, 'DATUM', self);
    self.frames.pop();
    this.sequence = sequence;
    self.frames.push(1);
    return self.evaluate(args[1], internalOptions);
  }).then(function(other) {
    util.assertType(other, 'DATUM', self);
    self.frames.pop();
    util.assertType(this.sequence, 'ARRAY', self);
    util.assertType(other, 'ARRAY', self);
    return this.sequence.setDifference(other, self);
  });
};

Query.prototype.slice = function(args, options, internalOptions) {
  var self = this;
  util.assertArityRange(2, 3, args, self);
  self.frames.push(0);
  return self.evaluate(options, internalOptions).bind({}).then(function(options) {
    this.options = options;
    return self.evaluate(args[0], internalOptions);
  }).then(function(sequenceOrBinOrStr) {
    // Only strings, arrays or binaries
    if (typeof sequenceOrBinOrStr !== 'string') {
      util.assertType(sequenceOrBinOrStr, ["ARRAY", "PTYPE<BINARY>"], self);
    }
    this.sequenceOrBinOrStr = sequenceOrBinOrStr;
    self.frames.pop();
    self.frames.push(1);
    return self.evaluate(args[1], internalOptions);
  }).then(function(start) {
    util.assertType(start, 'NUMBER', self);
    this.start = start;
    self.frames.pop();
    if (typeof sequenceOrBinOrStr === 'string') {
      return sequenceOrBinOrStr.slice.apply(sequenceOrBinOrStr, args.slice(1));
    }
    if (args.length > 2) {
      self.frames.push(2);
      return self.evaluate(args[2], internalOptions).bind(this).then(function(end) {
        util.assertType(end, 'NUMBER', self);
        self.frames.pop();
        return this.sequenceOrBinOrStr.slice(this.start, end, this.options, self);
      });
    }
    else {
      return this.sequenceOrBinOrStr.slice(this.start, undefined, this.options, self);
    }
  });
};

Query.prototype.skip = function(args, options, internalOptions) {
  var self = this;
  util.assertArity(2, args, self);
  self.frames.push(0);
  return self.evaluate(args[0], internalOptions).then(function(sequenceOrBinOrStr) {
    // Only strings, arrays or binaries
    if (typeof sequenceOrBinOrStr !== 'string') {
      util.assertType(sequenceOrBinOrStr, ['ARRAY', 'PTYPE<BINARY>'], self);
    }
    this.sequenceOrBinOrStr = sequenceOrBinOrStr;
    self.frames.pop();
    self.frames.push(1);
    return self.evaluate(args[1], internalOptions);
  }).then(function(skip) {
    util.assertType(skip, "NUMBER", self);
    self.frames.pop();
    if (typeof sequenceOrBinOrStr === 'string') {
      return this.sequenceOrBinOrStr.slice(skip);
    } else if (util.isBinary(this.sequenceOrBinOrStr)) {
      var buffer = new Buffer(this.sequenceOrBinOrStr.data, 'base64');
      buffer = buffer.slice(skip);
      this.sequenceOrBinOrStr.data = buffer.toString('base64');
      return this.sequenceOrBinOrStr;
    }
    else {
      return this.sequenceOrBinOrStr.skip(skip);
    }
  });
};

Query.prototype.limit = function(args, options, internalOptions) {
  var self = this;
  util.assertArity(2, args, this);
  self.frames.push(0);
  return self.evaluate(args[0], internalOptions).bind({}).then(function(sequence) {
    self.frames.pop();
    util.toSequence(sequence, self);
    this.sequence = sequence;
    self.frames.push(1);
    return self.evaluate(args[1], internalOptions);
  }).then(function(limit) {
    util.assertType(limit, "NUMBER", self);
    if (limit < 0) {
      throw new Error.ReqlRuntimeError('LIMIT takes a non-negative argument (got '+limit+')', this.frames);
    }
    self.frames.pop();
    return this.sequence.limit(limit, this);
  });
};
Query.prototype.offsetsOf = function(args, options, internalOptions) {
  var self = this;
  util.assertArity(2, args, self);
  self.frames.push(0);
  return self.evaluate(args[0], internalOptions).then(function(sequence) {
    self.frames.pop();
    util.toSequence(sequence, self);
    return sequence.offsetsOf(args[1], self);
  });
};
Query.prototype.contains = function(args, options, internalOptions) {
  var self = this;
  //util.assertArityRange(0, Infinity, term[1], this);
  self.frames.push(0);
  return self.evaluate(args[0], internalOptions).then(function(sequence) {
    self.frames.pop();
    util.toSequence(sequence, self);
    var predicates = [];
    for(var i=1; i<args.length; i++) {
      predicates.push(args[i]);
    }
    return sequence.contains(predicates, self, internalOptions);
  });
};

Query.prototype.getField = function(args, options, internalOptions) {
  var self = this;
  util.assertArity(2, args, self);
  self.frames.push(0);
  return self.evaluate(args[0], internalOptions).bind({}).then(function(sequenceOrObject) {
    try {
      util.assertType(sequenceOrObject, 'OBJECT', self);
    }
    catch(err) {
      util.cannotPerformOp('get_field', sequenceOrObject, self);
    }
    this.sequenceOrObject = sequenceOrObject;
    self.frames.pop();
    self.frames.push(1);
    return self.evaluate(args[1], internalOptions);
  }).then(function(field) {
    util.assertType(field, 'DATUM', self);
    self.frames.pop();
    if (this.sequenceOrObject instanceof Document) {
      if (this.sequenceOrObject.doc[field] === undefined) {
        throw new Error.ReqlRuntimeError("No attribute `"+field+"` in object:\n"+JSON.stringify(util.toDatum(this.sequenceOrObject), null, 2), this.frames);
      }
      return this.sequenceOrObject.doc[field];
    }
    else if (util.isSequence(this.sequenceOrObject)) {
      return this.sequenceOrObject.getField(field);
    }
    else {
      if (this.sequenceOrObject[field] === undefined) {
        throw new Error.ReqlRuntimeError("No attribute `"+field+"` in object:\n"+JSON.stringify(util.toDatum(this.sequenceOrObject), null, 2), this.frames);
      }
      return this.sequenceOrObject[field];
    }
  });
};

Query.prototype.keys = function(args, options, internalOptions) {
  var self = this;
  util.assertArity(1, args, self);
  self.frames.push(0);
  return self.evaluate(args[0], internalOptions).then(function(obj) {
    try {
      util.assertType(obj, 'OBJECT', self);
    }
    catch(err) {
      throw new Error.ReqlRuntimeError('Cannot call `keys` on objects of type `'+util.typeOf(obj)+'`', self.frames);
    }
    self.frames.pop();
    if (typeof obj.keys === 'function') {
      return obj.keys();
    }
    else {
      var keys = Object.keys(obj);
      return new Sequence(keys);
    }
  });
};

//TODO Refactor code with keys, note that we should evaluate arguments just once.
Query.prototype.values = function(args, options, internalOptions) {
  var self = this;
  util.assertArity(1, args, self);
  self.frames.push(0);
  return self.evaluate(args[0], internalOptions).then(function(obj) {
    try {
      util.assertType(obj, 'OBJECT', self);
    }
    catch(err) {
      throw new Error.ReqlRuntimeError('Cannot call `values` on objects of type `'+util.typeOf(obj)+'`', self.frames);
    }
    self.frames.pop();
    var keys;
    if (typeof obj.keys === 'function') {
      keys = obj.keys();
    }
    else {
      keys =  new Sequence(Object.keys(obj));
    }
    var result = new Sequence();
    for(var i=0; i<keys.length; i++) {
      result.push(util.getField(obj, keys.get(i)))
    }
    return result;
  });
};


Query.prototype.object = function(args, options, internalOptions) {
  //util.assertArityRange(0, Infinity, args, this);
  var self = this;
  if (args.length%2 === 1) {
    throw new Error.ReqlRuntimeError("OBJECT expects an even number of arguments (but found "+args.length+")");
  }
  var key;
  return Promise.reduce(args, function(result, arg, index) {
    self.frames.push(index);
    return self.evaluate(arg, internalOptions).then(function(value) {
      if (key === undefined) {
        util.assertType(value, "STRING", self);
        self.frames.pop();
        key = value;
      }
      else {
        util.assertType(value, "DATUM", self);
        self.frames.pop();
        if (result[key] !== undefined) {
          throw new Error.ReqlRuntimeError('Duplicate key "'+key+'" in object.  (got '+JSON.stringify(result[key], null, 4)+' and '+JSON.stringify(value, null, 4)+' as values)', self.frames);
        }
        result[key] = value;
        key = undefined;
      }
      return result;
    });
  }, {});
};

Query.prototype.hasFields = function(args, options, internalOptions) {
  var self = this;
  util.assertArityRange(1, Infinity, args, self);
  self.frames.push(0);
  return self.evaluate(args[0], internalOptions).bind({}).then(function(sequenceOrObject) {
    if (!util.isObject(sequenceOrObject) && !util.isSequence(sequenceOrObject)) {
      util.cannotPerformOp('has_fields', util.toDatum(sequenceOrObject), self);
    }
    this.sequenceOrObject = sequenceOrObject;
    self.frames.pop();
    return Promise.map(args.slice(1), function(arg, index) {
      self.frames.push(index+1);
      return self.evaluate(arg, internalOptions).then(function(result) {
        util.assertType(result, 'DATUM', self);
        self.frames.pop();
        return result;
      });
    });
  }).then(function(keys) {
    keys = new Sequence(keys);
    return util.hasFields(this.sequenceOrObject, keys);
  });
};

Query.prototype.withFields = function(args, options, internalOptions) {
  var self = this;
  util.assertArityRange(1, Infinity, args, self);

  self.frames.push(0);
  return self.evaluate(args[0], internalOptions).bind({}).then(function(sequenceOrObject) {
    if (util.isPlainObject(sequenceOrObject) && !util.isSequence(sequenceOrObject)) {
      util.cannotPerformOp('pluck', util.toDatum(sequenceOrObject), self);
    }
    if (!util.isSequence(sequenceOrObject)) {
      util.cannotPerformOp('has_fields', util.toDatum(sequenceOrObject), self);
    }
    self.frames.pop();
    this.sequenceOrObject = sequenceOrObject;
    return Promise.map(args.slice(1), function(arg, index) {
      self.frames.push(index+1);
      return self.evaluate(arg, internalOptions).then(function(result) {
        util.assertType(result, 'DATUM', self);
        self.frames.pop();
        return result;
      });
    });
  }).then(function(keys) {
    keys = new Sequence(keys);
    return this.sequenceOrObject.withFields(keys);
  });
};

Query.prototype.pluck = function(args, options, internalOptions) {
  var self = this;
  util.assertArityRange(1, Infinity, args, this);
  self.frames.push(0);
  return self.evaluate(args[0], internalOptions).bind({}).then(function(sequenceOrObject) {
    if (!util.isObject(sequenceOrObject) && !util.isSequence(sequenceOrObject)) {
      util.cannotPerformOp('pluck', util.toDatum(sequenceOrObject), self);
    }
    this.sequenceOrObject = sequenceOrObject;
    self.frames.pop();
    return Promise.map(args.slice(1), function(arg, index) {
      self.frames.push(index+1);
      return self.evaluate(arg, internalOptions).then(function(result) {
        util.assertType(result, 'DATUM', self);
        self.frames.pop();
        return result;
      });
    }, {concurrency: 1}).then(function(keys) {
      keys = new Sequence(keys);
      for(var i=0; i<keys.length; i++) {
        util.assertPath(keys.get(i), this);
      }
      return util.pluck(sequenceOrObject, keys);
    });
  });
};

Query.prototype.without = function(args, options, internalOptions) {
  var self = this;
  util.assertArityRange(1, Infinity, args, this);
  self.frames.push(0);
  return self.evaluate(args[0], internalOptions).bind({}).then(function(sequenceOrObject) {
    if (!util.isObject(sequenceOrObject) && !util.isSequence(sequenceOrObject)) {
      util.cannotPerformOp('without', util.toDatum(sequenceOrObject), self);
    }
    this.sequenceOrObject = sequenceOrObject;
    self.frames.pop();
    return Promise.map(args.slice(1), function(arg, index) {
      self.frames.push(index+1);
      return self.evaluate(arg, internalOptions).then(function(result) {
        util.assertType(result, 'DATUM', self);
        self.frames.pop();
        return result;
      });
    }, {concurrency: 1}).then(function(keys) {
      keys = new Sequence(keys);
      for(var i=0; i<keys.length; i++) {
        util.assertPath(keys.get(i), this);
      }
      return util.without(sequenceOrObject, keys);
    });
  });
};


Query.prototype.merge = function(args, options, internalOptions) {
  //TODO Test with Documents
  var self = this;
  util.assertArityRange(1, Infinity, args, self);
  self.frames.push(0);
  return self.evaluate(args[0], internalOptions).then(function(result) {
    if (!util.isObject(result) && !util.isSequence(result) || util.isMissingDoc(result)) {
      util.cannotPerformOp('merge', util.toDatum(result), self);
    }
    self.frames.pop();
    this.result = result;
    return Promise.reduce(args.slice(1), function(result, toMerge, index) {
      self.frames.push(index+1);
      return util.merge(result, toMerge, self, internalOptions).then(function(result) {
        self.frames.pop();
        return result;
      });
    }, result).then(function(result) {
      return result;
    });
  });
};

Query.prototype.between = function(args, options, internalOptions) {
  var self = this;
  util.assertArity(3, args, self);
  return self.evaluate(options, internalOptions).bind({}).then(function(options) {
    if (options.left_bound === undefined) {
      options.left_bound = "closed";
    }
    else if ((options.left_bound !== 'closed') && (options.left_bound !== 'open')) {
      throw new Error.ReqlRuntimeError('Expected `open` or `closed` for optarg `left_bound` (got `"'+options.left_bound+'"`)', self.frames);
    }
    if (options.right_bound === undefined) {
      options.right_bound = "open";
    }
    else if ((options.right_bound !== 'closed') && (options.right_bound !== 'open')) {
      throw new Error.ReqlRuntimeError('Expected `open` or `closed` for optarg `right_bound` (got `"'+options.right_bound+'"`)', self.frames);
    }
    this.options = options;
    self.frames.push(0);
    return self.evaluate(args[0], internalOptions);
  }).then(function(table) {
    util.assertType(table, 'TABLE_SLICE', this);
    self.frames.pop();
    this.table = table;
    self.frames.push(1);
    return self.evaluate(args[1], internalOptions);
  }).then(function(left) {
    util.assertType(left, 'DATUM', self);
    self.frames.pop();
    this.left = left;
    self.frames.push(2);
    return self.evaluate(args[2], internalOptions);
  }).then(function(right) {
    util.assertType(right, 'DATUM', self);
    self.frames.pop();
    return this.table.between(this.left, right, this.options, self, internalOptions);
  });
};

Query.prototype.minval = function(args, options, internalOptions) {
  util.assertArity(0, args, this);
  return Promise.resolve(new Minval());
};
Query.prototype.maxval = function(args, options, internalOptions) {
  util.assertArity(0, args, this);
  return Promise.resolve(new Maxval());
};

Query.prototype.fold = function(args, options, internalOptions) {
  var self = this;
  util.assertArity(3, args, self);
  self.frames.push(0);
  return self.evaluate(args[0], internalOptions).bind({}).then(function(sequence) {
    self.frames.pop();
    util.toSequence(sequence, self);
    this.sequence = sequence;
    return self.evaluate(args[1], internalOptions)
  }).then(function(base) {
    return this.sequence.fold(base, args[2], options, self, internalOptions);
  });
}

Query.prototype.reduce = function(args, options, internalOptions) {
  var self = this;
  util.assertArity(2, args, self);
  self.frames.push(0);
  return self.evaluate(args[0], internalOptions).then(function(sequence) {
    self.frames.pop();
    util.toSequence(sequence, self);
    return sequence.reduce(args[1], self, internalOptions);
  });
};

Query.prototype.map = function(args, options, internalOptions) {
  var self = this;
  util.assertArityRange(2, Infinity, args, self);
  var index = 0;
  return Promise.map(args.slice(0, -1), function(arg) {
    self.frames.push(index);
    return self.evaluate(arg, internalOptions).then(function(sequence) {
      self.frames.pop();
      return sequence;
    });
  }, {concurrency: 1}).then(function(sequences) {
    var mapper = args[args.length-1];
    // You currently cannot have nested groups, so this is safe
    if (sequences[0] instanceof Group) {
      return sequences[0].map(mapper, self, internalOptions);
    }
    for(var i=0; i<sequences.length; i++) {
      sequences[i] = util.toSequence(sequences[i], self);
    }
    // RethinkDB seem to currently accept only one change max
    // See rethinkdb/rethinkdb/issues/4242
    if ((sequences.length === 1) && util.isChanges(sequences[0])) {
      return sequences[0].map(mapper, self, internalOptions);
    }
    return Sequence.map(sequences, mapper, self, internalOptions);
  });
};

Query.prototype.filter = function(args, options, internalOptions) {
 // function(sequence, predicate, options, internalOptions) {
  var self = this;
  util.assertArity(2, args, self);
  return self.evaluate(options, internalOptions).bind({}).then(function(options) {
    util.assertOptions(options, ['default'], self);
    this.options = options;
    self.frames.push(0);
    return self.evaluate(args[0], internalOptions);
  }).then(function(sequence) {
    self.frames.pop();
    // Do not reassign here as we want to keep selections
    util.toSequence(sequence, self);
    return sequence.filter(args[1], this.options, self, internalOptions);
  });
};

Query.prototype.concatMap = function(args, options, internalOptions) {
  var self = this;
  util.assertArity(2, args, self);
  return self.evaluate(args[0], internalOptions).then(function(sequence) {
    util.toSequence(sequence, self);
    return sequence.concatMap(args[1], self, internalOptions);
  });
};

Query.prototype.orderBy = function(args, options, internalOptions) {
  var self = this;
  self.frames.push(0);
  return self.evaluate(options, internalOptions).bind({}).then(function(options) {
    this.options = options;
    this.fields = args.slice(1);
    return self.evaluate(args[0], internalOptions);
  }).then(function(sequence) {
    // Do not copy as we want to keep the selection type
    self.frames.pop();
    util.toSequence(sequence, self);
    // There's a special arity error for orderBy...
    if ((this.fields.length === 0) && (this.options.index === undefined)) {
      throw new Error.ReqlRuntimeError('Must specify something to order by', self.frames);
    }
    return sequence.orderBy(this.fields, this.options, self, internalOptions);
  });

};

Query.prototype.distinct = function(args, options, internalOptions) {
  var self = this;
  util.assertArity(1, args, self);
  self.frames.push(0);
  return self.evaluate(args[0], internalOptions).then(function(sequenceOrSelection) {
    self.frames.pop();
    util.toSequence(sequenceOrSelection, self);
    return sequenceOrSelection.distinct(options, self, internalOptions);
  });
};

Query.prototype.count = function(args, options, internalOptions) {
  var self = this;
  util.assertArityRange(1, 2, args, self);
  self.frames.push(0);
  return self.evaluate(args[0], internalOptions).then(function(sequenceOrBinOrStr) {
    self.frames.pop();
    if (typeof sequenceOrBinOrStr === 'string') {
      return Promise.resolve(sequenceOrBinOrStr.length);
    }
    else if (util.isBinary(sequenceOrBinOrStr) && (args.length === 1)) {
      // See rethinkdb/rethinkdb#2804
      // This is equivalent to: new Buffer(sequenceOrBin.data, 'base64').length
      var length = sequenceOrBinOrStr.data.length;
      var blocksOf78 = Math.floor(length/78);
      var remainderOf78 = length%78;
      var base64Digits = blocksOf78*76+remainderOf78;
      var blocksOf4 = Math.floor(base64Digits/4);
      var numberOfEquals2;
      if (/==$/.test(sequenceOrBinOrStr.data)) {
        numberOfEquals2 = 2;
      }
      else if (/=$/.test(sequenceOrBinOrStr.data)) {
        numberOfEquals2 = 1;
      }
      else {
        numberOfEquals2 = 0;
      }
      return 3*blocksOf4-numberOfEquals2;
    }
    // else we have a sequence
    util.toSequence(sequenceOrBinOrStr, self);
    return sequenceOrBinOrStr.count(args[1], self, internalOptions);
  });
};

Query.prototype.isEmpty = function(args, options, internalOptions) {
  var self = this;
  util.assertArity(1, args, self);
  self.frames.push(0);
  return self.evaluate(args[0], internalOptions).then(function(sequence) {
    self.frames.pop();
    util.toSequence(sequence, self);
    return sequence.isEmpty();
  });
};

Query.prototype.union = function(args, options, internalOptions) {
  var self = this;
  util.assertArityRange(1, Infinity, args, self);
  return Promise.reduce(args, function(result, arg, index) {
    self.frames.push(index);
    return self.evaluate(arg, internalOptions).then(function(toConcat) {
      self.frames.pop();
      util.toSequence(toConcat, self);
      result = result.concat(toConcat);
      return result;
    });
  }, new Sequence());
};

Query.prototype.nth = function(args, options, internalOptions) {
  var self = this;
  util.assertArity(2, args, self);
  self.frames.push(0);
  return self.evaluate(args[0], internalOptions).then(function(sequence) {
    self.frames.pop();
    util.toSequence(sequence, self);
    this.sequence =  sequence;
    self.frames.push(1);
    return self.evaluate(args[1], internalOptions);
  }).then(function(index) {
    self.frames.pop();
    util.assertType(index, "NUMBER", self);
    return this.sequence.nth(index, self);
  });
};

Query.prototype.bracket = function(args, options, internalOptions) {
  var self = this;
  util.assertArity(2, args, self);

  self.frames.push(0);
  return self.evaluate(args[0], internalOptions).bind({}).then(function(sequenceOrObject) {
    if (!util.isObject(sequenceOrObject) && !util.isSequence(sequenceOrObject)) {
      util.cannotPerformOp('bracket', sequenceOrObject, self);
    }
    self.frames.pop();
    this.sequenceOrObject = sequenceOrObject;
    self.frames.push(1);
    return self.evaluate(args[1], internalOptions);
  }).then(function(key) {
    self.frames.pop();
    if (typeof key === 'number') {
      util.toSequence(this.sequenceOrObject, self);
    }
    return util.getBracket(this.sequenceOrObject, key, self);
  });
};

// innerJoin and outerJoin
Query.prototype.join = function(type, args, options, internalOptions) {
  var self = this;
  util.assertArity(3, args, self);
  self.frames.push(0);
  return self.evaluate(args[0], internalOptions).bind({}).then(function(sequence) {
    self.frames.pop();
    util.toSequence(sequence, self);
    this.sequence = sequence;
    self.frames.push(1);
    return self.evaluate(args[1], internalOptions);
  }).then(function(otherSequence) {
    self.frames.pop();
    // We do want to pass sequences, not tables/selections etc.
    otherSequence = util.toSequence(otherSequence, self);
    return this.sequence.join(type, otherSequence, args[2], self, internalOptions);
  });
};

Query.prototype.eqJoin = function(args, options, internalOptions) {
  var self = this;
  util.assertArity(3, args, self);
  self.frames.push(0);
  return self.evaluate(options, internalOptions).bind({}).then(function(options) {
    this.options = options;
    return self.evaluate(args[0], internalOptions);
  }).then(function(sequence) {
    self.frames.pop();
    util.toSequence(sequence, self);
    this.sequence = sequence;
    self.frames.push(2);
    return self.evaluate(args[2], internalOptions);
  }).then(function(rightTable) {
    self.frames.pop();
    util.assertType(rightTable, 'TABLE', self);
    return this.sequence.eqJoin(args[1], rightTable, options, self, internalOptions);
  });
};

Query.prototype.zip = function(args, options, internalOptions) {
  var self = this;
  util.assertArity(1, args, self);
  self.frames.push(0);
  return self.evaluate(args[0], internalOptions).then(function(sequence) {
    self.frames.pop();
    util.toSequence(sequence, self);
    return sequence.zip(self);
  });
};

Query.prototype.range = function(args, options, internalOptions) {
  var self = this;
  util.assertArityRange(0, 2, args, self);
  if (args.length === 0) {
    return Promise.resolve(Sequence.range());
  }
  else if (args.length === 1) {
    self.frames.push(0);
    return self.evaluate(args[0], internalOptions).then(function(start) {
      util.assertType(start, "NUMBER", self);
      self.frames.pop();
      return Sequence.range(start);
    });
  }
  else if (args.length === 2) {
    self.frames.push(0);
    return self.evaluate(args[0], internalOptions).bind({}).then(function(start) {
      util.assertType(start, "NUMBER", self);
      self.frames.pop();
      this.start = start;
      self.frames.push(1);
      return self.evaluate(args[1], internalOptions);
    }).then(function(end) {
      util.assertType(end, "NUMBER", self);
      self.frames.pop();
      return Sequence.range(this.start, end);
    });
  }
};

Query.prototype.insertAt = function(args, options, internalOptions) {
  var self = this;
  util.assertArity(3, args, self);
  self.frames.push(0);
  return self.evaluate(args[0], internalOptions).bind({}).then(function(sequence) {
    util.assertType(sequence, 'DATUM', self);
    this.sequence = sequence;
    self.frames.pop();
    util.assertType(sequence, 'ARRAY', self);
    self.frames.push(1);
    return self.evaluate(args[1], internalOptions);
  }).then(function(position) {
    this.position = position;
    self.frames.pop();
    util.assertType(position, 'NUMBER', self);
    if (!util.isChanges(this.sequence) && (position > this.sequence.length)) {
      throw new Error.ReqlRuntimeError("Index `"+position+"` out of bounds for array of size: `"+this.sequence.length+"`", self.frames);
    }
    self.frames.push(2);
    return self.evaluate(args[2], internalOptions);
  }).then(function(value) {
    util.assertType(value, 'DATUM', this);
    self.frames.pop();

    return this.sequence.insertAt(this.position, value, self);
  });
};


Query.prototype.deleteAt = function(args, options, internalOptions) {
  var self = this;
  util.assertArityRange(2, 3, args, self);
  self.frames.push(0);
  return self.evaluate(args[0], internalOptions).bind({}).then(function(sequence) {
    util.assertType(sequence, 'DATUM', self);
    this.sequence = sequence;
    self.frames.pop();
    util.assertType(sequence, 'ARRAY', self);
    self.frames.push(1);
    return self.evaluate(args[1], internalOptions);
  }).then(function(start) {
    this.start = start;
    self.frames.pop();
    util.assertType(start, 'NUMBER', self);
    if (start > this.sequence.length) {
      util.outOfBound(start, this.sequence.length, self);
    }
    if (args.length === 2) {
      return this.sequence.deleteAt(start, undefined, self);
    }
    self.frames.push(2);
    return self.evaluate(args[2], internalOptions).bind(this).then(function(end) {
      util.assertType(end, 'NUMBER', self);
      self.frames.pop();
      return this.sequence.deleteAt(this.start, end, self);
    });
  });
};


Query.prototype.changeAt = function(args, options, internalOptions) {
  var self = this;
  util.assertArity(3, args, self);
  self.frames.push(0);
  return self.evaluate(args[0], internalOptions).bind({}).then(function(sequence) {
    util.assertType(sequence, 'DATUM', self);
    this.sequence = sequence;
    self.frames.pop();
    util.assertType(sequence, 'ARRAY', self);
    self.frames.push(1);
    return self.evaluate(args[1], internalOptions);
  }).then(function(position) {
    this.position = position;
    self.frames.pop();
    util.assertType(position, 'NUMBER', self);
    if (!util.isChanges(this.sequence) && (position > this.sequence.length)) {
      throw new Error.ReqlRuntimeError("Index `"+position+"` out of bounds for array of size: `"+this.sequence.length+"`", this.frames);
    }
    self.frames.push(2);
    return self.evaluate(args[2], internalOptions);
  }).then(function(value) {
    util.assertType(value, 'DATUM', this);
    self.frames.pop();
    return this.sequence.changeAt(this.position, value, self);
  });

};

Query.prototype.spliceAt = function(args, options, internalOptions) {
  var self = this;
  util.assertArity(3, args, self);
  self.frames.push(0);
  return self.evaluate(args[0], internalOptions).bind({}).then(function(sequence) {
    util.assertType(sequence, 'DATUM', self);
    this.sequence = sequence;
    self.frames.pop();
    util.assertType(sequence, 'ARRAY', self);
    self.frames.push(1);
    return self.evaluate(args[1], internalOptions);
  }).then(function(position) {
    this.position = position;
    self.frames.pop();
    util.assertType(position, 'NUMBER', self);
    if (position > this.sequence.length) {
      util.outOfBound(position, this.sequence.length, this);
    }
    self.frames.push(2);
    return self.evaluate(args[2], internalOptions);
  }).then(function(other) {
    util.assertType(other, 'DATUM', this);
    self.frames.pop();
    util.assertType(other, 'ARRAY', this);
    return this.sequence.spliceAt(this.position, other, self);
  });



};

Query.prototype.coerceTo = function(args, options, internalOptions) {
  var self = this;
  util.assertArity(2, args, self);
  self.frames.push(0);
  return self.evaluate(args[0], internalOptions).bind({}).then(function(value) {
    self.frames.pop();
    this.value = value;
    self.frames.push(1);
    return self.evaluate(args[1], internalOptions);
  }).then(function(newType) {
    util.assertType(newType, 'STRING', self);
    self.frames.pop();
    newType = newType.toUpperCase();
    var value = this.value;
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
      self.frames.push(0);
      throw new Error.ReqlRuntimeError("Cannot coerce "+currentType+" to STRING", self.frames);
    }
    else if (newType === "ARRAY") {
      if (util.isSequence(value)) {
        return value.toSequence();
      }
      else if (util.isBinary(value)) {
        self.frames.push(0);
        throw new Error.ReqlRuntimeError("Cannot coerce BINARY to ARRAY", self.frames);
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
        self.frames.push(0);
        throw new Error.ReqlRuntimeError("Cannot coerce "+currentType+" to ARRAY", self.frames);
      }
    }
    else if (newType === "OBJECT") {
      if (util.isSequence(value)) {
        var result = {};
        var i=0;
        for(var i=0; i<value.length; i++) {
          pair = value.get(i);
          // WHy don't we push a frame here? Blame RethinkDB...
          if (!util.isSequence(pair)) {
            throw new Error.ReqlRuntimeError('Expected type ARRAY but found '+util.typeOf(pair), self.frames);
          }
          result[pair.get(0)] = pair.get(1);
        }
        return result;
      }
      else if (util.isBinary(value)) {
        self.frames.push(0);
        throw new Error.ReqlRuntimeError("Cannot coerce BINARY to OBJECT", self.frames);
      }
      else if (util.isPlainObject(value)) {
        return value;
      }
      self.frames.push(0);
      throw new Error.ReqlRuntimeError("Cannot coerce "+currentType+" to OBJECT", self.frames);
    }
    else if (newType === "BINARY") {
      if (typeof value === 'string') {
        return {
          $reql_type$: 'BINARY',
          data: new Buffer(value).toString('base64')
        };
      }
      else if (util.isBinary(value)) {
        return value;
      }
      self.frames.push(0);
      throw new Error.ReqlRuntimeError("Cannot coerce "+currentType+" to BINARY", self.frames);
    }
    else {
      util.notAvailable(null, self);
    }
  });
};

Query.prototype.typeOf = function(args, options, internalOptions) {
  var self = this;
  util.assertArity(1, args, this);
  self.frames.push(0);
  return self.evaluate(args[0], internalOptions).then(function(value) {
    // r.js(...).typeOf() is a valid query
    util.assertJavaScriptResult(value, this);
    value = util.revertDatum(value);
    self.frames.pop();
    return util.typeOf(value, this);
  });
};

Query.prototype.update = function(args, options, internalOptions) {
  var self = this;
  util.assertArity(2, args, self);
  self.frames.push(0);
  return self.evaluate(options, internalOptions).bind({}).then(function(options) {
    this.options = options;
    return self.evaluate(args[0], internalOptions);
  }).then(function(selection) {
    util.assertType(selection, 'SELECTION', self);
    self.frames.pop();
    return selection.update(args[1], this.options, self, internalOptions);
  });
};

Query.prototype.delete = function(args, options, internalOptions) {
  var self = this;
  util.assertArity(1, args, self);
  self.frames.push(0);
  return self.evaluate(options, internalOptions).bind({}).then(function(options) {
    this.options = options;
    return self.evaluate(args[0], internalOptions);
  }).then(function(selection) {
    util.assertType(selection, 'SELECTION', self);
    self.frames.pop();
    return selection.delete(this.options, self);
  });
};

Query.prototype.replace = function(args, options, internalOptions) {
  var self = this;
  util.assertArity(2, args, self);
  self.frames.push(0);
  return self.evaluate(options, internalOptions).bind({}).then(function(options) {
    this.options = options;
    return self.evaluate(args[0], internalOptions);
  }).then(function(selection) {
    util.assertType(selection, 'SELECTION', self);
    self.frames.pop();
    return selection.replace(args[1], this.options, self, internalOptions);
  });

};

Query.prototype.insert = function(args, options, internalOptions) {
  var self = this;
  util.assertArity(2, args, self);
  self.frames.push(0);
  return self.evaluate(args[0], internalOptions).then(function(table) {
    util.assertType(table, 'TABLE', self);
    this.table = table;
    self.frames.pop();
    self.frames.push(1);
    return self.evaluate(args[1], internalOptions);
  }).then(function(docs) {
    this.docs = docs;
    self.frames.pop();
    return self.evaluate(options, internalOptions);
  }).then(function(options) {
    util.assertOptions(options, ['durability', 'return_changes', 'conflict'], self);
    return this.table.insert(this.docs, options, this);
  });
};

Query.prototype.dbCreate = function(args, options, internalOptions) {
  var self = this;
  util.assertArity(1, args, self);
  self.frames.push(0);
  return self.evaluate(args[0], internalOptions).then(function(dbName) {
    util.assertType(dbName, 'STRING', self);
    util.assertNoSpecialChar(dbName, 'Database', self);
    self.frames.pop();

    if (self.server.databases[dbName] != null) {
      throw new Error.ReqlRuntimeError("Database `"+dbName+"` already exists", self.frames);
    }
    self.server.databases[dbName] = new Database(dbName);
    return {
      config_changes: [{
        new_val: {
          id: self.server.databases[dbName].id,
          name: dbName
        },
        old_val: null
      }],
      dbs_created: 1,
    };
  });
};


Query.prototype.dbDrop = function(args, options, internalOptions) {
  var self = this;
  util.assertArity(1, args, this);

  self.frames.push(0);
  return self.evaluate(args[0], internalOptions).bind({}).then(function(dbName) {
    util.assertType(dbName, 'STRING', self);
    util.assertNoSpecialChar(dbName, 'Database', self);
    self.frames.pop();

    if (self.server.databases[dbName] == null) {
      throw new Error.ReqlRuntimeError("Database `"+dbName+"` does not exist", self.frames);
    }

    var db = self.server.databases[dbName];
    delete self.server.databases[dbName];
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
    };
  });
};

Query.prototype.dbList = function(args, options, internalOptions) {
  var self = this;
  return Promise.resolve().then(function() {
    util.assertArity(0, args, self);
    return new Sequence(Object.keys(self.server.databases));
  });
};

Query.prototype.tableCreate = function(args, options, internalOptions) {
  var self = this;
  util.assertArityRange(1, 2, args, this);
  return self.evaluate(options, internalOptions).bind({}).then(function(options) {
    this.options = options;
    util.assertOptions(options, ['primary_key', 'durability', 'shards', 'replicas', 'primary_replica_tag'], self);
    if (args.length === 1) {
      this.dbProvided = false;
      if (self.options.db === undefined) {
        return self.db(['test'], {}, internalOptions);
      }
      else {
        return self.evaluate(self.options.db, internalOptions);
      }
    }
    else if (args.length === 2) {
      this.dbProvided = true;
      self.frames.push(0);
      return self.evaluate(args[0], internalOptions);
    }
  }).then(function(db) {
    this.db = db;
    util.assertType(this.db, 'DATABASE', self);
    if (this.dbProvided === false) {
      self.frames.push(0);
      return self.evaluate(args[0], internalOptions);
    }
    else {
      self.frames.pop();
      self.frames.push(1);
      return self.evaluate(args[1], internalOptions);
    }
  }).then(function(tableName) {
    util.assertType(tableName, 'STRING', self);
    util.assertNoSpecialChar(tableName, 'Table', self);
    self.frames.pop();
    if (this.db.tables[tableName] != null) {
      throw new Error.ReqlRuntimeError('Table `'+this.db.name+'.'+tableName+'` already exists', self.frames);
    }
    return this.db.tableCreate(tableName, options);
  });
};

Query.prototype.tableDrop = function(args, options, internalOptions) {
  var self = this;
  util.assertArityRange(1, 2, args, this);
  return self.evaluate(options, internalOptions).bind({}).then(function(options) {
    this.options = options;
    util.assertOptions(options, ['primary_key', 'durability', 'shards', 'replicas', 'primary_replica_tag'], self);
    if (args.length === 1) {
      this.dbProvided = false;
      if (self.options.db === undefined) {
        return self.db(['test'], {}, internalOptions);
      }
      else {
        return self.evaluate(self.options.db, internalOptions);
      }
    }
    else if (args.length === 2) {
      this.dbProvided = true;
      self.frames.push(0);
      return self.evaluate(args[0], internalOptions);
    }
  }).then(function(db) {
    this.db = db;
    util.assertType(this.db, 'DATABASE', self);
    if (this.dbProvided === false) {
      self.frames.push(0);
      return self.evaluate(args[0], internalOptions);
    }
    else {
      self.frames.pop();
      self.frames.push(1);
      return self.evaluate(args[1], internalOptions);
    }
  }).then(function(tableName) {
    util.assertType(tableName, 'STRING', self);
    util.assertNoSpecialChar(tableName, 'Table', self);
    self.frames.pop();
    if (this.db.tables[tableName] == null) {
      throw new Error.ReqlRuntimeError('Table `'+this.db.name+'.'+tableName+'` does not exist', self.frames);
    }
    var table = this.db.tables[tableName];
    delete this.db.tables[tableName];
    var indexes = Object.keys(table.indexes);
    for(var i=0; i<indexes.length; i++) {
      if (indexes[i] === table.options.primaryKey) {
        indexes.splice(i, 1);
      }
    }
    return {
      config_changes: [{
        old_val: {
          db: this.db.name,
          durability: "hard", //TODO Handle optarg
          write_acks: "majority", //TODO Handle optarg
          id: table.id,
          indexes: indexes,
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
    };
  });
};

Query.prototype.tableList = function(args, options, internalOptions) {
  var self = this;
  util.assertArityRange(0, 1, args, self);
  return Promise.resolve(options).bind({}).then(function(options) {
    this.options = options;
    var db;
    if (!Array.isArray(args) || args.length === 0) {
      this.dbProvided = false;
      if (self.options.db !== undefined) {
        db = self.evaluate(self.options.db, internalOptions);
      }
      else {
        db = Promise.resolve(self.server.databases['test']);
      }
    }
    else {
      this.dbProvided = true;
      self.frames.push(0);
      db = self.evaluate(args[0], internalOptions);
    }
    return db;
  }).then(function(db) {
    return new Sequence(Object.keys(db.tables));
  });
};

Query.prototype.sync = function(args, options, internalOptions) {
  var self = this;
  util.assertArity(1, args, self);
  self.frames.push(0);
  return self.evaluate(args[0], internalOptions).then(function(table) {
    util.assertType(table, 'TABLE', self);
    self.frames.pop();
    return {synced: 1};
  });
};

Query.prototype.grant = function() {
  throw new Error.ReqlRuntimeError("Grant is not supported by reqlite at the moment.", this.frames);
}

Query.prototype.indexCreate = function(args, options, internalOptions) {
  var self = this;
  util.assertArityRange(2, 3, args, self);
  return self.evaluate(options, internalOptions).bind({}).then(function(options) {
    util.assertOptions(options, ['multi', 'geo'], self);
    this.options = options;
    self.frames.push(0);
    return self.evaluate(args[0], internalOptions);
  }).then(function(table) {
    util.assertType(table, 'TABLE', self);
    self.frames.pop();
    this.table = table;
    return self.evaluate(args[1], internalOptions);
  }).then(function(name) {
    util.assertType(name, 'STRING', self);
    return this.table.indexCreate(name, args[2], options, self);
  });
};

Query.prototype.indexDrop = function(args, options, internalOptions) {
  var self = this;
  util.assertArity(2, args, self);
  self.frames.push(0);
  return self.evaluate(args[0], internalOptions).then(function(table) {
    util.assertType(table, 'TABLE', self);
    this.table = table;
    self.frames.pop();
    self.frames.push(1);
    return self.evaluate(args[1], internalOptions);
  }).then(function(index) {
    util.assertType(index, 'STRING', self);
    self.frames.pop();
    return this.table.indexDrop(index, self);
  });
};

Query.prototype.indexList = function(args, options, internalOptions) {
  var self = this;
  util.assertArity(1, args, self);
  self.frames.push(0);
  return self.evaluate(args[0], internalOptions).then(function(table) {
    util.assertType(table, 'TABLE', this);
    self.frames.pop();
    return table.indexList();
  });
};

Query.prototype.indexRename = function(args, options, internalOptions) {
  var self = this;
  util.assertArity(3, args, self);
  self.frames.push(0);
  return self.evaluate(args[0], internalOptions).then(function(table) {
    util.assertType(table, 'TABLE', self);
    this.table = table;
    self.frames.pop();
    self.frames.push(1);
    return self.evaluate(args[1], internalOptions);
  }).then(function(oldIndex) {
    util.assertType(oldIndex, 'STRING', self);
    this.oldIndex = oldIndex;
    self.frames.pop();
    self.frames.push(2);
    return self.evaluate(args[2], internalOptions);
  }).then(function(newIndex) {
    util.assertType(newIndex, 'STRING', self);
    this.newIndex = newIndex;
    self.frames.pop();
    return self.evaluate(options, internalOptions);
  }).then(function(options) {
    util.assertOptions(options, ['overwrite'], self);
    return this.table.indexRename(this.oldIndex, this.newIndex, options, self);
  });
};

Query.prototype.indexStatus = function(args, options, internalOptions) {
  var self = this;
  util.assertArityRange(1, Infinity, args, self);
  self.frames.push(0);
  return self.evaluate(args[0], internalOptions).then(function(table) {
    util.assertType(table, 'TABLE', self);
    this.table = table;
    self.frames.pop();
    var indexes = [];
    for(var i=1; i<args.length; i++) {
      self.frames.push(i);
      var index = self.evaluate(args[i], internalOptions);
      indexes.push(index);
      self.frames.pop();
    }
    return Promise.all(indexes);
  }).then(function(indexes) {
    for(var i=0; i<indexes.length; i++) {
      self.frames.push(i+1);
      util.assertType(indexes[i], 'STRING', self);
      self.frames.pop();
    }
    return this.table.indexWait(indexes, self);
  });
};
Query.prototype.indexWait = Query.prototype.indexStatus;

Query.prototype.funcall = function(args, options, internalOptions) {
  var self = this;
  // FUNCALL, FN [ AR[], BODY]
  //  0       1  [ 0 [ ...],
  util.assertArityRange(1, Infinity, args, this);
  if (args.length === 1) {
    return self.evaluate(args[0], internalOptions);
  }
  var fn = args[0];
  var argsFn = util.getVarIds(fn);

  return Promise.map(args.slice(1), function(context, index) {
    self.frames.push(index+1);
    util.assertPreDatum(context, self);
    return self.evaluate(context, internalOptions).then(function(result) {
      util.assertType(result, 'DATUM', self);
      self.frames.pop();
      return result;
    });
  }, {concurrency: 1}).then(function(resolvedContext) {
    this.resolvedContext = resolvedContext;
    for(var i=0; i<resolvedContext.length; i++) {
      self.context[argsFn[i]] = resolvedContext[i];
    }
    if (argsFn.length > args.length-1) {
      self.frames.push(0);
      util.assertArity(args.length-1, argsFn, self, fn);
    }

    return self.evaluate(fn, internalOptions);
  }).then(function(result) {
    for(var i=0; i<argsFn.length; i++) {
      delete self.context[argsFn[i]];
    }
    return result;
  });
};

Query.prototype.branch = function(args, options, internalOptions) {
  // function(condition, rightBranch, falseBranch, internalOptions) {
  var self = this;
  util.assertArityRange(3, Infinity, args, self);
  self.frames.push(0);
  util.assertPreDatum(args[0], self);
  return self.evaluate(args[0], internalOptions).then(function(condition) {
    util.assertType(condition, 'DATUM', self);
    self.frames.pop();
    if (condition === false || condition === null) {
      if (util.isFunction(args[2])) {
        throw new Error.ReqlRuntimeError("query result must be of type datum, grouped_data, or stream (got function)", self.frames);
      }
      self.frames.push(2);
      return self.evaluate(args[2], internalOptions).then(function(result) {
        self.frames.pop();
        return result;
      });
    }
    else {
      if (util.isFunction(args[1])) {
        throw new Error.ReqlRuntimeError("Query result must be of type DATUM, GROUPED_DATA, or STREAM (got FUNCTION)", this.frames);
      }
      self.frames.push(1);
      return self.evaluate(args[1], internalOptions).then(function(result) {
        self.frames.pop();
        return result;
      });
    }
  });
};

Query.prototype.or = function(args, options, internalOptions) {
  var self = this;
  util.assertArityRange(0, Infinity, args, self);
  return Promise.reduce(args, function(left, right, index) {
    if (util.isTrue(left)) {
      return left;
    }
    self.frames.push(index);
    util.assertPreDatum(right, self);
    return self.evaluate(right, internalOptions).then(function(right) {
      util.assertType(right, 'DATUM', self);
      self.frames.pop();
      return right;
    });
  }, false).then(function(result) {
    return result;
  });

};

Query.prototype.and = function(args, options, internalOptions) {
  var self = this;
  util.assertArityRange(0, Infinity, args, self);
  return Promise.reduce(args, function(left, right, index) {
    if (!util.isTrue(left)) {
      return left;
    }
    self.frames.push(index);
    util.assertPreDatum(right, self);
    return self.evaluate(right, internalOptions).then(function(right) {
      util.assertType(right, 'DATUM', self);
      self.frames.pop();
      return right;
    });
  }, true);
};

Query.prototype.forEach = function(args, options, internalOptions) {
  var self = this;
  util.assertArity(2, args, self);
  self.frames.push(0);
  return self.evaluate(args[0], internalOptions).then(function(sequence) {
    self.frames.pop();
    util.toSequence(sequence, self);
    return sequence.forEach(args[1], self, internalOptions);
  });
};

Query.prototype.func = function(args, options, internalOptions) {
  var self = this;
  util.assertArity(2, args, self);
  var body = args[1];
  self.frames.push(1);
  return self.evaluate(body, internalOptions).then(function(result) {
    //Table is a valid for type for concatMap
    //util.assertType(result, 'DATUM', self);
    self.frames.pop();
    return result;
  });
};

Query.prototype.asc = function(args, options, internalOptions) {
  util.assertArity(1, args, this);
  return Promise.resolve(new Asc(args[0]));
};

Query.prototype.desc = function(args, options, internalOptions) {
  util.assertArity(1, args, this);
  return Promise.resolve(new Desc(args[0]));
};

Query.prototype.info = function(args, options, internalOptions) {
  var self = this;
  util.assertArity(1, args, self);
  if (util.isFunction(args[0])) {
    //TODO Return the protobuf version
    return {
      source_code: 'function (captures = [(no implicit)]) (args = [])...'
    };
  }
  self.frames.push(0);
  return self.evaluate(args[0], internalOptions).then(function(element) {
    self.frames.pop();
    //TODO Check if we are covering all cases in tests
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
      };
    }
    else if (util.isBinary(element)) {
      //TODO Refactor with count
      var length = element.data.length;
      var blocksOf78 = Math.floor(length/78);
      var remainderOf78 = length%78;
      var base64Digits = blocksOf78*76+remainderOf78;
      var blocksOf4 = Math.floor(base64Digits/4);
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
      };

    }
    else if (element === null) { // Since RethinkDB 2.1
      return {
        type: "NULL"
      };
    }
    else if (util.isDatum(element)) {
      return {
        type: util.typeOf(element),
        value: JSON.stringify(util.toDatum(element))
      };
    }
    return "STRING";
  });
};

Query.prototype.match = function(args, options, internalOptions) {
  var self = this;
  util.assertArity(2, args, self);
  self.frames.push(0);
  return self.evaluate(args[0], internalOptions).bind({}).then(function(str) {
    util.assertType(str, "STRING", self);
    self.frames.pop();
    this.str = str;
    self.frames.push(1);
    return self.evaluate(args[1], internalOptions);
  }).then(function(regexRaw) {
    util.assertType(regexRaw, "STRING", self);
    self.frames.pop();
    var flags;
    var components = regexRaw.match(/^\(\?([a-z]*)\)(.*)/);
    if ((Array.isArray(components)) && (components.length > 2)) {
      regexRaw = components[2];
      flags = components[1].split('');
      for(var i=0; i<flags.length; i++) {
        if ((flags[i] !== 'i') && (flags[i] !== 'm')) {
          throw new Error.ReqlRuntimeError('Reqlite support only the flags `i` and `m`, found `'+flags[i]+'`');
        }
      }
    }
    else {
      flags = [];
    }
    var regex = new RegExp(regexRaw, flags.join(''));
    var resultRegex = regex.exec(this.str);
    if (resultRegex === null) {
      return null;
    }
    var result = {};
    result.start = resultRegex.index;
    result.end = resultRegex.index+resultRegex[0].length;
    result.str = resultRegex[0];
    result.groups = resultRegex.splice(1);
    return result;
  });
};

// We do NOT want to lower/upper case composite characters.
var lowerToUpper = {};
var upperToLower = {};
var alphabetLower = 'abcdefghijklmnopqrstuvwxyz';
var alphabetUpper = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ';
for(var i=0; i<alphabetLower.length; i++) {
  lowerToUpper[alphabetLower[i]] = alphabetUpper[i];
  upperToLower[alphabetUpper[i]] = alphabetLower[i];
}
Query.prototype.upcase = function(args, options, internalOptions) {
  var self = this;
  util.assertArity(1, args, self);
  self.frames.push(0);
  return self.evaluate(args[0], internalOptions).then(function(str) {
    util.assertType(str, 'STRING', self);
    self.frames.pop();
    var result = '';
    for(var i=0; i<str.length; i++) {
      if (lowerToUpper.hasOwnProperty(str[i])) {
        result += lowerToUpper[str[i]];
      }
      else {
        result += str[i];
      }
    }
    return result;
  });
};

Query.prototype.downcase = function(args, options, internalOptions) {
  var self = this;
  util.assertArity(1, args, self);
  self.frames.push(0);
  return self.evaluate(args[0], internalOptions).then(function(str) {
    util.assertType(str, 'STRING', self);
    self.frames.pop();

    var result = '';
    for(var i=0; i<str.length; i++) {
      if (upperToLower.hasOwnProperty(str[i])) {
        result += upperToLower[str[i]];
      }
      else {
        result += str[i];
      }
    }
    return result;
  });
};

Query.prototype.sample = function(args, options, internalOptions) {
  var self = this;
  util.assertArity(2, args, self);
  self.frames.push(0);
  return self.evaluate(args[0], internalOptions).bind({}).then(function(sequence) {
    self.frames.pop();
    util.toSequence(sequence, self);
    this.sequence = sequence;
    self.frames.push(1);
    return self.evaluate(args[1], internalOptions);
  }).then(function(sample) {
    util.assertType(sample, 'NUMBER', self);
    self.frames.pop();
    if (sample < 0) {
      throw new Error.ReqlRuntimeError('Number of items to sample must be non-negative, got `'+sample+'`', self.frames);
    }
    return this.sequence.sample(sample, self);
  });
};

Query.prototype.default = function(args, options, internalOptions) {
  var self = this;
  util.assertArity(2, args, self);

  self.frames.push(0);
  return new Promise(function(resolve, reject) {
    return self.evaluate(args[0], internalOptions).then(function(value) {
      self.frames.pop();
      if (value === null || util.isMissingDoc(value)) {
        self.frames.push(1);
        return self.evaluate(args[1], internalOptions).then(function(value) {
          self.frames.pop();
          resolve(value);
        }).catch(function(err) {
          reject(err);
        });
      }
      else {
        resolve(value);
      }
    }).catch(function(err) {
      if (err.message.match(/^No attribute `/)
          || err.message.match(/^Index out of bounds/)) {
        // Pop the frames 0
        self.frames.pop();
        self.frames.push(1);
        self.evaluate(args[1], internalOptions).then(function(value) {
          self.frames.pop();
          resolve(value);
        }).catch(function(err) {
          reject(err);
        });
      }
      else {
        reject(err);
      }
    });
  });
};

Query.prototype.json = function(args, options, internalOptions) {
  var self = this;
  util.assertArity(1, args, self);
  self.frames.push(0);
  return self.evaluate(args[0], internalOptions).then(function(str) {
    util.assertType(str, "STRING", self);
    self.frames.pop();
    try {
      var value = JSON.parse(str);
    }
    catch(err) {
      throw new Error.ReqlRuntimeError("Failed to parse \""+str+"\" as JSON: Invalid value", this.frames);
    }
    var result = util.revertDatum(value);
    return result;
  });
};

Query.prototype.toJsonString = function(args, options, internalOptions) {
  var self = this;
  util.assertArity(1, args, self);
  self.frames.push(0);
  return self.evaluate(args[0], internalOptions).then(function(value) {
    util.assertType(value, 'DATUM', self);
    self.frames.pop();
    return JSON.stringify(util.toDatum(value));
  });
};

Query.prototype.ISO8601 = function(args, options, internalOptions) {
  var self = this;
  util.assertArity(1, args, self);
  self.frames.push(0);
  return self.evaluate(options, internalOptions).bind({}).then(function(options) {
    this.options = options;
    return self.evaluate(args[0], internalOptions);
  }).then(function(date) {
    util.assertType(date, 'STRING', self);
    self.frames.pop();
    var timezone = ReqlDate.getTimezone(date, this.options);
    return ReqlDate.iso8601(date, timezone, self);
  });
};

Query.prototype.toISO8601 = function(args, options, internalOptions) {
  var self = this;
  util.assertArity(1, args, self);
  self.frames.push(0);
  return self.evaluate(args[0], internalOptions).then(function(date) {
    util.assertTime(date, self, true);
    self.frames.pop();
    var momentDate = date.toMoment();
    if (momentDate.milliseconds() === 0) {
      return momentDate.utcOffset(momentDate.utcOffset()).format('YYYY-MM-DDTHH:mm:ssZ');
    }
    else {
      return momentDate.utcOffset(momentDate.utcOffset()).format('YYYY-MM-DDTHH:mm:ss.SSSZ');
    }
  });
};

Query.prototype.epochTime = function(args, options, internalOptions) {
  var self = this;
  util.assertArity(1, args, self);
  self.frames.push(0);
  return self.evaluate(args[0], internalOptions).then(function(epochTime) {
    util.assertType(epochTime, "NUMBER", self);
    self.frames.pop();
    return new ReqlDate(epochTime);
  });
};

Query.prototype.toEpochTime = function(args, options, internalOptions) {
  var self = this;
  util.assertArity(1, args, self);
  self.frames.push(0);
  return self.evaluate(args[0], internalOptions).then(function(date) {
    util.assertTime(date, self, true);
    self.frames.pop();
    return date.epoch_time;
  });
};

Query.prototype.now = function(args, options, internalOptions) {
  util.assertArity(0, args, this);
  return Promise.resolve(this.nowValue);
};

Query.prototype.inTimezone = function(args, options, internalOptions) {
  var self = this;
  util.assertArity(2, args, self);
  self.frames.push(0);
  return self.evaluate(args[0], internalOptions).bind({}).then(function(date) {
    util.assertTime(date, self);
    this.date = date;
    self.frames.pop();
    self.frames.push(1);
    return self.evaluate(args[1], internalOptions);
  }).then(function(timezoneStr) {
    self.frames.pop();
    util.assertType(timezoneStr, "STRING", self);
    var timezone = ReqlDate.convertTimezone(timezoneStr, self);
    return this.date.inTimezone(timezone);
  });
};

Query.prototype.during = function(args, options, internalOptions) {
  var self = this;
  util.assertArity(3, args, self);
  return self.evaluate(options, internalOptions).bind({}).then(function(options) {
    util.assertOptions(options, ['left_bound', 'right_bound'], self);
    this.options = options;
    this.options.left_bound = this.options.left_bound || 'closed';
    this.options.right_bound = this.options.right_bound || 'open';
    self.frames.push(0);
    return self.evaluate(args[0], internalOptions);
  }).then(function(date) {
    util.assertTime(date, self, true);
    this.date = date.toMoment();
    self.frames.pop();
    self.frames.push(1);
    return self.evaluate(args[1], internalOptions);
  }).then(function(left) {
    util.assertTime(left, self, true);
    this.left = left.toMoment();
    self.frames.pop();
    self.frames.push(2);
    return self.evaluate(args[2], internalOptions);
  }).then(function(right) {
    util.assertTime(right, self, true);
    this.right = right.toMoment();
    self.frames.pop();
    var result = this.date.isBetween(this.left, this.right);
    if (this.options.left_bound === "closed") {
      result = result || this.date.isSame(this.left);
    }
    if (this.options.right_bound === "closed") {
      result = result || this.date.isSame(this.right);
    }
    return result;
  });
};

Query.prototype.date = function(args, options, internalOptions) {
  var self = this;
  util.assertArity(1, args, self);
  self.frames.push(0);
  return self.evaluate(args[0], internalOptions).then(function(date) {
    self.frames.pop();
    util.assertTime(date, self);

    var momentDate = date.toMoment();
    momentDate.subtract(momentDate.hour(), 'hours');
    momentDate.subtract(momentDate.minute(), 'minutes');
    momentDate.subtract(momentDate.second(), 'seconds');
    momentDate.subtract(momentDate.millisecond(), 'milliseconds');

    return ReqlDate.fromMoment(momentDate, date.timezone, self);
  });
};

Query.prototype.timeOfDay = function(args, options, internalOptions) {
  var self = this;
  util.assertArity(1, args, self);
  self.frames.push(0);
  return self.evaluate(args[0], internalOptions).then(function(date) {
    util.assertTime(date, self, true);
    self.frames.pop();
    var momentDate = date.toMoment();
    var result = momentDate.hour()*60*60;
    result += momentDate.minute()*60;
    result += momentDate.second();
    result += momentDate.millisecond()/1000;
    return result;
  });
};

Query.prototype.timezone = function(args, options, internalOptions) {
  var self = this;
  util.assertArity(1, args, self);
  self.frames.push(0);
  return self.evaluate(args[0], internalOptions).then(function(date) {
    util.assertTime(date, self, true);
    self.frames.pop();
    return date.timezone;
  });
};

Query.prototype.year = function(args, options, internalOptions) {
  var self = this;
  util.assertArity(1, args, self);
  self.frames.push(0);
  return self.evaluate(args[0], internalOptions).then(function(date) {
    util.assertTime(date, self, true);
    self.frames.pop();
    var momentDate = date.toMoment();
    return momentDate.year();
  });
};

Query.prototype.month = function(args, options, internalOptions) {
  var self = this;
  util.assertArity(1, args, self);
  self.frames.push(0);
  return self.evaluate(args[0], internalOptions).then(function(date) {
    util.assertTime(date, self, true);
    self.frames.pop();

    //TODO Why don't we use moment here?
    var dateStr = util.dateToString(date);
    return util.monthToInt(dateStr.match(/[^\s]* ([^\s]*)/)[1]);
  });
};

Query.prototype.day = function(args, options, internalOptions) {
  var self = this;
  util.assertArity(1, args, self);
  self.frames.push(0);
  return self.evaluate(args[0], internalOptions).then(function(date) {
    util.assertTime(date, self, true);
    self.frames.pop();
    var momentDate = date.toMoment();
    return momentDate.date();
  });
};

Query.prototype.dayOfWeek = function(args, options, internalOptions) {
  var self = this;
  util.assertArity(1, args, self);
  self.frames.push(0);
  return self.evaluate(args[0], internalOptions).then(function(date) {
    util.assertTime(date, self, true);
    self.frames.pop();
    var momentDate = date.toMoment();
    return momentDate.day();
  });
};

Query.prototype.dayOfYear = function(args, options, internalOptions) {
  var self = this;
  util.assertArity(1, args, self);
  self.frames.push(0);
  return self.evaluate(args[0], internalOptions).then(function(date) {
    util.assertTime(date, self, true);
    self.frames.pop();
    var momentDate = date.toMoment();
    return momentDate.dayOfYear();
  });
};

Query.prototype.hours = function(args, options, internalOptions) {
  var self = this;
  util.assertArity(1, args, self);
  self.frames.push(0);
  return self.evaluate(args[0], internalOptions).then(function(date) {
    util.assertTime(date, self, true);
    self.frames.pop();
    var momentDate = date.toMoment();
    return momentDate.hours();
  });
};

Query.prototype.minutes = function(args, options, internalOptions) {
  var self = this;
  util.assertArity(1, args, self);
  self.frames.push(0);
  return self.evaluate(args[0], internalOptions).then(function(date) {
    util.assertTime(date, self, true);
    self.frames.pop();
    var momentDate = date.toMoment();
    return momentDate.minutes();
  });
};

Query.prototype.seconds = function(args, options, internalOptions) {
  var self = this;
  util.assertArity(1, args, self);
  self.frames.push(0);
  return self.evaluate(args[0], internalOptions).then(function(date) {
    util.assertTime(date, self, true);
    self.frames.pop();
    var momentDate = date.toMoment();
    return momentDate.seconds()+momentDate.milliseconds()/1000;
  });
};

Query.prototype.time = function(args, options, internalOptions) {
  //TODO Keep promisifying
  var self = this;
  self.frames.push(0);
  return self.evaluate(args[0], internalOptions).bind({}).then(function(year) {
    this.year = year;
    util.assertType(year, 'NUMBER', self);
    self.frames.pop();
    self.frames.push(1);
    return self.evaluate(args[1], internalOptions);
  }).then(function(month) {
    this.month = month;
    util.assertType(month, 'NUMBER', self);
    self.frames.pop();
    self.frames.push(2);
    return self.evaluate(args[2], internalOptions);
  }).then(function(day) {
    this.day = day;
    util.assertType(day, 'NUMBER', self);
    self.frames.pop();
    self.frames.push(3);
    return self.evaluate(args[3], internalOptions);
  }).then(function(hoursOrTimezone) {
    if (args.length === 4) {
      util.assertType(hoursOrTimezone, 'STRING', self);
      self.frames.pop();
      this.hours = 0;
      this.minutes = 0;
      this.seconds = 0;
      this.milliseconds = 0;
      this.timezone = hoursOrTimezone;
      return this;
    }
    else if (args.length === 7) {
      util.assertType(hoursOrTimezone, 'NUMBER', self);
      self.frames.pop();
      this.hours = hoursOrTimezone;
      self.frames.push(4);
      return self.evaluate(args[4], internalOptions).bind(this).then(function(minutes) {
        util.assertType(minutes, 'NUMBER', self);
        this.minutes = minutes;
        self.frames.pop();
        self.frames.push(5);
        return self.evaluate(args[5], internalOptions);
      }).then(function(seconds) {
        util.assertType(seconds, 'NUMBER', self);
        self.frames.pop();
        this.seconds = seconds;
        this.milliseconds = seconds - Math.floor(seconds);
        self.frames.push(6);
        return self.evaluate(args[6], internalOptions);
      }).then(function(timezone) {
        util.assertType(timezone, 'STRING', self);
        self.frames.pop();
        this.timezone = timezone;
        return this;

      });
    }
    else {
      self.frames.pop();
      throw new Error.ReqlCompileError('Expected between 4 and 7 arguments but found '+args.length+'.', self.frames);
    }
  }).then(function() {
    return ReqlDate.time(this.year, this.month, this.day, this.hours, this.minutes, this.seconds, this.milliseconds, this.timezone, self);
  });
};

Query.prototype.monday = function(args, options, internalOptions) {
  util.assertArity(0, args, this);
  return 1;
};
Query.prototype.tuesday = function(args, options, internalOptions) {
  util.assertArity(0, args, this);
  return 2;
};
Query.prototype.wednesday = function(args, options, internalOptions) {
  util.assertArity(0, args, this);
  return 3;
};
Query.prototype.thursday = function(args, options, internalOptions) {
  util.assertArity(0, args, this);
  return 4;
};
Query.prototype.friday = function(args, options, internalOptions) {
  util.assertArity(0, args, this);
  return 5;
};
Query.prototype.saturday = function(args, options, internalOptions) {
  util.assertArity(0, args, this);
  return 6;
};
Query.prototype.sunday = function(args, options, internalOptions) {
  util.assertArity(0, args, this);
  return 7;
};

Query.prototype.january = function(args, options, internalOptions) {
  util.assertArity(0, args, this);
  return 1;
};
Query.prototype.february = function(args, options, internalOptions) {
  util.assertArity(0, args, this);
  return 2;
};
Query.prototype.march = function(args, options, internalOptions) {
  util.assertArity(0, args, this);
  return 3;
};
Query.prototype.april = function(args, options, internalOptions) {
  util.assertArity(0, args, this);
  return 4;
};
Query.prototype.may = function(args, options, internalOptions) {
  util.assertArity(0, args, this);
  return 5;
};
Query.prototype.june = function(args, options, internalOptions) {
  util.assertArity(0, args, this);
  return 6;
};
Query.prototype.july = function(args, options, internalOptions) {
  util.assertArity(0, args, this);
  return 7;
};
Query.prototype.august = function(args, options, internalOptions) {
  util.assertArity(0, args, this);
  return 8;
};
Query.prototype.september = function(args, options, internalOptions) {
  util.assertArity(0, args, this);
  return 9;
};
Query.prototype.october = function(args, options, internalOptions) {
  util.assertArity(0, args, this);
  return 10;
};
Query.prototype.november = function(args, options, internalOptions) {
  util.assertArity(0, args, this);
  return 11;
};
Query.prototype.december = function(args, options, internalOptions) {
  util.assertArity(0, args, this);
  return 12;
};

Query.prototype.literal = function(args, options, internalOptions) {
  var self = this;
  util.assertArityRange(0, 1, args, self);
  self.frames.push(0);
  if (!Array.isArray(args) || args.length === 0) {
    return Promise.resolve(new Literal());
  }
  return self.evaluate(args[0], internalOptions).then(function(value) {
    self.frames.pop();
    //TODO Enforce value's type?
    return new Literal(value);
  });
};

Query.prototype.group = function(args, options, internalOptions) {
  // Arity is handle inside the group method
  var self = this;
  return self.evaluate(options, internalOptions).bind({}).then(function(options) {
    this.options = options;
    self.frames.push(0);
    return self.evaluate(args[0], internalOptions);
  }).then(function(sequence) {
    self.frames.pop();
    util.toSequence(sequence, this);
    if ((args.length === 1) && (typeof this.options.index !== 'string')) {
      throw new Error.ReqlRuntimeError('Cannot group by nothing', self.frames);
    }
    util.assertOptions(options, ['index'], self);
    if (util.isChanges(sequence)) {
      return Group.fromChanges(sequence, args.slice(1), this.options, self,  internalOptions)
    }
    else if (util.isTable(sequence)) {
      return Group.fromTable(sequence, args.slice(1), this.options, self,  internalOptions);
    }
    else if (util.isSequence(sequence)) {
      return Group.fromSequence(sequence, args.slice(1), this.options, self,  internalOptions);
    }
    //return sequence.group(args.slice(1), this.options, self,  internalOptions);
  });
};

Query.prototype.sum = function(args, options, internalOptions) {
  var self = this;
  util.assertArityRange(1, 2, args, self);
  self.frames.push(0);
  return self.evaluate(args[0], internalOptions).then(function(sequence) {
    self.frames.pop();
    return sequence.sum(args[1], self, internalOptions);
  });
};

Query.prototype.avg = function(args, options, internalOptions) {
  //TODO Enforce sequence type?
  var self = this;
  util.assertArityRange(1, 2, args, self);
  self.frames.push(0);
  return self.evaluate(args[0], internalOptions).then(function(sequence) {
    self.frames.pop();
    return sequence.avg(args[1], self, internalOptions);
  });
};

Query.prototype.min = function(args, options, internalOptions) {
  // Arity check is performed inside the function
  var self = this;
  self.frames.push(0);
  return self.evaluate(options, internalOptions).bind({}).then(function(options) {
    this.options = options;
    return self.evaluate(args[0], internalOptions);
  }).then(function(sequence) {
    self.frames.pop();
    var extra = (typeof this.options.index === 'string') ? [this.options] : [];
    util.assertArityRange(1, 2, args.concat(extra), self);
    return sequence.min(args[1], this.options, self, internalOptions);
  });
};

Query.prototype.max = function(args, options, internalOptions) {
  // Arity check is performed inside the function
  var self = this;
  self.frames.push(0);
  return self.evaluate(options, internalOptions).bind({}).then(function(options) {
    this.options = options;
    return self.evaluate(args[0], internalOptions);
  }).then(function(sequence) {
    self.frames.pop();
    var extra = (typeof this.options.index === 'string') ? [this.options] : [];
    util.assertArityRange(1, 2, args.concat(extra), self);
    return sequence.max(args[1], this.options, self, internalOptions);
  });
};


Query.prototype.split = function(args, options, internalOptions) {
  var self = this;
  util.assertArityRange(1, 3, args, self);
  self.frames.push(0);
  return self.evaluate(args[0], internalOptions).bind({}).then(function(str) {
    util.assertType(str, "STRING", self);
    self.frames.pop();
    if (args.length > 1) {
      self.frames.push(1);
      return self.evaluate(args[1], internalOptions).then(function(separator) {
        self.frames.pop();
        if (separator === null) {
          separator = /\s+/;
        }
        else { // RethinkDB doesn't frame that...
            util.assertType(separator, "STRING", self);
        }
        if (args.length > 2) {
          self.frames.push(2);
          return self.evaluate(args[2], internalOptions).then(function(limit) {
            util.assertType(limit, "NUMBER", self);
            self.frames.pop();
            var result = str.split(separator);
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
                result = result.slice(0, limit).concat(result.slice(limit).join(separator));
              }
            }
            return new Sequence(result);
          });
        }
        return new Sequence(str.split(separator));
      });
    }
    var separator = /\s+/;
    return new Sequence(str.split(separator));
  });
};

Query.prototype.ungroup = function(args, options, internalOptions) {
  var self = this;
  util.assertArity(1, args, self);
  self.frames.push(0);
  return self.evaluate(args[0], internalOptions).then(function(groups) {
    self.frames.pop();
    util.assertType(groups, "GROUP", self);
    return groups.ungroup();
  });
};

Query.prototype.random = function(args, options, internalOptions) {
  var self = this;
  util.assertArityRange(0, 2, args, self);
  if (internalOptions.deterministic === true) {
    // Uh, RethinkDB highlight the whole write query
    self.frames.pop();
    util.nonDeterministicOp(self);
  }

  return self.evaluate(options, internalOptions).bind({}).then(function(options) {
    this.options = options;
    util.assertOptions(options, ['float'], this);
    this.min = undefined;
    this.max = undefined;
    if (args.length > 0) {
      if (args.length === 1) {
        self.frames.push(1);
        return self.evaluate(args[0], internalOptions).bind(this).then(function(_max) {
          util.assertType(_max, "NUMBER", self);
          this.min = 0;
          this.max = _max;
          self.frames.pop();
          return _max;
        });
      }
      else if (args.length === 2) {
        return self.evaluate(args[0], internalOptions).bind(this).then(function(_min) {
          util.assertType(_min, "NUMBER", self);
          this.min = _min;
          self.frames.pop();
          self.frames.push(1);
          return self.evaluate(args[1], internalOptions);
        }).then(function(_max) {
          util.assertType(_max, "NUMBER", self);
          self.frames.pop();
          this.max = _max;
          return _max;
        });
      }
    }
  }).then(function() {
    if (args.length === 0) {
      return Math.random();
    }
    else {
      if (options.float !== true) {
        try {
          util.assertType(this.min, "INT", this);
        }
        catch(err) {
          throw new Error.ReqlRuntimeError("Lower bound ("+this.min+") could not be safely converted to an integer");
        }
        try {
          util.assertType(this.max, "INT", this);
        }
        catch(err) {
          throw new Error.ReqlRuntimeError("Upper bound ("+this.max+") could not be safely converted to an integer");
        }
      }

      if (this.min > this.max) {
        if (this.options.float !== true) {
          var temp = this.max;
          this.max = this.min;
          this.min = temp;
        }
        else {
          throw new Error.ReqlRuntimeError("Lower bound ("+this.min+") is not less than upper bound ("+this.max+")");
        }
      }

      if (this.options.float === true) {
        return this.min+Math.random()*(this.max-this.min);
      }
      else {
        return Math.floor(this.min+Math.random()*(this.max-this.min));
      }
    }
  });
};

Query.prototype.changes = function(args, options, internalOptions) {
  var self = this;
  util.assertArity(1, args, self);
  self.frames.push(0);
  return self.evaluate(args[0], internalOptions).bind({}).then(function(tableSlice) {
    self.frames.pop();
    this.tableSlice = tableSlice
    return self.evaluate(options, internalOptions);
  }).then(function(options) {
    return new Changes(this.tableSlice, self, options);
  });
};

Query.prototype.point = function(args, options, internalOptions) {
  var self = this;
  util.assertArity(2, args, self);
  self.frames.push(0);
  return self.evaluate(args[0], internalOptions).then(function(longitude) {
    util.assertType(longitude, 'NUMBER', self);
    self.frames.pop();
    this.longitude = longitude;
    self.frames.push(1);
    return self.evaluate(args[1], internalOptions);
  }).then(function(latitude) {
    util.assertType(latitude, 'NUMBER', self);
    self.frames.pop();
    return new ReqlGeometry('Point', [this.longitude, latitude], self);
  });
};

Query.prototype.circle = function(args, options, internalOptions) {
  var self = this;
  util.assertArity(2, args, self);
  self.frames.push(0);
  return self.evaluate(args[0], internalOptions).then(function(center) {
    self.frames.pop();
    if (util.isGeometry(center)) {
      this.center = new Sequence(center.coordinates, {});
    }
    else {
      util.assertPointCoordinates(center, this);
      this.center = center;
    }
    self.frames.push(1);
    return self.evaluate(args[1], internalOptions);
  }).then(function(radius) {
    util.assertType(radius, 'NUMBER', self);
    self.frames.pop();
    var coordinates = util.generateCircle(this.center, radius, 32);
    return new ReqlGeometry('Polygon', coordinates, self);
  });
};

Query.prototype.line = function(args, options, internalOptions) {
  var self = this;
  util.assertArityRange(2, Infinity, args, self);
  return Promise.map(args, function(arg, index) {
    self.frames.push(index);
    return self.evaluate(arg, internalOptions).then(function(result) {
      self.frames.pop();
      return result;
    });
  }, {concurrency: 1}).then(function(coordinates) {
    for(var i=0; i<coordinates.length; i++) {
      if (util.isGeometry(coordinates[i])) {
        coordinates[i] = coordinates[i].coordinates;
      }
      else {
        util.assertPointCoordinates(coordinates[i], self);
        coordinates[i] = util.toDatum(coordinates[i]);
      }
    }
    return new ReqlGeometry('LineString', coordinates, self);
  });
};

Query.prototype.fill = function(args, options, internalOptions) {
  var self = this;
  util.assertArity(1, args, self);
  self.frames.push(0);
  return self.evaluate(args[0], internalOptions).then(function(line) {
    self.frames.pop();
    var coordinates = line.coordinates;
    if ((coordinates[0][0] !== coordinates[coordinates.length-1][0])
        || (coordinates[0][1] !== coordinates[coordinates.length-1][1])) {
      coordinates.push(coordinates[0]);
    }
    return new ReqlGeometry('Polygon', [coordinates], self);
  });
};

Query.prototype.geojson = function(args, options, internalOptions) {
  var self = this;
  util.assertArity(1, args, self);
  self.frames.push(0);
  return self.evaluate(args[0], internalOptions).then(function(geojson) {
    self.frames.pop();
    return ReqlGeometry.geojson(geojson, self);
  });
};

Query.prototype.toGeojson = function(args, options, internalOptions) {
  var self = this;
  util.assertArity(1, args, self);
  self.frames.push(0);
  return self.evaluate(args[0], internalOptions).then(function(geometry) {
    self.frames.pop();
    return geometry.toGeojson(self);
  });
};

Query.prototype.getIntersecting = function(args, options, internalOptions) {
  var self = this;
  util.assertArity(2, args, self);
  self.frames.push(0);
  return self.evaluate(options, internalOptions).bind({}).then(function(options) {
    this.options = options;
    return self.evaluate(args[0], internalOptions);
  }).then(function(table) {
    util.assertType(table, 'TABLE', self);
    this.table = table;
    self.frames.pop();
    self.frames.push(1);
    return self.evaluate(args[1], internalOptions);
  }).then(function(geometry) {
    util.assertType(geometry, 'GEOMETRY', self);
    self.frames.pop();
    return this.table.getIntersecting(geometry, this.options, self, internalOptions);
  });
};

Query.prototype.getNearest = function(args, options, internalOptions) {
  var self = this;
  util.assertArity(2, args, self);
  self.frames.push(0);
  return self.evaluate(options, internalOptions).bind({}).then(function(options) {
    this.options = options;
    return self.evaluate(args[0], internalOptions);
  }).then(function(table) {
    util.assertType(table, 'TABLE', self);
    this.table = table;
    self.frames.pop();
    self.frames.push(1);
    return self.evaluate(args[1], internalOptions);
  }).then(function(geometry) {
    self.frames.pop();
    // RethinkDB doesn't frame that...
    try {
      util.assertType(geometry, 'GEOMETRY', self);
    }
    catch(err) {
      util.assertType(geometry, 'ARRAY', self);
    }
    return this.table.getNearest(geometry, this.options, self, internalOptions);
  });
};

Query.prototype.includes = function(args, options, internalOptions) {
  var self = this;
  util.assertArity(2, args, this);
  self.frames.push(0);
  return self.evaluate(args[0], internalOptions).bind({}).then(function(geometry1) {
    if (!util.isGeometry(geometry1) && !util.isSequence(geometry1)) {
      util.cannotPerformOp('includes', geometry1, self);
    }
    self.frames.pop();
    this.geometry1 = geometry1;
    self.frames.push(1);
    return self.evaluate(args[1], internalOptions);
  }).then(function(geometry2) {
    util.assertType(geometry2, 'GEOMETRY', self);
    self.frames.pop();
    return this.geometry1.includes(geometry2, self);
  });
};

Query.prototype.intersects = function(args, options, internalOptions) {
  var self = this;
  util.assertArity(2, args, this);
  self.frames.push(0);
  return self.evaluate(args[0], internalOptions).bind({}).then(function(geometry1) {
    if (!util.isGeometry(geometry1) && !util.isSequence(geometry1)) {
      util.cannotPerformOp('intersects', geometry1, self);
    }
    self.frames.pop();
    this.geometry1 = geometry1;
    self.frames.push(1);
    return self.evaluate(args[1], internalOptions);
  }).then(function(geometry2) {
    util.assertType(geometry2, 'GEOMETRY', self);
    self.frames.pop();
    return this.geometry1.intersects(geometry2, self);
  });
};

Query.prototype.polygon = function(args, options, internalOptions) {
  var self = this;
  return Promise.map(args, function(arg, index) {
    self.frames.push(index);
    return self.evaluate(arg, internalOptions).then(function(result) {
      self.frames.pop();
      return util.toDatum(result);
    });
  }, {concurrency: 1}).then(function(coordinates) {
    if ((coordinates[0][0] !== coordinates[coordinates.length-1][0])
        || (coordinates[0][1] !== coordinates[coordinates.length-1][1])) {
      coordinates.push(coordinates[0]);
    }
    return new ReqlGeometry('Polygon', [coordinates], this);
  });
};

Query.prototype.polygonSub = function(args, options, internalOptions) {
  var self = this;
  util.assertArity(2, args, self);
  self.frames.push(0);
  return self.evaluate(args[0], internalOptions).bind({}).then(function(outerPolygon) {
    util.assertType(outerPolygon, 'GEOMETRY', self);
    if (outerPolygon.type !== 'Polygon') {
      throw new Error.ReqlRuntimeError("Expected a Polygon but found a "+outerPolygon.type, self.frames);
    }
    if (outerPolygon.coordinates.length > 1) {
      throw new Error.ReqlRuntimeError("Expected a Polygon with only an outer shell.  This one has holes", self.frames);
    }
    self.frames.pop();
    this.outerPolygon = outerPolygon;
    self.frames.push(1);
    return self.evaluate(args[1], internalOptions);
  }).then(function(innerPolygon) {
    util.assertType(innerPolygon, 'GEOMETRY', self);
    if (innerPolygon.type !== 'Polygon') {
      throw new Error.ReqlRuntimeError("Expected a Polygon but found a "+innerPolygon.type, self.frames);
    }
    self.frames.pop();
    return new ReqlGeometry(
        'Polygon',
        [this.outerPolygon.coordinates[0], innerPolygon.coordinates[0]],
        self);
  });

};

Query.prototype.distance = function(args, options, internalOptions) {
  var self = this;
  util.assertArity(2, args, self);
  self.frames.push(0);
  return self.evaluate(options, internalOptions).bind({}).then(function(options) {
    this.options = options;
    return self.evaluate(args[0], internalOptions);
  }).then(function(from) {
    util.assertType(from, 'GEOMETRY', self);
    self.frames.pop();
    this.from = from;
    self.frames.push(1);
    return self.evaluate(args[1], internalOptions);
  }).then(function(to) {
    util.assertType(to, 'GEOMETRY', self);
    self.frames.pop();
    return this.from.distance(to, this.options);
  });
};

Query.prototype.http = function(args, options, internalOptions) {
  var self = this;
  self.frames.push(0);
  return self.evaluate(args[0], internalOptions).then(function(url) {
    self.frames.pop();
    return new Promise(function(resolve, reject) {
      var options = {
        url: url,
        headers: {
          "Accept": "*/*" ,
          "Accept-Encoding": "deflate;q=1, gzip;q=0.5" ,
          "Host": "httpbin.org" ,
          "User-Agent": "RethinkDB/2.0.2"
        }
      };
      request.get(options, function(err, httpResponse, body) {
        if (err) {
          reject(err);
        }
        else {
          //TODO Handle more options
          var response = body;
          if (httpResponse.headers['content-type'] === 'application/json') {
            response = JSON.parse(response);
          }
          resolve(response);
        }
      });
    });
  });
};

module.exports = Query;
