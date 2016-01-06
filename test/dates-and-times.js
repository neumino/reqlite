var config = require('./../config.js');

var r = require('rethinkdb');
var assert = require('assert');

var connections = {};
var TEST_DB = 'reqlitetest';
var TEST_TABLE = 'reqlitetestaggregation';
var MISSING_ID = 'nonExistingId';
var MISSING_FIELD = 'nonExistingField';
var MISSING_INDEX = 'nonExistingIndex';

var compare = require('./util.js').generateCompare(connections);

describe('dates-and-times.js', function(){
  before(function(done) {
    setTimeout(function() { // Delay for nodemon to restart the server
      r.connect(config.rethinkdb).bind({}).then(function(conn) {
        connections.rethinkdb = conn;
        return r.connect(config.reqlite);
      }).then(function(conn) {
        connections.reqlite = conn;
        this.query = r.dbCreate(TEST_DB);
        return this.query.run(connections.rethinkdb);
      }).catch(function() { // ignore errors
      }).finally(function() {
        return this.query.run(connections.reqlite);
      }).catch(function() { // ignore errors
      }).finally(function() {
        done();
      });
    }, 500);
  });

  it('now - 1', function(done) {
    var query = r.now();
    compare(query, done, function(result) {
      return result instanceof Date;
    });
  });

  it('now - 2', function(done) {
    var query = r.now();
    compare(query, done, function(result) {
      // There should be less than one second between the two queries...
      // So this should be safe
      return Math.floor(result.getTime()/1000);
    });
  });

  it('now - 3', function(done) {
    var query = r.now().do(function(x) {
      return r.now().eq(x);
    });
    compare(query, done);
  });

  it('now - 4', function(done) {
    var query = r.now().add(1).sub(r.now());
    compare(query, done);
  });

  it('now - 5', function(done) {
    var query = r.now().sub(r.now().sub(1));
    compare(query, done);
  });

  it('time - 1', function(done) {
    var query = r.time(1986, 11, 3, 'Z');
    compare(query, done);
  });

  it('time - 2', function(done) {
    var query = r.time(1986, 1, 3, 'Z');
    compare(query, done);
  });

  it('time - 3', function(done) {
    var query = r.time(1986, 1, 3, 12, 3, 2.45, 'Z');
    compare(query, done);
  });

  it('time - 4', function(done) {
    var query = r.time(1986, 1, 3, 12, 3, 2.45, '+02:34');
    compare(query, done);
  });

  it('time - 5', function(done) {
    var query = r.time(1986, 1, 3, 12, 3, 2.45, '-02:34');
    compare(query, done);
  });

  it('time - 6', function(done) {
    var query = r.time(1986, 1, 3, '-02:34');
    compare(query, done);
  });

  it('time - 7', function(done) {
    var query = r.time(1986, 1, 3, '+02:34');
    compare(query, done);
  });

  it('time - 8', function(done) {
    var query = r.time(1986, 1, 3, '02:34');
    compare(query, done);
  });

  it('time - 9', function(done) {
    var query = r.time(1986, 1, 3, '-1:23');
    compare(query, done);
  });

  it('time - 10', function(done) {
    var query = r.time(1986, 1, 3, '-01234');
    compare(query, done);
  });

  it('time - 11', function(done) {
    var query = r.time(1986, 1, 3, '-01:3');
    compare(query, done);
  });

  it('time - 12', function(done) {
    var query = r.time(1986, 1, 3, '-01:123');
    compare(query, done);
  });

  it('time - 12', function(done) {
    var query = r.time(1986, 1, 3, '-01:12.3');
    compare(query, done);
  });

  it('time - 13', function(done) {
    var query = r.time(1986, 1, 3, '-01:a2');
    compare(query, done);
  });

  it('time - 14', function(done) {
    var query = r.time(1986, 1, 3, '-01:2a');
    compare(query, done);
  });

  it('time - 15', function(done) {
    var query = r.time('1986', 1, 3, '-01:2a');
    compare(query, done);
  });

  it('time - 16', function(done) {
    var query = r.time(1986, '1', 3, '-01:2a');
    compare(query, done);
  });

  it('time - 17', function(done) {
    var query = r.time(1986, 1, '3', '-01:2a');
    compare(query, done);
  });

  it('time - 18', function(done) {
    var query = r.time(1986, 1, 3, 2);
    compare(query, done);
  });

  it('time - 19', function(done) {
    var query = r.time(1986, 1, 3, '10', 12, 13, '-01:00');
    compare(query, done);
  });

  it('time - 19', function(done) {
    var query = r.time(1986, 1, 3, 10, '12', 13, '-01:00');
    compare(query, done);
  });

  it('time - 19', function(done) {
    var query = r.time(1986, 1, 3, 10, 12, '13', '-01:00');
    compare(query, done);
  });

  it('time - 20', function(done) {
    var query = r.time(1986, 1, 3, 10, 12, 13, 2);
    compare(query, done);
  });

  it('time - 21', function(done) {
    var query = r.time(1986, 11, 3);
    compare(query, done);
  });

  it('epochTime - 1', function(done) {
    var query = r.epochTime(Date.now());
    compare(query, done);
  });

  it('epochTime - 2', function(done) {
    var query = r.epochTime(123456);
    compare(query, done);
  });

  it('epochTime - 3', function(done) {
    var query = r.epochTime('foo');
    compare(query, done);
  });

  it('epochTime - 4', function(done) {
    var query = r.epochTime(200, 200);
    compare(query, done);
  });

  it('epochTime - 5', function(done) {
    var query = r.epochTime(-3000);
    compare(query, done);
  });

  it('iso8601 - 1', function(done) {
    var query = r.ISO8601('1986-11-03T08:30:00-07:00');
    compare(query, done);
  });

  it('iso8601 - 2', function(done) {
    var query = r.ISO8601('1986-11-03T08:30:00+00:00');
    compare(query, done);
  });

  it('iso8601 - 3', function(done) {
    var query = r.ISO8601('1986-11-03T08:30:00+04:30');
    compare(query, done);
  });

  it('iso8601 - 4', function(done) {
    var query = r.ISO8601('19x6-11-03T08:30:00+04:30');
    compare(query, done);
  });

  it('iso8601 - 5', function(done) {
    var query = r.ISO8601('1986-x1-03T08:30:00+04:30');
    compare(query, done);
  });

  it('iso8601 - 6', function(done) {
    var query = r.ISO8601('1986-11-x3T08:30:00+04:30');
    compare(query, done);
  });

  it('iso8601 - 7', function(done) {
    var query = r.ISO8601('1986-11-03T08:30:00+04:30');
    compare(query, done);
  });

  it('iso8601 - 8', function(done) {
    var query = r.ISO8601(r.args(['1986-11-03T08:30:00+04:30', '1986-11-03T08:30:00+04:30']));
    compare(query, done);
  });

  it('iso8601 - 9', function(done) {
    var query = r.ISO8601(2);
    compare(query, done);
  });

  it('inTimezone - 1', function(done) {
    var query = r.ISO8601('1986-11-03T08:30:00+04:30').inTimezone('+04:00').hours();
    compare(query, done);
  });

  it('inTimezone - 2', function(done) {
    var query = r.ISO8601('1986-11-03T08:30:00+04:30').inTimezone('-03:00').hours();
    compare(query, done);
  });

  it('inTimezone - 3', function(done) {
    var query = r.ISO8601('1986-11-03T08:30:00+04:30').inTimezone(3).hours();
    compare(query, done);
  });

  it('timezone - 1', function(done) {
    var query = r.ISO8601('1986-11-03T08:30:00+04:30').inTimezone('+04:00');
    compare(query, done);
  });

  it('timezone - 2', function(done) {
    var query = r.ISO8601('1986-11-03T08:30:00+04:30').inTimezone('-03:00');
    compare(query, done);
  });

  it('timezone - 3', function(done) {
    var query = r.expr({timezone: 1}).inTimezone('-03:00');
    compare(query, done, function(error) {
      var message = error.split(':')[0];
      assert(message.length > 0);
      return message;
    });
  });

  it('lt - 1', function(done) {
    var query = r.epochTime(1).lt(r.epochTime(0));
    compare(query, done);
  });

  it('lt - 2', function(done) {
    var query = r.epochTime(1).lt(r.epochTime(1));
    compare(query, done);
  });

  it('lt - 3', function(done) {
    var query = r.epochTime(1).lt(r.epochTime(2));
    compare(query, done);
  });

  it('lt - 4', function(done) {
    var query = r.epochTime(1).lt(new Date(0));
    compare(query, done);
  });

  it('lt - 5', function(done) {
    var query = r.epochTime(1).lt(new Date(1));
    compare(query, done);
  });

  it('lt - 6', function(done) {
    var query = r.epochTime(1).lt(new Date(2));
    compare(query, done);
  });

  it('eq - 1', function(done) {
    var query = r.epochTime(1).eq(r.epochTime(0));
    compare(query, done);
  });

  it('eq - 2', function(done) {
    var query = r.epochTime(1).eq(r.epochTime(1));
    compare(query, done);
  });

  it('eq - 3', function(done) {
    var query = r.epochTime(1).eq(new Date(0));
    compare(query, done);
  });

  it('eq - 4', function(done) {
    var query = r.epochTime(1).eq(new Date(1));
    compare(query, done);
  });

  it('eq - 5', function(done) {
    var query = r.time(2016, 1, 1, 'Z').inTimezone('+08:00').eq(new Date(Date.UTC(2016, 0, 1)));
    compare(query, done);
  });

  it('eq - 6', function(done) {
    var query = r.time(2016, 1, 1, 'Z').inTimezone('+08:00').eq(r.time(2016, 1, 1, 'Z'))
    compare(query, done);
  });

  it('eq - 7', function(done) {
    var query = r.time(2016, 1, 1, 8, 0, 0, 'Z').inTimezone('+08:00').eq(new Date(Date.UTC(2016, 0, 1)));
    compare(query, done);
  });

  it('eq - 8', function(done) {
    var query = r.time(2016, 1, 1, 8, 0, 0, 'Z').inTimezone('+08:00').eq(r.time(2016, 1, 1, 'Z'))
    compare(query, done);
  });

  it('during - 1', function(done) {
    var query = r.time(2013, 12, 4, 'Z').during(r.time(2013, 12, 1, "Z"), r.time(2013, 12, 10, "Z"));
    compare(query, done);
  });

  it('during - 2', function(done) {
    var query = r.time(2013, 11, 1, 'Z').during(r.time(2013, 12, 1, "Z"), r.time(2013, 12, 10, "Z"));
    compare(query, done);
  });

  it('during - 3', function(done) {
    var query = r.time(2013, 12, 14, 'Z').during(r.time(2013, 12, 1, "Z"), r.time(2013, 12, 10, "Z"));
    compare(query, done);
  });

  it('during - 4', function(done) {
    var query = r.time(2013, 12, 1, 'Z').during(r.time(2013, 12, 1, "Z"), r.time(2013, 12, 10, "Z"));
    compare(query, done);
  });

  it('during - 5', function(done) {
    var query = r.time(2013, 12, 1, 'Z').during(r.time(2013, 12, 1, "Z"), r.time(2013, 12, 10, "Z"), {leftBound: 'open'});
    compare(query, done);
  });

  it('during - 5', function(done) { // Testing that options are actually being evaluated
    var query = r.time(2013, 12, 1, 'Z').during(
        r.time(2013, 12, 1, "Z"), r.time(2013, 12, 10, "Z"),
        {leftBound: r.expr('close').add('d')});
    compare(query, done);
  });

  it('during - 6', function(done) {
    var query = r.time(2013, 12, 1, 'Z').during(r.time(2013, 12, 1, "Z"), r.time(2013, 12, 10, "Z"), {leftBound: 'closed'});
    compare(query, done);
  });

  it('during - 7', function(done) {
    var query = r.time(2013, 12, 10, 'Z').during(r.time(2013, 12, 1, "Z"), r.time(2013, 12, 10, "Z"));
    compare(query, done);
  });

  it('during - 8', function(done) {
    var query = r.time(2013, 12, 10, 'Z').during(r.time(2013, 12, 1, "Z"), r.time(2013, 12, 10, "Z"), {rightBound: 'closed'});
    compare(query, done);
  });

  it('during - 9', function(done) {
    var query = r.time(2013, 12, 10, 'Z').during(r.time(2013, 12, 1, "Z"), r.time(2013, 12, 10, "Z"), {rightBound: 'open'});
    compare(query, done);
  });

  it('during - 10', function(done) {
    var query = r.time(2013, 12, 10, 'Z').during(r.args([r.time(2011, 12, 1, "Z"), r.time(2013, 12, 1, "Z"), r.time(2013, 12, 10, "Z")]), {rightBound: 'open'});
    compare(query, done);
  });

  it('during - 11', function(done) {
    var query = r.expr('foo').during(r.time(2013, 12, 1, "Z"), r.time(2013, 12, 10, "Z"), {rightBound: 'open'});
    compare(query, done);
  });

  it('during - 12', function(done) {
    var query = r.time(2013, 12, 10, 'Z').during('foo', r.time(2013, 12, 10, "Z"), {rightBound: 'open'});
    compare(query, done);
  });

  it('during - 13', function(done) {
    var query = r.time(2013, 12, 10, 'Z').during(r.time(2013, 12, 1, "Z"), 'bar', {rightBound: 'open'});
    compare(query, done);
  });

  it('during - 14', function(done) {
    var query = r.time(2013, 12, 10, 'Z').during(r.time(2013, 12, 1, "Z"), r.time(2013, 12, 10, "Z"), {foo: 'buzz'});
    compare(query, done);
  });

  it('date - 1', function(done) {
    var query = r.time(1986, 1, 3, 12, 3, 2.45, '-02:00').date();
    compare(query, done);
  });

  it('date - 2', function(done) {
    var query = r.time(1986, 1, 3, 12, 3, 2.45, '+00:00').date();
    compare(query, done);
  });

  it('date - 3', function(done) {
    var query = r.time(1986, 1, 3, 12, 3, 2.45, '-03:00').date();
    compare(query, done);
  });

  it('date - 4', function(done) {
    var query = r.expr('foo').date();
    compare(query, done, function(error) {
      var message = error.split(':')[0];
      assert(message.length > 0);
      return message;
    });
  });

  it('date - 5', function(done) {
    var query = r.time(1986, 1, 3, 12, 3, 2.45, '+00:00').date('foo');
    compare(query, done);
  });

  it('timeOfDay - 1', function(done) {
    var query = r.time(1986, 1, 3, 12, 3, 2.45, '-02:00').timeOfDay();
    compare(query, done);
  });

  it('timeOfDay - 2', function(done) {
    var query = r.time(1986, 1, 3, 12, 3, 2.45, '+00:00').timeOfDay();
    compare(query, done);
  });

  it('timeOfDay - 3', function(done) {
    var query = r.time(1986, 1, 3, 12, 3, 2.45, '+02:00').timeOfDay();
    compare(query, done);
  });

  it('timeOfDay - 4', function(done) {
    var query = r.expr('foo').timeOfDay();
    compare(query, done);
  });

  it('timeOfDay - 5', function(done) {
    var query = r.time(1986, 1, 3, 12, 3, 2.45, '+02:00').timeOfDay('foo');
    compare(query, done);
  });

  it('timezone - 1', function(done) {
    var query = r.time(1986, 1, 3, 12, 3, 2.45, '-02:00').timezone();
    compare(query, done);
  });

  it('timezone - 2', function(done) {
    var query = r.time(1986, 1, 3, 12, 3, 2.45, '-05:23').timezone();
    compare(query, done);
  });

  it('timezone - 3', function(done) {
    var query = r.time(1986, 1, 3, 12, 3, 2.45, '-05:23').timezone('foo');
    compare(query, done);
  });

  it('timezone - 4', function(done) {
    var query = r.expr('foo').timezone();
    compare(query, done);
  });

  it('year - 1', function(done) {
    var query = r.time(1986, 1, 3, 12, 3, 2.45, '+02:00').year();
    compare(query, done);
  });

  it('year - 2', function(done) {
    var query = r.time(1986, 1, 1, 0, 0, 0, '+00:00').year();
    compare(query, done);
  });

  it('year - 3', function(done) {
    var query = r.time(1986, 1, 1, 0, 0, 0, '-02:00').year();
    compare(query, done);
  });

  it('year - 4', function(done) {
    var query = r.expr('foo').year();
    compare(query, done);
  });

  it('year - 5', function(done) {
    var query = r.time(1986, 1, 1, 0, 0, 0, '-02:00').year('bar');
    compare(query, done);
  });

  it('month - 1', function(done) {
    var query = r.time(1986, 1, 1, 0, 0, 0, '+02:00').month();
    compare(query, done);
  });

  it('month - 2', function(done) {
    var query = r.time(1986, 1, 1, 0, 0, 0, '+00:00').month();
    compare(query, done);
  });

  it('month - 3', function(done) {
    var query = r.time(1986, 1, 1, 0, 0, 0, '-02:00').month();
    compare(query, done);
  });

  it('month - 4', function(done) {
    var query = r.time(1986, 1, 1, 0, 0, 0, '-02:00').month('foo');
    compare(query, done);
  });

  it('month - 5', function(done) {
    var query = r.expr(1986).month();
    compare(query, done);
  });


  it('day - 1', function(done) {
    var query = r.time(1986, 1, 1, 0, 0, 0, '+02:00').day();
    compare(query, done);
  });

  it('day - 2', function(done) {
    var query = r.time(1986, 1, 1, 0, 0, 0, '+00:00').day();
    compare(query, done);
  });

  it('day - 3', function(done) {
    var query = r.time(1986, 1, 1, 0, 0, 0, '-02:00').day();
    compare(query, done);
  });

  it('day - 4', function(done) {
    var query = r.expr('foo').day();
    compare(query, done);
  });

  it('day - 5', function(done) {
    var query = r.time(1986, 1, 1, 0, 0, 0, '-02:00').day('foo');
    compare(query, done);
  });

  it('dayOfWeek - 1', function(done) {
    var query = r.time(1986, 1, 1, 0, 0, 0, '+02:00').dayOfWeek();
    compare(query, done);
  });

  it('dayOfWeek - 2', function(done) {
    var query = r.time(1986, 1, 1, 0, 0, 0, '+00:00').dayOfWeek();
    compare(query, done);
  });

  it('dayOfWeek - 3', function(done) {
    var query = r.time(1986, 1, 1, 0, 0, 0, '-02:00').dayOfWeek();
    compare(query, done);
  });

  it('dayOfWeek - 4', function(done) {
    var query = r.expr('foo').dayOfWeek();
    compare(query, done);
  });

  it('dayOfWeek - 5', function(done) {
    var query = r.time(1986, 1, 1, 0, 0, 0, '-02:00').dayOfWeek('foo');
    compare(query, done);
  });

  it('dayOfYear - 1', function(done) {
    var query = r.time(1986, 1, 1, 0, 0, 0, '+02:00').dayOfYear();
    compare(query, done);
  });

  it('dayOfYear - 2', function(done) {
    var query = r.time(1986, 1, 1, 0, 0, 0, '+00:00').dayOfYear();
    compare(query, done);
  });

  it('dayOfYear - 3', function(done) {
    var query = r.time(1986, 1, 1, 0, 0, 0, '-02:00').dayOfYear();
    compare(query, done);
  });

  it('dayOfYear - 4', function(done) {
    var query = r.expr('foo').dayOfYear();
    compare(query, done);
  });

  it('dayOfYear - 5', function(done) {
    var query = r.time(1986, 1, 1, 0, 0, 0, '-02:00').dayOfYear('foo');
    compare(query, done);
  });

  it('hours - 1', function(done) {
    var query = r.time(1986, 1, 1, 0, 0, 0, '+02:00').hours();
    compare(query, done);
  });

  it('hours - 2', function(done) {
    var query = r.time(1986, 1, 1, 0, 0, 0, '+00:00').hours();
    compare(query, done);
  });

  it('hours - 3', function(done) {
    var query = r.time(1986, 1, 1, 0, 0, 0, '-02:00').hours();
    compare(query, done);
  });

  it('hours - 4', function(done) {
    var query = r.expr('foo').hours();
    compare(query, done);
  });

  it('hours - 5', function(done) {
    var query = r.time(1986, 1, 1, 0, 0, 0, '+02:00').hours('foo');
    compare(query, done);
  });

  it('minutes - 1', function(done) {
    var query = r.time(1986, 1, 1, 0, 0, 0, '+02:00').minutes();
    compare(query, done);
  });

  it('minutes - 2', function(done) {
    var query = r.time(1986, 1, 1, 0, 0, 0, '+00:00').minutes();
    compare(query, done);
  });

  it('minutes - 3', function(done) {
    var query = r.time(1986, 1, 1, 0, 0, 0, '-02:00').minutes();
    compare(query, done);
  });

  it('minutes - 4', function(done) {
    var query = r.expr('foo').minutes();
    compare(query, done);
  });

  it('minutes - 5', function(done) {
    var query = r.time(1986, 1, 1, 0, 0, 0, '+02:00').minutes('foo');
    compare(query, done);
  });

  it('seconds - 1', function(done) {
    var query = r.time(1986, 1, 1, 0, 0, 2.35, '+02:00').seconds();
    compare(query, done);
  });

  it('seconds - 2', function(done) {
    var query = r.time(1986, 1, 1, 0, 0, 2.35, '+00:00').seconds();
    compare(query, done);
  });

  it('seconds - 3', function(done) {
    var query = r.time(1986, 1, 1, 0, 0, 2.35, '-02:00').seconds();
    compare(query, done);
  });

  it('seconds - 4', function(done) {
    var query = r.expr('foo').seconds();
    compare(query, done);
  });

  it('seconds - 5', function(done) {
    var query = r.time(1986, 1, 1, 0, 0, 2.35, '-02:00').seconds('foo');
    compare(query, done);
  });

  it('toISO8601 - 1', function(done) {
    var query = r.time(1986, 1, 1, 0, 0, 2.35, '-02:00').toISO8601();
    compare(query, done);
  });

  it('toISO8601 - 2', function(done) {
    var query = r.time(1986, 1, 1, 0, 0, 2.35, '+00:00').toISO8601();
    compare(query, done);
  });

  it('toISO8601 - 3', function(done) {
    var query = r.time(1986, 1, 1, 0, 0, 2.35, '+02:00').toISO8601();
    compare(query, done);
  });

  it('toISO8601 - 4', function(done) {
    var query = r.expr('foo').toISO8601();
    compare(query, done);
  });

  it('toISO8601 - 5', function(done) {
    var query = r.time(1986, 11, 3, 10, 11, 12.32, 'Z').toISO8601();
    compare(query, done);
  });

  it('toISO8601 - 6', function(done) {
    var query = r.time(1986, 11, 3, 'Z').toISO8601();
    compare(query, done);
  });

  it('toISO8601 - 7', function(done) {
    var query = r.time(1986, 11, 3, 'Z').toISO8601('foo');
    compare(query, done);
  });

  it('toEpochTime - 1', function(done) {
    var query = r.time(1986, 1, 1, 0, 0, 2.35, '+02:00').toEpochTime();
    compare(query, done);
  });

  it('toEpochTime - 2', function(done) {
    var query = r.time(1986, 1, 1, 0, 0, 2.35, '+00:00').toEpochTime();
    compare(query, done);
  });

  it('toEpochTime - 3', function(done) {
    var query = r.time(1986, 1, 1, 0, 0, 2.35, '-02:00').toEpochTime();
    compare(query, done);
  });

  it('toEpochTime - 4', function(done) {
    var query = r.expr('foo').toEpochTime();
    compare(query, done);
  });

  it('toEpochTime - 5', function(done) {
    var query = r.time(1986, 1, 1, 0, 0, 2.35, '-02:00').toEpochTime('foo');
    compare(query, done);
  });

  it('constant time - 1', function(done) {
    var query = r.monday;
    compare(query, done);
  });
  it('constant time - 2', function(done) {
    var query = r.tuesday;
    compare(query, done);
  });
  it('constant time - 3', function(done) {
    var query = r.wednesday;
    compare(query, done);
  });
  it('constant time - 4', function(done) {
    var query = r.thursday;
    compare(query, done);
  });
  it('constant time - 5', function(done) {
    var query = r.friday;
    compare(query, done);
  });
  it('constant time - 6', function(done) {
    var query = r.saturday;
    compare(query, done);
  });
  it('constant time - 7', function(done) {
    var query = r.sunday;
    compare(query, done);
  });
  it('constant time - 8', function(done) {
    var query = r.january;
    compare(query, done);
  });
  it('constant time - 9', function(done) {
    var query = r.february;
    compare(query, done);
  });
  it('constant time - 10', function(done) {
    var query = r.march;
    compare(query, done);
  });
  it('constant time - 11', function(done) {
    var query = r.april;
    compare(query, done);
  });
  it('constant time - 12', function(done) {
    var query = r.may;
    compare(query, done);
  });
  it('constant time - 13', function(done) {
    var query = r.june;
    compare(query, done);
  });
  it('constant time - 14', function(done) {
    var query = r.july;
    compare(query, done);
  });
  it('constant time - 15', function(done) {
    var query = r.september;
    compare(query, done);
  });
  it('constant time - 16', function(done) {
    var query = r.october;
    compare(query, done);
  });
  it('constant time - 17', function(done) {
    var query = r.november;
    compare(query, done);
  });
  it('constant time - 18', function(done) {
    var query = r.december;
    compare(query, done);
  });
  /*
  */
});
