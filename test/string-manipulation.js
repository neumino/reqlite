var config = require('./../config.js');

var r = require('rethinkdb');
var assert = require('assert');

var connections = {};
var TEST_DB = 'reqlitetest';
var TEST_TABLE = 'reqliteteststringmanipulation';
var MISSING_ID = 'nonExistingId';
var MISSING_FIELD = 'nonExistingField';
var MISSING_INDEX = 'nonExistingIndex';
var COMPLEX_OBJECT = {
  id: 1,
  foo: 2,
  buzz: {
    hello: [10, 11, 12],
    world: {
      nested: 100,
      field: 200
    },
    bonjour: [
      {monde: 1000, ciao: 4000},
      {monde: 2000, ciao: 5000},
      {monde: 3000, ciao: 6000},
    ]
  },
};

var compare = require('./util.js').generateCompare(connections);

describe('string-manipulation.js', function(){
  before(function(done) {
    setTimeout(function() { // Delay for nodemon to restart the server
      r.connect(config.rethinkdb).bind({}).then(function(conn) {
        connections.rethinkdb = conn;
        return r.connect(config.reqlite);
      }).then(function(conn) {
        connections.reqlite = conn;
        done();
      });
    }, 500);
  });

  it('match - 1', function(done) {
    var query = r.expr('foo').match('foo');
    compare(query, done);
  });

  it('match - 2', function(done) {
    var query = r.expr('foo').match('bar');
    compare(query, done);
  });

  it('match - 3', function(done) {
    var query = r.expr('afoo').match('foo');
    compare(query, done);
  });

  it('match - 4', function(done) {
    var query = r.expr('afoobbb').match('foo');
    compare(query, done);
  });

  it('match - 5', function(done) {
    var query = r.expr('afoobbb').match('^afoo');
    compare(query, done);
  });

  it('match - 6', function(done) {
    var query = r.expr('afoobbb').match('^b');
    compare(query, done);
  });

  it('match - 7', function(done) {
    var query = r.expr('John').match('(?i)john');
    compare(query, done);
  });

  it('match - 8', function(done) {
    var query = r.expr('JOHN').match('(?i)john');
    compare(query, done);
  });

  it('match - 9', function(done) {
    var query = r.expr('JOHN').match('john');
    compare(query, done);
  });

  it('match - 10', function(done) {
    var query = r.expr(1).match('john');
    compare(query, done);
  });

  it('match - 11', function(done) {
    var query = r.expr('foo').match(1);
    compare(query, done);
  });

  it('match - 12', function(done) {
    var query = r.expr('aaafoo').match('a+foo');
    compare(query, done);
  });

  it('match - 13', function(done) {
    var query = r.expr('aaa\nfoo').match('a+\n?foo');
    compare(query, done);
  });

  it('match - 14', function(done) {
    var query = r.expr('aaafoo').match('a+\n?foo');
    compare(query, done);
  });

  it('match - 15', function(done) {
    var query = r.expr('aaafoo').match('a+\n?foo', 'bar');
    compare(query, done);
  });

  it('split - 1', function(done) {
    var query = r.expr("foo  bar bax").split();
    compare(query, done);
  });

  it('split - 2', function(done) {
    var query = r.expr("12,37,,22,").split(',');
    compare(query, done);
  });

  it('split - 3', function(done) {
    var query = r.expr("mlucy").split('');
    compare(query, done);
  });

  it('split - 4', function(done) {
    var query = r.expr("12,37,,22,").split(",", 3);
    compare(query, done);
  });

  it('split - 5', function(done) {
    var query = r.expr("foo  bar bax").split(null, 1);
    compare(query, done);
  });

  it('split - 6', function(done) {
    var query = r.expr("foo  bar  bax").split(null, 1);
    compare(query, done);
  });

  it('split - 7', function(done) {
    var query = r.expr("foo  bar  bax").split('foo', 'bar', 'buzz');
    compare(query, done);
  });

  it('split - 8', function(done) {
    var query = r.expr(2).split(null, 1);
    compare(query, done);
  });

  it('split - 9', function(done) {
    var query = r.expr('foo bar').split(2, 1);
    compare(query, done);
  });

  it('split - 10', function(done) {
    var query = r.expr('foo bar').split('foo', 'bar');
    compare(query, done);
  });

  it('split - 11', function(done) {
    var query = r.expr('foo bar buzz').split().count();
    compare(query, done);
  });

  it('upcase - 1', function(done) {
    var query = r.expr("Sentence about LaTeX.").upcase();
    compare(query, done);
  });

  it('upcase - 2', function(done) {
    var query = r.expr("Sentence about LaTeX.").upcase('bar');
    compare(query, done);
  });

  it('upcase - 3', function(done) {
    var query = r.expr(2).upcase('bar');
    compare(query, done);
  });

  it('upcase - 3', function(done) {
    var query = r.expr("C'est l'été.").upcase();
    compare(query, done);
  });

  it('downcase - 1', function(done) {
    var query = r.expr("Sentence about LaTeX.").downcase();
    compare(query, done);
  });

  it('downcase - 2', function(done) {
    var query = r.expr("Sentence about LaTeX.").downcase('bar');
    compare(query, done);
  });

  it('downcase - 3', function(done) {
    var query = r.expr(3).downcase('bar');
    compare(query, done);
  });

  it('downcase - 4', function(done) {
    var query = r.expr("C'ÉTÉ ouhYou").downcase();
    compare(query, done);
  });

  /*
  */

});
