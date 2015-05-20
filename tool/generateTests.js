var values = [
  '"foo"',
  '""',
  '0',
  '1',
  '-1',
  '10000',
  'true',
  'false',
  '[]',
  '[1,2,3]',
  '[["foo", 2]]',
  '[["foo", 2], ["bar", 1]]',
  '{}',
  '{foo: "bar"}',
  '{foo: "bar", buzz: 1}',
  'null',
  'r.now()',
  'function() { return 1 }',
  'r.db(TEST_DB).table(TEST_TABLE)',
  'r.binary(new Buffer("hello"))',
]
var types = [
  '"array"',
  '"string"',
  '"object"',
  '"array"',
  '"binary"'
]

var count = 1;
for(var i in values) {
    var value1 = values[i]
    console.log("  it('info - "+count+"', function(done) {");
    console.log("    var query = r.expr("+value1+").info()");
    console.log("    compare(query, done)");
    console.log("  })");
    console.log("");
    count++;
}
