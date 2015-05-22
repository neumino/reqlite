## Contributing

You can contribute in different ways:

- Open an issue. If you do, please provide enough information for us to be
able to reproduce the bug:

  - The query that failed.
  - The expected output.
  - The data in your table if your query involve accessing data from a table.

- Submit a pull requests. Before submiting a pull request:

  - Make sure that the tests pass.
  - Follow the existing coding style, there are no strong rules at the moment, but please
  try to keep a similar style.
  - If you fix a bug, add a test. This make merging the request easier since with Travis, we
  won't have to locally test your code


## How Reqlite works?

You first need to understand how queries are represented in ReQL. [This document](http://rethinkdb.com/docs/writing-drivers/)
should give a good introduction - you can skip the begining about opening a TCP connection and the token/identification
mechanism.

Given a query, we evaluate it by recursing as needed.
The main function you are probably interested is `Query.prototype.evaluate`.

## How the tests work?

You can add a test by adding the following code:

```js
it('<method tested> - <num of the test>', function(done) {
  var query = r.expr(1) // Write your query here
  compare(query, done);
});
```

The method `compare` will execute the query against Reqlite and RethinkDB and compare the output.
If your output includes generated keys, random numbers, you can provide a third argument to `compare`
to alter the results:

```js
it('<method tested> - <num of the test>', function(done) {
  var query = r.table('test').insert({}) // Write your query here
  compare(query, done, function(result) {
    delete result.generated_keys;
    return result;
  });
});
```

## Have more questions?

Open an issue such that we can improve the docs, the code and everything else.

## Have fun!

It's the most important part :-)
