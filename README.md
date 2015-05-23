Reqlite - RethinkDB in JavaScript
=====

[![Build Status](https://travis-ci.org/neumino/reqlite.svg?branch=master)](https://travis-ci.org/neumino/reqlite)

## What is it?

Reqlite is an implementation of a ReQL server in JavaScript. Meaning that
you can connect to reqlite with the RethinkDB driver and send queries (create
a table, insert documents, filter them, open a chanfeed etc.

```
node ./lib/node.js
```

## Does it work?

Most of the methods work. There are currently 2000+ tests passing.

## Why?

The main reasons why this project was started were:

- Provide an easy way for node developers to test their code without having
to start a RethinkDB server.
- Maybe Meteor will use it to build a mini-rethinkdb - See [meteor-rethinkdb](https://github.com/Slava/meteor-rethinkdb)
- Use it on Windows instead of running a Docker container in Vagrant
- Because it's kind of fun :-)


## Version of RethinkDB supported

It currently tries to match RethinkDB 2.0.x.


## Run tests

Most of the tests run queries against RethinkDB and reqlite at the same time, and compare the output.
Start RethinkDB on port `28015`
```
rethinkdb
```

And reqlite on port `28016`:
```
node ./lib/node.js --port-offset 1
```

Then run:
```
npm test
```

## Roadmap

There are tons of things left to do!
See the [issues tracker](https://github.com/neumino/reqlite/issues)


## Who did that?

Mostly [Michel Tu](https://github.com/neumino), but hopefully people will love this project and send tons
of pull requests!


## Contribute

See [CONTRIBUTING.md](https://github.com/neumino/reqlite/blob/master/CONTRIBUTING.md), don't be shy :)

See also the [contributors](https://github.com/neumino/reqlite/graphs/contributors)

## Contact

Michel Tu:
- [neumino@freenode.org](irc://irc.freenode.org/rethinkdb)
- [@neumino](https://twitter.com/neumino)
- [orphee@gmail.com](orphee@gmail.com)


## License

MIT, see the [LICENSE FILE](https://github.com/neumino/reqlite/blob/master/LICENSE)


## Note

This is a personal project and has nothing to do with my current employer (whoever that is) or
a previous one (whoever that is).
