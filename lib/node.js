// For testing
var Server = require(__dirname+"/index.js");
var argv = require('minimist')(process.argv.slice(2));
var options = {};
for(var key in argv) {
  switch(key) {
    case "o":
      options['driver-port'] = parseInt(argv[key], 10)+28015;
      break;
    case "port-offset":
      options['driver-port'] = parseInt(argv[key], 10)+28015;
      break;
    //case "debug"
    //case "silent"
    default:
      options[key] = argv[key];
  }
}

// Start a new server
new Server(options);

