var moment = require('moment');

function ReqlDate(epoch_time, timezone) {
  this.$reql_type$ = 'TIME';
  this.epoch_time = epoch_time;
  if (timezone === undefined) {
    this.timezone = '+00:00';
  }
  else {
    this.timezone = timezone;
  }
}
module.exports = ReqlDate;

var Error = require("./error.js");

ReqlDate.buildFromDatum = function(value, query) {
  return new ReqlDate(value.epoch_time, value.timezone);
};

ReqlDate.prototype.toDatum = function() {
  return {
    $reql_type$: this.$reql_type$,
    epoch_time: this.epoch_time,
    timezone: this.timezone
  };
};

ReqlDate.now = function() {
  return new ReqlDate(Date.now()/1000, '+00:00');
};

ReqlDate.time = function(year, month, day, hours, minutes, seconds, milliseconds, timezone, query) {
  milliseconds = milliseconds*1000; // 0.234 -> 234
  var millisecondsStr = ''+milliseconds;
  while (millisecondsStr.length < 5) {
    millisecondsStr += '0';
  }
  millisecondsStr = millisecondsStr.slice(0, 3);
  var dateStr = ''+year+'-'+month+'-'+day+' '+hours+':'+minutes+':'+seconds+'.'+millisecondsStr+' '+timezone;

  var date = moment(dateStr, 'YYYY-M-D H:m:s.SSS ZZ');
  return new ReqlDate(date.unix()+date.milliseconds()/1000, ReqlDate.convertTimezone(timezone, query));
};

ReqlDate.fromMoment = function(date, timezone, query) {
  return new ReqlDate(date.unix()+date.milliseconds()/1000, ReqlDate.convertTimezone(timezone, query));
};

ReqlDate.iso8601 = function(date, timezone, query) {
  var dateObject = new Date(date);
  if (isNaN(dateObject.getTime())) {
    query.frames.push(0);
    var match = [
      'digit', // year
      'digit',
      'digit',
      'digit',
      'dash',
      'digit', // month
      'digit',
      'dash',
      'digit', // day
      'digit',
      'T',
      'digit', // hours
      'digit',
      'colon',
      'digit', // minutes
      'digit',
      'colon',
      'digit', // seconds
      'digit',
      '+-',
      'digit', // hours timezone
      'digit',
      'colon',
      'digit', // minutes timezone
      'digit',
    ];
    //TODO Fix error handling. RethinkDB looks for T etc.
    for(var i=0; i<match.length; i++) {
      if (match[i] === 'digit') {
        if (/\d/.test(date[i]) === false) {
          if (i <= 10) {
            throw new Error.ReqlRuntimeError('Invalid date string `'+date.slice(0, 10)+'` (got `'+date[i]+'` but expected a '+match[i]+')', query.frames);
          }
          else {
            throw new Error.ReqlRuntimeError('Invalid date string `'+date+'` (got `'+date[i]+'` but expected a '+match[i]+')', query.frames);
          }
        }
      }
      else if (match[i] === 'colon') {
        if (/:/.test(date[i]) === false) {
          throw new Error.ReqlRuntimeError('Invalid date string `'+date+'` (got `'+date[i]+'` but expected a '+match[i]+')', query.frames);
        }
      }
      else if (match[i] === 'dash') {
        if (/-/.test(date[i]) === false) {
          throw new Error.ReqlRuntimeError('Invalid date string `'+date+'` (got `'+date[i]+'` but expected a '+match[i]+')', query.frames);
        }
      }
      else if (match[i] === 'T') {
        if (/T/.test(date[i]) === false) {
          throw new Error.ReqlRuntimeError('Invalid date string `'+date+'` (got `'+date[i]+'` but expected a '+match[i]+')', query.frames);
        }
      }
      else if (match[i] === '+-') {
        if (/[+-]/.test(date[i]) === false) {
          throw new Error.ReqlRuntimeError('Invalid date string `'+date+'` (got `'+date[i]+'` but expected a '+match[i]+')', query.frames);
        }
      }
    }
    throw new Error.ReqlRuntimeError('Err -_-', query.frames);
  }
  return new ReqlDate(dateObject.getTime()/1000, timezone);
};

ReqlDate.getTimezone = function(date, options) {
  options = options || {};

  if (date.match(/Z$/)) {
    return "+00:00";
  }
  else {
    var regexResult = date.match(/[+-]{1}[0-9]{2}:[0-9]{2}$/);
    if ((Array.isArray(regexResult)) && (regexResult.length > 0)) {
      return regexResult[0];
    }
    else if (options.default_timezone !== undefined) {
      return options.default_timezone;
    }
    else {
      throw new Error.ReqlRuntimeError("ISO 8601 string has no time zone, and no default time zone was provided", frames);
    }
  }
};

ReqlDate.prototype.inTimezone = function(timezone) {
  return new ReqlDate(this.epoch_time, timezone);
};

ReqlDate.prototype.toMoment = function() {
  return moment(this.epoch_time, 'X').utcOffset(this.timezone);
};


ReqlDate.convertTimezone = function(timezone, query) {
  //TODO Mimic better errors
  if (timezone === "Z") {
    return "+00:00";
  }
  //TODO Safeguard against null
  else if (timezone.indexOf(":") === -1) { // TODO Refactor these errors...
    throw new Error.ReqlRuntimeError('Garbage characters `'+timezone[timezone.length-1]+'` at end of timezone string `'+timezone+'`', query.frames);
  }
  else if ((timezone[0] !== '-') && (timezone[0] !== '+')) {
    throw new Error.ReqlRuntimeError('Timezone `'+timezone+'` does not start with `-` or `+`', query.frames);
  }
  else if (!/^\d$/.test(timezone[1])) {
    throw new Error.ReqlRuntimeError('Invalid date string `'+timezone+'` (got `'+timezone[1]+'` but expected a digit)', query.frames);
  }
  else if (!/^\d$/.test(timezone[2])) {
    throw new Error.ReqlRuntimeError('Invalid date string `'+timezone+'` (got `'+timezone[2]+'` but expected a digit)', query.frames);
  }
  else if (timezone.length < 6) {
    //throw new Error.ReqlRuntimeError('Truncated date string `'+timezone+'`', query.frames);
    throw new Error.ReqlRuntimeError('Invalid time zone string `'+timezone+'`. Valid time zones are `+hh:mm`, `-hh:mm`, `+hhmm`, `-hhmm`, `+hh`, `-hh` and `Z`', query.frames);
  }
  else if (!/^\d$/.test(timezone[4])) {
    throw new Error.ReqlRuntimeError('Invalid date string `'+timezone+'` (got `'+timezone[4]+'` but expected a digit)', query.frames);
  }
  else if (!/^\d$/.test(timezone[5])) {
    throw new Error.ReqlRuntimeError('Invalid date string `'+timezone+'` (got `'+timezone[5]+'` but expected a digit)', query.frames);
  }
  else if (timezone.length > 6) {
    throw new Error.ReqlRuntimeError('Garbage characters `'+timezone.slice(6)+'` at end of timezone string `'+timezone+'`', query.frames);
  }

  return timezone;

};

