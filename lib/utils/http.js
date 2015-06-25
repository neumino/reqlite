var httpUtil = module.exports;

httpUtil.responseFormat = function (requestedFormat, contentType) {
  contentType = contentType.split(';')[0];

  if (requestedFormat === undefined || requestedFormat === 'auto') {
    switch (contentType) {
      case 'application/json':
        return 'json';
      case 'application/json-p':
      case 'text/json-p':
      case 'text/javascript':
        return 'jsonp';
      case 'application/octet-stream':
        return 'binary';
      default:
        var topLevel = contentType.split('/')[0];
        switch (topLevel) {
          case 'audio':
          case 'video':
          case 'image':
            return 'binary';
          default:
            return 'text';
        }
    }
  }

  return requestedFormat;
}

httpUtil.headers = function (headers) {
  if (headers === undefined) {
    headers = {};
  }
  // handle array of strings
  else if (Array.isArray(headers)) {
    headers = headers.reduce(function (prev, val) {
      var parts = val.split(/:\s*/);
      prev[parts[0]] = parts[1];
      return prev;
    });
  }

  // defaults
  headers['Accept'] = headers['Accept'] || '*/*';
  headers['Accept-Encoding'] = headers['Accept-Encoding'] || 'deflate;q=1, gzip;q=0.5';
  headers['User-Agent'] = headers['User-Agent'] || 'RethinkDB/2.0.2'; // TODO: read version from package.json

  return headers;
}
