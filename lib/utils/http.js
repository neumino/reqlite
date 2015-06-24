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
