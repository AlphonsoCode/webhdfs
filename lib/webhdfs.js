var extend = require('extend');
var util = require('util');
var url = require('url');
var querystring = require('querystring');
var request = require('request');
var StringReader = require('./string-reader');

function WebHDFS (opts) {
  if (!(this instanceof WebHDFS)) {
    return new WebHDFS(opts);
  }

  [ 'user', 'host', 'port', 'path' ].some(function iterate (property) {
    if (!opts.hasOwnProperty(property)) {
      throw new Error(
        util.format('Unable to create WebHDFS client: missing option %s', property)
      );
    }
  });

  this._opts = opts;
  this._url = {
    protocol: 'http',
    hostname: opts.host,
    port: parseInt(opts.port) || 80,
    pathname: opts.path
  };
}

WebHDFS.prototype._getOperationEndpoint = function _getOperationEndpoint (operation, path, params) {
  var endpoint = this._url;

  endpoint.pathname = this._opts.path + path;
  endpoint.search = querystring.stringify(extend({
    'op': operation,
    'user.name': this._opts.user
  }, params || {}));

  return url.format(endpoint);
};

WebHDFS.prototype._parseError = function _parseError (body) {
  var error = null;

  if (typeof body === 'string') {
    try {
      body = JSON.parse(body);
    } catch (err) {
      body = null;
    }
  }

  if (body && body.hasOwnProperty('RemoteException')) {
    error = body.RemoteException;
  } else {
    error = {
      message: 'Unknown error'
    };
  }

  return new Error(error.message);
};

WebHDFS.prototype._isRedirect = function _isRedirect (res) {
  return [ 301, 307 ].indexOf(res.statusCode) !== -1 &&
    res.headers.hasOwnProperty('location');
};

WebHDFS.prototype._isSuccess = function _isRedirect (res) {
  return [ 200, 201 ].indexOf(res.statusCode) !== -1;
};

WebHDFS.prototype._isError = function _isRedirect (res) {
  return [ 400, 401, 402, 403, 404, 500 ].indexOf(res.statusCode) !== -1;
};

WebHDFS.prototype._sendRequest = function _sendRequest (method, url, opts, callback) {
  if (typeof callback === 'undefined') {
    callback = opts;
    opts = {};
  }

  var self = this;
  return request(extend({
    method: method,
    url: url,
    json: true
  }, opts), function onComplete(err, res, body) {
    if (err) {
      return callback(err);
    }

    // Handle remote exceptions
    if (self._isError(res)) {
      return callback(self._parseError(body));
    } else if (self._isSuccess(res)) {
      return callback(err, res, body);
    } else {
      return callback(new Error('Unexpected redirect'), res, body);
    }
  });
};

WebHDFS.prototype.chmod = function writeFile (path, mode, callback) {

};

WebHDFS.prototype.mkdir = function writeFile (path, mode, callback) {
  if (typeof callback === 'undefined') {
    callback = mode;
    mode = null;
  }

  // Validate path
  if (!path || typeof path !== 'string') {
    throw new Error('path must be a string');
  }

  var endpoint = this._getOperationEndpoint('mkdirs', path, {
    permissions: mode || '0677'
  });

  return this._sendRequest('PUT', endpoint, function (err) {
    callback(err);
  });
};

WebHDFS.prototype.writeFile = function writeFile (path, data, append, opts, callback) {
  if (typeof append === 'function') {
    callback = append;
    append = false;
    opts = {};
  } else if (typeof append === 'object') {
    callback = opts;
    opts = append;
    append = false;
  } else if (typeof opts === 'function') {
    callback = opts;
    opts = {};
  }

  // Validate path
  if (!path || typeof path !== 'string') {
    throw new Error('path must be a string');
  }

  var error = null;
  var localStream = new StringReader(data);
  var remoteStream = this.createWriteStream(path, append, opts);

  localStream.pipe(remoteStream); // Pipe data

  // Handle events
  remoteStream.on('error', function onError (err) {
    error = err;
  });

  remoteStream.on('finish', function onFinish () {
    return callback(error);
  });

  return remoteStream;
};

WebHDFS.prototype.appendFile = function writeFile (path, data, opts, callback) {
  return this.writeFile(path, data, true, opts, callback);
};

WebHDFS.prototype.readFile = function readFile (path, callback) {
  var remoteFileStream = this.createReadStream(path);
  var data = [];

  remoteFileStream.on('error', function onError (err) {
    callback(err);
  });

  remoteFileStream.on('data', function onData (chunk) {
    data.push(chunk);
  });

  remoteFileStream.on('finish', function () {
    callback(null, Buffer.concat(data, data.length));
  });
};

WebHDFS.prototype.createWriteStream = function createWriteStream (path, append, opts) {
  if (typeof append === 'object') {
    opts = append;
    append = false;
  }

  // Validate path
  if (!path || typeof path !== 'string') {
    throw new Error('path must be a string');
  }

  var endpoint = this._getOperationEndpoint(append ? 'append' : 'create', path, extend({
    overwrite: true,
    permissions: '0677'
  }, opts));

  var self = this;
  var stream = null;
  var params = {
    method: append ? 'POST' : 'PUT',
    url: endpoint,
    json: true
  };

  var req = request(params, function (err, res, body) {
    if (err) {
      return callback(err);
    }

    // Handle redirect
    if (self._isRedirect(res)) {
      var upload = request(extend(params, { url: res.headers.location }), function (err, res, body) {
        if (err) {
          req.emit('error', err);
        }

        // Handle remote exceptions
        if (self._isError(res)) {
          req.emit('error', self._parseError(body));
        }

        req.emit('finish'); // Request is finished
      });

      stream.pipe(upload);
      stream.resume();
    }
  });

  req.on('pipe', function (src) {
    // Unpipe initial request
    src.unpipe(req);
    req.end();

    // Pause read stream
    stream = src;
    stream.pause();
  });

  return req;
};

WebHDFS.prototype.createReadStream = function createReadStream (path, opts) {
  // Validate path
  if (!path || typeof path !== 'string') {
    throw new Error('path must be a string');
  }

  var self = this;
  var endpoint = this._getOperationEndpoint('open', path, opts);
  var stream = null;
  var params = {
    method: 'GET',
    url: endpoint,
    json: true
  };

  var req = request(params);
  req.on('error', function (err) {
    req.emit('finish');
  });

  req.on('complete', function (err) {
    req.emit('finish');
  });

  req.on('response', function (res) {
    // Handle remote exceptions
    // Remove all data handlers and parse error data
    if (self._isError(res)) {
      req.removeAllListeners('data');
      req.on('data', function onData (data) {
        req.emit('error', self._parseError(data.toString()));
        req.end();
      });
    } else if (self._isRedirect(res)) {
      var download = request(params);

      download.on('complete', function (err) {
        req.emit('finish');
      });

      // Proxy data to original data handler
      // Not the nicest way but hey
      download.on('data', function onData (chunk) {
        req.emit('data', chunk);
      });

      // Handle subrequest
      download.on('response', function onResponse (res) {
        if (self._isError(res)) {
          download.removeAllListeners('data');
          download.on('data', function onData (data) {
            req.emit('error', self._parseError(data.toString()));
            req.end();
          });
        }
      });
    }

    // No need to interrupt the request
    // data will be automatically sent to the data handler
  });

  return req;
};

WebHDFS.prototype.unlink = function writeFile (path, recursive, callback) {
  if (typeof callback === 'undefined') {
    callback = recursive;
    recursive = null;
  }

  // Validate path
  if (!path || typeof path !== 'string') {
    throw new Error('path must be a string');
  }

  var endpoint = this._getOperationEndpoint('delete', path, {
    recursive: recursive || false
  });

  return this._sendRequest('DELETE', endpoint, function (err) {
    callback(err);
  });
};

WebHDFS.prototype.rmdir = WebHDFS.prototype.unlink;

module.exports = {
  createClient: function createClient (opts) {
    return new WebHDFS(extend({
      user: 'webuser',
      host: 'localhost',
      port: '50070',
      path: '/webhdfs/v1'
    }, opts));
  }
};