/**
 * @module WebHDFS
 */

var extend = require('extend');
var util = require('util');
var $url = require('url');
var querystring = require('querystring');
var request = require('request');
var BufferStreamReader = require('buffer-stream-reader');
var $path   = require('path');
var log = require('log4js').getLogger('webhdfs');
log.level = 'info';

/**
 * Initializes new WebHDFS instance
 *
 * @param {Object} [opts]
 * @param {Object} [requestParams]
 * @returns {WebHDFS}
 *
 * @constructor
 */
function WebHDFS (opts, requestParams) {
  if (!(this instanceof WebHDFS)) {
    return new WebHDFS(opts, requestParams);
  }

  [ 'user', 'host', 'port', 'path' ].some(function iterate (property) {
    if (!opts.hasOwnProperty(property)) {
      throw new Error(
        util.format('Unable to create WebHDFS client: missing option %s', property)
      );
    }
  });

  this._requestParams = requestParams;

  if(!this._requestParams)
      this._requestParams = {};

  if( ! ('forever' in this._requestParams) )
    this._requestParams.forever = true;

  var hosts = [];
  opts.host.split(',').forEach(function(host){ // comma separated hosts for HA support
    hosts.push(host.trim(''));
  });
  opts.host = hosts;

  this._dn_crash_retry_limit_min = opts.dn_crash_retry_limit_min || 10; // default 10 minutes;
  this._dn_crash_retry_interval_ms = opts.dn_crash_retry_interval_ms || 100;

  this._opts = opts;
  this._curr_host_idx = 0;
  this._url = {
    protocol: opts.protocol || 'http',
    hostname: opts.host[this._curr_host_idx], // pick one host to startwith
    port: parseInt(opts.port) || 80,
    pathname: opts.path
  };
}

/**
 *  Switch to the next suitable host for retrying.
 *
 *  The switch happens if the reported index of host array by caller is same as the current index,
 *  otherwise the caller uses the same index.
 *
 *  @method _switchHost
 *
 *  @param {Int} Current host index as reported by the caller
 */
WebHDFS.prototype._switchHost = function(idx) {
    if(this._curr_host_idx == idx)
        this._curr_host_idx = !this._curr_host_idx + 0; // move to next index

    this._url.hostname    = this._opts.host[this._curr_host_idx];
}

/**
 * Generate WebHDFS REST API endpoint URL for given operation
 *
 * @method _getOperationEndpoint
 *
 * @param {String} operation WebHDFS operation name
 * @param {String} path
 * @param {Object} params
 *
 * @returns {String}
 * @private
 */
WebHDFS.prototype._getOperationEndpoint = function _getOperationEndpoint (operation, path, params) {
  var endpoint = this._url;

  endpoint.pathname = $path.join(this._opts.path, path);
  endpoint.search = querystring.stringify(extend({
    'op': operation,
    'user.name': this._opts.user
  }, params || {}));

  return $url.format(endpoint);
};

/**
 * Parse 'RemoteException' structure and return valid Error object
 *
 * @method _parseError
 *
 * @param {String|Object} body Response body
 * @param {Boolean} strict If set true then RemoteException must be present in the body
 * @returns {Error}
 * @private
 */
WebHDFS.prototype._parseError = function _parseError (body, strict, error, statusCode) {
  var error = error || null;
  log.info('webhdfs error is ', body, strict, error, statusCode);

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
    if (!strict) {
      error = error || { message: 'Unknown error' };
      if(statusCode)
        error.code = statusCode;
    }
  }

  return error ? error : null;
};

/**
 * Check if response is redirect
 *
 * @method _isRedirect
 *
 * @param res
 * @returns {Boolean}
 * @private
 */
WebHDFS.prototype._isRedirect = function _isRedirect (res) {
  return [ 301, 307 ].indexOf(res.statusCode) !== -1 &&
    res.headers.hasOwnProperty('location');
};

/**
 * Check if response is successful
 *
 * @method _isSuccess
 *
 * @param res
 * @returns {Boolean}
 * @private
 */
WebHDFS.prototype._isSuccess = function _isRedirect (res) {
  return [ 200, 201 ].indexOf(res.statusCode) !== -1;
};

/**
 * Check if response is error
 *
 * @method _isError
 *
 * @param res
 * @returns {Boolean}
 * @private
 */
WebHDFS.prototype._isError = function _isRedirect (res) {
  return [ 400, 401, 402, 403, 404, 500 ].indexOf(res.statusCode) !== -1;
};

/**
 * Send a request to WebHDFS REST API
 *
 * @method _sendRequest
 *
 * @param {String} method HTTP method
 * @param {String} url
 * @param {Object} opts Options for request
 * @param {Function} callback
 *
 * @returns {Object} request instance
 *
 * @private
 */
WebHDFS.prototype._sendRequest = function _sendRequest (method, url, opts, callback) {
  if (typeof callback === 'undefined')
  {
    callback = opts;
    opts = {};
  }

  var self      = this;
  var params    = extend({ method: method, url: url, json: true }, this._requestParams, opts);
  var req       = request(params, onComplete);
  // Remember for switchHost
  req._requested_host_idx = self._curr_host_idx;

  function onComplete(err, res, body)
  {
    /*
     * Ignore ECONNREFUSED thrown from namenode as it's handled later with retry to other namenode.
     * 50075 is datanode port
     * */
    if (err && !( err.code === 'ECONNREFUSED' && err.port != 50075 ) )
    {
        return callback && callback(err);
    }

    // Handle remote exceptions and `ECONNREFUSED` with retry
    if ((res && self._isError(res)) || err )
    {
      /*
       * No need to handle retry for datanode failure here as _sendRequest is used to query file
       * metadata which goes to a namenode.
       * */
      if(
            self._opts.host.length > 1 &&
            body && body.RemoteException && body.RemoteException.exception === 'StandbyException' &&
            !opts._retry
        )
      {
          self._switchHost(req._requested_host_idx);
          var urlJSON           = $url.parse(url);
          urlJSON.hostname      = self._url.hostname;
          delete urlJSON.host;
          url                   = $url.format(urlJSON);
          log.warn('[#sendRequest]: Retrying for url: ' + url);
          setTimeout(_sendRequest.bind(self), 0, method, url, extend({ _retry: true }, opts), callback);
          return;
      }
      else
        return callback && callback(self._parseError(body, null, err));
    }
    else if (self._isSuccess(res))
    {
      return callback && callback(err, res, body);
    }
    else
    {
      return callback && callback(new Error('Unexpected redirect'), res, body);
    }
  };

  return req;
};

/**
 * Change file permissions
 *
 * @method chmod
 *
 * @param {String} path
 * @param {String} mode
 * @param {Function} callback
 *
 * @returns {Object}
 */
WebHDFS.prototype.chmod = function chmod (path, mode, callback) {
  // Validate path
  if (!path || typeof path !== 'string') {
    throw new Error('path must be a string');
  }

  var endpoint = this._getOperationEndpoint('setpermission', path, extend({
    permission: mode
  }));

  return this._sendRequest('PUT', endpoint, function (err) {
    return callback && callback(err);
  });
};

/**
 * Change file owner
 *
 * @method chown
 *
 * @param {String} path
 * @param {String} uid User name
 * @param {String} gid Group name
 * @param {Function} callback
 *
 * @returns {Object}
 */
WebHDFS.prototype.chown = function chown (path, uid, gid, callback) {
  // Validate path
  if (!path || typeof path !== 'string') {
    throw new Error('path must be a string');
  }

  var endpoint = this._getOperationEndpoint('setowner', path, extend({
    owner: uid,
    group: gid
  }));

  return this._sendRequest('PUT', endpoint, function (err) {
    return callback && callback(err);
  });
};

/**
 * Read directory contents
 *
 * @method _readdir
 *
 * @param {String} path
 * @param {Function} callback
 *
 * @returns {Object}
 */
WebHDFS.prototype.readdir = function readdir (path, callback) {
  // Validate path
  if (!path || typeof path !== 'string') {
    throw new Error('path must be a string');
  }

  var endpoint = this._getOperationEndpoint('liststatus', path);
  return this._sendRequest('GET', endpoint, function (err, res, body) {
    if (err) {
      return callback && callback(err);
    }

    var files = [];
    if (res.body.hasOwnProperty('FileStatuses') &&
        res.body.FileStatuses.hasOwnProperty('FileStatus')) {

      files = res.body.FileStatuses.FileStatus;
      return callback && callback(null, files);
    } else {
      return callback && callback(new Error('Invalid data structure'));
    }
  });
};

/**
 * Make new directory
 *
 * @method mkdir
 *
 * @param {String} path
 * @param {String} [mode=0777]
 * @param {Function} callback
 *
 * @returns {Object}
 */
WebHDFS.prototype.mkdir = function mkdir (path, mode, callback) {
  if (typeof callback === 'undefined') {
    callback = mode;
    mode = null;
  }

  // Validate path
  if (!path || typeof path !== 'string') {
    throw new Error('path must be a string');
  }

  var endpoint = this._getOperationEndpoint('mkdirs', path, {
    permissions: mode || '0777'
  });

  return this._sendRequest('PUT', endpoint, function (err) {
    return callback && callback(err);
  });
};

/**
 * Rename path
 *
 * @method rename
 *
 * @param {String} path
 * @param {String} destination
 * @param {Function} callback
 *
 * @returns {Object}
 */
WebHDFS.prototype.rename = function rename (path, destination, callback) {
  // Validate path
  if (!path || typeof path !== 'string') {
    throw new Error('path must be a string');
  }

  var endpoint = this._getOperationEndpoint('rename', path, extend({
    destination: destination
  }));

  return this._sendRequest('PUT', endpoint, function (err) {
    return callback && callback(err);
  });
};

/**
 * Stat path
 *
 * @method stat
 *
 * @param {String} path
 * @param {Function} callback
 *
 * @returns {Object}
 */
WebHDFS.prototype.stat = function stat (path, callback) {
  // Validate path
  if (!path || typeof path !== 'string') {
    throw new Error('path must be a string');
  }

  var endpoint = this._getOperationEndpoint('getfilestatus', path);
  return this._sendRequest('GET', endpoint, function (err, res) {
    if (err) {
      return callback && callback(err);
    }

    if (res.body.hasOwnProperty('FileStatus')) {
      return callback && callback(null, res.body.FileStatus);
    } else {
      return callback && callback(new Error('Invalid data structure'));
    }
  });
};

/**
 * Check file existence
 * Wraps stat method
 *
 * @method stat
 * @see WebHDFS.stat
 *
 * @param {String} path
 * @param {Function} callback
 *
 * @returns {Object}
 */
WebHDFS.prototype.exists = function stat (path, callback) {
  return this.stat(path, function onStatResult (err, stats) {
    return callback(err || !stats ? false : true);
  });
};

/**
 * Write data to the file
 *
 * @method writeFile
 *
 * @param {String} path
 * @param {Buffer|String} data
 * @param {Boolean} [append] If set to true then append data to the file
 * @param {Object} [opts]
 * @param {Function} callback
 *
 * @returns {Object}
 */
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
  var localStream = new BufferStreamReader(data);
  var remoteStream = this.createWriteStream(path, append, opts);

  // Handle events
  remoteStream.once('error', function onError (err) {
    error = err;
  });

  remoteStream.once('finish', function onFinish () {
    return callback && callback(error);
  });

  localStream.pipe(remoteStream); // Pipe data

  return remoteStream;
};

/**
 * Append data to the file
 *
 * @see writeFile
 * @param {String} path
 * @param {Buffer|String} data
 * @param {Object} [opts]
 * @param {Function} callback
 *
 * @returns {Object}
 */
WebHDFS.prototype.appendFile = function appendFile (path, data, opts, callback) {
  return this.writeFile(path, data, true, opts, callback);
};

/**
 * Read data from the file
 *
 * @method readFile
 *
 * @fires Request#data
 * @fires WebHDFS#finish
 *
 * @param {String} path
 * @param {Function} callback
 *
 * @returns {Object}
 */
WebHDFS.prototype.readFile = function readFile (path, opts, callback) {
  if (!callback) {
    callback = opts;
    opts = {};
  }
  var self = this;
  var remoteFileStream = this.createReadStream(path, opts);
  var data = [];
  var error = null;
  var timeoutDuration = 1000 * 60 * 2; // 2 minutes default timeout
  if(opts.hasOwnProperty("timeout")) {
    timeoutDuration = opts.timeout;
  }
  var timedOut = false;

  var timeoutOperation = function() {
    timedOut = true;
    return callback && callback({error : "Operation timed out"});
  }
  var hdfsWatchdog = setTimeout(timeoutOperation, timeoutDuration);

  remoteFileStream.on('error', function onError (err) {
    /* Ignore all ECONNREFUSED error as we handle it in createReadStream and sendRequest
    *  appropriately.
    */
    if(!(err && err.code === 'ECONNREFUSED'))
      error = err;
  });

  remoteFileStream.on('data', function onData (chunk) {
    var hdfs_err = self._parseError(chunk.toString(), true);
    if(!timedOut) {
      clearTimeout(hdfsWatchdog);
      hdfsWatchdog = setTimeout(timeoutOperation, timeoutDuration);
      if(!hdfs_err)
        data.push(chunk);
      else
      {
        /*
         *  Reset data in case when exception was thrown while halfway reading through the data
         *  stream.
         * */
        data = [];
      }
    }
  });

  remoteFileStream.once('done', function () {
    if(!timedOut) {
      clearTimeout(hdfsWatchdog);
      hdfsWatchdog = null;
      if (!error) {
        return callback && callback(null, Buffer.concat(data));
      } else {
        return callback && callback(error);
      }
    }
  });
};

var emitError = function (instance, err) {
    const isErrorEmitted = instance.errorEmitted;

    if (!isErrorEmitted) {
      instance.emit('error', err);
      instance.emit('finish');
    }

    instance.errorEmitted = true;
  };

/**
 * Create writable stream for given path
 *
 * @example
 *
 * var WebHDFS = require('webhdfs');
 * var hdfs = WebHDFS.createClient();
 *
 * var localFileStream = fs.createReadStream('/path/to/local/file');
 * var remoteFileStream = hdfs.createWriteStream('/path/to/remote/file');
 *
 * localFileStream.pipe(remoteFileStream);
 *
 * remoteFileStream.on('error', function onError (err) {
 *   // Do something with the error
 * });
 *
 * remoteFileStream.on('finish', function onFinish () {
 *  // Upload is done
 * });
 *
 * @method createWriteStream
 * @fires WebHDFS#finish
 *
 * @param {String} path
 * @param {Boolean} [append] If set to true then append data to the file
 * @param {Object} [opts]
 *
 * @returns {Object}
 */
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
    permissions: '0777'
  }, opts));

  var self = this;
  var stream = null;
  var canResume = true;
  var params = extend({
    method: append ? 'POST' : 'PUT',
    url: endpoint,
    json: true,
    headers: { 'content-type': 'application/octet-stream' }
  }, this._requestParams);

  var req = request
            (
                params,
                function (err, res, body)
                {
                    // Handle redirect only if there was not an error (e.g. res is defined)
                    if (res && self._isRedirect(res)) {
                      var upload = request
                                   (
                                       extend(params, { url: res.headers.location }),
                                       function (err, res, body)
                                       {
                                           if (err || self._isError(res)) {
                                             if(err && err.code === 'ECONNREFUSED') // this one comes from datanode
                                             {
                                               req._retry_start_ms = Date.now();
                                               process.nextTick(
                                                self._doWriteStreamRetry.bind
                                                (
                                                    self, req, params,
                                                    append, path, opts, stream, err.port != 50075
                                                )
                                               );
                                             }
                                             else
                                             {

                                               emitError(req, err || self._parseError(body));
                                               req.end();
                                             }
                                             return;
                                           }

                                           if (res.headers.hasOwnProperty('location')) {
                                             return req.emit('finish', res.headers.location);
                                           } else {
                                             return req.emit('finish');
                                           }
                                        }
                                   );
                      self._canResume = true; // Enable resume

                      stream.pipe(upload);
                      stream.resume();
                    }

                    if (err || self._isError(res)) {
                      if(err &&  err.code === 'ECONNREFUSED') // this one comes from namenode
                      {
                        req._retry_start_ms = Date.now();
                        process.nextTick(
                                         self._doWriteStreamRetry.bind
                                         (
                                             self, req, params,
                                             append, path, opts, stream, err.port != 50075
                                         )
                                        );
                      }
                      else
                      {
                        emitError(req, err || self._parseError(body, null, null, res.statusCode));
                        req.end();
                      }
                      return;
                    }
                }
            );

  // remember for the switchHost
  req._requested_host_idx = self._curr_host_idx;

  req.on('pipe', function onPipe (src) {
    // Pause read stream
    stream = src;
    stream.pause();

    // This is not an elegant solution but here we go
    // Basically we don't allow pipe() method to resume reading input
    // and set internal _readableState.flowing to false
    self._canResume = false;
    stream.on('resume', function () {
      if (!self._canResume) {
        stream._readableState.flowing = false;
      }
    });

    // Unpipe initial request
    src.unpipe(req);
    req.end();
  });

  return req;
};

WebHDFS.prototype._retry_if_dn_failure = function(retry_func, req, body, err, ...retry_func_extra_args){
    var self = this;
    log.info(`[${retry_func.name}]: Received error after retrying ${req._curr_retry_idx} times `, err);
    if(err && err.code == 'ECONNREFUSED') // this one comes from namenode
    {
      if(err.port != 50075)
      {
        err.message   = 'No alive namenode found';
        err.exception = 'NoAliveNamenodeException';
        err.code      = 'NoAliveNamenode';
        emitError(req, err);
        req.end();
      }
      else
      {
          log.info(`[${retry_func.name}]: Time elapsed since first retry `,
                      Date.now() - req._retry_start_ms );
          log.info(`[${retry_func.name}]: Time left until giving up on retry`,
                  self._dn_crash_retry_limit_min * 60 * 1e3 - Date.now() + req._retry_start_ms );
          if(Date.now() - req._retry_start_ms > (self._dn_crash_retry_limit_min * 60 * 1e3))
          {
              err.message   = 'No alive datanode found';
              err.exception = 'NoAliveDatanodeException';
              err.code      = 'NoAliveDatanode';
              log.info(`[${retry_func.name}]: Timeout!!`);
              emitError(req, err);
              return;
          }
          setTimeout( retry_func,
                      self._dn_crash_retry_interval_ms,
                      req,
                      ...retry_func_extra_args
                    );
      }
    }
    else
      emitError(req, err || self._parseError(body));
    return;
}

WebHDFS.prototype._doWriteStreamRetry = function(req, params, append, path, opts, stream, namenode_failure){

    var self = this;
    req._curr_retry_idx = req._curr_retry_idx || 0;
    req._curr_retry_idx++;

    if(namenode_failure)
    {
        self._switchHost(req._requested_host_idx);
    }

    params.url = self._getOperationEndpoint(append ? 'append' : 'create', path, extend({
                 overwrite: true,
                 permissions: '0777'
               }, opts));
    log.warn('[#dowriteStreamRetry]: Retrying with url: '+ params.url);

    var _retry_req = request
                     (
                        params,
                        function (err, res, body)
                        {
                            // Handle redirect only if there was not an error (e.g. res is defined)
                            if (res && self._isRedirect(res)) {
                              var upload = request
                                           (
                                               extend(params, { url: res.headers.location }),
                                               function (err, res, body)
                                               {
                                                   if (err || self._isError(res)) {
                                                     self._retry_if_dn_failure(self._doWriteStreamRetry.bind(self),
                                                                               req,
                                                                               body,
                                                                               err,
                                                                               params,
                                                                               append,
                                                                               path,
                                                                               opts,
                                                                               stream,
                                                                               err && err.port != 50075);
                                                     return;
                                                   }

                                                   if (res.headers.hasOwnProperty('location')) {
                                                     return req.emit('finish', res.headers.location);
                                                   } else {
                                                     return req.emit('finish');
                                                   }
                                                }
                                           );

                              self._canResume = true; // Enable resume

                              stream.pipe(upload);
                              stream.resume();
                            }

                            if (err || self._isError(res)) {
                              self._retry_if_dn_failure(self._doWriteStreamRetry.bind(self),
                                                        req,
                                                        body,
                                                        err,
                                                        params,
                                                        append,
                                                        path,
                                                        opts,
                                                        stream,
                                                        err && err.port != 50075);
                            }
                    }
                );
}

WebHDFS.prototype._doRetry = function(req, params){
    var self = this;
    var download = request(params);
    req._curr_retry_idx = req._curr_retry_idx || 0;
    req._curr_retry_idx++;

    download.on('error', function(err) {
      process.nextTick
      (
        self._retry_if_dn_failure.bind(self, self._doRetry.bind(self), req, null, err, params)
      );
    });

    download.on('complete', function (err) {
      req.emit('done');
    });

    download.on('end', function(){
      req.emit('done');
    })

    // Proxy data to original data handler
    download.on('data', function onData (chunk) {
      var error = self._parseError(chunk.toString(), true);
      if(!error)
        req.emit('data', chunk);
    });

    // Handle subrequest
    download.on('response', function onResponse (res) {
      if (self._isError(res)) {
        download.on('data', function onData (data) {
          var error = self._parseError(data.toString());
          log.info('[#_doRetry]: Received error resp after retrying ', error);
          // override error metadata with custom exceptions which will pass through ahdfs listeners
          if(error && (error.exception == 'StandbyException' || (error.code == 'ECONNREFUSED' && error.port != 50075) ))
          {
            error.message     = 'No active namenode found';
            error.code        = 'NoActiveNameNode';
            error.exception   = 'NoActiveNamenodeException'; // to avoid recursively error emitting loop
          }
          req.emit('error', error);
          req.emit('done');
        });
      }
    });
}

/**
 * Create readable stream for given path
 *
 * @example
 * var WebHDFS = require('webhdfs');
 * var hdfs = WebHDFS.createClient();
 *
 * var remoteFileStream = hdfs.createReadStream('/path/to/remote/file');
 *
 * remoteFileStream.on('error', function onError (err) {
 *  // Do something with the error
 * });
 *
 * remoteFileStream.on('data', function onChunk (chunk) {
 *  // Do something with the data chunk
 * });
 *
 * remoteFileStream.on('finish', function onFinish () {
 *  // Upload is done
 * });
 *
 * @method createReadStream
 * @fires Request#data
 * @fires WebHDFS#finish
 *
 * @param {String} path
 * @param {Object} [opts]
 *
 * @returns {Object}
 */
WebHDFS.prototype.createReadStream = function createReadStream (path, opts) {
  // Validate path
  if (!path || typeof path !== 'string') {
    throw new Error('path must be a string');
  }

  var self = this;
  var endpoint = this._getOperationEndpoint('open', path, opts);
  var stream = null;
  var params = extend({
    method: 'GET',
    url: endpoint,
    json: true
  }, this._requestParams);

  var req = request(params);
  // remember for switchHost
  req._requested_host_idx = self._curr_host_idx;

  req.on('end', function(){
    /* Check if to ignore the first `end` event emitted before retrying in case of a failover switch.
    * Due to a peculiar way nodejs processes the callback phase in event loop, this gets executed
    * after the data event listener is executed completely where we set req.retrying.
    * */
    if(!req.retrying)
    {
        req.emit('done');
    }
  });

  req.on('error', function(error){
    if(self._opts.host.length > 1 && error.code === 'ECONNREFUSED') // if remote process crash
    {
      if( error.port != 50075 ) // retry other namenode if the error didn't come from a datanode
      {
        self._switchHost(req._requested_host_idx);
        params.url          = self._getOperationEndpoint('open', path, opts);
        // remember for the next time, if needed.
        // FIXME: May not be needed again at all as we are making a last retry here.
        req._requested_host_idx = self._curr_host_idx;
      }
      log.warn('[#createReadStream]: Retrying with url: '+ params.url);
      req.retrying = true;
      req._retry_start_ms = Date.now();
      process.nextTick(self._doRetry.bind(self, req, params));
    }
  });

  req.on('response', function (res) {
    // Handle remote exceptions
    if (self._isError(res)) {
      req.on('data', function onData (data) {
        var error = self._parseError(data.toString(), true);
        if(error)
        {
          if(self._opts.host.length < 2 || // retry iff multiple namenode hosts are available
             error.exception !== 'StandbyException')
          {
            req.emit('error', error);
            req.emit('done');
          }
          else
          {
            self._switchHost(req._requested_host_idx);
            params.url          = self._getOperationEndpoint('open', path, opts);
            // remember for the next time, if needed. FIXME: May not be needed again at all as
            // we are making a last retry here.
            req._requested_host_idx = self._curr_host_idx;
            req.retrying = true;
            req._retry_start_ms = Date.now();
            log.warn('[#createReadStream]: Retrying with url: '+ params.url);
            process.nextTick(self._doRetry.bind(self, req, params));
          }
        }
      });
    } else if (self._isRedirect(res)) {
        req._retry_start_ms = Date.now();
        process.nextTick(self._doRetry.bind(self, req, params));
    }

    // No need to interrupt the request
    // data will be automatically sent to the data handler
  });

  return req;
};

/**
 * Create symbolic link to the destination path
 *
 * @method symlink
 *
 * @param {String} src
 * @param {String} dest
 * @param {Boolean} [createParent=false]
 * @param {Function} callback
 *
 * @returns {Object}
 */
WebHDFS.prototype.symlink = function symlink (src, dest, createParent, callback) {
  if (typeof createParent === 'function') {
    callback = createParent;
    createParent = false;
  }

  // Validate path
  if (!src || typeof src !== 'string') {
    throw new Error('src path must be a string');
  }

  if (!dest || typeof dest !== 'string') {
    throw new Error('dest path must be a string');
  }

  var endpoint = this._getOperationEndpoint('createsymlink', src, {
    createParent: createParent || false,
    destination: dest
  });

  return this._sendRequest('PUT', endpoint, function (err) {
    return callback && callback(err);
  });
};

/**
 * Unlink path
 *
 * @method unlink
 *
 * @param {String} path
 * @param {Boolean} [recursive=false]
 * @param {Function} callback
 *
 * @returns {Object}
 */
WebHDFS.prototype.unlink = function unlink (path, recursive, callback) {
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
    return callback && callback(err);
  });
};

/**
 * @alias WebHDFS.unlink
 */
WebHDFS.prototype.rmdir = WebHDFS.prototype.unlink;

module.exports = {
  createClient: function createClient (opts, requestParams) {
    return new WebHDFS(extend({
      user: 'webuser',
      host: 'localhost',
      port: '50070',
      path: '/webhdfs/v1'
    }, opts), requestParams);
  }
};
