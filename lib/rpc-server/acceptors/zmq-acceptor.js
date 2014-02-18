var EventEmitter = require('events').EventEmitter;
var util = require('util');
var utils = require('../../util/utils');
var zmq = require('zmq');
var logger = require('pomelo-logger').getLogger('pomelo-rpc', __filename);
var Tracer = require('../../util/tracer');

var Acceptor = function(opts, cb) {
  EventEmitter.call(this);
  this.bufferMsg = opts.bufferMsg;
  this.interval = opts.interval;  // flush interval in ms
  this.rpcDebugLog = opts.rpcDebugLog;
  this.rpcLogger = opts.rpcLogger;
  this.whitelist = opts.whitelist;
  this._interval = null;          // interval object
  this.sockets = {};
  this.msgQueues = {};
  this.cb = cb;
};
util.inherits(Acceptor, EventEmitter);

var pro = Acceptor.prototype;

pro.listen = function(port) {
  var self = this;
  this.socket = zmq.socket('router');
  this.socket.bind('tcp://*:' + port, function(err) {
    if (!!err) {
      console.error('rpc server bind host with err: %j', err.stack);
      return;
    }
    self.socket.on('message', function(clientId, pkg) {
      pkg = JSON.parse(pkg);
      try {
        if(pkg instanceof Array) {
          processMsgs(clientId, self.socket, self, pkg);
        } else {
          processMsg(clientId, self.socket, self, pkg);
        }
      } catch(e) {
        console.error('rpc server process message error: %j', e.stack);
      }
    });
  });

  if(this.bufferMsg) {
    this._interval = setInterval(function() {
      flush(self);
    }, this.interval);
  }
};

pro.close = function() {
  if(this._interval) {
    clearInterval(this._interval);
    this._interval = null;
  }
  try {
    this.socket.close();
  } catch(err) {
    console.error('rpc server close error: %j', err.stack);
  }
};

var cloneError = function(origin) {
  var res = {
    msg: origin.msg,
    stack: origin.stack
  };
  return res;
};

var processMsg = function(clientId, socket, acceptor, pkg) {
  var tracer = new Tracer(acceptor.rpcLogger, acceptor.rpcDebugLog, pkg.remote, pkg.source, pkg.msg, pkg.traceId, pkg.seqId);
  tracer.info('server', __filename, 'processMsg', 'zmq-acceptor receive message and try to process message');
  acceptor.cb.call(null, tracer, pkg.msg, function() {
    var args = Array.prototype.slice.call(arguments, 0);
    for(var i=0, l=args.length; i<l; i++) {
      if(args[i] instanceof Error) {
        args[i] = cloneError(args[i]);
      }
    }
    var resp;
    if(tracer.isEnabled) {
      resp = {traceId: tracer.id, seqId: tracer.seq, source: tracer.source, id: pkg.id, resp: Array.prototype.slice.call(args, 0)};
    }
    else {
      resp = {id: pkg.id, resp: Array.prototype.slice.call(args, 0)};
    }
    if(acceptor.bufferMsg) {
      enqueue(clientId, acceptor, resp);
    } else {
      socket.send([clientId, JSON.stringify(resp)]);
    }
  });
};

var processMsgs = function(clientId, socket, acceptor, pkgs) {
  for(var i=0, l=pkgs.length; i<l; i++) {
    processMsg(clientId, socket, acceptor, pkgs[i]);
  }
};

var enqueue = function(clientId, acceptor, msg) {
  var queue = acceptor.msgQueues[clientId];
  if(!queue) {
    queue = acceptor.msgQueues[clientId] = [];
  }
  queue.push(msg);
};

var flush = function(acceptor) {
  var socket = acceptor.socket, queues = acceptor.msgQueues, queue;
  for(var clientId in queues) {
    queue = queues[clientId];
    if(!queue.length) {
      continue;
    }
    socket.send([clientId, JSON.stringify(queue)]);
    queues[clientId] = [];
  }
};

/**
 * create acceptor
 *
 * @param opts init params
 * @param cb(tracer, msg, cb) callback function that would be invoked when new message arrives
 */
module.exports.create = function(opts, cb) {
  return new Acceptor(opts || {}, cb);
};