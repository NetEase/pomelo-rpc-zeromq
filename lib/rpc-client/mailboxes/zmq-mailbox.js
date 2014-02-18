var EventEmitter = require('events').EventEmitter;
var util = require('util');
var utils = require('../../util/utils');
var Tracer = require('../../util/tracer');
var zmq = require('zmq');
var DEFAULT_INTERVAL = 50;

var MailBox = function(server, opts) {
  EventEmitter.call(this);
  this.id = server.id;
  this.host = server.host;
  this.port = server.port;
  this.requests = {};
  this.timeout = {};
  this.curId = 0;
  this.queue = [];
  this.bufferMsg = opts.bufferMsg;
  this.interval = opts.interval || DEFAULT_INTERVAL;
  this.opts = opts;
};
util.inherits(MailBox, EventEmitter);

var  pro = MailBox.prototype;

pro.connect = function(tracer, cb) {
  tracer.info('client', __filename, 'connect', 'zmq-mailbox try to connect');

  var self = this;
  this.socket = zmq.socket('dealer');
  this.socket.identity = this.opts.clientId;
  this.socket.connect('tcp://' + this.host + ':' + this.port);

  this.socket.on('message', function(pkg) {
    pkg = JSON.parse(pkg);
    try {
      if(pkg instanceof Array) {
        processMsgs(self, pkg);
      } else {
        processMsg(self, pkg);
      }
    } catch(e) {
      console.error('rpc client process message with error: %j', e.stack);
    }
  });

  process.nextTick(function() {
    if(self.bufferMsg) {
      self._interval = setInterval(function() {
        flush(self);
      }, self.interval);
    }
    utils.invokeCallback(cb);
  });
};

/**
 * close mailbox
 */
pro.close = function() {
  if(this._interval) {
    clearInterval(this._interval);
    this._interval = null;
  }
  this.socket.close();
};

/**
 * send message to remote server
 *
 * @param msg {service:"", method:"", args:[]}
 * @param opts {} attach info to send method
 * @param cb declaration decided by remote interface
 */
pro.send = function(tracer, msg, opts, cb) {
  tracer.info('client', __filename, 'send', 'zmq-mailbox try to send');

  var id = this.curId++;
  this.requests[id] = cb;

  var pkg;
  if(tracer.isEnabled) {
    pkg = {traceId: tracer.id, seqId: tracer.seq, source: tracer.source, remote: tracer.remote, id: id, msg: msg};
  }
  else {
    pkg = {id: id, msg: msg};
  }
  if(this.bufferMsg) {
    enqueue(this, pkg);
  } else {
    this.socket.send(JSON.stringify(pkg));
  }
};

var enqueue = function(mailbox, msg) {
  mailbox.queue.push(msg);
};

var flush = function(mailbox) {
  if(mailbox.closed || !mailbox.queue.length) {
    return;
  }
  mailbox.socket.send(JSON.stringify(mailbox.queue));
  mailbox.queue = [];
};

var processMsgs = function(mailbox, pkgs) {
  for(var i=0, l=pkgs.length; i<l; i++) {
    processMsg(mailbox, pkgs[i]);
  }
};

var processMsg = function(mailbox, pkg) {
  var cb = mailbox.requests[pkg.id];
  delete mailbox.requests[pkg.id];

  if(!cb) {
    return;
  }
  
  var tracer = new Tracer(mailbox.opts.rpcLogger, mailbox.opts.rpcDebugLog, mailbox.opts.clientId, pkg.source, pkg.resp, pkg.traceId, pkg.seqId);
  var args = [tracer, null];

  pkg.resp.forEach(function(arg){
    args.push(arg);
  });

  cb.apply(null, args);
};

/**
 * Factory method to create mailbox
 *
 * @param {Object} server remote server info {id:"", host:"", port:""}
 * @param {Object} opts construct parameters
 *                      opts.bufferMsg {Boolean} msg should be buffered or send immediately.
 *                      opts.interval {Boolean} msg queue flush interval if bufferMsg is true. default is 50 ms
 */
module.exports.create = function(server, opts) {
  return new MailBox(server, opts || {});
};