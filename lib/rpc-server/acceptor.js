var acceptor = require('./acceptors/zmq-acceptor');

module.exports.create = function(opts, cb) {
  return acceptor.create(opts, cb);
};