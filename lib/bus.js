// env
if (!process.env.NSQLOOKUPD_ADDRESSES) {
  console.log("NSQLOOKUPD_ADDRESSES environment variable required.");
  process.exit(1);
}
if (!process.env.NSQD_ADDRESS) {
  console.log("NSQD_ADDRESS environment variable required.");
  process.exit(1);
}

var events = require('events');
var util = require('util');
var nsq = require('nsqjs');
var debug = require('debug')('clickberry:shiva:bus');

/**
 * Touch the message to prevent timeout.
 *
 * @method     touch
 * @param      {Object}  msg     NSQ Message.
 */
function touch(msg, debugInfo) {
  if (!msg.hasResponded) {
    var str = 'Touch [' + msg.id + ']';
    if (debugInfo) {
      str += ' ' + debugInfo;
    }

    debug(str);
    msg.touch();

    // touch the message again a second before the next timeout. 
    setTimeout(touch, msg.timeUntilTimeout() - 1000, msg, debugInfo);
  }
}

/**
 * Listens for bus messages and emits corresponding events.
 *
 * @class
 * @param      {Object}  options  Configuration options.
 */
function Bus(options) {
  options = options || {};
  options.nsqlookupdAddresses = options.nsqlookupdAddresses || process.env.NSQLOOKUPD_ADDRESSES;
  options.nsqdAddress = options.nsqdAddress || process.env.NSQD_ADDRESS;
  options.nsqdPort = options.nsqdPort || process.env.NSQD_PORT || '4150';
  this.options = options;

  events.EventEmitter.call(this);

  // Writers
  debug('nsqdAddress: ' + options.nsqdAddress);
  this._writer = new nsq.Writer(options.nsqdAddress, parseInt(options.nsqdPort, 10));
  this._writer.connect();
}

util.inherits(Bus, events.EventEmitter);


Bus.prototype.listen = function () {
  var bus = this;

  var lookupdHTTPAddresses = this.options.nsqlookupdAddresses.split(',');
  debug('lookupdHTTPAddresses: ' + JSON.stringify(lookupdHTTPAddresses));

  // videos
  new nsq.Reader('shiva-videos', 'extract-segments', {
    lookupdHTTPAddresses: lookupdHTTPAddresses
    })
    .on('message', function (msg) {
      setTimeout(touch, msg.timeUntilTimeout() - 1000, msg, 'shiva-videos');
      bus.emit('video', msg);
    })
    .connect();

  // segments
  new nsq.Reader('shiva-segments', 'process-segments', {
    lookupdHTTPAddresses: lookupdHTTPAddresses
    })
    .on('message', function (msg) {
      setTimeout(touch, msg.timeUntilTimeout() - 1000, msg, 'shiva-segments');
      bus.emit('segment', msg);
    })
    .connect();

  // processed segments
  // new nsq.Reader('shiva-processed-segments', 'merge-segments', {
  //   lookupdHTTPAddresses: lookupdHTTPAddresses
  //   })
  //   .on('message', function (msg) {
  //     setTimeout(touch, msg.timeUntilTimeout() - 1000, msg, 'shiva-processed-segments');
  //     bus.emit('processed-segments', msg);
  //   })
  //   .connect();
};

Bus.prototype.publishVideoUploaded = function (data, fn) {
  this._writer.publish('shiva-videos', data, fn);
};

Bus.prototype.publishSegmentExtracted = function (data, fn) {
  this._writer.publish('shiva-segments', data, fn);
};

Bus.prototype.publishSegmentProcessed = function (data, fn) {
  this._writer.publish('shiva-processed-segments', data, fn);
};

Bus.prototype.publishVideoProcessed = function (data, fn) {
  this._writer.publish('shiva-processed-videos', data, fn);
};

module.exports = Bus;
