/**
 * @fileOverview Message processing worker service.
 */

if (!process.env.S3_BUCKET) {
  console.log("S3_BUCKET environment variable required.");
  process.exit(1);
}
var bucket = process.env.S3_BUCKET;

var debug = require('debug')('clickberry:shiva:worker');
var path = require('path');
var Bus = require('./lib/bus');
var bus = new Bus();
var Shiva = require('./lib/shiva');


function handleError(err) {
  if (err) console.error(err);
}

function publishSegmentEvent(video, segment, fn) {
  video.segment = segment;
  bus.publishSegmentExtracted(video, fn);
}

function publishSegmentEncodedEvent(video, segmentOutputs, fn) {
  var segmentOutput = video.segment;
  segmentOutput.outputs = segmentOutputs;

  delete video.segment;
  video.segmentOutputs = [segmentOutput];
  bus.publishSegmentProcessed(video, fn);
}

function splitVideo(msg) {
  var video = JSON.parse(msg.body);
  debug('New video: ' + JSON.stringify(video));

  var shiva = new Shiva();
  shiva
    .on('segment', function (segment) {
      // segment extracted
      publishSegmentEvent(video, segment, handleError);
    })
    .on('error', function(err) {
      handleError(err);
    });

  // split video to segments
  shiva.extractSegmentsToS3(video.uri, bucket, function (err) {
    if (err) {
      if (err.fatal) {
        handleError(err);
      } else {
        // re-queue the message again if not fatal
        debug('Video segmenting failed for ' + video.uri +  ', skipping the file: ' + err);
      }
      return;
    }

    debug('Video segmenting completed successfully for file ' + video.uri);
    msg.finish();
  });
}

function encodeSegment(msg) {
  var videoSegment = JSON.parse(msg.body);
  debug('Video ' + videoSegment.id + ' segment received for encoding: ' + JSON.stringify(videoSegment));

  var segmentUri = videoSegment.segment.uri;

  var formats = [];
  var sizes = [];
  videoSegment.outputs.forEach(function (o) {
    if (formats.indexOf(o.format) === -1) {
      formats.push(o.format);
    }
    if (sizes.indexOf(o.size) === -1) {
      sizes.push(o.size);
    }
  });

  var shiva = new Shiva();
  shiva
    .on('encoded', function (data) {
      // segment extracted
      publishSegmentEncodedEvent(videoSegment, data, handleError);
    })
    .on('error', function(err) {
      handleError(err);
    });

  // encode segment
  shiva.encodeAndUploadVideoToS3(segmentUri, formats, sizes, bucket, function (err) {
    if (err) {
      if (err.fatal) {
        handleError(err);
      } else {
        // re-queue the message again if not fatal
        debug('Video encoding failed for ' + segmentUri +  ', skipping the file: ' + err);
      }
      return;
    }

    debug('Video encoding completed successfully for file ' + segmentUri);
    msg.finish();
  });
}

function mergeSegment(msg) {
  var video = JSON.parse(msg.body);
  debug('Video ' + video.id + ' processed segments: ' + JSON.stringify(video));

  // merge segment messages or concat segments into video if all processed
}

module.exports = function () {
  bus
    // Videos
    .on('video', function (msg) {
      splitVideo(msg);
    })

    // Segments
    .on('segment', function (msg) {
      encodeSegment(msg);
    })

    // Processed Segments
    .on('processed-segments', function (msg) {
      mergeSegment(msg);
    })
    
    .listen();

  debug('Listening for messages...');
};