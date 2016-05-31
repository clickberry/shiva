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
var shivaUtils = require('./lib/utils');


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
  video.segmentOutputs = segmentOutput;
  bus.publishSegmentProcessed(video, fn);
}

function publishAllSegmentsProcessedEvents(video, segments, fn) {
  delete video.segmentOutputs;

  var segmentsMap = {};
  segments.forEach(function (key) {
    var format = path.extname(key).substring(1);
    var size = /_\d+_(\d+)\./.exec(key)[1];
    var uri = shivaUtils.getS3Uri(bucket, key);
    if (!segmentsMap[format]) {
      segmentsMap[format] = {};
    }
    if (!segmentsMap[format][size]) {
      segmentsMap[format][size] = [];
    }
    segmentsMap[format][size].push(uri);
  });

  video.outputs.forEach(function (output) {
    var segments = segmentsMap[output.format][output.size];
    video.segments = {
      format: output.format,
      size: output.size,
      uri: output.uri,
      chunks: segments
    };
    bus.publishAllSegmentsProcessed(video, fn);
  });
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
        msg.requeue();
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
        msg.requeue();
        debug('Video encoding failed for ' + segmentUri +  ', skipping the file: ' + err);
      }
      return;
    }

    debug('Video encoding completed successfully for file ' + segmentUri);
    msg.finish();
  });
}

function mergeSegments(msg) {
  var video = JSON.parse(msg.body);
  debug('Video ' + video.id + ' processed segment: ' + JSON.stringify(video));

  if (video.segmentOutputs.index !== (video.segmentOutputs.count - 1)) {
    // processing only last segment to avoid duplicated events
    return msg.finish();
  }

  // merge segment messages or concat segments into video if all processed
  var shiva = new Shiva();
  shiva.checkIfAllSegmentsProcessed(video.segmentOutputs.uri, video.outputs.length, video.segmentOutputs.count, bucket, function (err, segments) {
    if (err) {
      // re-queue the message again if not fatal
      msg.requeue();
      return debug('Video segment merging failed for ' + video.segmentOutputs.uri +  ' segment, skipping the file: ' + err);
    }

    if (!segments) {
      msg.requeue(); 
    }

    debug('All video segments have been encoded for video ' + video.uri);
    publishAllSegmentsProcessedEvents(video, segments, handleError);
    msg.finish();
  });
}

function concatVideo(msg) {
  var video = JSON.parse(msg.body);
  debug('Video ' + video.id + ' processed segments: ' + JSON.stringify(video));

  var shiva = new Shiva();
  shiva.concatSegmentsToS3(video.segments.chunks, video.segments.uri, video.segments.format, bucket, function (err, uri) {
    if (err) {
      // re-queue the message again if not fatal
      msg.requeue();
      return debug('Video segment concatination failed for ' + video.segments.uri +  ' target file: ' + err);
    }

    debug('All video segments concatenated for ' + video.segments.format + ' ' + video.segments.size + 'p format and uploaded to ' + video.segments.uri);
    msg.finish();
  });
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
      mergeSegments(msg);
    })

    // Processed Video Segments
    .on('processed-video-segments', function (msg) {
      concatVideo(msg);
    })

    .listen();

  debug('Listening for messages...');
};