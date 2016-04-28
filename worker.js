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

function publishSegmentEvent(video, uri, fn) {
  var fileName = path.basename(uri);
  var pattern = /_(\d+)/;
  var match = pattern.exec(fileName);
  var idx = parseInt(match[1]);

  video.segment = {
    index: idx,
    uri: uri
  };
  bus.publishSegmentExtracted(video, fn);
}

module.exports = function () {
  bus
    // Videos
    .on('video', function (msg) {
      var video = JSON.parse(msg.body);
      debug('New video: ' + JSON.stringify(video));

      // split video to segments
      var shiva = new Shiva();
      shiva
        .on('segment', function (uri) {
          // generate segment event
          publishSegmentEvent(video, uri, handleError);
        })
        .on('error', function(err) {
          handleError(err);
        });

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
    })

    // Segments
    .on('segment', function (msg) {
      var segment = JSON.parse(msg.body);
      debug('Video ' + segment.id + ' segment: ' + JSON.stringify(segment));

      // encode segment
    })
    .on('processed-segments', function (msg) {
      var segments = JSON.parse(msg.body);
      debug('Video ' + segments.videoId + ' processed segments: ' + JSON.stringify(segments));

      // merge segment messages or concat segments into video if all processed
    })
    .listen();

  debug('Listening for messages...');
};